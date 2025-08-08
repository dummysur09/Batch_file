import os
import json
import asyncio
import logging
import time
import random
import heapq
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import aiofiles
import aiohttp
from telegram import Bot
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
from telethon import TelegramClient
from telethon.tl.types import InputPeerChannel
from filelock import FileLock

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

base_dir = "/data/data/com.termux/files/home/test"  

# Configuration
API_ID = 27584203
API_HASH = 'f78f372cb63ed6e0588ec88f4d7012c7'
PHONE_NUMBER = '+919996471384'
TELEGRAM_CHANNEL_ID = -1002856592532  # Integer version for Telethon
TELEGRAM_CHANNEL_ID_STR = '-1002856592532'  # String version for python-telegram-bot
LOCAL_BASE_DIR = os.path.join(base_dir, '2_batches')
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 60  # seconds
FILE_SIZE_THRESHOLD = 49 * 1024 * 1024  # 49MB - use bot for files smaller than this
MAX_CONCURRENT_BOT_UPLOADS = 3  # Maximum number of concurrent bot uploads
MAX_CONCURRENT_CLIENT_UPLOADS = 2  # Maximum number of concurrent client uploads
JSON_LOCK_PATH = os.path.join(base_dir, 'uploader.json.lock')

# Initialize Telegram bot with increased timeout
TOKEN = '8233277052:AAGIa8dBQvFtjK-1hjrLMjWw1udoMXuMw-U'
request = HTTPXRequest(connection_pool_size=8, read_timeout=600, write_timeout=600, connect_timeout=60)
bot = Bot(token=TOKEN, request=request)

# Initialize JSON backup bot
JSON_BOT_TOKEN = '7648774676:AAHIgJh1ND6U81Bhn3oI6IrRgvK4b9iKvdk'
JSON_CHANNEL_ID = '-1002757649300'
json_bot = Bot(token=JSON_BOT_TOKEN, request=request)

# Initialize Telegram client (for user account)
client_session_file = os.path.join(base_dir, 'telegram_session')
client = None

# Track last JSON upload
last_json_upload_time = 0
last_json_message_id = None
json_upload_interval = 300  # 5 minutes minimum between full JSON uploads

# Semaphores to limit concurrent uploads
bot_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BOT_UPLOADS)
client_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CLIENT_UPLOADS)

def get_telegram_link(channel_id, message_id):
    """Generate a Telegram link for the message."""
    # Extract the channel ID number without the -100 prefix
    if isinstance(channel_id, str) and channel_id.startswith('-100'):
        channel_id = channel_id[4:]
    elif isinstance(channel_id, int) and channel_id < 0:
        channel_id = str(abs(channel_id))[1:]  # Remove the negative sign and first digit (which is 1 for -100)
    
    # Create the link
    return f"https://t.me/c/{channel_id}/{message_id}"

async def initialize_client():
    """Initialize and connect the Telethon client."""
    global client
    if client is None or not client.is_connected():
        try:
            client = TelegramClient(client_session_file, API_ID, API_HASH)
            await client.start(phone=PHONE_NUMBER)
            logger.info("Telegram user client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Telethon client: {e}")
            client = None # Ensure client is None if initialization fails
    return client

async def upload_with_bot(file_path, max_retries=MAX_RETRIES):
    """Upload a file using the Telegram bot API with semaphore to limit concurrency."""
    async with bot_semaphore:
        file_name = os.path.basename(file_path)
        rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Uploading with bot: {rel_path} (Attempt {attempt}/{max_retries})")
                
                # Use aiofiles for async file opening if possible, or ThreadPoolExecutor for sync open
                # For bot.send_document, it expects a file-like object, which can be opened synchronously
                # as the actual network I/O is handled by the bot library.
                with open(file_path, 'rb') as file:
                    # Add caption with file path
                    caption = f"📄 {rel_path}"
                    if len(caption) > 1024:  # Telegram caption limit
                        caption = caption[:1021] + "..."
                    
                    message = await bot.send_document(
                        chat_id=TELEGRAM_CHANNEL_ID_STR, 
                        document=file,
                        caption=caption
                    )
                
                logger.info(f"Successfully uploaded with bot: {rel_path}")
                
                # Fix for the 'NoneType' object has no attribute 'file_id' error
                file_id = None
                if hasattr(message, 'document') and message.document is not None:
                    file_id = message.document.file_id
                else:
                    logger.warning(f"Message doesn't have document.file_id attribute for {rel_path}, but upload was successful")
                
                # Return upload details
                return {
                    "message_id": message.message_id,
                    "file_id": file_id,  # This might be None, but that's okay
                    "method": "bot",
                    "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID_STR, message.message_id),
                    "file_path": rel_path, # Include original relative path for main loop
                    "size": os.path.getsize(file_path), # Include size for main loop
                    "name": file_name # Include name for main loop
                }
                
            except TelegramError as e:
                error_str = str(e).lower()
                logger.error(f"Bot attempt {attempt} failed to upload {rel_path}: {str(e)}")
                
                if attempt < max_retries:
                    # Calculate backoff time with jitter
                    backoff = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
                    jitter = random.uniform(0.5, 1.5)
                    wait_time = backoff * jitter
                    
                    logger.info(f"Retrying in {wait_time:.1f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All bot {max_retries} attempts failed for {rel_path}")
                    return None
                    
            except Exception as e:
                logger.error(f"Unexpected error in bot upload for {rel_path}: {str(e)}")
                logger.exception("Exception details:")
                
                if attempt < max_retries:
                    wait_time = INITIAL_RETRY_DELAY * (2 ** (attempt - 1))
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    return None
        
        return None

async def upload_with_client(file_path, max_retries=MAX_RETRIES):
    """Upload a file using the Telegram user account API (Telethon) with semaphore to limit concurrency."""
    async with client_semaphore:
        global client
        file_name = os.path.basename(file_path)
        rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
        
        # Ensure client is initialized and connected
        if client is None or not client.is_connected():
            await initialize_client()
            if client is None or not client.is_connected():
                logger.error(f"Telethon client not connected, cannot upload {rel_path}")
                return None
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Uploading with user account: {rel_path} (Attempt {attempt}/{max_retries})")
                
                # Add caption with file path
                caption = f"📄 {rel_path}"
                if len(caption) > 1024:  # Telegram caption limit
                    caption = caption[:1021] + "..."
                
                # Get the channel entity
                channel = await client.get_entity(TELEGRAM_CHANNEL_ID)
                
                # Upload the file
                message = await client.send_file(
                    channel,
                    file_path,
                    caption=caption,
                    progress_callback=lambda current, total: logger.info(f"Upload progress for {rel_path}: {current/total*100:.1f}%") if current % (1024*1024*5) == 0 else None
                )
                
                logger.info(f"Successfully uploaded with user account: {rel_path}")
                
                # Return upload details
                return {
                    "message_id": message.id,
                    "method": "user_account",
                    "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID, message.id),
                    "file_path": rel_path, # Include original relative path for main loop
                    "size": os.path.getsize(file_path), # Include size for main loop
                    "name": file_name # Include name for main loop
                }
                
            except Exception as e:
                logger.error(f"User account attempt {attempt} failed to upload {rel_path}: {str(e)}")
                
                if attempt < max_retries:
                    # Calculate backoff time with jitter
                    backoff = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
                    jitter = random.uniform(0.5, 1.5)
                    wait_time = backoff * jitter
                    
                    logger.info(f"Retrying in {wait_time:.1f} seconds...")
                    await asyncio.sleep(wait_time)
                    
                    # Try to reconnect if needed
                    if client and not client.is_connected():
                        logger.info("Attempting to reconnect Telethon client...")
                        await client.connect()
                else:
                    logger.error(f"All user account {max_retries} attempts failed for {rel_path}")
                    return None
        
        return None

async def upload_json_to_telegram(uploaded_files, json_file_path, force_full_upload=False):
    """Upload the JSON file to a separate Telegram channel."""
    global last_json_upload_time, last_json_message_id
    
    current_time = time.time()
    
    # Determine if we should do a full upload or just an update
    do_full_upload = force_full_upload or (current_time - last_json_upload_time > json_upload_interval)
    
    try:
        if do_full_upload:
            # Full upload - send the entire JSON file
            logger.info("Uploading full JSON file to backup channel")
            
            # Create a temporary file with the current JSON content
            temp_json_path = json_file_path + ".temp"
            async with aiofiles.open(temp_json_path, 'w') as f:
                await f.write(json.dumps(uploaded_files, indent=2))
            
            # Upload the file
            with open(temp_json_path, 'rb') as f:
                message = await json_bot.send_document(
                    chat_id=JSON_CHANNEL_ID,
                    document=f,
                    caption=f"📊 Full upload records backup - {len(uploaded_files)} files - {time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            
            # Clean up temp file
            os.remove(temp_json_path)
            
            # Update tracking variables
            last_json_upload_time = current_time
            last_json_message_id = message.message_id
            
            logger.info(f"Successfully uploaded full JSON backup to channel (message ID: {message.message_id})")
            
        else:
            # Just send a text update with the latest entries
            # Find entries added since last upload
            recent_entries = {}
            # Iterate through uploaded_files in reverse to get most recent first
            for key in sorted(uploaded_files.keys(), reverse=True):
                entry = uploaded_files[key]
                # Assuming 'file_' prefix and timestamp in key for simplicity
                try:
                    # Extract timestamp from key (e.g., "file_1678901234_filename.ext")
                    key_timestamp_str = key.split('_')[1]
                    key_timestamp = int(key_timestamp_str)
                    if key_timestamp > last_json_upload_time:
                        recent_entries[key] = entry
                except (IndexError, ValueError):
                    # Fallback for keys not matching expected format
                    # Or, if you have a 'timestamp' field in your entry, use that
                    if 'timestamp' in entry and entry['timestamp'] > last_json_upload_time:
                        recent_entries[key] = entry
                    continue
            
            if recent_entries:
                # Sort recent entries by timestamp for consistent output
                sorted_recent_entries = sorted(recent_entries.items(), key=lambda item: int(item[0].split('_')[1]) if '_' in item[0] else 0, reverse=True)
                
                message_text = f"📝 Update: {len(recent_entries)} new files added\n\n"
                
                for key, entry in list(sorted_recent_entries)[:10]:  # Limit to 10 entries to avoid message size limits
                    message_text += f"• {entry.get('Name', 'Unknown')} - {entry.get('telegram_link', 'No link')}\n"
                
                if len(recent_entries) > 10:
                    message_text += f"... and {len(recent_entries) - 10} more files\n"
                
                # Send the update
                await json_bot.send_message(
                    chat_id=JSON_CHANNEL_ID,
                    text=message_text
                )
                
                logger.info(f"Sent JSON update with {len(recent_entries)} new entries")
            else:
                logger.info("No new entries to update in JSON backup")
        
        return True
    
    except Exception as e:
        logger.error(f"Failed to upload JSON to Telegram: {str(e)}")
        logger.exception("Exception details:")
        return False

async def find_all_files(directory):
    """Recursively find all files in a directory using async operations."""
    all_files = []
    
    # Use ThreadPoolExecutor for file system operations that are blocking
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        # os.walk is blocking, so run it in the executor
        for root, dirs, files in await loop.run_in_executor(executor, partial(os.walk, directory)):
            for file in files:
                file_path = os.path.join(root, file)
                all_files.append(file_path)
    
    return all_files

def is_file_uploaded(file_path, uploaded_files):
    """Check if a file has already been uploaded by checking its name and path."""
    file_name = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
    
    try:
        file_size = os.path.getsize(file_path)
    except FileNotFoundError:
        logger.warning(f"File not found when checking if uploaded: {file_path}")
        return False # If file doesn't exist, it can't be uploaded
    except Exception as e:
        logger.error(f"Error getting size for {file_path} during upload check: {e}")
        return False # Assume not uploaded if we can't verify

    # Check if any entry has the same file path
    for entry in uploaded_files.values():
        if "file_path" in entry and entry["file_path"] == rel_path:
            return True
    
    # Check if any entry has the same file name AND size (to be more accurate)
    # This is a fallback/additional check, less reliable than full path
    for entry in uploaded_files.values():
        if ("Name" in entry and entry["Name"] == file_name and 
            "size" in entry and entry["size"] == file_size):
            return True
    
    return False

def should_skip_file(file_path):
    """Check if a file should be skipped (part files or 0KB files)."""
    # Skip .part files
    if file_path.endswith('.part'):
        logger.debug(f"Skipping {file_path}: .part file")
        return True
    
    # Skip 0KB files
    try:
        size = os.path.getsize(file_path)
        if size == 0:
            logger.debug(f"Skipping {file_path}: 0KB file")
            return True
    except FileNotFoundError:
        logger.warning(f"File not found when checking if should skip: {file_path}")
        return True # If file doesn't exist, skip it
    except Exception as e:
        logger.error(f"Error checking file size for {file_path}: {str(e)}")
        # If we can't get the size, better to skip
        return True
    
    return False

async def truncate_file(file_path):
    """Safely truncate a file to 0 bytes."""
    try:
        # Use ThreadPoolExecutor for blocking os.remove if needed, or aiofiles for async truncate
        # aiofiles.open('w') and then f.truncate(0) is async
        async with aiofiles.open(file_path, 'w') as f:
            await f.truncate(0)
        logger.info(f"Truncated file: {os.path.relpath(file_path, LOCAL_BASE_DIR)}")
        return True
    except Exception as e:
        logger.error(f"Failed to truncate file {os.path.relpath(file_path, LOCAL_BASE_DIR)}: {str(e)}")
        return False

async def save_upload_records(uploaded_files, json_file_path):
    """Safely save upload records to JSON with file locking."""
    lock = FileLock(JSON_LOCK_PATH)
    # Use async with for the lock to ensure it's non-blocking for other async tasks
    # FileLock itself is blocking, so we run it in a thread pool
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        await loop.run_in_executor(executor, lock.acquire)
        try:
            async with aiofiles.open(json_file_path, 'w') as f:
                await f.write(json.dumps(uploaded_files, indent=2))
            logger.info(f"Updated upload records in {json_file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save upload records: {str(e)}")
            return False
        finally:
            await loop.run_in_executor(executor, lock.release)


async def main():
    """Main function to monitor directory and upload files."""
    global last_json_upload_time
    
    uploaded_files = {}
    json_file_path = os.path.join(base_dir, 'uploader.json')

    # Load existing uploads from JSON if exists
    if os.path.exists(json_file_path):
        try:
            # Use FileLock for initial load as well to prevent race with other potential processes
            lock = FileLock(JSON_LOCK_PATH)
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                await loop.run_in_executor(executor, lock.acquire)
                try:
                    async with aiofiles.open(json_file_path, 'r') as f:
                        contents = await f.read()
                        uploaded_files = json.loads(contents)
                    logger.info(f"Loaded {len(uploaded_files)} previously uploaded files from JSON")
                except json.JSONDecodeError:
                    logger.warning("JSON file exists but is invalid. Starting with empty record.")
                    # Try to backup the invalid file
                    try:
                        backup_path = json_file_path + ".bak"
                        # Use run_in_executor for blocking os.rename
                        await loop.run_in_executor(executor, partial(os.rename, json_file_path, backup_path))
                        logger.info(f"Backed up invalid JSON to {backup_path}")
                    except Exception as e:
                        logger.error(f"Failed to backup invalid JSON: {e}")
                finally:
                    await loop.run_in_executor(executor, lock.release)
        except Exception as e:
            logger.error(f"Error during initial JSON load: {e}")
            uploaded_files = {} # Ensure it's empty if load fails

    # Create batch directory if it doesn't exist
    if not os.path.exists(LOCAL_BASE_DIR):
        os.makedirs(LOCAL_BASE_DIR)
        logger.info(f"Created directory: {LOCAL_BASE_DIR}")

    try:
        # Test bot connection
        me = await bot.get_me()
        logger.info(f"Bot connected successfully. Bot username: @{me.username}")
        
        # Initialize Telegram client (will be re-checked before each client upload)
        await initialize_client()
        
        # Send a startup message to the channel
        await bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID_STR,
            text="📤 File uploader started. Monitoring for new files..."
        )

        while True:
            logger.info("Checking for new files to upload...")
            
            try:
                # Find all files recursively
                all_files = await find_all_files(LOCAL_BASE_DIR)
                
                if not all_files:
                    logger.info("No files found in directory.")
                else:
                    logger.info(f"Found {len(all_files)} files in directory tree.")
                
                files_to_upload_tasks = []
                files_skipped_count = 0
                
                # Prioritize large files first (optional, but can be useful)
                # Sort by size, but ensure we get size safely
                files_with_sizes = []
                for f_path in all_files:
                    try:
                        files_with_sizes.append((f_path, os.path.getsize(f_path)))
                    except FileNotFoundError:
                        logger.warning(f"File disappeared during scan: {f_path}")
                        continue
                    except Exception as e:
                        logger.error(f"Error getting size for {f_path}: {e}")
                        continue
                
                files_with_sizes.sort(key=lambda x: x[1], reverse=True)

                for file_path, size in files_with_sizes:
                    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
                    
                    # Skip .part files and 0KB files
                    if should_skip_file(file_path):
                        logger.info(f"Skipping {rel_path} (part file or 0KB file)")
                        files_skipped_count += 1
                        continue
                    
                    # Check if file has already been uploaded
                    if is_file_uploaded(file_path, uploaded_files):
                        logger.info(f"File {rel_path} already uploaded, skipping.")
                        files_skipped_count += 1
                        continue
                    
                    logger.info(f"Preparing to upload: {rel_path} ({size/1024/1024:.2f} MB)")
                    
                    # Choose upload method based on file size and add to tasks list
                    if size < FILE_SIZE_THRESHOLD:
                        files_to_upload_tasks.append(upload_with_bot(file_path))
                    else:
                        files_to_upload_tasks.append(upload_with_client(file_path))
                
                if files_to_upload_tasks:
                    logger.info(f"Starting {len(files_to_upload_tasks)} concurrent upload tasks...")
                    # Run all selected upload tasks concurrently
                    # return_exceptions=True ensures that if one task fails, others still complete
                    upload_results = await asyncio.gather(*files_to_upload_tasks, return_exceptions=True)
                    
                    files_uploaded_this_cycle = 0
                    for result in upload_results:
                        if isinstance(result, Exception):
                            logger.error(f"An upload task failed with an exception: {result}")
                            # The specific upload function already logs details, so just note here.
                        elif result: # If result is not None (i.e., upload was successful)
                            # Create a unique key for the file
                            # Use the timestamp when the upload was *completed* for the key
                            key = f"file_{int(time.time())}_{result['name']}"
                            
                            # Store file info in the format you requested
                            uploaded_files[key] = {
                                "Name": result["name"],
                                "file_path": result["file_path"],
                                "size": result["size"],
                                "telegram_link": result["telegram_link"],
                                "upload_method": result["method"],
                                "message_id": result["message_id"],
                                "timestamp": int(time.time()) # Add a timestamp for easier tracking of recent uploads
                            }
                            
                            # Only add file_id if it exists
                            if "file_id" in result and result["file_id"] is not None:
                                uploaded_files[key]["file_id"] = result["file_id"]
                            
                            # Save upload records to JSON after each successful upload
                            # This is done inside the loop to ensure progress is saved frequently
                            # The FileLock will handle concurrency for the JSON file itself.
                            await save_upload_records(uploaded_files, json_file_path)
                            
                            # Truncate file to 0 bytes AFTER saving JSON
                            # Use the original file_path from the result, not the rel_path
                            original_full_path = os.path.join(LOCAL_BASE_DIR, result["file_path"])
                            await truncate_file(original_full_path)
                            
                            files_uploaded_this_cycle += 1
                        else:
                            logger.warning("One of the upload tasks returned None (failed after retries).")
                    
                    logger.info(f"Upload cycle completed: {files_uploaded_this_cycle} files uploaded, {files_skipped_count} files skipped")
                else:
                    logger.info(f"No new files to upload in this cycle. {files_skipped_count} files skipped.")
                
                # Upload JSON records to the separate channel
                await upload_json_to_telegram(uploaded_files, json_file_path)
                
            except Exception as e:
                logger.error(f"Error during file processing loop: {str(e)}")
                logger.exception("Exception details:")
            
            logger.info(f"Sleeping for 60 seconds before next check...")
            await asyncio.sleep(60)
    
    except KeyboardInterrupt:
        logger.info("Script stopped by user.")
    except Exception as e:
        logger.error(f"An unhandled error occurred in main: {str(e)}")
        logger.exception("Exception details:")
    finally:
        # Disconnect the client if connected
        if client and client.is_connected():
            await client.disconnect()
            logger.info("Disconnected Telegram client")
        # Close bot session if it has one (python-telegram-bot handles this usually on exit)
        await bot.shutdown()
        await json_bot.shutdown()
        logger.info("Bot sessions shut down.")

if __name__ == "__main__":
    logger.info("Starting Telegram file uploader...")
    asyncio.run(main())

