import os
import json
import asyncio
import logging
import time
import random
from telegram import Bot
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
from telethon import TelegramClient
from telethon.tl.types import InputPeerChannel

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

base_dir = "/home/co"

# Configuration
API_ID = 27584203
API_HASH = 'f78f372cb63ed6e0588ec88f4d7012c7'
PHONE_NUMBER = '+919996471384'
TELEGRAM_CHANNEL_ID = -1002856592532  # Integer version for Telethon
TELEGRAM_CHANNEL_ID_STR = '-1002856592532'  # String version for python-telegram-bot
LOCAL_BASE_DIR = os.path.join(base_dir, '2_batches')
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 60  # seconds
FILE_SIZE_THRESHOLD = 49 * 1024 * 1024  # 20MB - use bot for files smaller than this

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
    client = TelegramClient(client_session_file, API_ID, API_HASH)
    await client.start(phone=PHONE_NUMBER)
    logger.info("Telegram user client initialized successfully")
    return client

async def upload_with_bot(file_path, max_retries=MAX_RETRIES):
    """Upload a file using the Telegram bot API."""
    file_name = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Uploading with bot: {rel_path} (Attempt {attempt}/{max_retries})")
            
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
            # Check if message has document attribute before accessing file_id
            file_id = None
            if hasattr(message, 'document') and message.document is not None:
                file_id = message.document.file_id
            else:
                logger.warning(f"Message doesn't have document.file_id attribute, but upload was successful")
            
            # Return upload details
            return {
                "message_id": message.message_id,
                "file_id": file_id,  # This might be None, but that's okay
                "method": "bot",
                "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID_STR, message.message_id)
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
    """Upload a file using the Telegram user account API (Telethon)."""
    global client
    file_name = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
    
    # Ensure client is initialized
    if client is None or not client.is_connected():
        await initialize_client()
    
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
                progress_callback=lambda current, total: logger.info(f"Upload progress: {current/total*100:.1f}%") if current % (1024*1024*5) == 0 else None
            )
            
            logger.info(f"Successfully uploaded with user account: {rel_path}")
            
            # Return upload details
            return {
                "message_id": message.id,
                "method": "user_account",
                "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID, message.id)
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
                if not client.is_connected():
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
            with open(temp_json_path, 'w') as f:
                json.dump(uploaded_files, f, indent=2)
            
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
            for key, value in uploaded_files.items():
                # Simple heuristic: if the key contains the timestamp and it's recent, include it
                try:
                    if "file_" in key and int(key.split("_")[1]) > last_json_upload_time:
                        recent_entries[key] = value
                except (IndexError, ValueError):
                    continue
            
            if recent_entries:
                # Format a message with recent entries
                message_text = f"📝 Update: {len(recent_entries)} new files added\n\n"
                
                for key, entry in list(recent_entries.items())[:10]:  # Limit to 10 entries to avoid message size limits
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

def find_all_files(directory):
    """Recursively find all files in a directory."""
    all_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)
    return all_files

def is_file_uploaded(file_path, uploaded_files):
    """Check if a file has already been uploaded by checking its name and path."""
    file_name = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
    file_size = os.path.getsize(file_path)
    
    # Check if any entry has the same file path
    for entry in uploaded_files.values():
        if "file_path" in entry and entry["file_path"] == rel_path:
            return True
    
    # Check if any entry has the same file name AND size (to be more accurate)
    for entry in uploaded_files.values():
        if ("Name" in entry and entry["Name"] == file_name and 
            "size" in entry and entry["size"] == file_size):
            return True
    
    return False

def should_skip_file(file_path):
    """Check if a file should be skipped (part files or 0KB files)."""
    # Skip .part files
    if file_path.endswith('.part'):
        return True
    
    # Skip 0KB files
    try:
        size = os.path.getsize(file_path)
        if size == 0:
            return True
    except Exception as e:
        logger.error(f"Error checking file size for {file_path}: {str(e)}")
        # If we can't get the size, better to skip
        return True
    
    return False

async def truncate_file(file_path):
    """Safely truncate a file to 0 bytes."""
    try:
        open(file_path, 'w').close()
        logger.info(f"Truncated file: {os.path.relpath(file_path, LOCAL_BASE_DIR)}")
        return True
    except Exception as e:
        logger.error(f"Failed to truncate file {os.path.relpath(file_path, LOCAL_BASE_DIR)}: {str(e)}")
        return False

async def save_upload_records(uploaded_files, json_file_path):
    """Safely save upload records to JSON."""
    try:
        with open(json_file_path, 'w') as f:
            json.dump(uploaded_files, f, indent=2)
        logger.info(f"Updated upload records in {json_file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save upload records: {str(e)}")
        return False

async def main():
    """Main function to monitor directory and upload files."""
    global last_json_upload_time
    
    uploaded_files = {}
    json_file_path = os.path.join(base_dir, 'uploader.json')
    json_backup_count = 0

    # Load existing uploads from JSON if exists
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r') as f:
                uploaded_files = json.load(f)
            logger.info(f"Loaded {len(uploaded_files)} previously uploaded files from JSON")
        except json.JSONDecodeError:
            logger.warning("JSON file exists but is invalid. Starting with empty record.")
            # Try to backup the invalid file
            try:
                backup_path = json_file_path + ".bak"
                os.rename(json_file_path, backup_path)
                logger.info(f"Backed up invali d JSON to {backup_path}")
            except Exception:
                pass
    
    # Create batch directory if it doesn't exist
    if not os.path.exists(LOCAL_BASE_DIR):
        os.makedirs(LOCAL_BASE_DIR)
        logger.info(f"Created directory: {LOCAL_BASE_DIR}")

    try:
        # Test bot connection
        me = await bot.get_me()
        logger.info(f"Bot connected successfully. Bot username: @{me.username}")
        
        # Initialize Telegram client
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
                all_files = find_all_files(LOCAL_BASE_DIR)
                
                if not all_files:
                    logger.info("No files found in directory.")
                else:
                    logger.info(f"Found {len(all_files)} files in directory tree.")
                
                files_uploaded = 0
                files_skipped = 0
                
                for file_path in all_files:
                    file_name = os.path.basename(file_path)
                    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
                    
                    # Skip .part files and 0KB files
                    if should_skip_file(file_path):
                        logger.info(f"Skipping {rel_path} (part file or 0KB file)")
                        files_skipped += 1
                        continue
                    
                    size = os.path.getsize(file_path)
                    logger.info(f"Processing file: {rel_path} ({size/1024/1024:.2f} MB)")
                    
                    # Check if file has already been uploaded
                    if is_file_uploaded(file_path, uploaded_files):
                        logger.info(f"File {rel_path} already uploaded, skipping.")
                        files_skipped += 1
                        continue
                    
                    # Choose upload method based on file size
                    upload_result = None
                    if size < FILE_SIZE_THRESHOLD:
                        logger.info(f"Using bot to upload {file_name} ({size/1024/1024:.2f} MB)")
                        upload_result = await upload_with_bot(file_path)
                    else:
                        logger.info(f"Using user account to upload {file_name} ({size/1024/1024:.2f} MB)")
                        upload_result = await upload_with_client(file_path)
                    
                    if upload_result:
                        # Create a unique key for the file
                        key = f"file_{int(time.time())}_{file_name}"
                        
                        # Store file info in the format you requested
                        uploaded_files[key] = {
                            "Name": file_name,
                            "file_path": rel_path,
                            "size": size,
                            "telegram_link": upload_result["telegram_link"],
                            "upload_method": upload_result["method"],
                            "message_id": upload_result["message_id"]
                        }
                        
                        # Only add file_id if it exists
                        if "file_id" in upload_result and upload_result["file_id"] is not None:
                            uploaded_files[key]["file_id"] = upload_result["file_id"]
                        
                        # Save upload records to JSON after each successful upload
                        await save_upload_records(uploaded_files, json_file_path)
                        
                        # Truncate file to 0 bytes AFTER saving JSON (so if truncation fails, we still have record)
                        await truncate_file(file_path)
                        
                        files_uploaded += 1
                    else:
                        logger.warning(f"Failed to upload {rel_path} after all retries, will try again next cycle.")
                
                logger.info(f"Upload cycle completed: {files_uploaded} files uploaded, {files_skipped} files skipped")
                
                # Upload JSON records to the separate channel
                await upload_json_to_telegram(uploaded_files, json_file_path)
                
            except Exception as e:
                logger.error(f"Error during file processing: {str(e)}")
                logger.exception("Exception details:")
            
            logger.info(f"Sleeping for 60 seconds before next check...")
            await asyncio.sleep(60)
    
    except KeyboardInterrupt:
        logger.info("Script stopped by user.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.exception("Exception details:")
    finally:
        # Disconnect the client if connected
        if client and client.is_connected():
            await client.disconnect()
            logger.info("Disconnected Telegram client")

if __name__ == "__main__":
    logger.info("Starting Telegram file uploader...")
    asyncio.run(main())