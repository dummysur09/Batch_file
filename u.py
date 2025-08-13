import os
import json
import asyncio
import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import aiofiles
from telegram import Bot
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
from pyrogram import Client, errors as pyrogram_errors
from filelock import FileLock
import signal
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Assume the base directory is the script's directory for portability
base_dir = os.path.dirname(os.path.abspath(__file__))

# Configuration
API_ID = 27584203
API_HASH = 'f78f372cb63ed6e0588ec88f4d7012c7'
PHONE_NUMBER = '+919996471384'
TELEGRAM_CHANNEL_ID = -1002856592532
TELEGRAM_CHANNEL_ID_STR = '-1002856592532'
LOCAL_BASE_DIR = os.path.join(base_dir, '2_batches')
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 5
MAX_RETRY_DELAY = 60
FILE_SIZE_THRESHOLD = 49 * 1024 * 1024
MAX_CONCURRENT_BOT_UPLOADS = 3
MAX_CONCURRENT_CLIENT_UPLOADS = 10
JSON_LOCK_PATH = os.path.join(base_dir, 'uploader.json.lock')
SAVE_BATCH_SIZE = 20 # Save after every 20 files

# Initialize Telegram bot
TOKEN = '8019101845:AAEjWmjvzdpUDp8-ftA9asbgQ_df38O_Ij8'
request = HTTPXRequest(
    connection_pool_size=10, read_timeout=60, write_timeout=60, connect_timeout=30
)
bot = Bot(token=TOKEN, request=request)

# Initialize JSON backup bot
JSON_BOT_TOKEN = '7648774676:AAHIgJh1ND6U81Bhn3oI6IrRgvK4b9iKvdk'
JSON_CHANNEL_ID = '-1002757649300'
json_bot = Bot(token=JSON_BOT_TOKEN, request=request)

# Initialize Telegram client
client_session_file = os.path.join(base_dir, 'pyrogram_session')
client = None

# Track last JSON upload
last_json_upload_time = 0
json_upload_interval = 300

# Semaphores
bot_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BOT_UPLOADS)
client_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CLIENT_UPLOADS)

def get_telegram_link(channel_id, message_id):
    if isinstance(channel_id, str) and channel_id.startswith('-100'):
        channel_id_num = channel_id[4:]
    elif isinstance(channel_id, int) and channel_id < 0:
        channel_id_num = str(abs(channel_id))[4:]
    else:
        channel_id_num = str(channel_id)
    return f"https://t.me/c/{channel_id_num}/{message_id}"

async def initialize_client():
    global client
    if client is None:
        try:
            client = Client(
                name=client_session_file, api_id=API_ID, api_hash=API_HASH, workdir=base_dir
            )
            await client.start()
            logger.info("Pyrogram user client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Pyrogram client: {e}")
            client = None
    elif not client.is_connected:
        try:
            await client.start()
            logger.info("Pyrogram user client reconnected successfully")
        except Exception as e:
            logger.error(f"Failed to reconnect Pyrogram client: {e}")
            client = None
    return client

async def upload_with_bot(file_path, max_retries=MAX_RETRIES):
    async with bot_semaphore:
        rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Uploading with bot: {rel_path} (Attempt {attempt})")
                with open(file_path, 'rb') as file:
                    caption = f"📄 {rel_path}"
                    message = await bot.send_document(
                        chat_id=TELEGRAM_CHANNEL_ID_STR, document=file, caption=caption
                    )
                logger.info(f"Successfully uploaded with bot: {rel_path}")
                return {
                    "message_id": message.message_id, "upload_method": "bot",
                    "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID_STR, message.message_id),
                    "file_path": rel_path, "size": os.path.getsize(file_path),
                    "Name": os.path.basename(file_path)
                }
            except Exception as e:
                logger.error(f"Bot attempt {attempt} failed for {rel_path}: {e}")
                if attempt < max_retries: await asyncio.sleep(INITIAL_RETRY_DELAY * attempt)
                else: return None
        return None

async def upload_with_client(file_path, max_retries=MAX_RETRIES):
    async with client_semaphore:
        rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Uploading with user account: {rel_path} (Attempt {attempt})")
                caption = f"📄 {rel_path}"
                message = await client.send_document(
                    chat_id=TELEGRAM_CHANNEL_ID, document=file_path, caption=caption
                )
                logger.info(f"Successfully uploaded with user account: {rel_path}")
                return {
                    "message_id": message.id, "upload_method": "user_account",
                    "telegram_link": get_telegram_link(TELEGRAM_CHANNEL_ID, message.id),
                    "file_path": rel_path, "size": os.path.getsize(file_path),
                    "Name": os.path.basename(file_path)
                }
            except Exception as e:
                logger.error(f"User account attempt {attempt} failed for {rel_path}: {e}")
                if isinstance(e, pyrogram_errors.exceptions.FloodWait):
                    logger.warning(f"FloodWait for {e.value} seconds.")
                    await asyncio.sleep(e.value + 5)
                elif attempt < max_retries: await asyncio.sleep(INITIAL_RETRY_DELAY * attempt)
                else: return None
        return None

async def upload_json_to_telegram(uploaded_files, json_file_path):
    global last_json_upload_time
    current_time = time.time()
    if current_time - last_json_upload_time > json_upload_interval:
        try:
            logger.info("Uploading full JSON file to backup channel")
            with open(json_file_path, 'rb') as f:
                caption = f"📊 Full upload records backup - {len(uploaded_files)} files"
                await json_bot.send_document(chat_id=JSON_CHANNEL_ID, document=f, caption=caption)
            last_json_upload_time = current_time
            logger.info("Successfully uploaded JSON backup to channel.")
        except Exception as e:
            logger.error(f"Failed to upload JSON to Telegram: {e}")

async def find_all_files(directory):
    return [os.path.join(root, file) for root, _, files in os.walk(directory) for file in files]

def is_file_uploaded(file_path, uploaded_files):
    rel_path = os.path.relpath(file_path, LOCAL_BASE_DIR)
    return any(entry.get("file_path") == rel_path for entry in uploaded_files.values())

def should_skip_file(file_path):
    try:
        return file_path.endswith('.part') or os.path.getsize(file_path) == 0
    except FileNotFoundError:
        return True

async def truncate_file(file_path):
    try:
        async with aiofiles.open(file_path, 'w') as f:
            await f.truncate(0)
        logger.info(f"Truncated file: {os.path.relpath(file_path, LOCAL_BASE_DIR)}")
    except Exception as e:
        logger.error(f"Failed to truncate file {file_path}: {e}")

async def save_and_truncate_batch(batch_to_process, uploaded_files, json_file_path):
    """Helper function to save records and truncate files for a given batch."""
    if not batch_to_process:
        return

    logger.info(f"Saving a batch of {len(batch_to_process)} records...")
    save_ok = await save_upload_records(uploaded_files, json_file_path)
    
    if save_ok:
        logger.info("Save successful. Truncating files in the batch.")
        for path in batch_to_process:
            await truncate_file(path)
        await upload_json_to_telegram(uploaded_files, json_file_path)
    else:
        logger.error("Failed to save upload records. Batch will NOT be truncated to allow for retry.")

async def save_upload_records(uploaded_files, json_file_path):
    lock = FileLock(JSON_LOCK_PATH, timeout=10)
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, lock.acquire)
        temp_path = json_file_path + '.tmp'
        async with aiofiles.open(temp_path, 'w') as f:
            await f.write(json.dumps(uploaded_files, indent=2))
        os.replace(temp_path, json_file_path)
        logger.info(f"Successfully saved {len(uploaded_files)} records to {json_file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save JSON file: {e}")
        return False
    finally:
        if lock.is_locked:
            await loop.run_in_executor(None, lock.release)

async def main():
    uploaded_files = {}
    json_file_path = os.path.join(base_dir, 'uploader.json')

    async def shutdown(signum, frame):
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        if client and client.is_connected:
            await client.stop()
        if uploaded_files:
            logger.info("Performing final save before exit...")
            await save_upload_records(uploaded_files, json_file_path)
        sys.exit(0)

    signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(shutdown(s, f)))
    signal.signal(signal.SIGTERM, lambda s, f: asyncio.create_task(shutdown(s, f)))

    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r') as f:
                uploaded_files = json.load(f)
            logger.info(f"Loaded {len(uploaded_files)} previously uploaded files.")
        except json.JSONDecodeError:
            logger.warning("Could not decode JSON file. Starting fresh.")

    if not os.path.exists(LOCAL_BASE_DIR):
        os.makedirs(LOCAL_BASE_DIR)

    await initialize_client()
    if not client or not client.is_connected:
        logger.error("Pyrogram client failed to connect. Exiting.")
        return

    while True:
        try:
            logger.info("Checking for new files...")
            all_files = await find_all_files(LOCAL_BASE_DIR)
            files_to_upload = [
                f for f in all_files if not should_skip_file(f) and not is_file_uploaded(f, uploaded_files)
            ]

            if not files_to_upload:
                logger.info("No new files to upload. Sleeping for 60 seconds.")
                await asyncio.sleep(60)
                continue

            logger.info(f"Found {len(files_to_upload)} new files to upload.")
            
            task_to_path = {}
            for file_path in files_to_upload:
                upload_func = upload_with_client if os.path.getsize(file_path) > FILE_SIZE_THRESHOLD else upload_with_bot
                task = asyncio.create_task(upload_func(file_path))
                task_to_path[task] = file_path
            
            # This list holds file paths for the current batch to be saved and truncated
            batch_to_process = []
            
            pending_tasks = set(task_to_path.keys())
            while pending_tasks:
                done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    file_path = task_to_path[task]
                    try:
                        result = task.result()
                        if result:
                            key = f"file_{int(time.time())}_{os.path.basename(file_path)}"
                            counter = 0
                            while key in uploaded_files:
                                counter += 1
                                key = f"file_{int(time.time())}_{os.path.basename(file_path)}_{counter}"
                            
                            uploaded_files[key] = result
                            batch_to_process.append(file_path)
                            logger.info(f"Processed '{os.path.basename(file_path)}'. Batch size: {len(batch_to_process)}/{SAVE_BATCH_SIZE}")

                            # Check if the batch is full and needs to be saved
                            if len(batch_to_process) >= SAVE_BATCH_SIZE:
                                await save_and_truncate_batch(batch_to_process, uploaded_files, json_file_path)
                                # Clear the batch list after processing
                                batch_to_process.clear()

                        else:
                            logger.warning(f"Upload failed permanently for {os.path.relpath(file_path, LOCAL_BASE_DIR)}")
                    except Exception as e:
                        logger.error(f"An exception occurred in task for {os.path.relpath(file_path, LOCAL_BASE_DIR)}: {e}")

            # After the main loop, save any remaining items that didn't make a full batch
            if batch_to_process:
                logger.info("Saving final batch for this cycle...")
                await save_and_truncate_batch(batch_to_process, uploaded_files, json_file_path)

        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}", exc_info=True)
            await asyncio.sleep(30)

        logger.info("Cycle complete. Sleeping for 60 seconds.")
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped by user.")
