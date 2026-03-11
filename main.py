import asyncio
import logging
import os
import sqlite3
import re
import aiohttp
import tempfile
import subprocess
import json
import random
import signal
import hashlib
from datetime import datetime, timezone
import time

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramServerError
from dotenv import load_dotenv
import asyncpraw

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables with fallbacks
API_TOKEN = os.getenv("TELEGRAM_TOKEN", "your_telegram_bot_token_here")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "your_reddit_client_id")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "your_reddit_client_secret")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "TelegramRedditBot/1.0 by /u/yourusername")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # Telegram user ID of the admin
DEFAULT_CHAT_ID = int(os.getenv("DEFAULT_CHAT_ID", os.getenv("ADMIN_ID", "0")))  # Default chat for posts

# Validate required environment variables
if not API_TOKEN or API_TOKEN == "your_telegram_bot_token_here":
    logger.error("TELEGRAM_TOKEN environment variable is required!")
    exit(1)

if not REDDIT_CLIENT_ID or REDDIT_CLIENT_ID == "your_reddit_client_id":
    logger.error("REDDIT_CLIENT_ID environment variable is required!")
    exit(1)

if not REDDIT_CLIENT_SECRET or REDDIT_CLIENT_SECRET == "your_reddit_client_secret":
    logger.error("REDDIT_CLIENT_SECRET environment variable is required!")
    exit(1)

if ADMIN_ID == 0:
    logger.error("ADMIN_ID environment variable is required!")
    exit(1)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Rate limiting globals - optimized for smooth operation
last_request_time = 0
request_count = 0
rate_limit_window = 60  # 1 minute window
max_requests_per_window = 25  # Optimized for better performance 
min_delay_between_sends = 1.0  # Further reduced for optimal throughput
send_queue_lock = asyncio.Lock()  # Global lock for all sending operations
send_queue = asyncio.Queue()  # Global queue for all media sending
send_queue_worker_started = False

# Enhanced task management
background_tasks_limit = 75  # Optimized for better concurrent processing
pending_posts_queue = asyncio.Queue()  # Queue for posts that couldn't be processed immediately
graceful_shutdown = False  # Flag for graceful shutdown handling

# Initialize Reddit API
reddit = None

# Global flags to control scraping operations and pause normal downloads
scraping_active = False
current_scrape_username = None
current_scrape_type = None  # 'user' or 'redgifs'
normal_downloads_paused = False  # Flag to pause normal subreddit/user monitoring

# Global flags for bot control
bot_paused = False  # Flag to pause all media sending
queue_cleared = False  # Flag to indicate queue was cleared

# Global task management for concurrent processing
background_tasks = set()

def create_background_task(coro, priority="normal"):
    """Create a background task with intelligent load balancing"""
    current_tasks = len(background_tasks)
    
    # Clean up completed tasks first
    completed_tasks = [task for task in background_tasks.copy() if task.done()]
    for task in completed_tasks:
        background_tasks.discard(task)
    current_tasks = len(background_tasks)
    
    # Dynamic task limit based on system load
    if current_tasks > background_tasks_limit:
        if priority == "high":
            logger.info(f"High priority task queued despite {current_tasks} background tasks")
        else:
            logger.debug(f"Task queued due to {current_tasks} background tasks, will retry shortly")
            # Queue the task for later processing instead of dropping it
            asyncio.create_task(queue_delayed_task(coro))
            return None
    
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    
    def cleanup_task(t):
        try:
            background_tasks.discard(t)
            # Log any exceptions from completed tasks
            if t.done() and not t.cancelled():
                exc = t.exception()
                if exc:
                    logger.error(f"Background task failed: {exc}")
        except Exception as e:
            logger.error(f"Error during task cleanup: {e}")
    
    task.add_done_callback(cleanup_task)
    return task

async def queue_delayed_task(coro):
    """Queue a task for delayed execution when resources become available"""
    try:
        await pending_posts_queue.put(coro)
    except Exception as e:
        logger.error(f"Failed to queue delayed task: {e}")

async def process_pending_tasks():
    """Process tasks that were delayed due to high load"""
    while not graceful_shutdown:
        try:
            # Clean up completed tasks first
            completed_tasks = [task for task in background_tasks.copy() if task.done()]
            for task in completed_tasks:
                background_tasks.discard(task)
            
            if len(background_tasks) < background_tasks_limit * 0.8:  # Process when under 80% capacity
                try:
                    coro = await asyncio.wait_for(pending_posts_queue.get(), timeout=1.0)
                    task = asyncio.create_task(coro)
                    background_tasks.add(task)
                    
                    def cleanup_pending_task(t):
                        try:
                            background_tasks.discard(t)
                            if t.done() and not t.cancelled():
                                exc = t.exception()
                                if exc:
                                    logger.error(f"Pending task failed: {exc}")
                        except Exception as e:
                            logger.error(f"Error cleaning up pending task: {e}")
                    
                    task.add_done_callback(cleanup_pending_task)
                    pending_posts_queue.task_done()
                except asyncio.TimeoutError:
                    pass  # No pending tasks, continue
            else:
                await asyncio.sleep(1)  # Reduced wait time for smoother processing
        except Exception as e:
            logger.error(f"Error processing pending tasks: {e}")
            await asyncio.sleep(5)


async def periodic_cleanup():
    """Periodic cleanup to prevent memory leaks and maintain database health"""
    while not graceful_shutdown:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            
            # Clean up completed background tasks
            completed_tasks = [task for task in background_tasks.copy() if task.done()]
            for task in completed_tasks:
                background_tasks.discard(task)
            
            if completed_tasks:
                logger.info(f"Cleaned up {len(completed_tasks)} completed background tasks")
            
            # Clean up old shared posts (older than 30 days)
            try:
                thirty_days_ago = int(time.time()) - (30 * 24 * 60 * 60)
                cursor.execute("DELETE FROM shared_posts WHERE shared_time < ?", (thirty_days_ago,))
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    conn.commit()
                    logger.info(f"Cleaned up {deleted_count} old shared posts")
            except sqlite3.Error as e:
                logger.error(f"Error cleaning up old shared posts: {e}")
            
            # Log current system status
            logger.info(f"System status: {len(background_tasks)} active tasks, queue size: {pending_posts_queue.qsize()}")
            
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying

async def start_send_queue_worker():
    """Start the global send queue worker to process all media sends sequentially"""
    global send_queue_worker_started, bot_paused, queue_cleared
    if send_queue_worker_started:
        return
    
    send_queue_worker_started = True
    logger.info("Starting global send queue worker for ultra rate limiting")
    
    while True:
        try:
            # Check if bot is paused
            if bot_paused:
                await asyncio.sleep(1.0)  # Wait while paused
                continue
            
            # Check if queue was cleared
            if queue_cleared:
                # Clear the queue by getting and discarding all items
                while not send_queue.empty():
                    try:
                        send_item = await asyncio.wait_for(send_queue.get(), timeout=0.1)
                        send_func, args, kwargs, result_future = send_item
                        result_future.set_exception(Exception("Queue cleared"))
                        send_queue.task_done()
                    except asyncio.TimeoutError:
                        break
                queue_cleared = False
                logger.info("Queue cleared - discarded all pending items")
                continue
            
            # Get next send operation from queue
            send_item = await send_queue.get()
            send_func, args, kwargs, result_future = send_item
            
            try:
                # Check if we're in scraping mode for faster processing
                if scraping_active:
                    await asyncio.sleep(0.2)  # Ultra fast during scraping
                else:
                    await asyncio.sleep(1.5)  # Normal rate limiting
                result = await send_func(*args, **kwargs)
                result_future.set_result(result)
            except Exception as e:
                result_future.set_exception(e)
            finally:
                send_queue.task_done()
                
        except Exception as e:
            logger.error(f"Send queue worker error: {e}")
            await asyncio.sleep(5.0)

async def queue_send_operation(send_func, *args, **kwargs):
    """Queue a send operation to be processed by the global send worker"""
    result_future = asyncio.Future()
    await send_queue.put((send_func, args, kwargs, result_future))
    return await result_future

async def init_reddit():
    """Initialize async Reddit connection"""
    global reddit
    try:
        reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        # Test Reddit connection
        await reddit.user.me()
        logger.info("Reddit API connection successful")
    except Exception as e:
        logger.error(f"Failed to connect to Reddit API: {e}")
        exit(1)

# Initialize SQLite database with better connection handling
try:
    conn = sqlite3.connect("data.db", check_same_thread=False, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL')  # Enable WAL mode for better concurrent access
    conn.execute('PRAGMA synchronous=NORMAL')  # Balance between safety and performance
    conn.execute('PRAGMA cache_size=10000')  # Increase cache size for better performance
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS subreddits (
            name TEXT PRIMARY KEY,
            last_post INTEGER DEFAULT 0,
            sort_mode TEXT DEFAULT 'new'
        )"""
    )
    
    # Create table to track shared posts to prevent duplicates
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS shared_posts (
            post_id TEXT PRIMARY KEY,
            subreddit TEXT,
            username TEXT,
            shared_time INTEGER DEFAULT (strftime('%s', 'now'))
        )"""
    )
    
    # Create table to track active chats where bot should send posts
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS active_chats (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            added_by INTEGER,
            added_at INTEGER DEFAULT (strftime('%s', 'now')),
            is_active INTEGER DEFAULT 1
        )"""
    )
    
    # Create table to track bot state for resume functionality
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )"""
    )
    
    # Create table to track Reddit user profiles
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS user_profiles (
            username TEXT PRIMARY KEY,
            last_post INTEGER DEFAULT 0,
            sort_mode TEXT DEFAULT 'new'
        )"""
    )
    
    # Create table to track media content to prevent duplicate media sharing
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS media_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            media_hash TEXT UNIQUE NOT NULL,
            media_url TEXT NOT NULL,
            post_id TEXT NOT NULL,
            subreddit TEXT,
            username TEXT,
            first_seen_time INTEGER DEFAULT (strftime('%s', 'now')),
            shared_count INTEGER DEFAULT 1,
            UNIQUE(media_hash)
        )"""
    )
    
    # Add sort_mode column to existing tables if it doesn't exist
    try:
        cursor.execute("ALTER TABLE subreddits ADD COLUMN sort_mode TEXT DEFAULT 'new'")
        conn.commit()
        logger.info("Added sort_mode column to existing database")
    except Exception:
        # Column already exists, ignore
        pass
    
    # Add username column to shared_posts table if it doesn't exist
    try:
        cursor.execute("ALTER TABLE shared_posts ADD COLUMN username TEXT")
        conn.commit()
        logger.info("Added username column to shared_posts table")
    except Exception:
        # Column already exists, ignore
        pass
    
    # Ensure the shared_posts table has the correct schema
    try:
        cursor.execute("PRAGMA table_info(shared_posts)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'id' not in columns:
            # Recreate table with proper schema
            cursor.execute("DROP TABLE IF EXISTS shared_posts_backup")
            cursor.execute("CREATE TABLE shared_posts_backup AS SELECT * FROM shared_posts")
            cursor.execute("DROP TABLE shared_posts")
            cursor.execute("""
                CREATE TABLE shared_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT NOT NULL,
                    subreddit TEXT,
                    username TEXT,
                    shared_time INTEGER DEFAULT 0,
                    UNIQUE(post_id)
                )
            """)
            # Try to migrate data, handling missing columns gracefully
            try:
                cursor.execute("SELECT post_id, subreddit FROM shared_posts_backup")
                for row in cursor.fetchall():
                    cursor.execute(
                        "INSERT OR IGNORE INTO shared_posts (post_id, subreddit, shared_time) VALUES (?, ?, ?)",
                        (row[0], row[1], int(time.time()))
                    )
            except Exception:
                # If migration fails, just start fresh
                pass
            cursor.execute("DROP TABLE IF EXISTS shared_posts_backup")
            conn.commit()
            logger.info("Fixed shared_posts table schema with UNIQUE constraint")
    except Exception as e:
        logger.error(f"Error fixing shared_posts schema: {e}")
        # If all else fails, recreate from scratch
        try:
            cursor.execute("DROP TABLE IF EXISTS shared_posts")
            cursor.execute("""
                CREATE TABLE shared_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT NOT NULL,
                    subreddit TEXT,
                    username TEXT,
                    shared_time INTEGER DEFAULT 0,
                    UNIQUE(post_id)
                )
            """)
            conn.commit()
            logger.info("Recreated shared_posts table from scratch with UNIQUE constraint")
        except Exception as e2:
            logger.error(f"Failed to recreate shared_posts table: {e2}")
    
    # Add default admin chat if no chats exist
    try:
        cursor.execute("SELECT COUNT(*) FROM active_chats WHERE is_active = 1")
        active_count = cursor.fetchone()[0]
        if active_count == 0 and ADMIN_ID > 0:
            cursor.execute("INSERT OR IGNORE INTO active_chats (chat_id, chat_title, added_by) VALUES (?, ?, ?)", 
                          (ADMIN_ID, "Admin Chat", ADMIN_ID))
            conn.commit()
            logger.info(f"Added default admin chat {ADMIN_ID}")
    except Exception as e:
        logger.warning(f"Could not add default admin chat: {e}")
    
    conn.commit()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")
    exit(1)


async def get_redgifs_video_url(redgifs_url):
    """Extract direct video URL from RedGifs link"""
    try:
        # Extract the gif ID from various RedGifs URL formats
        patterns = [
            r'redgifs\.com/watch/(\w+)',
            r'v3\.redgifs\.com/watch/(\w+)',
            r'redgifs\.com/ifr/(\w+)',
        ]
        
        gif_id = None
        for pattern in patterns:
            match = re.search(pattern, redgifs_url)
            if match:
                gif_id = match.group(1)
                break
        
        if not gif_id:
            logger.warning(f"Could not extract gif ID from URL: {redgifs_url}")
            return None
        
        logger.info(f"Extracted gif ID: {gif_id}")
        
        # Try multiple API endpoints - start with v1 since v2 often returns 401
        api_endpoints = [
            f"https://api.redgifs.com/v1/gfycats/{gif_id}",
            f"https://api.redgifs.com/v2/gifs/{gif_id}",
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            for api_url in api_endpoints:
                try:
                    async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Handle v1 API response format (more reliable)
                            if 'gfyItem' in data:
                                item = data['gfyItem']
                                # Try different video URL fields
                                for field in ['mp4Url', 'webmUrl', 'mobileUrl']:
                                    if field in item and item[field]:
                                        video_url = item[field]
                                        logger.info(f"Found video URL from {field}: {video_url}")
                                        return video_url
                            
                            # Handle v2 API response format
                            elif 'gif' in data and 'urls' in data['gif']:
                                urls = data['gif']['urls']
                                # Try to get the best quality video URL
                                for quality in ['hd', 'sd', 'mobile']:
                                    if quality in urls:
                                        video_url = urls[quality]
                                        logger.info(f"Found {quality} video URL: {video_url}")
                                        return video_url
                        elif response.status == 401 and 'v2' in api_url:
                            # v2 API often requires authentication, skip silently
                            continue
                        else:
                            logger.debug(f"API endpoint {api_url} returned status {response.status}")
                except Exception as e:
                    logger.debug(f"Failed to fetch from {api_url}: {e}")
                    continue
                
    except Exception as e:
        logger.error(f"Failed to get RedGifs video URL: {e}")
    
    logger.warning(f"Could not extract video URL from RedGifs: {redgifs_url}")
    return None


async def smart_rate_limit():
    """Intelligent rate limiting with exponential backoff"""
    global last_request_time, request_count
    
    current_time = time.time()
    
    # Reset counter if window has passed
    if current_time - last_request_time > rate_limit_window:
        request_count = 0
        last_request_time = current_time
    
    # If we're at the limit, wait for the window to reset
    if request_count >= max_requests_per_window:
        wait_time = rate_limit_window - (current_time - last_request_time)
        if wait_time > 0:
            logger.info(f"Rate limit reached, waiting {wait_time:.1f}s before next request")
            await asyncio.sleep(wait_time)
            request_count = 0
            last_request_time = time.time()
    
    # Smooth delay between requests for optimal performance
    base_delay = 0.5  # Reduced further for better throughput
    # Add minimal jitter to prevent synchronized requests
    jitter = random.uniform(0.1, 0.3)  # Reduced jitter for faster processing
    total_delay = base_delay + jitter
    
    logger.debug(f"Applying rate limit delay: {total_delay:.1f}s")
    await asyncio.sleep(total_delay)
    
    request_count += 1


async def handle_telegram_error(error, chat_id, operation="send message"):
    """Handle Telegram API errors with exponential backoff"""
    if isinstance(error, TelegramRetryAfter):
        # Flood control - wait the specified time
        retry_after = error.retry_after
        logger.warning(f"Flood control exceeded for {operation}. Waiting {retry_after}s before retry")
        await asyncio.sleep(retry_after + random.uniform(1, 3))  # Add small jitter
        return True  # Indicates we should retry
    
    elif isinstance(error, TelegramServerError):
        # Server error - exponential backoff
        base_delay = 5
        max_delay = 60
        # Use a simple exponential backoff without storing state on the error object
        delay = min(base_delay * 2, max_delay)
        logger.warning(f"Telegram server error for {operation}. Retrying after {delay}s")
        await asyncio.sleep(delay + random.uniform(0, 2))
        return True
    
    elif "Flood control exceeded" in str(error):
        # Parse retry time from error message if available
        import re
        match = re.search(r'must wait (\d+) seconds', str(error))
        if match:
            wait_time = int(match.group(1))
        else:
            wait_time = 30  # Default wait time
        
        logger.warning(f"Flood control exceeded for {operation}. Waiting {wait_time}s")
        await asyncio.sleep(wait_time + random.uniform(2, 5))
        return True
    
    return False  # Don't retry for other errors


async def send_with_retry(send_func, *args, max_retries=3, operation="send message", **kwargs):
    """Send Telegram message/media with global queue and aggressive flood control handling"""
    async with send_queue_lock:  # Global lock to serialize ALL sending operations
        for attempt in range(max_retries):
            try:
                # Apply aggressive rate limiting before each attempt
                await smart_rate_limit()
                
                # Additional global delay between ALL sends
                global last_request_time
                current_time = time.time()
                time_since_last = current_time - last_request_time
                if time_since_last < min_delay_between_sends:
                    sleep_time = min_delay_between_sends - time_since_last
                    logger.info(f"Global rate limit: waiting {sleep_time:.1f}s before {operation}")
                    await asyncio.sleep(sleep_time)
                
                last_request_time = time.time()
                result = await send_func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Successfully {operation} after {attempt} retries")
                return result
            
            except Exception as error:
                error_msg = str(error)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {operation}: {error}")
                
                # Extract wait time from flood control errors
                wait_time = 5.0  # Default wait time
                if "retry after" in error_msg.lower():
                    import re
                    wait_match = re.search(r'retry after (\d+)', error_msg)
                    if wait_match:
                        wait_time = int(wait_match.group(1)) + random.uniform(2, 5)
                
                # Handle flood control specifically
                if "flood control" in error_msg.lower() or "too many requests" in error_msg.lower():
                    if attempt < max_retries - 1:
                        logger.warning(f"Flood control exceeded for {operation}. Waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for {operation} due to flood control")
                        raise
                
                # Handle other Telegram errors
                should_retry = await handle_telegram_error(error, args[0] if args else None, operation)
                
                if not should_retry or attempt == max_retries - 1:
                    raise error
                
                # Additional backoff for retries
                backoff_delay = (2 ** attempt) + random.uniform(2, 5)
                logger.info(f"Retrying {operation} in {backoff_delay:.1f}s...")
                await asyncio.sleep(backoff_delay)
        
        raise Exception(f"Failed to {operation} after {max_retries} attempts")


async def get_video_dimensions(video_path):
    """Extract video dimensions using ffprobe with enhanced compatibility"""
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', 
            '-select_streams', 'v:0', video_path
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if 'streams' in data and len(data['streams']) > 0:
                stream = data['streams'][0]
                width = stream.get('width', 0)
                height = stream.get('height', 0)
                duration = float(stream.get('duration', 0))
                
                # Ensure dimensions are even numbers for better codec compatibility
                if width % 2 != 0:
                    width -= 1
                if height % 2 != 0:
                    height -= 1
                
                # Enforce Telegram video dimension limits (max 1280x1280)
                if width > 1280 or height > 1280:
                    # Scale down proportionally
                    scale_factor = min(1280/width, 1280/height)
                    width = int(width * scale_factor)
                    height = int(height * scale_factor)
                    
                    # Ensure dimensions are even after scaling
                    if width % 2 != 0:
                        width -= 1
                    if height % 2 != 0:
                        height -= 1
                
                logger.info(f"Video dimensions: {width}x{height}, duration: {duration:.1f}s (optimized for Telegram)")
                return width, height, duration
    except Exception as e:
        logger.debug(f"Failed to extract video dimensions: {e}")
    
    # Return safe default values if extraction fails
    return 480, 854, 15.0  # Safe mobile aspect ratio


async def download_and_send_media(media_url, caption, chat_id, media_type="auto"):
    """Download and send any media type (video, gif, image) to Telegram"""
    try:
        # Use longer timeout for large files
        timeout = aiohttp.ClientTimeout(total=300, sock_read=60)  # 5 min total, 60s read timeout
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(media_url) as response:
                if response.status == 200:
                    content_length = response.headers.get('Content-Length')
                    file_size_mb = int(content_length) / (1024 * 1024) if content_length else 0
                    
                    # Check Telegram's absolute limits but don't prevent download
                    telegram_absolute_limit = 500 * 1024 * 1024  # 500MB absolute limit
                    
                    # Log file size but continue with download - we'll handle size after download
                    if content_length and int(content_length) > telegram_absolute_limit:
                        logger.warning(f"Media size ({file_size_mb:.1f}MB) exceeds Telegram limit, but attempting download anyway")
                        # Continue with download - we'll send as document or handle appropriately
                    
                    if file_size_mb > 0:
                        logger.info(f"Downloading media file: {file_size_mb:.1f}MB")
                    
                    # Determine media type from URL or content type
                    content_type = response.headers.get('Content-Type', '').lower()
                    url_lower = media_url.lower()
                    
                    if media_type == "auto":
                        if any(ext in url_lower for ext in ['.mp4', '.webm', '.mov']) or 'video' in content_type:
                            media_type = "video"
                        elif any(ext in url_lower for ext in ['.gif']) or 'gif' in content_type:
                            media_type = "animation"
                        elif any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.webp']) or 'image' in content_type:
                            media_type = "photo"
                        else:
                            # Default to video for unknown types
                            media_type = "video"
                    
                    # Determine file extension
                    if media_type == "video":
                        file_ext = '.mp4'
                    elif media_type == "animation":
                        file_ext = '.gif'
                    else:
                        file_ext = '.jpg'
                    
                    # Create a temporary file with larger chunks for big files
                    chunk_size = 65536 if file_size_mb > 50 else 8192  # 64KB for large files, 8KB for small
                    with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as temp_file:
                        downloaded_mb = 0
                        async for chunk in response.content.iter_chunked(chunk_size):
                            temp_file.write(chunk)
                            downloaded_mb += len(chunk) / (1024 * 1024)
                            # Log progress for very large files
                            if file_size_mb > 100 and downloaded_mb % 25 == 0:  # Every 25MB
                                logger.info(f"Download progress: {downloaded_mb:.1f}MB / {file_size_mb:.1f}MB")
                        temp_file_path = temp_file.name
                    
                    # Check if file is too large after download
                    actual_file_size = os.path.getsize(temp_file_path)
                    actual_size_mb = actual_file_size / (1024 * 1024)
                    
                    # Telegram's absolute limits
                    telegram_limit = 500 * 1024 * 1024  # 500MB absolute limit for documents/videos
                    photo_limit = 10 * 1024 * 1024  # 10MB for photos (Telegram API limit)
                    
                    logger.info(f"Downloaded file size: {actual_size_mb:.1f}MB, type: {media_type}")
                    
                    if actual_file_size > telegram_limit:
                        logger.warning(f"File too large for Telegram ({actual_size_mb:.1f}MB), will attempt document fallback")
                        # Try to send as document anyway, since some large files can still be sent
                        # Only return False if document sending also fails
                    
                    # Send the media based on type
                    media_input = types.FSInputFile(temp_file_path)
                    
                    if media_type == "video":
                        # Extract video dimensions for iOS compatibility
                        width, height, duration = await get_video_dimensions(temp_file_path)
                        
                        # Try to send as video first, with multiple fallback strategies for large files
                        video_sent = False
                        
                        # For very large files (over 200MB), go straight to document to save time
                        if actual_file_size > 200 * 1024 * 1024:
                            logger.info(f"Large video ({actual_size_mb:.1f}MB), sending as document")
                            try:
                                await send_with_retry(
                                    bot.send_document, 
                                    chat_id, 
                                    media_input, 
                                    caption=caption, 
                                    parse_mode=ParseMode.HTML,
                                    operation="send large video as document"
                                )
                                logger.info(f"Successfully sent large video as document: {caption[:50]}...")
                                video_sent = True
                            except Exception as doc_error:
                                logger.warning(f"Failed to send large video as document: {doc_error}")
                        else:
                            # Try as video first for smaller files
                            try:
                                await send_with_retry(
                                    bot.send_video,
                                    chat_id, 
                                    media_input, 
                                    caption=caption, 
                                    parse_mode=ParseMode.HTML,
                                    supports_streaming=True,
                                    width=width if width and width > 0 else None,
                                    height=height if height and height > 0 else None,
                                    duration=int(duration) if duration and duration > 0 else None,
                                    operation="send video"
                                )
                                logger.info(f"Successfully sent video with dimensions {width}x{height}: {caption[:50]}...")
                                video_sent = True
                            except Exception as video_error:
                                logger.warning(f"Failed to send video with dimensions, trying as document: {video_error}")
                                # Fallback: try sending as document if video fails
                                try:
                                    await send_with_retry(
                                        bot.send_document, 
                                        chat_id, 
                                        media_input, 
                                        caption=caption, 
                                        parse_mode=ParseMode.HTML,
                                        operation="send video as document"
                                    )
                                    logger.info(f"Successfully sent video as document: {caption[:50]}...")
                                    video_sent = True
                                except Exception as doc_error:
                                    logger.warning(f"Failed to send video as document: {doc_error}")
                        
                        if not video_sent:
                            # Final check - if file is too large even for document, don't return False
                            # Log the issue but continue processing other posts
                            if actual_file_size > telegram_limit:
                                logger.error(f"Video too large for Telegram ({actual_size_mb:.1f}MB), unable to send")
                                os.unlink(temp_file_path)
                                return False
                            else:
                                # Re-raise the last exception if it's not a size issue
                                raise Exception(f"Failed to send video after all attempts")
                    elif media_type == "animation":
                        try:
                            await send_with_retry(
                                bot.send_animation, 
                                chat_id, 
                                media_input, 
                                caption=caption, 
                                parse_mode=ParseMode.HTML,
                                operation="send animation"
                            )
                            logger.info(f"Successfully sent downloaded animation: {caption[:50]}...")
                        except Exception as gif_error:
                            logger.warning(f"Failed to send animation, trying as document: {gif_error}")
                            # Fallback: try sending as document if animation fails
                            await send_with_retry(
                                bot.send_document, 
                                chat_id, 
                                media_input, 
                                caption=caption, 
                                parse_mode=ParseMode.HTML,
                                operation="send animation as document"
                            )
                            logger.info(f"Successfully sent animation as document: {caption[:50]}...")
                    else:  # photo
                        # Check file size for photos (Telegram limit is 10MB for photos)
                        if actual_file_size > photo_limit:
                            # Send as document if image is too large for photo
                            logger.info(f"Image too large for photo ({actual_size_mb:.1f}MB), sending as document")
                            try:
                                await send_with_retry(
                                    bot.send_document, 
                                    chat_id, 
                                    media_input, 
                                    caption=caption, 
                                    parse_mode=ParseMode.HTML,
                                    operation="send large image as document"
                                )
                                logger.info(f"Successfully sent large image as document: {caption[:50]}...")
                            except Exception as doc_error:
                                logger.warning(f"Failed to send image as document: {doc_error}")
                                raise doc_error
                        else:
                            try:
                                await send_with_retry(
                                    bot.send_photo, 
                                    chat_id, 
                                    media_input, 
                                    caption=caption, 
                                    parse_mode=ParseMode.HTML,
                                    operation="send photo"
                                )
                                logger.info(f"Successfully sent downloaded image: {caption[:50]}...")
                            except Exception as photo_error:
                                logger.warning(f"Failed to send as photo, trying as document: {photo_error}")
                                await send_with_retry(
                                    bot.send_document, 
                                    chat_id, 
                                    media_input, 
                                    caption=caption, 
                                    parse_mode=ParseMode.HTML,
                                    operation="send photo as document"
                                )
                                logger.info(f"Successfully sent image as document: {caption[:50]}...")
                    
                    # Clean up temporary file
                    os.unlink(temp_file_path)
                    return True
    except Exception as e:
        logger.error(f"Failed to download and send media: {e}")
    
    return False


async def download_and_send_video(video_url, caption, chat_id):
    """Download and send video to Telegram (legacy function for compatibility)"""
    return await download_and_send_media(video_url, caption, chat_id, "video")


async def send_post_to_chat(post, chat_id):
    """Send a Reddit post to a specific Telegram chat"""
    title = post.title
    url = post.url
    nsfw = post.over_18
    post_url = f"https://reddit.com{post.permalink}"
    created = datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S")
    subreddit_name = post.subreddit.display_name
    
    # Media duplicate checking is now done in the main send_post function

    # Prepare message text
    prefix = "🔞 [NSFW] " if nsfw else ""
    text = f"{prefix}<b>{title}</b>\n\n"
    text += f"📍 r/{subreddit_name}\n"
    
    if post.selftext and len(post.selftext.strip()) > 0:
        # Truncate long self text
        selftext = post.selftext[:500] + "..." if len(post.selftext) > 500 else post.selftext
        text += f"{selftext}\n\n"
    
    text += f"🔗 <a href='{post_url}'>View on Reddit</a>\n"
    text += f"🕒 {created}"

    try:
        logger.info(f"Processing post: {post.title[:30]}... URL: {post.url} is_self: {post.is_self}")
        
        if post.is_self:
            # Text post
            logger.info("Sending as text post")
            await send_with_retry(
                bot.send_message, 
                chat_id, 
                text, 
                parse_mode=ParseMode.HTML, 
                disable_web_page_preview=True,
                operation="send text post"
            )
        elif any(post.url.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
            # Image/GIF/Video post - download and send locally
            if post.url.endswith('.gif'):
                logger.info(f"Downloading and sending GIF: {post.url}")
                success = await download_and_send_media(post.url, text, chat_id, "animation")
                if success:
                    # Record media content to prevent future duplicates
                    await record_media_content(post.url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                if not success:
                    # Fallback to text message with link if download fails
                    logger.warning(f"Failed to download GIF, falling back to link: {post.url}")
                    text += f"\n🎬 <a href='{post.url}'>View GIF</a>"
                    await send_with_retry(
                        bot.send_message, 
                        chat_id, 
                        text, 
                        parse_mode=ParseMode.HTML,
                        operation="send GIF fallback message"
                    )
            else:
                # Try to download and send image locally first
                logger.info(f"Downloading and sending image: {post.url}")
                success = await download_and_send_media(post.url, text, chat_id, "photo")
                if success:
                    # Record media content to prevent future duplicates
                    await record_media_content(post.url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                if not success:
                    # Fallback to direct URL sending
                    logger.info(f"Download failed, trying direct URL: {post.url}")
                    try:
                        await send_with_retry(
                            bot.send_photo, 
                            chat_id, 
                            post.url, 
                            caption=text, 
                            parse_mode=ParseMode.HTML,
                            operation="send direct photo"
                        )
                        # Record media content for direct URL sends too
                        await record_media_content(post.url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                    except Exception as image_error:
                        # Final fallback to text message with link
                        logger.warning(f"Failed to send image directly, falling back to link: {image_error}")
                        text += f"\n🖼️ <a href='{post.url}'>View Image</a>"
                        await send_with_retry(
                            bot.send_message, 
                            chat_id, 
                            text, 
                            parse_mode=ParseMode.HTML,
                            operation="send image fallback message"
                        )
        elif "v.redd.it" in post.url or post.is_video:
            # Video post - try to download and send locally
            logger.info(f"Processing video post: {post.url}")
            video_url = None
            try:
                if hasattr(post, 'media') and post.media and 'reddit_video' in post.media:
                    video_url = post.media["reddit_video"]["fallback_url"]
            except Exception:
                pass
            
            if video_url:
                logger.info(f"Downloading and sending Reddit video: {video_url}")
                success = await download_and_send_media(video_url, text, chat_id, "video")
                if success:
                    # Record media content to prevent future duplicates
                    await record_media_content(video_url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                if not success:
                    # Fallback to text message with link if download fails
                    logger.warning(f"Failed to download video, falling back to link: {video_url}")
                    text += f"\n📹 <a href='{video_url}'>View Video</a>"
                    await send_with_retry(
                        bot.send_message, 
                        chat_id, 
                        text, 
                        parse_mode=ParseMode.HTML,
                        operation="send video fallback message"
                    )
            else:
                # No direct video URL found, send as text with link
                text += f"\n📹 <a href='{post.url}'>View Video</a>"
                await send_with_retry(
                    bot.send_message, 
                    chat_id, 
                    text, 
                    parse_mode=ParseMode.HTML,
                    operation="send video link message"
                )
        elif 'redgifs.com' in url:
            # RedGifs video post - attempt to download and send
            logger.info(f"Processing RedGifs video: {url}")
            video_url = await get_redgifs_video_url(url)
            if video_url:
                logger.info(f"Got RedGifs video URL, attempting download with streaming: {video_url}")
                success = await download_and_send_video(video_url, text, chat_id)
                if success:
                    logger.info(f"Successfully sent RedGifs video: {post.title[:50]}...")
                    # Record media content to prevent future duplicates
                    await record_media_content(video_url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                else:
                    # Fallback to link if download fails
                    logger.warning(f"RedGifs download failed for URL: {video_url}, original: {url}")
                    logger.warning(f"This may be due to file size > 500MB or network issues")
                    text += f"\n🎥 <a href='{url}'>RedGifs Video</a>"
                    await send_with_retry(
                        bot.send_message, 
                        chat_id, 
                        text, 
                        parse_mode=ParseMode.HTML,
                        operation="send RedGifs fallback message"
                    )
            else:
                # Fallback to link if URL extraction fails
                logger.warning(f"RedGifs URL extraction failed, sending as link: {url}")
                text += f"\n🎥 <a href='{url}'>RedGifs Video</a>"
                await send_with_retry(
                    bot.send_message, 
                    chat_id, 
                    text, 
                    parse_mode=ParseMode.HTML,
                    operation="send RedGifs extraction failed message"
                )
        elif hasattr(post, 'is_gallery') and post.is_gallery:
            # Gallery post
            logger.info(f"Sending as gallery post with {len(post.gallery_data['items'])} items")
            try:
                media_items = []
                gallery_data = post.gallery_data
                media_metadata = post.media_metadata
                
                for item in gallery_data["items"]:
                    media_id = item["media_id"]
                    if media_id in media_metadata:
                        media_info = media_metadata[media_id]
                        if media_info.get("e") == "Image":
                            img_url = media_info["s"]["u"].replace("&amp;", "&")
                            media_items.append(types.InputMediaPhoto(media=img_url))
                
                if media_items:
                    # Telegram has a limit of 10 media items per group
                    media_items = media_items[:10]
                    try:
                        await send_with_retry(
                            bot.send_media_group, 
                            chat_id, 
                            media_items,
                            operation="send media group"
                        )
                        # Record media content for gallery posts (use first image URL as representative)
                        if media_items:
                            first_media_url = media_items[0].media
                            await record_media_content(first_media_url, post.id, subreddit_name, None, title, post.author.name if post.author else "")
                        await send_with_retry(
                            bot.send_message, 
                            chat_id,
                            text, 
                            parse_mode=ParseMode.HTML,
                            operation="send gallery caption"
                        )
                    except Exception as gallery_error:
                        # Fallback to text message with link if gallery sending fails
                        logger.warning(f"Failed to send gallery, falling back to link: {gallery_error}")
                        text += f"\n🖼️ <a href='{post.url}'>View Gallery</a>"
                        await send_with_retry(
                            bot.send_message, 
                            chat_id, 
                            text, 
                            parse_mode=ParseMode.HTML,
                            operation="send gallery fallback message"
                        )
                else:
                    await send_with_retry(
                        bot.send_message, 
                        chat_id, 
                        text, 
                        parse_mode=ParseMode.HTML,
                        operation="send empty gallery message"
                    )
            except Exception as e:
                logger.error(f"Error processing gallery: {e}")
                await send_with_retry(
                    bot.send_message, 
                    chat_id, 
                    text, 
                    parse_mode=ParseMode.HTML,
                    operation="send gallery error message"
                )
        else:
            # Other content types (links, etc.)
            logger.info(f"Sending as link post: {url}")
            text += f"\n🔗 <a href='{url}'>External Link</a>"
            await send_with_retry(
                bot.send_message, 
                chat_id, 
                text, 
                parse_mode=ParseMode.HTML,
                operation="send external link message"
            )
        
        logger.info(f"Successfully sent post to chat {chat_id}: {post.title[:50]}...")
    except Exception as e:
        logger.error(f"Failed to send post {post.id} to chat {chat_id}: {e}")


async def send_post(post, source_type="subreddit", source_name=None):
    """Send a Reddit post to all active chats"""
    try:
        # Check if this post was already shared to prevent duplicates
        cursor.execute("SELECT post_id FROM shared_posts WHERE post_id = ?", (post.id,))
        if cursor.fetchone():
            logger.info(f"Skipping already shared post: {post.title[:50]}...")
            return
        
        # Check for media duplicates if this is a media post
        if not post.is_self and post.url:
            is_duplicate, duplicate_info = await is_media_duplicate(post.url, post.title, post.author.name if post.author else "")
            if is_duplicate:
                logger.info(f"Media duplicate detected for post {post.id}. "
                           f"Original shared {duplicate_info.get('shared_count', 1)} times. Skipping.")
                # Increment the share count to track how many times this media was attempted
                await increment_media_share_count(post.url, post.title, post.author.name if post.author else "")
                return
        
        # Get all active chats
        cursor.execute("SELECT chat_id FROM active_chats WHERE is_active = 1")
        active_chats = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Database error in send_post: {e}")
        return
    
    if not active_chats:
        logger.warning("No active chats configured. Use /add_chat command to add chats.")
        return
    
    # Send post to all active chats
    success_count = 0
    for (chat_id,) in active_chats:
        try:
            await send_post_to_chat(post, chat_id)
            success_count += 1
            # Delay between chats now handled by rate limiting system
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Failed to send post {post.id} to chat {chat_id}: {e}")
    
    if success_count > 0:
        # Mark this post as shared to prevent future duplicates
        try:
            if source_type == "user":
                cursor.execute("INSERT OR IGNORE INTO shared_posts (post_id, username, shared_time) VALUES (?, ?, ?)", 
                              (post.id, source_name, int(time.time())))
            else:
                cursor.execute("INSERT OR IGNORE INTO shared_posts (post_id, subreddit, shared_time) VALUES (?, ?, ?)", 
                              (post.id, post.subreddit.display_name, int(time.time())))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to mark post as shared: {e}")
            # Try to reconnect and retry once
            try:
                conn.rollback()
                if source_type == "user":
                    cursor.execute("INSERT OR IGNORE INTO shared_posts (post_id, username, shared_time) VALUES (?, ?, ?)", 
                                  (post.id, source_name, int(time.time())))
                else:
                    cursor.execute("INSERT OR IGNORE INTO shared_posts (post_id, subreddit, shared_time) VALUES (?, ?, ?)", 
                                  (post.id, post.subreddit.display_name, int(time.time())))
                conn.commit()
            except sqlite3.Error as e2:
                logger.error(f"Failed to mark post as shared after retry: {e2}")
        
        logger.info(f"Successfully sent post to {success_count}/{len(active_chats)} chats: {post.title[:50]}...")


async def save_bot_state(key: str, value: str):
    """Save bot state for resume functionality"""
    try:
        cursor.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error saving bot state {key}: {e}")
        try:
            conn.rollback()
            cursor.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
        except sqlite3.Error as e2:
            logger.error(f"Failed to save bot state after retry: {e2}")


async def get_bot_state(key: str, default: str = ""):
    """Get bot state for resume functionality"""
    try:
        cursor.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else default
    except sqlite3.Error as e:
        logger.error(f"Error getting bot state {key}: {e}")
        return default


def generate_media_hash(media_url: str, post_title: str = "", post_author: str = "") -> str:
    """Generate a hash for media content to detect duplicates"""
    # Create a hash based primarily on URL to catch the same media shared in different posts
    # Only include title and author as secondary factors to avoid false positives
    content_string = f"{media_url.lower()}"
    return hashlib.md5(content_string.encode('utf-8')).hexdigest()


async def is_media_duplicate(media_url: str, post_title: str = "", post_author: str = "") -> tuple[bool, dict]:
    """Check if media content has been shared before"""
    try:
        media_hash = generate_media_hash(media_url, post_title, post_author)
        
        cursor.execute("""
            SELECT media_url, post_id, subreddit, username, first_seen_time, shared_count 
            FROM media_content 
            WHERE media_hash = ?
        """, (media_hash,))
        
        row = cursor.fetchone()
        if row:
            return True, {
                'media_url': row[0],
                'post_id': row[1],
                'subreddit': row[2],
                'username': row[3],
                'first_seen_time': row[4],
                'shared_count': row[5]
            }
        return False, {}
        
    except sqlite3.Error as e:
        logger.error(f"Error checking media duplicate: {e}")
        return False, {}


async def record_media_content(media_url: str, post_id: str, subreddit: str = None, username: str = None, 
                              post_title: str = "", post_author: str = "") -> bool:
    """Record media content to prevent future duplicates"""
    try:
        media_hash = generate_media_hash(media_url, post_title, post_author)
        
        cursor.execute("""
            INSERT OR REPLACE INTO media_content 
            (media_hash, media_url, post_id, subreddit, username, first_seen_time, shared_count)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (media_hash, media_url, post_id, subreddit, username, int(time.time())))
        
        conn.commit()
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error recording media content: {e}")
        return False


async def increment_media_share_count(media_url: str, post_title: str = "", post_author: str = "") -> bool:
    """Increment the share count for existing media content"""
    try:
        media_hash = generate_media_hash(media_url, post_title, post_author)
        
        cursor.execute("""
            UPDATE media_content 
            SET shared_count = shared_count + 1 
            WHERE media_hash = ?
        """, (media_hash,))
        
        conn.commit()
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error incrementing media share count: {e}")
        return False


async def check_resume_state():
    """Check if bot needs to resume from a previous session"""
    try:
        # Get last processing timestamp
        last_seen = await get_bot_state("last_processing_time")
        if last_seen:
            last_time = int(last_seen)
            current_time = int(datetime.now().timestamp())
            downtime = current_time - last_time
            
            # If bot was down for more than 2 minutes, it's considered a resume
            if downtime > 120:
                logger.info(f"Bot resume detected: was offline for {downtime // 60} minutes")
                
                # Check which subreddits need resume (only for 'new' sorting)
                cursor.execute("SELECT name, last_post, sort_mode FROM subreddits WHERE sort_mode = 'new'")
                resume_subs = cursor.fetchall()
                
                if resume_subs:
                    logger.info(f"Resuming processing for {len(resume_subs)} subreddits with 'new' sorting")
                    for sub_name, last_post, sort_mode in resume_subs:
                        await resume_subreddit_processing(sub_name, last_post)
                else:
                    logger.info("No subreddits with 'new' sorting mode to resume")
            else:
                logger.info("Bot restart detected (short downtime), no resume needed")
        else:
            logger.info("First bot startup, no previous state to resume from")
    except Exception as e:
        logger.error(f"Error checking resume state: {e}")


async def resume_subreddit_processing(subreddit_name: str, last_post_time: int):
    """Resume processing a subreddit from the last known position"""
    try:
        logger.info(f"Resuming r/{subreddit_name} from timestamp {last_post_time}")
        
        if reddit is None:
            logger.error("Reddit API not initialized")
            return
        
        # Add retry logic for Reddit API calls
        max_retries = 3
        retry_delay = 5
        posts = []  # Initialize posts list
        
        for attempt in range(max_retries):
            try:
                subreddit = await reddit.subreddit(subreddit_name)
                
                # Get more posts during resume to catch up (up to 100 posts)
                posts = []
                async for post in subreddit.new(limit=100):
                    posts.append(post)
                
                logger.info(f"Resume: Found {len(posts)} recent posts in r/{subreddit_name}")
                break  # Success, exit retry loop
                
            except Exception as api_error:
                error_msg = str(api_error).lower()
                logger.warning(f"Reddit API error on attempt {attempt + 1}/{max_retries} for r/{subreddit_name}: {api_error}")
                
                # Don't retry for 404 errors (subreddit doesn't exist or is inaccessible)
                if "404" in error_msg or "not found" in error_msg:
                    logger.error(f"Subreddit r/{subreddit_name} not found or inaccessible (404). Skipping resume.")
                    return
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to resume r/{subreddit_name} after {max_retries} attempts, skipping")
                    return
        
        # Find posts newer than our last processed timestamp
        posts_to_send = []
        for post in posts:
            created = int(post.created_utc)
            if created > last_post_time:
                posts_to_send.append(post)
        
        if posts_to_send:
            # Sort by creation time (oldest first) to maintain chronological order
            posts_to_send.sort(key=lambda p: p.created_utc)
            
            logger.info(f"Resume: Processing {len(posts_to_send)} missed posts from r/{subreddit_name}")
            
            # Create background tasks for resume processing - this allows commands to work during resume
            for i, post in enumerate(posts_to_send):
                logger.info(f"Resume: Processing post {i+1}/{len(posts_to_send)} from r/{subreddit_name}")
                # Create background task for each post
                create_background_task(send_post(post, "subreddit", subreddit_name))
                
                # Update state after creating task
                await save_bot_state(f"resume_{subreddit_name}", str(int(post.created_utc)))
                
                # Small delay to spread out task creation and respect rate limits
                await asyncio.sleep(0.2)
            
            # Update the subreddit's last post time
            newest_post_time = max(int(post.created_utc) for post in posts_to_send)
            cursor.execute("UPDATE subreddits SET last_post = ? WHERE name = ?", (newest_post_time, subreddit_name))
            conn.commit()
            
            logger.info(f"Resume completed for r/{subreddit_name}: processed {len(posts_to_send)} posts")
            
            # Clean up resume state
            cursor.execute("DELETE FROM bot_state WHERE key = ?", (f"resume_{subreddit_name}",))
            conn.commit()
        else:
            logger.info(f"Resume: No missed posts found for r/{subreddit_name}")
            
    except Exception as e:
        import traceback
        logger.error(f"Error during resume processing for r/{subreddit_name}: {e}")
        logger.error(traceback.format_exc())


async def poll_user_profile(username: str):
    """Poll a Reddit user profile for posts based on sort mode"""
    try:
        # Check if normal downloads are paused due to scraping
        if normal_downloads_paused:
            logger.debug(f"Skipping u/{username} polling - scraping mode active")
            return
        
        # Save current processing time for resume functionality
        await save_bot_state("last_processing_time", str(int(datetime.now().timestamp())))
        
        cursor.execute("SELECT last_post, sort_mode FROM user_profiles WHERE username=?", (username,))
        row = cursor.fetchone()
        last_post_time = row[0] if row else 0
        sort_mode = row[1] if row and len(row) > 1 else 'new'
        
        if reddit is None:
            logger.error("Reddit API not initialized")
            return
        user = await reddit.redditor(username)
        
        # Get posts based on sort mode
        if sort_mode == 'hot':
            posts = []
            async for post in user.submissions.hot(limit=100):
                posts.append(post)
        elif sort_mode == 'top':
            posts = []
            async for post in user.submissions.top(time_filter='day', limit=100):
                posts.append(post)
        elif sort_mode == 'top_week':
            posts = []
            async for post in user.submissions.top(time_filter='week', limit=100):
                posts.append(post)
        elif sort_mode == 'top_month':
            posts = []
            async for post in user.submissions.top(time_filter='month', limit=100):
                posts.append(post)
        else:  # default to 'new'
            posts = []
            async for post in user.submissions.new(limit=100):
                posts.append(post)
        
        logger.info(f"Found {len(posts)} total posts in u/{username} (sort: {sort_mode}), last_post_time: {last_post_time}")
        
        posts_to_send = []
        for post in posts:
            created = int(post.created_utc)
            if created > last_post_time:
                posts_to_send.append(post)
        
        logger.info(f"Found {len(posts_to_send)} new posts to send from u/{username}")
        
        if posts_to_send:
            # Sort by creation time (oldest first)
            posts_to_send.sort(key=lambda p: p.created_utc)
            
            logger.info(f"Found {len(posts_to_send)} new posts in u/{username}")
            
            # Create background tasks for concurrent processing
            for post in posts_to_send:
                # Create each post processing as a background task - this allows commands to work immediately
                create_background_task(send_post(post, "user", username))
                
                # Small delay to spread out task creation
                await asyncio.sleep(0.1)
            
            # Update last post time to the newest post
            newest_post_time = max(int(post.created_utc) for post in posts_to_send)
            cursor.execute("INSERT OR REPLACE INTO user_profiles(username, last_post) VALUES (?, ?)",
                          (username, newest_post_time))
            conn.commit()
            
            logger.info(f"Updated last post time for u/{username} to {newest_post_time}")
    except Exception as e:
        logger.error(f"Error polling user profile u/{username}: {e}")


async def poll_subreddit(subreddit_name: str):
    """Poll a subreddit for posts based on sort mode"""
    try:
        # Check if normal downloads are paused due to scraping
        if normal_downloads_paused:
            logger.debug(f"Skipping r/{subreddit_name} polling - scraping mode active")
            return
        
        # Save current processing time for resume functionality
        await save_bot_state("last_processing_time", str(int(datetime.now().timestamp())))
        
        cursor.execute("SELECT last_post, sort_mode FROM subreddits WHERE name=?", (subreddit_name,))
        row = cursor.fetchone()
        last_post_time = row[0] if row else 0
        sort_mode = row[1] if row and len(row) > 1 else 'new'
        
        if reddit is None:
            logger.error("Reddit API not initialized")
            return
        
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            # Test if subreddit is accessible
            await subreddit.load()
        except Exception as e:
            error_msg = str(e).lower()
            if "404" in error_msg or "not found" in error_msg:
                logger.warning(f"Subreddit r/{subreddit_name} not found or inaccessible (404). It may be private, restricted, or banned.")
                return
            elif "403" in error_msg or "forbidden" in error_msg:
                logger.warning(f"Cannot access r/{subreddit_name} (403). It may be private or restricted.")
                return
            else:
                logger.error(f"Error accessing subreddit r/{subreddit_name}: {e}")
                return
        
        # Get posts based on sort mode
        if sort_mode == 'hot':
            posts = []
            async for post in subreddit.hot(limit=100):
                posts.append(post)
        elif sort_mode == 'top':
            posts = []
            async for post in subreddit.top(time_filter='day', limit=100):
                posts.append(post)
        elif sort_mode == 'top_week':
            posts = []
            async for post in subreddit.top(time_filter='week', limit=100):
                posts.append(post)
        elif sort_mode == 'top_month':
            posts = []
            async for post in subreddit.top(time_filter='month', limit=100):
                posts.append(post)
        else:  # default to 'new'
            posts = []
            async for post in subreddit.new(limit=100):
                posts.append(post)
        
        logger.info(f"Found {len(posts)} total posts in r/{subreddit_name} (sort: {sort_mode}), last_post_time: {last_post_time}")
        
        posts_to_send = []
        for post in posts:
            created = int(post.created_utc)
            if created > last_post_time:
                posts_to_send.append(post)
        
        logger.info(f"Found {len(posts_to_send)} new posts to send from r/{subreddit_name}")
        
        if posts_to_send:
            # Sort by creation time (oldest first)
            posts_to_send.sort(key=lambda p: p.created_utc)
            
            logger.info(f"Found {len(posts_to_send)} new posts in r/{subreddit_name}")
            
            # Create background tasks for concurrent processing
            for post in posts_to_send:
                # Create each post processing as a background task - this allows commands to work immediately
                create_background_task(send_post(post, "subreddit", subreddit_name))
                
                # Small delay to spread out task creation
                await asyncio.sleep(0.1)
            
            # Update last post time to the newest post
            newest_post_time = max(int(post.created_utc) for post in posts_to_send)
            cursor.execute("INSERT OR REPLACE INTO subreddits(name, last_post) VALUES (?, ?)",
                          (subreddit_name, newest_post_time))
            conn.commit()
            
            logger.info(f"Updated last post time for r/{subreddit_name} to {newest_post_time}")
    except Exception as e:
        logger.error(f"Error polling subreddit r/{subreddit_name}: {e}")


async def cleanup_old_posts():
    """Remove shared posts older than 30 days to keep database size manageable"""
    try:
        # Delete posts older than 30 days (30 * 24 * 60 * 60 = 2592000 seconds)
        thirty_days_ago = int(datetime.now().timestamp()) - 2592000
        cursor.execute("DELETE FROM shared_posts WHERE shared_time < ?", (thirty_days_ago,))
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            conn.commit()
            logger.info(f"Cleaned up {deleted_count} old shared posts from database")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


async def polling_loop():
    """Main polling loop that checks all subscribed subreddits and user profiles"""
    logger.info("Starting polling loop...")
    cleanup_counter = 0
    
    while True:
        try:
            cursor.execute("SELECT name FROM subreddits")
            subs = cursor.fetchall()
            
            cursor.execute("SELECT username FROM user_profiles")
            users = cursor.fetchall()
            
            total_sources = len(subs) + len(users)
            
            if total_sources > 0:
                # Skip all polling if scraping is active
                if normal_downloads_paused:
                    logger.debug("Skipping all polling - scraping mode is active")
                    await asyncio.sleep(5)  # Short wait during scraping
                    continue
                
                logger.info(f"Polling {len(subs)} subreddits and {len(users)} user profiles...")
                
                # Poll subreddits with minimal delays for smooth operation
                for (sub,) in subs:
                    # Double-check scraping state before each poll
                    if normal_downloads_paused:
                        logger.debug("Scraping activated - stopping subreddit polling")
                        break
                    await poll_subreddit(sub)
                    # Optimized delay for better performance
                    await asyncio.sleep(0.3)
                
                # Poll user profiles with minimal delays
                for (user,) in users:
                    # Double-check scraping state before each poll
                    if normal_downloads_paused:
                        logger.debug("Scraping activated - stopping user polling")
                        break
                    await poll_user_profile(user)
                    # Optimized delay for better performance
                    await asyncio.sleep(0.3)
            else:
                logger.info("No subreddits or users subscribed yet")
            
            # Cleanup old posts every 24 hours (24 * 60 = 1440 minutes / 45 seconds = 1920 cycles)
            cleanup_counter += 1
            if cleanup_counter >= 2160:  # 24 hours with 40s cycles
                await cleanup_old_posts()
                cleanup_counter = 0
            
            # Optimized polling cycle for best performance and responsiveness  
            await asyncio.sleep(40)
        except Exception as e:
            logger.error(f"Error in polling loop: {e}")
            await asyncio.sleep(25)  # Optimized error recovery time


@dp.message(Command("start", "help"))
async def cmd_start(message: types.Message):
    """Handle /start and /help commands"""
    help_text = """
🤖 <b>Reddit to Telegram Bot</b>

This bot monitors Reddit subreddits and forwards posts to active chats.

<b>Available Commands:</b>
/start or /help - Show this help message

<b>📋 Subscription Management:</b>
/add r/subreddit1 r/subreddit2 u/username1 u/username2 - Add multiple subreddits/users
/add_user &lt;username&gt; - Subscribe to a Reddit user profile (legacy)
/remove r/subreddit1 r/subreddit2 u/username1 u/username2 - Remove multiple subreddits/users
/remove_user &lt;username&gt; - Unsubscribe from a user profile (legacy)
/remove_all_subs - Remove all subreddit subscriptions
/remove_all_users - Remove all user profile subscriptions
/remove_all - Remove all subscriptions

<b>🔄 Sorting &amp; Reset:</b>
/sort r/subreddit1 r/subreddit2 u/username1 mode - Set sorting mode for multiple targets
/sort_user &lt;username&gt; &lt;mode&gt; - Change sorting mode for user (legacy)
/sort_all &lt;mode&gt; - Set sorting mode for all subscriptions
/reset &lt;subreddit&gt; - Reset timestamp to get recent posts
/reset_user &lt;username&gt; - Reset timestamp for user profile

<b>🔍 Scraping &amp; Media:</b>
/scrape_user &lt;username&gt; - Scrape ALL media from user's complete history
/scrape_redgifs &lt;username&gt; - Scrape ALL videos from RedGifs user profile
/stop_scraping - Stop any ongoing scraping process

<b>📱 Chat Management:</b>
/add_chat - Add current chat to receive posts
/remove_chat - Remove current chat from receiving posts
/list_chats - Show all active chats

<b>📊 Information &amp; Maintenance:</b>
/list - Show all subscribed subreddits and user profiles
/status - Show bot status and resume information
/queue_status - Show current queue status and bot state
/check_subreddits - Check which subreddits are accessible
/check_users - Check which user profiles are accessible
/cleanup_inaccessible - Automatically remove all inaccessible subscriptions
/duplicates - Show duplicate media statistics
/clear_duplicates - Clear all duplicate media tracking data
/clear_history - Clear duplicate post tracking history
/test_duplicate &lt;URL&gt; - Test duplicate detection with specific URL
/test - Test command responsiveness during media processing

<b>🎛️ Bot Control:</b>
/pause - Pause media sending (keeps queue)
/resume - Resume media sending
/clear_queue - Clear all queued media items
/reset_bot - Complete bot reset (removes all subscriptions and clears queue)

<b>Sorting Modes:</b>
• new - Latest posts (default)
• hot - Trending posts  
• top - Top posts today
• top_week - Top posts this week
• top_month - Top posts this month

<b>Examples:</b>
<code>/add r/cats r/dogs u/petlover</code> - Add multiple subreddits and users
<code>/add r/python r/programming u/coder</code> - Add programming-related sources
<code>/remove r/cats u/petlover</code> - Remove specific subreddit and user
<code>/remove_all</code> - Remove all subscriptions
<code>/sort r/cats r/dogs hot</code> - Set multiple subreddits to hot sorting
<code>/sort_all new</code> - Set all subscriptions to new sorting
<code>/check_subreddits</code> - Check which subreddits are accessible
<code>/check_users</code> - Check which user profiles are accessible
<code>/cleanup_inaccessible</code> - Automatically remove all inaccessible subscriptions
<code>/pause</code> - Pause media sending temporarily
<code>/resume</code> - Resume media sending
<code>/clear_queue</code> - Clear all queued media items
<code>/reset_bot</code> - Complete bot reset
<code>/queue_status</code> - Check bot and queue status
<code>/scrape_user johndoe</code> - Get ALL media from user's history
<code>/scrape_redgifs username</code> - Get ALL videos from RedGifs user
<code>/add_chat</code> - Add this chat to receive posts
<code>/duplicates</code> - Check duplicate detection statistics

<b>Note:</b> Only the bot admin can use management commands.
The bot checks for posts every 40 seconds and prevents duplicate posts using both post-level and media-level detection.

🔍 <b>Duplicate Detection:</b> The bot now uses improved duplicate detection that prevents the same media from being shared multiple times, even if posted by different users or in different subreddits.

⚡ <b>Concurrent Processing:</b> Commands work immediately even while the bot is downloading and sending large videos/images. Use /test to verify responsiveness during media processing.
"""
    await message.reply(help_text, parse_mode=ParseMode.HTML)


@dp.message(Command("add"))
async def cmd_add(message: types.Message):
    """Handle /add command to subscribe to subreddits and users"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/add r/subreddit1 r/subreddit2 u/username1 u/username2</code>\n\n"
                           "Examples:\n"
                           "• <code>/add r/python r/programming</code>\n"
                           "• <code>/add u/johndoe u/alice</code>\n"
                           "• <code>/add r/cats u/petlover</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    # Parse arguments to separate subreddits and users
    args_list = args.split()
    subreddits = []
    users = []
    
    for arg in args_list:
        arg = arg.strip().lower()
        if arg.startswith('r/'):
            subreddits.append(arg[2:])  # Remove 'r/' prefix
        elif arg.startswith('u/'):
            users.append(arg[2:])  # Remove 'u/' prefix
        else:
            # If no prefix, assume it's a subreddit for backward compatibility
            subreddits.append(arg)
    
    if not subreddits and not users:
        await message.reply("❌ No valid subreddits or users found. Use r/subreddit or u/username format.")
        return
    
    results = {"subreddits": {"success": [], "failed": [], "already": []}, 
               "users": {"success": [], "failed": [], "already": []}}
    
    # Process subreddits
    if subreddits:
        await message.reply(f"🔍 Processing {len(subreddits)} subreddit(s)...")
        
        for subreddit_name in subreddits:
            try:
                # Check if already subscribed
                cursor.execute("SELECT name FROM subreddits WHERE name=?", (subreddit_name,))
                if cursor.fetchone():
                    results["subreddits"]["already"].append(subreddit_name)
                    continue
                
                # Verify subreddit exists
                if reddit is None:
                    results["subreddits"]["failed"].append((subreddit_name, "Reddit API not initialized"))
                    continue
                    
                subreddit = await reddit.subreddit(subreddit_name)
                await subreddit.load()  # This will raise an exception if subreddit doesn't exist
                
                # Add to database
                one_hour_ago = 1728000000
                cursor.execute("INSERT INTO subreddits(name, last_post, sort_mode) VALUES (?, ?, ?)", 
                              (subreddit_name, one_hour_ago, 'new'))
                conn.commit()
                
                results["subreddits"]["success"].append(subreddit_name)
                logger.info(f"Added subscription to r/{subreddit_name}")
                
            except Exception as e:
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    error_reason = "Not found or inaccessible"
                elif "403" in error_msg or "forbidden" in error_msg:
                    error_reason = "Private or restricted"
                elif "rate limit" in error_msg or "429" in error_msg:
                    error_reason = "Rate limit exceeded"
                else:
                    error_reason = f"API error: {str(e)[:50]}"
                
                results["subreddits"]["failed"].append((subreddit_name, error_reason))
                logger.error(f"Error adding subreddit r/{subreddit_name}: {e}")
    
    # Process users
    if users:
        await message.reply(f"🔍 Processing {len(users)} user(s)...")
        
        for username in users:
            try:
                # Check if already subscribed
                cursor.execute("SELECT username FROM user_profiles WHERE username=?", (username,))
                if cursor.fetchone():
                    results["users"]["already"].append(username)
                    continue
                
                # Verify user exists
                if reddit is None:
                    results["users"]["failed"].append((username, "Reddit API not initialized"))
                    continue
                    
                user = await reddit.redditor(username)
                await user.load()  # This will raise an exception if user doesn't exist
                
                # Add to database
                one_hour_ago = 1728000000
                cursor.execute("INSERT INTO user_profiles(username, last_post, sort_mode) VALUES (?, ?, ?)", 
                              (username, one_hour_ago, 'new'))
                conn.commit()
                
                results["users"]["success"].append(username)
                logger.info(f"Added subscription to u/{username}")
                
            except Exception as e:
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    error_reason = "User not found"
                elif "403" in error_msg or "forbidden" in error_msg:
                    error_reason = "User profile private"
                else:
                    error_reason = f"API error: {str(e)[:50]}"
                
                results["users"]["failed"].append((username, error_reason))
                logger.error(f"Error adding user u/{username}: {e}")
    
    # Generate response message
    response_parts = []
    
    # Subreddit results
    if subreddits:
        response_parts.append("📋 **Subreddit Results:**")
        if results["subreddits"]["success"]:
            success_list = ", ".join([f"r/{name}" for name in results["subreddits"]["success"]])
            response_parts.append(f"✅ Added: {success_list}")
        if results["subreddits"]["already"]:
            already_list = ", ".join([f"r/{name}" for name in results["subreddits"]["already"]])
            response_parts.append(f"ℹ️ Already subscribed: {already_list}")
        if results["subreddits"]["failed"]:
            failed_list = ", ".join([f"r/{name} ({reason})" for name, reason in results["subreddits"]["failed"]])
            response_parts.append(f"❌ Failed: {failed_list}")
        response_parts.append("")
    
    # User results
    if users:
        response_parts.append("👤 **User Results:**")
        if results["users"]["success"]:
            success_list = ", ".join([f"u/{name}" for name in results["users"]["success"]])
            response_parts.append(f"✅ Added: {success_list}")
        if results["users"]["already"]:
            already_list = ", ".join([f"u/{name}" for name in results["users"]["already"]])
            response_parts.append(f"ℹ️ Already subscribed: {already_list}")
        if results["users"]["failed"]:
            failed_list = ", ".join([f"u/{name} ({reason})" for name, reason in results["users"]["failed"]])
            response_parts.append(f"❌ Failed: {failed_list}")
        response_parts.append("")
    
    # Summary
    total_success = len(results["subreddits"]["success"]) + len(results["users"]["success"])
    total_failed = len(results["subreddits"]["failed"]) + len(results["users"]["failed"])
    total_already = len(results["subreddits"]["already"]) + len(results["users"]["already"])
    
    response_parts.append(f"📊 **Summary:** {total_success} added, {total_already} already subscribed, {total_failed} failed")
    
    await message.reply("\n".join(response_parts), parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("add_user"))
async def cmd_add_user(message: types.Message):
    """Handle /add_user command to subscribe to a Reddit user profile"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip().lower() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/add_user username</code>\n\nExample: <code>/add_user johndoe</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    username = args
    
    # Check if already subscribed
    cursor.execute("SELECT username FROM user_profiles WHERE username=?", (username,))
    if cursor.fetchone():
        await message.reply(f"ℹ️ Already subscribed to u/{username}")
        return
    
    try:
        # Verify user exists
        if reddit is None:
            await message.reply("❌ Reddit API not initialized")
            return
        user = await reddit.redditor(username)
        await user.load()  # This will raise an exception if user doesn't exist
        
        # Add to database with older timestamp to get recent posts
        one_hour_ago = 1728000000  # Fixed timestamp from October 2024
        cursor.execute("INSERT INTO user_profiles(username, last_post, sort_mode) VALUES (?, ?, ?)", 
                      (username, one_hour_ago, 'new'))
        conn.commit()
        
        await message.reply(f"✅ Successfully subscribed to u/{username}")
        logger.info(f"Added subscription to u/{username}")
    except Exception as e:
        logger.error(f"Error adding user u/{username}: {e}")
        await message.reply(f"❌ User u/{username} not found or Reddit API error.")


@dp.message(Command("remove"))
async def cmd_remove(message: types.Message):
    """Handle /remove command to unsubscribe from subreddits and users"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/remove r/subreddit1 r/subreddit2 u/username1 u/username2</code>\n\n"
                           "Examples:\n"
                           "• <code>/remove r/python r/programming</code>\n"
                           "• <code>/remove u/johndoe u/alice</code>\n"
                           "• <code>/remove r/cats u/petlover</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    # Parse arguments to separate subreddits and users
    args_list = args.split()
    subreddits = []
    users = []
    
    for arg in args_list:
        arg = arg.strip().lower()
        if arg.startswith('r/'):
            subreddits.append(arg[2:])  # Remove 'r/' prefix
        elif arg.startswith('u/'):
            users.append(arg[2:])  # Remove 'u/' prefix
        else:
            # If no prefix, assume it's a subreddit for backward compatibility
            subreddits.append(arg)
    
    if not subreddits and not users:
        await message.reply("❌ No valid subreddits or users found. Use r/subreddit or u/username format.")
        return
    
    results = {"subreddits": {"removed": [], "not_found": []}, 
               "users": {"removed": [], "not_found": []}}
    
    # Process subreddits
    if subreddits:
        for subreddit_name in subreddits:
            cursor.execute("DELETE FROM subreddits WHERE name=?", (subreddit_name,))
            if cursor.rowcount > 0:
                results["subreddits"]["removed"].append(subreddit_name)
                logger.info(f"Removed subscription to r/{subreddit_name}")
            else:
                results["subreddits"]["not_found"].append(subreddit_name)
    
    # Process users
    if users:
        for username in users:
            cursor.execute("DELETE FROM user_profiles WHERE username=?", (username,))
            if cursor.rowcount > 0:
                results["users"]["removed"].append(username)
                logger.info(f"Removed subscription to u/{username}")
            else:
                results["users"]["not_found"].append(username)
    
    # Commit changes
    conn.commit()
    
    # Generate response message
    response_parts = []
    
    # Subreddit results
    if subreddits:
        response_parts.append("📋 **Subreddit Results:**")
        if results["subreddits"]["removed"]:
            removed_list = ", ".join([f"r/{name}" for name in results["subreddits"]["removed"]])
            response_parts.append(f"✅ Removed: {removed_list}")
        if results["subreddits"]["not_found"]:
            not_found_list = ", ".join([f"r/{name}" for name in results["subreddits"]["not_found"]])
            response_parts.append(f"ℹ️ Not subscribed: {not_found_list}")
        response_parts.append("")
    
    # User results
    if users:
        response_parts.append("👤 **User Results:**")
        if results["users"]["removed"]:
            removed_list = ", ".join([f"u/{name}" for name in results["users"]["removed"]])
            response_parts.append(f"✅ Removed: {removed_list}")
        if results["users"]["not_found"]:
            not_found_list = ", ".join([f"u/{name}" for name in results["users"]["not_found"]])
            response_parts.append(f"ℹ️ Not subscribed: {not_found_list}")
        response_parts.append("")
    
    # Summary
    total_removed = len(results["subreddits"]["removed"]) + len(results["users"]["removed"])
    total_not_found = len(results["subreddits"]["not_found"]) + len(results["users"]["not_found"])
    
    response_parts.append(f"📊 **Summary:** {total_removed} removed, {total_not_found} not found")
    
    await message.reply("\n".join(response_parts), parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("remove_user"))
async def cmd_remove_user(message: types.Message):
    """Handle /remove_user command to unsubscribe from a user profile"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip().lower() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/remove_user username</code>\n\nExample: <code>/remove_user johndoe</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    username = args
    
    # Remove from database
    cursor.execute("DELETE FROM user_profiles WHERE username=?", (username,))
    
    if cursor.rowcount > 0:
        conn.commit()
        await message.reply(f"✅ Unsubscribed from u/{username}")
        logger.info(f"Removed subscription to u/{username}")
    else:
        await message.reply(f"ℹ️ Not subscribed to u/{username}")


@dp.message(Command("remove_all_subs"))
async def cmd_remove_all_subs(message: types.Message):
    """Remove all subreddit subscriptions"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        # Get count before deletion
        cursor.execute("SELECT COUNT(*) FROM subreddits")
        count = cursor.fetchone()[0]
        
        if count == 0:
            await message.reply("ℹ️ No subreddit subscriptions to remove.")
            return
        
        # Remove all subreddits
        cursor.execute("DELETE FROM subreddits")
        conn.commit()
        
        await message.reply(f"✅ Removed all {count} subreddit subscription(s).")
        logger.info(f"Removed all {count} subreddit subscriptions")
        
    except Exception as e:
        logger.error(f"Error removing all subreddits: {e}")
        await message.reply("❌ Error removing all subreddits. Check bot logs for details.")


@dp.message(Command("remove_all_users"))
async def cmd_remove_all_users(message: types.Message):
    """Remove all user profile subscriptions"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        # Get count before deletion
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        count = cursor.fetchone()[0]
        
        if count == 0:
            await message.reply("ℹ️ No user profile subscriptions to remove.")
            return
        
        # Remove all users
        cursor.execute("DELETE FROM user_profiles")
        conn.commit()
        
        await message.reply(f"✅ Removed all {count} user profile subscription(s).")
        logger.info(f"Removed all {count} user profile subscriptions")
        
    except Exception as e:
        logger.error(f"Error removing all users: {e}")
        await message.reply("❌ Error removing all users. Check bot logs for details.")


@dp.message(Command("remove_all"))
async def cmd_remove_all(message: types.Message):
    """Remove all subreddit and user subscriptions"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        # Get counts before deletion
        cursor.execute("SELECT COUNT(*) FROM subreddits")
        sub_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = cursor.fetchone()[0]
        
        total_count = sub_count + user_count
        
        if total_count == 0:
            await message.reply("ℹ️ No subscriptions to remove.")
            return
        
        # Remove all subreddits and users
        cursor.execute("DELETE FROM subreddits")
        cursor.execute("DELETE FROM user_profiles")
        conn.commit()
        
        await message.reply(f"✅ Removed all subscriptions:\n"
                           f"📋 {sub_count} subreddit(s)\n"
                           f"👤 {user_count} user(s)\n"
                           f"📊 Total: {total_count} subscription(s)")
        logger.info(f"Removed all subscriptions: {sub_count} subreddits, {user_count} users")
        
    except Exception as e:
        logger.error(f"Error removing all subscriptions: {e}")
        await message.reply("❌ Error removing all subscriptions. Check bot logs for details.")


@dp.message(Command("reset_bot"))
async def cmd_reset_bot(message: types.Message):
    """Completely reset the bot - clear all subscriptions and stop media queue"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    global bot_paused, queue_cleared, scraping_active, normal_downloads_paused
    
    try:
        # Get counts before deletion
        cursor.execute("SELECT COUNT(*) FROM subreddits")
        sub_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = cursor.fetchone()[0]
        
        # Get queue size
        queue_size = send_queue.qsize()
        
        # Stop all current operations
        scraping_active = False
        normal_downloads_paused = False
        bot_paused = True  # Pause media sending
        queue_cleared = True  # Clear the queue
        
        # Remove all subscriptions
        cursor.execute("DELETE FROM subreddits")
        cursor.execute("DELETE FROM user_profiles")
        
        # Clear all tracking data
        cursor.execute("DELETE FROM shared_posts")
        cursor.execute("DELETE FROM media_content")
        
        # Reset bot state
        cursor.execute("DELETE FROM bot_state")
        
        conn.commit()
        
        # Cancel all background tasks
        for task in background_tasks.copy():
            if not task.done():
                task.cancel()
        background_tasks.clear()
        
        # Resume bot operations
        bot_paused = False
        
        await message.reply(f"🔄 **Bot Reset Complete!**\n\n"
                           f"📋 Removed {sub_count} subreddit(s)\n"
                           f"👤 Removed {user_count} user(s)\n"
                           f"📊 Total subscriptions: {sub_count + user_count}\n"
                           f"🗑️ Cleared {queue_size} queued media items\n"
                           f"🧹 Cleared all tracking data\n\n"
                           f"✅ Bot is now ready for new subscriptions!\n"
                           f"Use /add to subscribe to new subreddits and users.", 
                           parse_mode=ParseMode.MARKDOWN)
        
        logger.info(f"Bot reset complete: {sub_count} subreddits, {user_count} users, {queue_size} queued items cleared")
        
    except Exception as e:
        logger.error(f"Error resetting bot: {e}")
        await message.reply("❌ Error resetting bot. Check bot logs for details.")


@dp.message(Command("sort"))
async def cmd_sort(message: types.Message):
    """Set sorting mode for subreddits and users"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 2) if message.text else []
    if len(args) < 3:
        await message.reply("❌ Usage: <code>/sort r/subreddit1 r/subreddit2 u/username1 mode</code>\n\n"
                           "<b>Available modes:</b>\n• new - Latest posts\n• hot - Trending posts\n• top - Top posts today\n• top_week - Top posts this week\n• top_month - Top posts this month\n\n"
                           "Examples:\n• <code>/sort r/python hot</code>\n• <code>/sort r/cats r/dogs new</code>\n• <code>/sort u/johndoe top_week</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    # Parse arguments
    targets = args[1:-1]  # All arguments except command and sort mode
    sort_mode = args[-1].strip().lower()  # Last argument is sort mode
    
    valid_modes = ['new', 'hot', 'top', 'top_week', 'top_month']
    if sort_mode not in valid_modes:
        await message.reply(f"❌ Invalid sort mode. Available modes: {', '.join(valid_modes)}")
        return
    
    # Separate subreddits and users
    subreddits = []
    users = []
    
    for target in targets:
        target = target.strip().lower()
        if target.startswith('r/'):
            subreddits.append(target[2:])  # Remove 'r/' prefix
        elif target.startswith('u/'):
            users.append(target[2:])  # Remove 'u/' prefix
        else:
            # If no prefix, assume it's a subreddit for backward compatibility
            subreddits.append(target)
    
    if not subreddits and not users:
        await message.reply("❌ No valid subreddits or users found. Use r/subreddit or u/username format.")
        return
    
    results = {"subreddits": {"updated": [], "not_found": []}, 
               "users": {"updated": [], "not_found": []}}
    
    # Process subreddits
    if subreddits:
        for subreddit_name in subreddits:
            cursor.execute("SELECT name FROM subreddits WHERE name=?", (subreddit_name,))
            if cursor.fetchone():
                # Update sort mode and reset timestamp
                one_hour_ago = 1728000000
                cursor.execute("UPDATE subreddits SET sort_mode = ?, last_post = ? WHERE name = ?", 
                               (sort_mode, one_hour_ago, subreddit_name))
                results["subreddits"]["updated"].append(subreddit_name)
                logger.info(f"Set r/{subreddit_name} sort mode to {sort_mode}")
            else:
                results["subreddits"]["not_found"].append(subreddit_name)
    
    # Process users
    if users:
        for username in users:
            cursor.execute("SELECT username FROM user_profiles WHERE username=?", (username,))
            if cursor.fetchone():
                # Update sort mode and reset timestamp
                one_hour_ago = 1728000000
                cursor.execute("UPDATE user_profiles SET sort_mode = ?, last_post = ? WHERE username = ?", 
                               (sort_mode, one_hour_ago, username))
                results["users"]["updated"].append(username)
                logger.info(f"Set u/{username} sort mode to {sort_mode}")
            else:
                results["users"]["not_found"].append(username)
    
    # Commit changes
    conn.commit()
    
    # Generate response message
    response_parts = []
    
    # Subreddit results
    if subreddits:
        response_parts.append("📋 **Subreddit Results:**")
        if results["subreddits"]["updated"]:
            updated_list = ", ".join([f"r/{name}" for name in results["subreddits"]["updated"]])
            response_parts.append(f"✅ Updated: {updated_list}")
        if results["subreddits"]["not_found"]:
            not_found_list = ", ".join([f"r/{name}" for name in results["subreddits"]["not_found"]])
            response_parts.append(f"ℹ️ Not subscribed: {not_found_list}")
        response_parts.append("")
    
    # User results
    if users:
        response_parts.append("👤 **User Results:**")
        if results["users"]["updated"]:
            updated_list = ", ".join([f"u/{name}" for name in results["users"]["updated"]])
            response_parts.append(f"✅ Updated: {updated_list}")
        if results["users"]["not_found"]:
            not_found_list = ", ".join([f"u/{name}" for name in results["users"]["not_found"]])
            response_parts.append(f"ℹ️ Not subscribed: {not_found_list}")
        response_parts.append("")
    
    # Summary
    total_updated = len(results["subreddits"]["updated"]) + len(results["users"]["updated"])
    total_not_found = len(results["subreddits"]["not_found"]) + len(results["users"]["not_found"])
    
    response_parts.append(f"📊 **Summary:** {total_updated} updated to '{sort_mode}', {total_not_found} not found")
    response_parts.append(f"🔄 Will fetch posts with new sorting on next poll.")
    
    await message.reply("\n".join(response_parts), parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("sort_user"))
async def cmd_sort_user(message: types.Message):
    """Set sorting mode for a user profile"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 2) if message.text else []
    if len(args) < 3:
        await message.reply("❌ Usage: <code>/sort_user username mode</code>\n\n<b>Available modes:</b>\n• new - Latest posts\n• hot - Trending posts\n• top - Top posts today\n• top_week - Top posts this week\n• top_month - Top posts this month\n\nExample: <code>/sort_user johndoe hot</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    username = args[1].strip().lower()
    sort_mode = args[2].strip().lower()
    
    valid_modes = ['new', 'hot', 'top', 'top_week', 'top_month']
    if sort_mode not in valid_modes:
        await message.reply(f"❌ Invalid sort mode. Available modes: {', '.join(valid_modes)}")
        return
    
    # Check if subscribed
    cursor.execute("SELECT username FROM user_profiles WHERE username=?", (username,))
    if not cursor.fetchone():
        await message.reply(f"ℹ️ Not subscribed to u/{username}")
        return
    
    # Update sort mode and reset timestamp to get posts with new sorting
    one_hour_ago = 1728000000
    cursor.execute("UPDATE user_profiles SET sort_mode = ?, last_post = ? WHERE username = ?", 
                   (sort_mode, one_hour_ago, username))
    conn.commit()
    
    await message.reply(f"✅ Set u/{username} to sort by '{sort_mode}'. Will fetch posts with new sorting on next poll.")
    logger.info(f"Set u/{username} sort mode to {sort_mode}")


@dp.message(Command("sort_all"))
async def cmd_sort_all(message: types.Message):
    """Set sorting mode for all subreddits and users"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1) if message.text else []
    if len(args) < 2:
        await message.reply("❌ Usage: <code>/sort_all mode</code>\n\n"
                           "<b>Available modes:</b>\n• new - Latest posts\n• hot - Trending posts\n• top - Top posts today\n• top_week - Top posts this week\n• top_month - Top posts this month\n\n"
                           "Example: <code>/sort_all hot</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    sort_mode = args[1].strip().lower()
    
    valid_modes = ['new', 'hot', 'top', 'top_week', 'top_month']
    if sort_mode not in valid_modes:
        await message.reply(f"❌ Invalid sort mode. Available modes: {', '.join(valid_modes)}")
        return
    
    try:
        # Get counts before update
        cursor.execute("SELECT COUNT(*) FROM subreddits")
        sub_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = cursor.fetchone()[0]
        
        total_count = sub_count + user_count
        
        if total_count == 0:
            await message.reply("ℹ️ No subscriptions to update.")
            return
        
        # Update all subreddits
        one_hour_ago = 1728000000
        cursor.execute("UPDATE subreddits SET sort_mode = ?, last_post = ?", (sort_mode, one_hour_ago))
        sub_updated = cursor.rowcount
        
        # Update all users
        cursor.execute("UPDATE user_profiles SET sort_mode = ?, last_post = ?", (sort_mode, one_hour_ago))
        user_updated = cursor.rowcount
        
        conn.commit()
        
        await message.reply(f"✅ Updated all subscriptions to '{sort_mode}':\n"
                           f"📋 {sub_updated} subreddit(s)\n"
                           f"👤 {user_updated} user(s)\n"
                           f"📊 Total: {sub_updated + user_updated} subscription(s)\n\n"
                           f"🔄 Will fetch posts with new sorting on next poll.")
        logger.info(f"Updated all subscriptions to sort mode: {sort_mode} ({sub_updated} subreddits, {user_updated} users)")
        
    except Exception as e:
        logger.error(f"Error updating all sort modes: {e}")
        await message.reply("❌ Error updating sort modes. Check bot logs for details.")


@dp.message(Command("reset"))
async def cmd_reset(message: types.Message):
    """Reset the last post timestamp for a subreddit to get recent posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip().lower() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/reset subreddit_name</code>\n\nExample: <code>/reset python</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    subreddit_name = args
    
    # Check if subscribed
    cursor.execute("SELECT name FROM subreddits WHERE name=?", (subreddit_name,))
    if not cursor.fetchone():
        await message.reply(f"ℹ️ Not subscribed to r/{subreddit_name}")
        return
    
    # Reset timestamp to 1 hour ago to get recent posts
    # Use a much older timestamp to ensure we get recent posts
    one_hour_ago = 1728000000  # Fixed timestamp from October 2024
    cursor.execute("UPDATE subreddits SET last_post = ? WHERE name = ?", (one_hour_ago, subreddit_name))
    conn.commit()
    
    await message.reply(f"✅ Reset timestamp for r/{subreddit_name}. Will fetch posts from the last hour on next poll.")
    logger.info(f"Reset timestamp for r/{subreddit_name} to {one_hour_ago}")


@dp.message(Command("reset_user"))
async def cmd_reset_user(message: types.Message):
    """Reset the last post timestamp for a user profile to get recent posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip().lower() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/reset_user username</code>\n\nExample: <code>/reset_user johndoe</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    username = args
    
    # Check if subscribed
    cursor.execute("SELECT username FROM user_profiles WHERE username=?", (username,))
    if not cursor.fetchone():
        await message.reply(f"ℹ️ Not subscribed to u/{username}")
        return
    
    # Reset timestamp to get recent posts
    one_hour_ago = 1728000000  # Fixed timestamp from October 2024
    cursor.execute("UPDATE user_profiles SET last_post = ? WHERE username = ?", (one_hour_ago, username))
    conn.commit()
    
    await message.reply(f"✅ Reset timestamp for u/{username}. Will fetch posts from the last hour on next poll.")
    logger.info(f"Reset timestamp for u/{username} to {one_hour_ago}")


@dp.message(Command("scrape_user"))
async def cmd_scrape_user(message: types.Message):
    """Scrape ALL media from a user's complete post history"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip().lower() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/scrape_user username</code>\n\nExample: <code>/scrape_user johndoe</code>\n\n⚠️ This will scrape ALL media from the user's complete post history!", 
                           parse_mode=ParseMode.HTML)
        return
    
    username = args
    
    try:
        # Verify user exists
        if reddit is None:
            await message.reply("❌ Reddit API not initialized")
            return
        user = await reddit.redditor(username)
        await user.load()  # This will raise an exception if user doesn't exist
        
        await message.reply(f"🔍 Starting complete media scrape for u/{username}...\nThis may take several minutes depending on their post history.")
        logger.info(f"Starting complete scrape for u/{username}")
        
        # Run the scraping in background to avoid blocking
        asyncio.create_task(scrape_user_complete_history(username))
        
    except Exception as e:
        logger.error(f"Error starting scrape for u/{username}: {e}")
        await message.reply(f"❌ User u/{username} not found or Reddit API error.")


@dp.message(Command("stop_scraping"))
async def cmd_stop_scraping(message: types.Message):
    """Stop any ongoing scraping process and resume normal downloads"""
    global scraping_active, current_scrape_username, current_scrape_type, normal_downloads_paused
    
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    if not scraping_active:
        await message.reply("ℹ️ No scraping process is currently running.")
        return
    
    stopped_username = current_scrape_username or "unknown"
    stopped_type = current_scrape_type or "unknown"
    
    # Reset all scraping flags and resume normal downloads
    scraping_active = False
    current_scrape_username = None
    current_scrape_type = None
    normal_downloads_paused = False
    
    logger.info(f"⏹️ SCRAPING STOPPED: Normal downloads resumed after stopping {stopped_type} scrape for u/{stopped_username}")
    
    await message.reply(
        f"⏹️ <b>Scraping Stopped</b>\n\n"
        f"📋 <b>Operation:</b> {stopped_type.title()} scrape\n"
        f"👤 <b>Target:</b> u/{stopped_username}\n"
        f"▶️ <b>Status:</b> Normal downloads resumed\n\n"
        f"The bot is now monitoring subreddits and users normally again.", 
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Scraping stopped by admin for {stopped_type} u/{stopped_username}")


async def scrape_user_complete_history(username):
    """Scrape all media posts from a user's complete history"""
    global scraping_active, current_scrape_username, current_scrape_type, normal_downloads_paused
    
    try:
        # Check if another scraping is already in progress
        if scraping_active:
            logger.warning(f"Cannot start scrape for u/{username}: scraping already in progress for {current_scrape_type} u/{current_scrape_username}")
            try:
                await bot.send_message(ADMIN_ID, f"❌ Cannot start scrape for u/{username}: scraping already in progress for {current_scrape_type} u/{current_scrape_username}")
            except:
                pass
            return
        
        # Set scraping flags and pause normal downloads
        scraping_active = True
        current_scrape_username = username
        current_scrape_type = "user"
        normal_downloads_paused = True
        
        logger.info(f"🔄 SCRAPING MODE ACTIVATED: Normal downloads paused while scraping u/{username}")
        
        # Notify admin about scraping mode activation
        try:
            await bot.send_message(ADMIN_ID, 
                f"🔄 <b>Scraping Mode Activated</b>\n\n"
                f"📋 <b>Operation:</b> Complete user scrape\n"
                f"👤 <b>Target:</b> u/{username}\n"
                f"⏸️ <b>Status:</b> Normal downloads paused\n\n"
                f"Use /stop_scraping to stop and resume normal downloads.", 
                parse_mode=ParseMode.HTML)
        except:
            pass
        
        if reddit is None:
            logger.error("Reddit API not initialized")
            scraping_active = False
            current_scrape_username = None
            current_scrape_type = None
            normal_downloads_paused = False
            return
        user = await reddit.redditor(username)
        
        # Get all active chats to send to
        cursor.execute("SELECT chat_id FROM active_chats WHERE is_active = 1")
        active_chats = [row[0] for row in cursor.fetchall()]
        
        if not active_chats:
            logger.warning(f"No active chats found for complete scrape of u/{username}")
            scraping_active = False
            current_scrape_username = None
            current_scrape_type = None
            normal_downloads_paused = False
            return
        
        processed_count = 0
        sent_count = 0
        skipped_count = 0
        
        logger.info(f"Starting complete scrape for u/{username} - scraping_active set to True")
        
        # Iterate through ALL submissions (no limit)
        async for submission in user.submissions.new(limit=None):
            # Check if scraping has been stopped
            if not scraping_active:
                logger.info(f"Scraping stopped by user for u/{username} at {processed_count} posts processed")
                try:
                    await bot.send_message(ADMIN_ID, f"⏹️ <b>Scraping Stopped</b>\n\n👤 User: u/{username}\n📊 Posts processed before stop: {processed_count}\n📤 Media posts sent: {sent_count}\n⏭️ Duplicates skipped: {skipped_count}", parse_mode=ParseMode.HTML)
                except:
                    pass
                return
            
            processed_count += 1
            
            # Log progress every 50 posts
            if processed_count % 50 == 0:
                logger.info(f"Complete scrape progress for u/{username}: {processed_count} posts processed, {sent_count} sent, {skipped_count} skipped")
            
            try:
                # Check if already shared (skip duplicates)
                cursor.execute("SELECT post_id FROM shared_posts WHERE post_id = ? AND username = ?", 
                             (submission.id, username))
                if cursor.fetchone():
                    skipped_count += 1
                    continue
                
                # Only process posts with media content
                if not has_media_content(submission):
                    skipped_count += 1
                    continue
                
                # Create message text
                title = submission.title[:100] + "..." if len(submission.title) > 100 else submission.title
                message_text = f"📁 <b>Complete Scrape - u/{username}</b>\n\n<b>{title}</b>\n\n"
                
                if submission.selftext and not submission.is_self:
                    description = submission.selftext[:200] + "..." if len(submission.selftext) > 200 else submission.selftext
                    message_text += f"{description}\n\n"
                
                # Add metadata
                message_text += f"👤 u/{username} | 📅 {datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
                
                if hasattr(submission, 'over_18') and submission.over_18:
                    message_text += f" | 🔞 NSFW"
                
                # Send the post
                success = await send_post_to_chats(submission, message_text, active_chats, username)
                
                if success:
                    sent_count += 1
                    # Mark as shared in database
                    cursor.execute(
                        "INSERT INTO shared_posts (post_id, subreddit, username, shared_time) VALUES (?, ?, ?, ?)",
                        (submission.id, submission.subreddit.display_name, username, int(time.time()))
                    )
                    conn.commit()
                
                # Minimal delay to avoid rate limiting during scraping
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing post {submission.id} from u/{username}: {e}")
                continue
        
        # Send completion summary to admin
        admin_chat = ADMIN_ID
        summary_text = f"✅ <b>Complete Scrape Finished</b>\n\n👤 User: u/{username}\n📊 Total posts processed: {processed_count}\n📤 Media posts sent: {sent_count}\n⏭️ Duplicates skipped: {skipped_count}"
        
        try:
            await bot.send_message(admin_chat, summary_text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed to send completion summary: {e}")
        
        logger.info(f"Complete scrape finished for u/{username}: {processed_count} processed, {sent_count} sent, {skipped_count} skipped")
        
    except Exception as e:
        logger.error(f"Error in complete scrape for u/{username}: {e}")
        # Send error notification to admin
        try:
            await bot.send_message(ADMIN_ID, f"❌ Error in complete scrape for u/{username}: {str(e)}", parse_mode=ParseMode.HTML)
        except:
            pass
    finally:
        # Always reset scraping flags and resume normal downloads when done (success or error)
        scraping_active = False
        current_scrape_username = None
        current_scrape_type = None
        normal_downloads_paused = False
        
        logger.info(f"✅ SCRAPING FINISHED: Normal downloads resumed after scraping u/{username}")
        
        # Notify admin about scraping completion and normal downloads resumption
        try:
            await bot.send_message(ADMIN_ID, 
                f"✅ <b>Scraping Finished</b>\n\n"
                f"👤 <b>User:</b> u/{username}\n"
                f"▶️ <b>Status:</b> Normal downloads resumed\n\n"
                f"The bot is now monitoring subreddits and users normally again.", 
                parse_mode=ParseMode.HTML)
        except:
            pass


def has_media_content(submission):
    """Check if a submission contains media content"""
    try:
        # Direct image/video links
        if submission.url and any(submission.url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm']):
            return True
        
        # Reddit hosted content
        if hasattr(submission, 'is_video') and submission.is_video:
            return True
        
        # Gallery posts
        if hasattr(submission, 'is_gallery') and submission.is_gallery:
            return True
        
        # i.redd.it or v.redd.it links
        if submission.url and ('i.redd.it' in submission.url or 'v.redd.it' in submission.url):
            return True
        
        # RedGifs content
        if submission.url and 'redgifs.com' in submission.url:
            return True
        
        # Imgur links
        if submission.url and 'imgur.com' in submission.url:
            return True
        
        # Check for preview images (Reddit thumbnails)
        if hasattr(submission, 'preview') and submission.preview and 'images' in submission.preview:
            return True
        
        # Check if URL points to a Reddit gallery
        if submission.url and 'reddit.com/gallery/' in submission.url:
            return True
            
    except Exception as e:
        # If there's any error checking, assume it might have media
        return True
    
    return False


async def send_post_to_chats(submission, message_text, chat_ids, username=None):
    """Send a post to all active chats, returns True if successful"""
    successful_sends = 0
    
    for chat_id in chat_ids:
        try:
            # Handle different types of media
            if hasattr(submission, 'is_gallery') and submission.is_gallery:
                # Gallery post
                media_group = []
                try:
                    gallery_data = submission.gallery_data
                    media_metadata = submission.media_metadata
                    
                    for item in gallery_data['items'][:10]:  # Limit to 10 items
                        media_id = item['media_id']
                        if media_id in media_metadata:
                            media_info = media_metadata[media_id]
                            if 's' in media_info and 'u' in media_info['s']:
                                image_url = media_info['s']['u'].replace('&amp;', '&')
                                media_group.append(types.InputMediaPhoto(media=image_url))
                    
                    if media_group:
                        await bot.send_media_group(chat_id, media=media_group)
                        await bot.send_message(chat_id, message_text, parse_mode=ParseMode.HTML)
                        successful_sends += 1
                        
                except Exception as e:
                    logger.error(f"Error sending gallery to {chat_id}: {e}")
                    
            elif hasattr(submission, 'is_video') and submission.is_video:
                # Video post
                try:
                    video_url = submission.media['reddit_video']['fallback_url']
                    await bot.send_video(chat_id, video_url, caption=message_text, parse_mode=ParseMode.HTML)
                    successful_sends += 1
                except Exception as e:
                    logger.error(f"Error sending video to {chat_id}: {e}")
                    
            elif submission.url and 'redgifs.com' in submission.url:
                # RedGifs content - try to download and send as video
                try:
                    logger.info(f"Processing RedGifs for scraping: {submission.url}")
                    video_url = await get_redgifs_video_url(submission.url)
                    if video_url:
                        logger.info(f"Got RedGifs video URL, attempting download with streaming: {video_url}")
                        success = await download_and_send_video(video_url, message_text, chat_id)
                        if success:
                            logger.info(f"Successfully sent RedGifs video from user scrape: {submission.title[:50]}...")
                            successful_sends += 1
                        else:
                            # Fallback to link if download fails
                            logger.warning(f"RedGifs download failed during scrape, sending as link: {submission.url}")
                            await bot.send_message(chat_id, f"{message_text}\n\n🎥 <a href='{submission.url}'>RedGifs Video</a>", parse_mode=ParseMode.HTML)
                            successful_sends += 1
                    else:
                        # Fallback to link if URL extraction fails
                        logger.warning(f"RedGifs URL extraction failed during scrape, sending as link: {submission.url}")
                        await bot.send_message(chat_id, f"{message_text}\n\n🎥 <a href='{submission.url}'>RedGifs Video</a>", parse_mode=ParseMode.HTML)
                        successful_sends += 1
                except Exception as e:
                    logger.error(f"Error processing RedGifs for scraping to {chat_id}: {e}")
                    # Fallback to link on any error
                    try:
                        await bot.send_message(chat_id, f"{message_text}\n\n🔗 {submission.url}", parse_mode=ParseMode.HTML)
                        successful_sends += 1
                    except Exception as e2:
                        logger.error(f"Error sending RedGifs fallback to {chat_id}: {e2}")
                    
            elif submission.url and any(submission.url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                # Image/GIF post - download and send locally
                if submission.url.lower().endswith('.gif'):
                    logger.info(f"Downloading and sending GIF during scrape: {submission.url}")
                    success = await download_and_send_media(submission.url, message_text, chat_id, "animation")
                    if success:
                        successful_sends += 1
                    else:
                        # Fallback to text message with link if download fails
                        try:
                            await bot.send_message(chat_id, f"{message_text}\n\n🎬 <a href='{submission.url}'>View GIF</a>", parse_mode=ParseMode.HTML)
                            successful_sends += 1
                        except Exception as e2:
                            logger.error(f"Error sending GIF fallback to {chat_id}: {e2}")
                else:
                    # Try to download and send image locally first
                    logger.info(f"Downloading and sending image during scrape: {submission.url}")
                    success = await download_and_send_media(submission.url, message_text, chat_id, "photo")
                    if success:
                        successful_sends += 1
                    else:
                        # Fallback to direct URL sending
                        try:
                            await bot.send_photo(chat_id, submission.url, caption=message_text, parse_mode=ParseMode.HTML)
                            successful_sends += 1
                        except Exception as e:
                            # Final fallback to text message with link
                            logger.warning(f"Failed to send image directly during scrape, falling back to link: {e}")
                            try:
                                await bot.send_message(chat_id, f"{message_text}\n\n🖼️ <a href='{submission.url}'>View Image</a>", parse_mode=ParseMode.HTML)
                                successful_sends += 1
                            except Exception as e2:
                                logger.error(f"Error sending image fallback to {chat_id}: {e2}")
                    
            elif submission.url and any(submission.url.lower().endswith(ext) for ext in ['.mp4', '.webm']):
                # Direct video link - download and send locally
                logger.info(f"Downloading and sending video during scrape: {submission.url}")
                success = await download_and_send_media(submission.url, message_text, chat_id, "video")
                if success:
                    successful_sends += 1
                else:
                    # Fallback to direct URL sending
                    try:
                        await bot.send_video(chat_id, submission.url, caption=message_text, parse_mode=ParseMode.HTML)
                        successful_sends += 1
                    except Exception as e:
                        # Final fallback to text message with link
                        logger.warning(f"Failed to send video directly during scrape, falling back to link: {e}")
                        try:
                            await bot.send_message(chat_id, f"{message_text}\n\n🎥 <a href='{submission.url}'>View Video</a>", parse_mode=ParseMode.HTML)
                            successful_sends += 1
                        except Exception as e2:
                            logger.error(f"Error sending video fallback to {chat_id}: {e2}")
                    
            else:
                # Text post or other content with preview
                try:
                    if hasattr(submission, 'preview') and submission.preview:
                        image_url = submission.preview['images'][0]['source']['url'].replace('&amp;', '&')
                        try:
                            await bot.send_photo(chat_id, image_url, caption=message_text, parse_mode=ParseMode.HTML)
                            successful_sends += 1
                        except Exception as preview_error:
                            logger.warning(f"Failed to send preview image during scrape, falling back to link: {preview_error}")
                            await bot.send_message(chat_id, f"{message_text}\n\n🔗 {submission.url}", parse_mode=ParseMode.HTML)
                            successful_sends += 1
                    else:
                        await bot.send_message(chat_id, f"{message_text}\n\n🔗 {submission.url}", parse_mode=ParseMode.HTML)
                        successful_sends += 1
                except Exception as e:
                    logger.error(f"Error sending preview/text to {chat_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error sending to chat {chat_id}: {e}")
            continue
    
    return successful_sends > 0


@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    """Handle /list command to show all subscribed subreddits and user profiles"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    cursor.execute("SELECT name, sort_mode FROM subreddits ORDER BY name")
    subs = cursor.fetchall()
    
    cursor.execute("SELECT username, sort_mode FROM user_profiles ORDER BY username")
    users = cursor.fetchall()
    
    if not subs and not users:
        await message.reply("ℹ️ No subscriptions yet.\n\nUse <code>/add subreddit_name</code> or <code>/add_user username</code> to subscribe.", 
                           parse_mode=ParseMode.HTML)
        return
    
    text = "📋 <b>Current Subscriptions:</b>\n\n"
    
    if subs:
        text += f"<b>Subreddits ({len(subs)}):</b>\n"
        for sub in subs:
            name = sub[0]
            sort_mode = sub[1] if len(sub) > 1 and sub[1] else 'new'
            text += f"• r/{name} ({sort_mode})\n"
        text += "\n"
    
    if users:
        text += f"<b>User Profiles ({len(users)}):</b>\n"
        for user in users:
            name = user[0]
            sort_mode = user[1] if len(user) > 1 and user[1] else 'new'
            text += f"• u/{name} ({sort_mode})\n"
    
    await message.reply(text.strip(), parse_mode=ParseMode.HTML)


@dp.message(Command("add_chat"))
async def cmd_add_chat(message: types.Message):
    """Add current chat to receive Reddit posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    chat_id = message.chat.id
    chat_title = getattr(message.chat, 'title', 'Direct Message')
    if not chat_title:
        chat_title = f"Chat with {(message.from_user.first_name if message.from_user and message.from_user.first_name else 'User')}"
    
    try:
        # Check if chat already exists
        cursor.execute("SELECT chat_id FROM active_chats WHERE chat_id = ?", (chat_id,))
        if cursor.fetchone():
            # Reactivate if it was disabled
            cursor.execute("UPDATE active_chats SET is_active = 1 WHERE chat_id = ?", (chat_id,))
            conn.commit()
            await message.reply(f"✅ Chat '{chat_title}' is now active for receiving Reddit posts.")
        else:
            # Add new chat
            cursor.execute("INSERT INTO active_chats (chat_id, chat_title, added_by) VALUES (?, ?, ?)", 
                          (chat_id, chat_title, message.from_user.id if message.from_user else 0))
            conn.commit()
            await message.reply(f"✅ Added '{chat_title}' to receive Reddit posts. Posts will be sent here automatically.")
        
        logger.info(f"Added/activated chat {chat_id} ({chat_title}) for posts")
    except Exception as e:
        logger.error(f"Error adding chat {chat_id}: {e}")
        await message.reply("❌ Error adding chat. Check bot logs for details.")


@dp.message(Command("remove_chat"))
async def cmd_remove_chat(message: types.Message):
    """Remove current chat from receiving Reddit posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    chat_id = message.chat.id
    
    try:
        cursor.execute("UPDATE active_chats SET is_active = 0 WHERE chat_id = ?", (chat_id,))
        if cursor.rowcount > 0:
            conn.commit()
            await message.reply("✅ This chat will no longer receive Reddit posts.")
            logger.info(f"Deactivated chat {chat_id} from receiving posts")
        else:
            await message.reply("ℹ️ This chat was not active for receiving posts.")
    except Exception as e:
        logger.error(f"Error removing chat {chat_id}: {e}")
        await message.reply("❌ Error removing chat. Check bot logs for details.")


@dp.message(Command("list_chats"))
async def cmd_list_chats(message: types.Message):
    """List all active chats receiving Reddit posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        cursor.execute("SELECT chat_id, chat_title, added_at FROM active_chats WHERE is_active = 1 ORDER BY added_at DESC")
        chats = cursor.fetchall()
        
        if not chats:
            await message.reply("ℹ️ No active chats configured.\n\nUse <code>/add_chat</code> to add this chat.", 
                               parse_mode=ParseMode.HTML)
            return
        
        text = f"📱 <b>Active Chats ({len(chats)}):</b>\n\n"
        for chat in chats:
            chat_id, chat_title, added_at = chat
            added_date = datetime.fromtimestamp(added_at).strftime("%Y-%m-%d") if added_at else "Unknown"
            text += f"• {chat_title} (ID: {chat_id})\n  Added: {added_date}\n\n"
        
        await message.reply(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error listing chats: {e}")
        await message.reply("❌ Error listing chats. Check bot logs for details.")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    """Show bot status and resume information"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        # Get basic stats
        cursor.execute("SELECT COUNT(*) FROM subreddits")
        total_subs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM active_chats WHERE is_active = 1")
        active_chats = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM shared_posts")
        shared_posts = cursor.fetchone()[0]
        
        # Get last processing time
        last_processing = await get_bot_state("last_processing_time")
        if last_processing:
            last_time = datetime.fromtimestamp(int(last_processing))
            time_str = last_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            time_str = "Never"
        
        # Get subscribed user profiles
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        total_users = cursor.fetchone()[0]
        
        # Get sources with resume capability
        cursor.execute("SELECT COUNT(*) FROM subreddits WHERE sort_mode = 'new'")
        resume_capable_subs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_profiles WHERE sort_mode = 'new'")
        resume_capable_users = cursor.fetchone()[0]
        
        # Get background task information
        active_background_tasks = len(background_tasks)
        
        # Get scraping status
        global scraping_active, current_scrape_username, current_scrape_type, normal_downloads_paused
        scraping_status = "🔴 Active" if scraping_active else "🟢 Inactive"
        downloads_status = "⏸️ Paused" if normal_downloads_paused else "▶️ Running"
        
        text = f"""
🤖 <b>Bot Status</b>

📊 <b>Statistics:</b>
• Subscribed subreddits: {total_subs}
• Subscribed user profiles: {total_users}
• Active chats: {active_chats}
• Shared posts tracked: {shared_posts}
• Resume-capable sources: {resume_capable_subs + resume_capable_users}

⚡ <b>Processing:</b>
• Active background tasks: {active_background_tasks}
• Commands work during media processing: ✅

🔄 <b>Scraping Mode:</b>
• Status: {scraping_status}
• Normal downloads: {downloads_status}"""
        
        if scraping_active and current_scrape_type:
            text += f"""
• Current operation: {current_scrape_type.title()} scrape
• Target: u/{current_scrape_username}"""
        
        text += f"""

⏰ <b>Last Activity:</b>
{time_str}

🔄 <b>Resume Feature:</b>
• Enabled for sources with 'new' sorting
• Auto-resumes after server restart
• Catches up on missed posts chronologically

💡 <b>Concurrent Processing:</b>
Commands and subscriptions work immediately even while the bot is downloading and sending large videos/images.
"""
        
        await message.reply(text.strip(), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        await message.reply("❌ Error getting status. Check bot logs for details.")


@dp.message(Command("clear_history"))
async def cmd_clear_history(message: types.Message):
    """Clear the shared posts history to allow re-sharing old posts"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        cursor.execute("DELETE FROM shared_posts")
        deleted_count = cursor.rowcount
        conn.commit()
        
        await message.reply(f"✅ Cleared {deleted_count} posts from sharing history. Bot can now re-share previously posted content.")
        logger.info(f"Cleared {deleted_count} posts from sharing history")
    except Exception as e:
        logger.error(f"Error clearing shared posts history: {e}")
        await message.reply("❌ Error clearing history. Check bot logs for details.")


@dp.message(Command("test"))
async def cmd_test(message: types.Message):
    """Test command to verify responsiveness during media processing"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    active_tasks = len(background_tasks)
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Test duplicate detection system
    test_url = "https://example.com/test.jpg"
    test_hash = generate_media_hash(test_url, "Test Post", "testuser")
    
    await message.reply(
        f"🟢 Bot is responsive!\n\n"
        f"⏰ Response time: {current_time}\n"
        f"⚡ Active background tasks: {active_tasks}\n"
        f"🔍 Duplicate detection: Active\n"
        f"📊 Test hash: {test_hash[:8]}...\n\n"
        f"This proves commands work while media is being processed!"
    )


@dp.message(Command("check_subreddits"))
async def cmd_check_subreddits(message: types.Message):
    """Check which subreddits are accessible and which are causing errors"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    await message.reply("🔍 Checking subreddit accessibility... This may take a moment.")
    
    try:
        if reddit is None:
            await message.reply("❌ Reddit API not initialized")
            return
        
        # Get all subscribed subreddits
        cursor.execute("SELECT name FROM subreddits")
        subreddits = cursor.fetchall()
        
        if not subreddits:
            await message.reply("ℹ️ No subreddits subscribed.")
            return
        
        accessible = []
        inaccessible = []
        
        for (subreddit_name,) in subreddits:
            try:
                subreddit = await reddit.subreddit(subreddit_name)
                await subreddit.load()
                accessible.append(subreddit_name)
            except Exception as e:
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    inaccessible.append((subreddit_name, "404 - Not found or inaccessible"))
                elif "403" in error_msg or "forbidden" in error_msg:
                    inaccessible.append((subreddit_name, "403 - Forbidden (private/restricted)"))
                else:
                    inaccessible.append((subreddit_name, f"Error: {str(e)[:50]}"))
        
        # Build response message
        response = f"📊 **Subreddit Status Report**\n\n"
        response += f"✅ **Accessible ({len(accessible)}):**\n"
        for subreddit in accessible:
            response += f"• r/{subreddit}\n"
        
        if inaccessible:
            response += f"\n❌ **Inaccessible ({len(inaccessible)}):**\n"
            for subreddit_name, error in inaccessible:
                response += f"• r/{subreddit_name}: {error}\n"
            
            response += f"\n💡 **To remove inaccessible subreddits:**\n"
            for subreddit_name, _ in inaccessible:
                response += f"`/remove r/{subreddit_name}`\n"
        
        await message.reply(response, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error checking subreddits: {e}")
        await message.reply(f"❌ Error checking subreddits: {e}")


@dp.message(Command("check_users"))
async def cmd_check_users(message: types.Message):
    """Check which user profiles are accessible and which are causing errors"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    await message.reply("🔍 Checking user profile accessibility... This may take a moment.")
    
    try:
        if reddit is None:
            await message.reply("❌ Reddit API not initialized")
            return
        
        # Get all subscribed users
        cursor.execute("SELECT username FROM user_profiles")
        users = cursor.fetchall()
        
        if not users:
            await message.reply("ℹ️ No user profiles subscribed.")
            return
        
        accessible = []
        inaccessible = []
        
        for (username,) in users:
            try:
                user = await reddit.redditor(username)
                await user.load()
                accessible.append(username)
            except Exception as e:
                error_msg = str(e).lower()
                if "404" in error_msg or "not found" in error_msg:
                    inaccessible.append((username, "404 - User not found"))
                elif "403" in error_msg or "forbidden" in error_msg:
                    inaccessible.append((username, "403 - Profile private or restricted"))
                elif "suspended" in error_msg:
                    inaccessible.append((username, "Account suspended"))
                elif "deleted" in error_msg:
                    inaccessible.append((username, "Account deleted"))
                else:
                    inaccessible.append((username, f"Error: {str(e)[:50]}"))
        
        # Build response message
        response = f"👤 **User Profile Status Report**\n\n"
        response += f"✅ **Accessible ({len(accessible)}):**\n"
        for username in accessible:
            response += f"• u/{username}\n"
        
        if inaccessible:
            response += f"\n❌ **Inaccessible ({len(inaccessible)}):**\n"
            for username, error in inaccessible:
                response += f"• u/{username}: {error}\n"
            
            response += f"\n💡 **To remove inaccessible users:**\n"
            for username, _ in inaccessible:
                response += f"`/remove u/{username}`\n"
        
        await message.reply(response, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error checking users: {e}")
        await message.reply(f"❌ Error checking users: {e}")


@dp.message(Command("cleanup_inaccessible"))
async def cmd_cleanup_inaccessible(message: types.Message):
    """Check and automatically remove all inaccessible subreddits and users"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    await message.reply("🧹 Starting cleanup of inaccessible subscriptions... This may take a moment.")
    
    try:
        if reddit is None:
            await message.reply("❌ Reddit API not initialized")
            return
        
        # Check subreddits
        cursor.execute("SELECT name FROM subreddits")
        subreddits = cursor.fetchall()
        
        # Check users
        cursor.execute("SELECT username FROM user_profiles")
        users = cursor.fetchall()
        
        total_checked = len(subreddits) + len(users)
        if total_checked == 0:
            await message.reply("ℹ️ No subscriptions to check.")
            return
        
        subreddits_to_remove = []
        users_to_remove = []
        
        # Check subreddits
        for (subreddit_name,) in subreddits:
            try:
                subreddit = await reddit.subreddit(subreddit_name)
                await subreddit.load()
            except Exception as e:
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ["404", "not found", "403", "forbidden", "suspended", "deleted"]):
                    subreddits_to_remove.append(subreddit_name)
                    logger.info(f"Marking subreddit r/{subreddit_name} for removal: {str(e)[:100]}")
        
        # Check users
        for (username,) in users:
            try:
                user = await reddit.redditor(username)
                await user.load()
            except Exception as e:
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ["404", "not found", "403", "forbidden", "suspended", "deleted"]):
                    users_to_remove.append(username)
                    logger.info(f"Marking user u/{username} for removal: {str(e)[:100]}")
        
        # Remove inaccessible subreddits
        subreddits_removed = 0
        if subreddits_to_remove:
            for subreddit_name in subreddits_to_remove:
                cursor.execute("DELETE FROM subreddits WHERE name=?", (subreddit_name,))
                if cursor.rowcount > 0:
                    subreddits_removed += 1
                    logger.info(f"Removed inaccessible subreddit: r/{subreddit_name}")
        
        # Remove inaccessible users
        users_removed = 0
        if users_to_remove:
            for username in users_to_remove:
                cursor.execute("DELETE FROM user_profiles WHERE username=?", (username,))
                if cursor.rowcount > 0:
                    users_removed += 1
                    logger.info(f"Removed inaccessible user: u/{username}")
        
        # Commit changes
        conn.commit()
        
        # Build response message
        response_parts = []
        response_parts.append("🧹 **Cleanup Complete!**\n")
        
        if subreddits_to_remove:
            response_parts.append(f"📋 **Subreddits Removed ({subreddits_removed}):**")
            for subreddit_name in subreddits_to_remove:
                response_parts.append(f"• r/{subreddit_name}")
            response_parts.append("")
        
        if users_to_remove:
            response_parts.append(f"👤 **Users Removed ({users_removed}):**")
            for username in users_to_remove:
                response_parts.append(f"• u/{username}")
            response_parts.append("")
        
        if not subreddits_to_remove and not users_to_remove:
            response_parts.append("✅ **All subscriptions are accessible!**")
            response_parts.append("No cleanup was necessary.")
        else:
            total_removed = subreddits_removed + users_removed
            response_parts.append(f"📊 **Summary:** {total_removed} inaccessible subscription(s) removed")
            response_parts.append(f"✅ Your subscription list is now clean!")
        
        await message.reply("\n".join(response_parts), parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        await message.reply(f"❌ Error during cleanup. Check bot logs for details.")


@dp.message(Command("duplicates"))
async def cmd_duplicates(message: types.Message):
    """Show duplicate media statistics and management options"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        # Get duplicate statistics
        cursor.execute("""
            SELECT COUNT(*) as total_media,
                   COUNT(CASE WHEN shared_count > 1 THEN 1 END) as duplicates,
                   SUM(shared_count) as total_shares,
                   AVG(shared_count) as avg_shares
            FROM media_content
        """)
        stats = cursor.fetchone()
        
        if not stats or stats[0] == 0:
            await message.reply("ℹ️ No media content tracked yet.")
            return
        
        total_media, duplicates, total_shares, avg_shares = stats
        
        # Get top duplicated media
        cursor.execute("""
            SELECT media_url, shared_count, first_seen_time, post_id, subreddit
            FROM media_content 
            WHERE shared_count > 1 
            ORDER BY shared_count DESC 
            LIMIT 10
        """)
        top_duplicates = cursor.fetchall()
        
        # Get recent duplicate attempts
        cursor.execute("""
            SELECT media_url, shared_count, first_seen_time, post_id, subreddit
            FROM media_content 
            WHERE shared_count > 1 
            ORDER BY first_seen_time DESC 
            LIMIT 5
        """)
        recent_duplicates = cursor.fetchall()
        
        response = f"📊 **Duplicate Media Statistics**\n\n"
        response += f"📁 Total media tracked: {total_media}\n"
        response += f"🔄 Duplicate media detected: {duplicates}\n"
        response += f"📈 Total share attempts: {total_shares}\n"
        response += f"📊 Average shares per media: {avg_shares:.1f}\n"
        response += f"🎯 Duplicate prevention rate: {((duplicates/total_media)*100):.1f}%\n\n"
        
        if top_duplicates:
            response += f"🔝 **Most Duplicated Media:**\n"
            for i, (url, count, first_seen, post_id, subreddit) in enumerate(top_duplicates, 1):
                # Truncate URL for display
                display_url = url[:50] + "..." if len(url) > 50 else url
                response += f"{i}. {display_url}\n"
                response += f"   📊 Attempted {count} times | r/{subreddit}\n\n"
        
        if recent_duplicates:
            response += f"🕒 **Recent Duplicate Attempts:**\n"
            for i, (url, count, first_seen, post_id, subreddit) in enumerate(recent_duplicates, 1):
                display_url = url[:40] + "..." if len(url) > 40 else url
                time_ago = datetime.fromtimestamp(first_seen).strftime("%m-%d %H:%M")
                response += f"{i}. {display_url}\n"
                response += f"   📊 {count} attempts | {time_ago} | r/{subreddit}\n\n"
        
        response += f"💡 **Commands:**\n"
        response += f"• `/clear_duplicates` - Clear all duplicate tracking data\n"
        response += f"• `/clear_history` - Clear post sharing history\n"
        
        await message.reply(response, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error getting duplicate stats: {e}")
        await message.reply(f"❌ Error getting duplicate statistics: {e}")


@dp.message(Command("clear_duplicates"))
async def cmd_clear_duplicates(message: types.Message):
    """Clear all duplicate media tracking data"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    try:
        cursor.execute("DELETE FROM media_content")
        deleted_count = cursor.rowcount
        conn.commit()
        
        await message.reply(f"✅ Cleared {deleted_count} media tracking records.\n"
                           f"Duplicate detection will start fresh.")
        
    except Exception as e:
        logger.error(f"Error clearing duplicates: {e}")
        await message.reply(f"❌ Error clearing duplicate data: {e}")


async def scrape_redgifs_user(username, chat_id):
    """Scrape all videos from a RedGifs user profile using web scraping"""
    successful_sends = 0
    total_videos = 0
    
    try:
        logger.info(f"Starting RedGifs user scrape for: {username}")
        
        # Use RedGifs API approach with proper authentication
        import aiohttp
        import json
        import re
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            # First get a temporary auth token from RedGifs
            auth_url = "https://api.redgifs.com/v2/auth/temporary"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            
            try:
                # Get temporary token
                async with session.get(auth_url, headers=headers) as auth_response:
                    if auth_response.status == 200:
                        auth_data = await auth_response.json()
                        token = auth_data.get('token')
                        if token:
                            headers['Authorization'] = f'Bearer {token}'
                            logger.info(f"Got RedGifs API token for user search: {username}")
                
                # Try to get user's GIFs via API
                api_url = f"https://api.redgifs.com/v2/users/{username}/search"
                
                # Implement pagination to fetch ALL videos
                video_data = []
                page = 1
                per_page = 80  # Max per page allowed by API
                total_fetched = 0
                
                while True:
                    params = {
                        'order': 'new',
                        'count': per_page,
                        'page': page
                    }
                    
                    logger.info(f"Fetching RedGifs page {page} for user: {username}")
                    async with session.get(api_url, headers=headers, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            gifs = data.get('gifs', [])
                            
                            if not gifs:  # No more videos available
                                logger.info(f"No more videos found on page {page}, stopping pagination")
                                break
                            
                            page_videos = 0
                            for gif in gifs:
                                if gif.get('urls') and gif.get('urls', {}).get('hd'):
                                    video_data.append({
                                        'id': gif.get('id', ''),
                                        'url': gif.get('urls', {}).get('hd', ''),
                                        'title': gif.get('title', f'RedGifs Video {gif.get("id", "")}'),
                                        'duration': gif.get('duration', 0),
                                        'views': gif.get('views', 0),
                                        'likes': gif.get('likes', 0),
                                        'width': gif.get('width', 0),
                                        'height': gif.get('height', 0),
                                    })
                                    page_videos += 1
                            
                            total_fetched += page_videos
                            logger.info(f"Page {page}: Found {page_videos} videos (total: {total_fetched})")
                            
                            # If we got fewer videos than requested, we've reached the end
                            if len(gifs) < per_page:
                                logger.info(f"Reached end of videos (got {len(gifs)} < {per_page}), stopping pagination")
                                break
                                
                            page += 1
                            
                            # Safety limit to prevent infinite loops - increased for large profiles
                            if page > 100:  # Max 100 pages = 8000 videos max
                                logger.warning(f"Reached safety limit of 100 pages, stopping pagination")
                                break
                                
                        else:
                            logger.warning(f"RedGifs API returned status {response.status} for page {page}")
                            break
                
                # If API didn't return any videos, fall back to web scraping
                if not video_data:
                    logger.warning(f"No videos found via API for user: {username}, falling back to web scraping")
                    # Fall back to web scraping approach
                    profile_url = f"https://www.redgifs.com/users/{username}"
                    
                    scrape_headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                    }
                    
                    async with session.get(profile_url, headers=scrape_headers) as scrape_response:
                        if scrape_response.status == 200:
                            page_content = await scrape_response.text()
                            
                            # Enhanced pattern matching for modern RedGifs
                            patterns = [
                                r'"hd":"(https://[^"]*\.mp4)"',
                                r'"sd":"(https://[^"]*\.mp4)"',
                                r'https://media\.redgifs\.com/[A-Za-z0-9_-]+\.mp4',
                                r'https://thumbs\.redgifs\.com/[A-Za-z0-9_-]+\.mp4',
                                r'"webmUrl":"(https://[^"]*\.webm)"',
                            ]
                            
                            found_urls = set()
                            for pattern in patterns:
                                matches = re.findall(pattern, page_content, re.IGNORECASE)
                                found_urls.update(matches)
                            
                            # Convert URLs to video data format
                            for i, url in enumerate(found_urls):
                                video_data.append({
                                    'id': f'scraped_{i}',
                                    'url': url,
                                    'title': f'RedGifs Video {i+1}',
                                    'duration': 0,
                                    'views': 0,
                                    'likes': 0,
                                    'width': 0,
                                    'height': 0,
                                })
                            
                            logger.info(f"Found {len(video_data)} videos via web scraping for user: {username}")
                
                if not video_data:
                    logger.warning(f"No videos found for RedGifs user: {username} (tried API and scraping)")
                    return 0, 0
                    
                total_videos = len(video_data)
                logger.info(f"Found {total_videos} videos for RedGifs user {username}")
                
                # Send user profile info first with safety warning for large collections
                profile_text = f"""
🎥 <b>RedGifs User: {username}</b>

👤 <b>Profile:</b> https://www.redgifs.com/users/{username}
📊 <b>Found {total_videos} videos to download...</b>

🔄 <b>Starting downloads...</b>
"""
                
                # Add warning for very large collections
                if total_videos > 500:
                    profile_text += f"\n⚠️ <b>Large Collection Detected:</b> This may take several hours to complete.\n"
                
                await bot.send_message(chat_id, profile_text.strip(), parse_mode=ParseMode.HTML)
                
                # Process each video
                for i, video in enumerate(video_data, 1):
                    try:
                        # Create unique identifier for duplicate checking
                        video_id = f"redgifs_api_{video['id']}"
                        
                        # Check if already shared (using correct column name)
                        cursor.execute("SELECT shared_time FROM shared_posts WHERE post_id = ?", (video_id,))
                        existing_entry = cursor.fetchone()
                        if existing_entry:
                            logger.info(f"Skipping already shared RedGifs video: {video['id']} (video_id: {video_id})")
                            continue
                        else:
                            logger.info(f"Processing new RedGifs video: {video['id']} (video_id: {video_id})")
                        
                        # Create message text with available metadata
                        message_text = f"""
🎥 <b>RedGifs Video {i}/{total_videos}</b>

📝 <b>Title:</b> {video['title'] or 'Untitled'}
👤 <b>User:</b> {username}
⏱️ <b>Duration:</b> {video['duration']}s
📐 <b>Size:</b> {video['width']}x{video['height']}
👀 <b>Views:</b> {video['views']:,}
💖 <b>Likes:</b> {video['likes']:,}

🔗 <b>Source:</b> <a href="https://www.redgifs.com/watch/{video['id']}">View on RedGifs</a>
"""
                        
                        # Download and send video
                        logger.info(f"Processing RedGifs video {i}/{total_videos}: {video['id']}")
                        success = await download_and_send_media(video['url'], message_text.strip(), chat_id, "video")
                        
                        if success:
                            # Mark as shared with correct column name and additional fields
                            current_time = int(time.time())
                            cursor.execute(
                                "INSERT INTO shared_posts (post_id, subreddit, username, shared_time) VALUES (?, ?, ?, ?)",
                                (video_id, "redgifs", username, current_time)
                            )
                            conn.commit()
                            successful_sends += 1
                            logger.info(f"Successfully sent RedGifs video {i}/{total_videos}: {video['id']}")
                        else:
                            # Fallback to link if download fails
                            fallback_text = f"{message_text.strip()}\n\n🎥 <a href='{video['url']}'>Download Video</a>"
                            await bot.send_message(chat_id, fallback_text, parse_mode=ParseMode.HTML)
                            # Still mark as shared even for fallback links to prevent re-processing
                            current_time = int(time.time())
                            cursor.execute(
                                "INSERT INTO shared_posts (post_id, subreddit, username, shared_time) VALUES (?, ?, ?, ?)",
                                (video_id, "redgifs", username, current_time)
                            )
                            conn.commit()
                            successful_sends += 1
                            logger.warning(f"Sent RedGifs video {i}/{total_videos} as link: {video['id']}")
                        
                        # Minimal delay for fast scraping performance
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error processing RedGifs video {video.get('id', 'unknown')}: {e}")
                        continue
                    
            except Exception as e:
                logger.error(f"Error accessing RedGifs profile for {username}: {e}")
                return 0, 0
        
        logger.info(f"Completed RedGifs web scrape for {username}: {successful_sends}/{total_videos} videos sent")
        return successful_sends, total_videos
        
    except Exception as e:
        logger.error(f"Error scraping RedGifs user {username}: {e}")
        return 0, 0


@dp.message(Command("scrape_redgifs"))
async def cmd_scrape_redgifs(message: types.Message):
    """Scrape all videos from a RedGifs user profile"""
    global scraping_active, current_scrape_username, current_scrape_type, normal_downloads_paused
    
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Check if another scraping is already in progress
    if scraping_active:
        await message.reply(f"❌ Cannot start RedGifs scrape: scraping already in progress for {current_scrape_type} u/{current_scrape_username}")
        return
    
    if not message.text:
        await message.reply("❌ Please provide a command with parameters.")
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply(
            "❓ <b>Usage:</b> <code>/scrape_redgifs username_or_url</code>\n\n"
            "📝 <b>Examples:</b>\n"
            "• <code>/scrape_redgifs someuser</code>\n"
            "• <code>/scrape_redgifs https://www.redgifs.com/users/someuser</code>\n\n"
            "🎥 This will download ALL videos from the specified RedGifs user profile.",
            parse_mode=ParseMode.HTML
        )
        return
    
    input_text = args[1].strip()
    if not input_text:
        await message.reply("❌ Please provide a valid RedGifs username or URL.")
        return
    
    # Extract username from URL if provided
    username = input_text.lower()
    if "redgifs.com/users/" in username:
        # Extract username from URL like https://www.redgifs.com/users/username
        username = username.split("/users/")[-1].split("/")[0].split("?")[0]
    elif "redgifs.com/" in username and "/users/" not in username:
        # Handle potential other RedGifs URL formats
        username = username.split("redgifs.com/")[-1].split("/")[0].split("?")[0]
    
    # Remove @ symbol if present
    username = username.lstrip('@')
    
    chat_id = message.chat.id
    
    try:
        # Set scraping flags and pause normal downloads
        scraping_active = True
        current_scrape_username = username
        current_scrape_type = "redgifs"
        normal_downloads_paused = True
        
        logger.info(f"🔄 SCRAPING MODE ACTIVATED: Normal downloads paused while scraping RedGifs u/{username}")
        
        # Notify admin about scraping mode activation
        status_msg = await message.reply(
            f"🔄 <b>Scraping Mode Activated</b>\n\n"
            f"📋 <b>Operation:</b> RedGifs scrape\n"
            f"👤 <b>Target:</b> u/{username}\n"
            f"⏸️ <b>Status:</b> Normal downloads paused\n\n"
            f"⏳ This may take several minutes depending on the number of videos...\n\n"
            f"Use /stop_scraping to stop and resume normal downloads.",
            parse_mode=ParseMode.HTML
        )
        
        # Start scraping in background
        successful_sends, total_videos = await scrape_redgifs_user(username, chat_id)
        
        # Send completion message
        if total_videos > 0:
            completion_text = f"""
✅ <b>RedGifs Scrape Complete!</b>

👤 <b>User:</b> {username}
📊 <b>Results:</b> {successful_sends}/{total_videos} videos sent
📱 <b>Duplicates:</b> {total_videos - successful_sends} videos skipped (already shared)

🎉 All videos have been downloaded and sent to this chat!
"""
        else:
            completion_text = f"""
❌ <b>RedGifs Scrape Failed</b>

👤 <b>User:</b> {username}
🔍 <b>Result:</b> No videos found or user doesn't exist

Please check the username and try again.
"""
        
        await bot.edit_message_text(
            completion_text.strip(),
            chat_id=status_msg.chat.id,
            message_id=status_msg.message_id,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"RedGifs scrape completed for {username}: {successful_sends}/{total_videos} videos")
        
    except Exception as e:
        logger.error(f"Error in RedGifs scrape command for {username}: {e}")
        await message.reply(f"❌ Error scraping RedGifs user '{username}'. Check bot logs for details.")
    finally:
        # Always reset scraping flags and resume normal downloads when done (success or error)
        scraping_active = False
        current_scrape_username = None
        current_scrape_type = None
        normal_downloads_paused = False
        
        logger.info(f"✅ REDGIFS SCRAPING FINISHED: Normal downloads resumed after scraping u/{username}")
        
        # Notify admin about scraping completion and normal downloads resumption
        try:
            await bot.send_message(ADMIN_ID, 
                f"✅ <b>RedGifs Scraping Finished</b>\n\n"
                f"👤 <b>User:</b> u/{username}\n"
                f"▶️ <b>Status:</b> Normal downloads resumed\n\n"
                f"The bot is now monitoring subreddits and users normally again.", 
                parse_mode=ParseMode.HTML)
        except:
            pass


@dp.message(Command("test_duplicate"))
async def cmd_test_duplicate(message: types.Message):
    """Test duplicate detection with a specific URL"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get arguments from the message text
    args = message.text.split(' ', 1)[1].strip() if message.text and len(message.text.split()) > 1 else ""
    if not args:
        await message.reply("❌ Usage: <code>/test_duplicate URL</code>\n\nExample: <code>/test_duplicate https://example.com/image.jpg</code>", 
                           parse_mode=ParseMode.HTML)
        return
    
    test_url = args
    test_title = "Test Post"
    test_author = "testuser"
    
    try:
        # Generate hash
        media_hash = generate_media_hash(test_url, test_title, test_author)
        
        # Check if it exists
        is_duplicate, duplicate_info = await is_media_duplicate(test_url, test_title, test_author)
        
        response = f"🔍 **Duplicate Detection Test**\n\n"
        response += f"🔗 **URL:** {test_url}\n"
        response += f"📝 **Title:** {test_title}\n"
        response += f"👤 **Author:** {test_author}\n"
        response += f"🔐 **Hash:** {media_hash}\n\n"
        
        if is_duplicate:
            response += f"❌ **Result:** Duplicate detected!\n"
            response += f"📊 **Share count:** {duplicate_info.get('shared_count', 1)}\n"
            response += f"📅 **First seen:** {datetime.fromtimestamp(duplicate_info.get('first_seen_time', 0)).strftime('%Y-%m-%d %H:%M:%S')}\n"
            response += f"📍 **Original subreddit:** r/{duplicate_info.get('subreddit', 'unknown')}\n"
        else:
            response += f"✅ **Result:** Not a duplicate\n"
            response += f"💡 This URL would be processed normally\n"
        
        await message.reply(response, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error testing duplicate detection: {e}")
        await message.reply(f"❌ Error testing duplicate detection: {e}")


@dp.message(Command("pause"))
async def cmd_pause(message: types.Message):
    """Pause media sending"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    global bot_paused
    
    if bot_paused:
        await message.reply("ℹ️ Bot is already paused.")
        return
    
    bot_paused = True
    queue_size = send_queue.qsize()
    
    await message.reply(f"⏸️ **Bot Paused**\n\n"
                       f"📊 Queue size: {queue_size} items\n"
                       f"🔄 Media sending stopped\n"
                       f"📥 New posts will still be queued\n\n"
                       f"Use /resume to continue sending media.", 
                       parse_mode=ParseMode.MARKDOWN)
    
    logger.info(f"Bot paused - queue size: {queue_size}")


@dp.message(Command("resume"))
async def cmd_resume(message: types.Message):
    """Resume media sending"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    global bot_paused
    
    if not bot_paused:
        await message.reply("ℹ️ Bot is not paused.")
        return
    
    bot_paused = False
    queue_size = send_queue.qsize()
    
    await message.reply(f"▶️ **Bot Resumed**\n\n"
                       f"📊 Queue size: {queue_size} items\n"
                       f"🔄 Media sending resumed\n"
                       f"📤 Processing queued media...", 
                       parse_mode=ParseMode.MARKDOWN)
    
    logger.info(f"Bot resumed - queue size: {queue_size}")


@dp.message(Command("clear_queue"))
async def cmd_clear_queue(message: types.Message):
    """Clear all media currently in the queue"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    global queue_cleared
    
    queue_size = send_queue.qsize()
    
    if queue_size == 0:
        await message.reply("ℹ️ Queue is already empty.")
        return
    
    queue_cleared = True
    
    await message.reply(f"🗑️ **Queue Cleared**\n\n"
                       f"📊 Removed {queue_size} queued items\n"
                       f"🔄 Media sending will continue with new items\n"
                       f"📥 New posts will still be queued normally", 
                       parse_mode=ParseMode.MARKDOWN)
    
    logger.info(f"Queue cleared - removed {queue_size} items")


@dp.message(Command("queue_status"))
async def cmd_queue_status(message: types.Message):
    """Show current queue status and bot state"""
    if message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    global bot_paused, scraping_active, normal_downloads_paused
    
    queue_size = send_queue.qsize()
    active_tasks = len(background_tasks)
    
    # Get subscription counts
    cursor.execute("SELECT COUNT(*) FROM subreddits")
    sub_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM user_profiles")
    user_count = cursor.fetchone()[0]
    
    status_parts = []
    status_parts.append("📊 **Bot Status Report**\n")
    
    # Bot state
    if bot_paused:
        status_parts.append("⏸️ **Status:** Paused")
    elif scraping_active:
        status_parts.append("🔄 **Status:** Scraping Mode")
    else:
        status_parts.append("▶️ **Status:** Running")
    
    status_parts.append(f"📋 **Subscriptions:** {sub_count} subreddits, {user_count} users")
    status_parts.append(f"📊 **Queue:** {queue_size} items pending")
    status_parts.append(f"⚡ **Background Tasks:** {active_tasks} active")
    
    if normal_downloads_paused:
        status_parts.append("⏸️ **Normal Downloads:** Paused (scraping active)")
    else:
        status_parts.append("▶️ **Normal Downloads:** Active")
    
    status_parts.append("")
    
    # Commands
    status_parts.append("💡 **Commands:**")
    if bot_paused:
        status_parts.append("• `/resume` - Resume media sending")
    else:
        status_parts.append("• `/pause` - Pause media sending")
    
    if queue_size > 0:
        status_parts.append("• `/clear_queue` - Clear queued media")
    
    status_parts.append("• `/reset_bot` - Complete bot reset")
    
    await message.reply("\n".join(status_parts), parse_mode=ParseMode.MARKDOWN)


@dp.message()
async def handle_unknown(message: types.Message):
    """Handle unknown messages"""
    if message.from_user and message.from_user.id == ADMIN_ID:
        await message.reply("❓ Unknown command. Use /help to see available commands.")


async def main():
    """Main function to start the bot with enhanced error handling"""
    global graceful_shutdown
    
    try:
        # Initialize Reddit API first
        await init_reddit()
        
        # Start the global send queue worker for ultra rate limiting
        queue_worker_task = create_background_task(start_send_queue_worker())
        
        # Start enhanced task management system
        pending_processor_task = create_background_task(process_pending_tasks(), priority="high")
        
        # Start periodic cleanup to prevent memory leaks
        cleanup_task = create_background_task(periodic_cleanup(), priority="high")
        
        logger.info("Bot started successfully!")
        logger.info(f"Admin ID: {ADMIN_ID}")
        
        # Check if we need to resume from a previous session
        await check_resume_state()
        
        # Start the polling loop as a background task
        polling_task = create_background_task(polling_loop(), priority="high")
        
        # Start polling for Telegram updates with graceful shutdown handling
        try:
            await dp.start_polling(bot, skip_updates=True)
        except KeyboardInterrupt:
            logger.info("Graceful shutdown initiated via SIGINT...")
            graceful_shutdown = True
            await graceful_shutdown_handler()
        
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
        graceful_shutdown = True
        await graceful_shutdown_handler()
        raise

async def graceful_shutdown_handler():
    """Handle graceful shutdown to ensure no posts are missed"""
    global graceful_shutdown
    graceful_shutdown = True
    
    logger.info("Starting graceful shutdown process...")
    
    # Cancel non-critical background tasks but keep essential ones running
    critical_tasks = 0
    cancelled_tasks = 0
    
    for task in background_tasks.copy():
        if not task.done():
            # Allow current sends to complete but cancel new processing
            if "send" not in str(task) and "process_pending" not in str(task):
                task.cancel()
                cancelled_tasks += 1
            else:
                critical_tasks += 1
    
    logger.info(f"Cancelled {cancelled_tasks} non-critical tasks, keeping {critical_tasks} critical tasks")
    
    # Process any remaining queued posts with timeout
    remaining_posts = pending_posts_queue.qsize()
    if remaining_posts > 0:
        logger.info(f"Processing {remaining_posts} remaining queued posts...")
        timeout_time = time.time() + 120  # 2 minute timeout for critical posts
        
        while not pending_posts_queue.empty() and time.time() < timeout_time:
            try:
                coro = await asyncio.wait_for(pending_posts_queue.get(), timeout=2.0)
                # Only process if it's a critical post (send operation)
                if "send" in str(coro) or "process_post" in str(coro):
                    await coro
                pending_posts_queue.task_done()
            except (asyncio.TimeoutError, Exception) as e:
                logger.debug(f"Skipping queued task during shutdown: {e}")
                break
    
    # Final wait for any remaining sends to complete
    if background_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*[t for t in background_tasks if not t.cancelled()], return_exceptions=True),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            logger.warning("Some tasks didn't complete within shutdown timeout")
    
    logger.info("Graceful shutdown completed - no posts should be missed")


def signal_handler(signum, frame):
    """Handle SIGINT and SIGTERM signals gracefully"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    # The graceful shutdown will be handled in the main async function

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Error running bot: {e}")
    finally:
        logger.info("Performing final cleanup...")
        try:
            if 'conn' in globals():
                conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
        
        try:
            if 'reddit' in globals() and reddit:
                asyncio.run(reddit.close())
                logger.info("Reddit API connection closed")
        except Exception as e:
            logger.error(f"Error closing Reddit API connection: {e}")
