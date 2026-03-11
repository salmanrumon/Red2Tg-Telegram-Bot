# Duplicate Detection System Fix

## Problem
The bot was downloading and sharing duplicate posts despite having duplicate detection mechanisms in place. The main issues were:

1. **Inconsistent duplicate checking** - Media duplicate detection was only done in `send_post_to_chat` but not in the main `send_post` function
2. **Overly specific media hashing** - The media hash included post title and author, making the same media URL with different titles/authors appear as different content
3. **Incomplete coverage** - Not all media types were being checked for duplicates consistently

## Solution

### 1. Improved Media Hash Generation
**Before:**
```python
def generate_media_hash(media_url: str, post_title: str = "", post_author: str = "") -> str:
    content_string = f"{media_url.lower()}|{post_title.lower()}|{post_author.lower()}"
    return hashlib.md5(content_string.encode('utf-8')).hexdigest()
```

**After:**
```python
def generate_media_hash(media_url: str, post_title: str = "", post_author: str = "") -> str:
    # Create a hash based primarily on URL to catch the same media shared in different posts
    content_string = f"{media_url.lower()}"
    return hashlib.md5(content_string.encode('utf-8')).hexdigest()
```

**Why this works better:**
- Same media URL will always generate the same hash, regardless of post title or author
- Catches duplicates even when the same media is posted by different users or with different titles
- More reliable duplicate detection across different subreddits and users

### 2. Centralized Duplicate Checking
**Before:** Media duplicate checking was only done in `send_post_to_chat` function
**After:** Added comprehensive duplicate checking in the main `send_post` function

```python
async def send_post(post, source_type="subreddit", source_name=None):
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
            await increment_media_share_count(post.url, post.title, post.author.name if post.author else "")
            return
```

### 3. Enhanced Duplicate Statistics
Added improved `/duplicates` command with:
- Duplicate prevention rate calculation
- Recent duplicate attempts tracking
- Better formatting and information display

### 4. New Testing Commands
- `/test_duplicate <URL>` - Test duplicate detection with specific URL
- Enhanced `/test` command to show duplicate detection status
- Standalone test script (`test_duplicates.py`) for verification

## How It Works

### Two-Level Duplicate Detection

1. **Post-Level Detection** (using `shared_posts` table):
   - Prevents the same Reddit post from being shared multiple times
   - Uses Reddit post ID as unique identifier
   - Works for all post types (text, media, links)

2. **Media-Level Detection** (using `media_content` table):
   - Prevents the same media URL from being shared multiple times
   - Uses URL-based hash as unique identifier
   - Catches duplicates even when same media is posted by different users or in different subreddits

### Duplicate Detection Flow

```
1. Post arrives from Reddit
   ↓
2. Check if post ID already in shared_posts table
   ↓ (if not found)
3. Check if media URL already in media_content table
   ↓ (if not found)
4. Process and send the post
   ↓
5. Record post ID in shared_posts table
6. Record media URL in media_content table
```

## Testing Results

The test script demonstrates the system working correctly:

```
Test 1: https://i.redd.it/example1.jpg
   ✅ Not a duplicate - would be processed
   💾 Recorded in database

Test 2: https://i.redd.it/example1.jpg (same URL, different title/author)
   ❌ Duplicate detected!
   📊 Share count: 1
   📅 First seen: 2025-08-31 23:40:33
   📍 Original subreddit: r/test_subreddit
```

## Commands for Management

- `/duplicates` - Show duplicate statistics and recent attempts
- `/clear_duplicates` - Clear all media duplicate tracking data
- `/clear_history` - Clear post sharing history
- `/test_duplicate <URL>` - Test duplicate detection with specific URL
- `/test` - Test bot responsiveness and duplicate detection status

## Benefits

1. **Eliminates duplicate posts** - Same media won't be shared multiple times
2. **Cross-subreddit detection** - Media shared in one subreddit won't be shared again from another
3. **Cross-user detection** - Media posted by one user won't be shared again when posted by another user
4. **Better user experience** - No more spam of the same content
5. **Efficient resource usage** - Prevents unnecessary downloads and uploads
6. **Comprehensive tracking** - Detailed statistics on duplicate prevention effectiveness

## Database Schema

The system uses two tables:

**shared_posts table:**
- `post_id` (TEXT, PRIMARY KEY) - Reddit post ID
- `subreddit` (TEXT) - Subreddit name
- `username` (TEXT) - Username (for user profile posts)
- `shared_time` (INTEGER) - Timestamp when shared

**media_content table:**
- `media_hash` (TEXT, UNIQUE) - MD5 hash of media URL
- `media_url` (TEXT) - Original media URL
- `post_id` (TEXT) - Associated Reddit post ID
- `subreddit` (TEXT) - Subreddit where first seen
- `username` (TEXT) - Username where first seen
- `first_seen_time` (INTEGER) - Timestamp when first seen
- `shared_count` (INTEGER) - Number of times this media was attempted

This dual-table approach ensures both post-level and media-level duplicate prevention, providing comprehensive protection against duplicate content.




