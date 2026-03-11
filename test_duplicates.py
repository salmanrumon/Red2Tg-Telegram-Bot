#!/usr/bin/env python3
"""
Test script for duplicate detection system
"""

import sqlite3
import hashlib
import time
from datetime import datetime

def generate_media_hash(media_url: str, post_title: str = "", post_author: str = "") -> str:
    """Generate a hash for media content to detect duplicates"""
    # Create a hash based primarily on URL to catch the same media shared in different posts
    content_string = f"{media_url.lower()}"
    return hashlib.md5(content_string.encode('utf-8')).hexdigest()

def test_duplicate_detection():
    """Test the duplicate detection system"""
    print("🔍 Testing Duplicate Detection System")
    print("=" * 50)
    
    # Connect to database
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    
    # Create media_content table if it doesn't exist
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
    conn.commit()
    print("✅ Database table created/verified")
    print()
    
    # Test URLs
    test_cases = [
        {
            "url": "https://i.redd.it/example1.jpg",
            "title": "First Post",
            "author": "user1"
        },
        {
            "url": "https://i.redd.it/example1.jpg",  # Same URL, different title/author
            "title": "Second Post with Different Title",
            "author": "user2"
        },
        {
            "url": "https://i.redd.it/example2.jpg",  # Different URL
            "title": "Third Post",
            "author": "user3"
        }
    ]
    
    print("📊 Test Cases:")
    for i, case in enumerate(test_cases, 1):
        hash_value = generate_media_hash(case["url"], case["title"], case["author"])
        print(f"{i}. URL: {case['url']}")
        print(f"   Title: {case['title']}")
        print(f"   Author: {case['author']}")
        print(f"   Hash: {hash_value}")
        print()
    
    print("🧪 Running Tests:")
    print("-" * 30)
    
    for i, case in enumerate(test_cases, 1):
        print(f"Test {i}: {case['url']}")
        
        # Generate hash
        media_hash = generate_media_hash(case["url"], case["title"], case["author"])
        
        # Check if it exists in database
        cursor.execute("""
            SELECT media_url, post_id, subreddit, username, first_seen_time, shared_count 
            FROM media_content 
            WHERE media_hash = ?
        """, (media_hash,))
        
        row = cursor.fetchone()
        
        if row:
            print(f"   ❌ Duplicate detected!")
            print(f"   📊 Share count: {row[5]}")
            print(f"   📅 First seen: {datetime.fromtimestamp(row[4]).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📍 Original subreddit: r/{row[2] or 'unknown'}")
        else:
            print(f"   ✅ Not a duplicate - would be processed")
            
            # Simulate recording the media
            cursor.execute("""
                INSERT OR REPLACE INTO media_content 
                (media_hash, media_url, post_id, subreddit, username, first_seen_time, shared_count)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (media_hash, case["url"], f"test_post_{i}", "test_subreddit", case["author"], int(time.time())))
            
            print(f"   💾 Recorded in database")
        
        print()
    
    # Show database statistics
    print("📈 Database Statistics:")
    print("-" * 30)
    
    cursor.execute("SELECT COUNT(*) FROM media_content")
    total_media = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM media_content WHERE shared_count > 1")
    duplicates = cursor.fetchone()[0]
    
    print(f"Total media tracked: {total_media}")
    print(f"Duplicate media: {duplicates}")
    
    if total_media > 0:
        prevention_rate = (duplicates / total_media) * 100
        print(f"Duplicate prevention rate: {prevention_rate:.1f}%")
    
    # Show recent entries
    print("\n🕒 Recent Media Entries:")
    print("-" * 30)
    
    cursor.execute("""
        SELECT media_url, shared_count, first_seen_time, subreddit
        FROM media_content 
        ORDER BY first_seen_time DESC 
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        url, count, timestamp, subreddit = row
        display_url = url[:50] + "..." if len(url) > 50 else url
        time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        print(f"• {display_url}")
        print(f"  📊 {count} attempts | {time_str} | r/{subreddit or 'unknown'}")
        print()
    
    conn.commit()
    conn.close()
    
    print("✅ Test completed!")

if __name__ == "__main__":
    test_duplicate_detection()
