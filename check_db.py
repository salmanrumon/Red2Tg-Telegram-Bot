#!/usr/bin/env python3
import sqlite3
import time
from datetime import datetime

# Connect to the database
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# First, check the table structure
print("=== Table structure ===")
cursor.execute("PRAGMA table_info(shared_posts);")
columns = cursor.fetchall()
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# Check for RedGifs entries
print("\n=== RedGifs entries in shared_posts ===")
cursor.execute("SELECT * FROM shared_posts WHERE post_id LIKE 'redgifs_api_%' ORDER BY shared_time DESC LIMIT 20;")
results = cursor.fetchall()

if results:
    print(f"Found {len(results)} RedGifs entries:")
    for result in results:
        print(f"  {result}")
else:
    print("No RedGifs entries found.")

print("\n=== Recent shared_posts entries ===")
cursor.execute("SELECT * FROM shared_posts ORDER BY shared_time DESC LIMIT 10;")
recent = cursor.fetchall()

for result in recent:
    print(f"  {result}")

# Check total count
cursor.execute("SELECT COUNT(*) FROM shared_posts;")
total = cursor.fetchone()[0]
print(f"\nTotal shared_posts entries: {total}")

# Check if there are any entries with the specific user we're testing
test_username = "fallen.emoangel"
cursor.execute("SELECT * FROM shared_posts WHERE post_id LIKE ? ORDER BY shared_time DESC;", (f'%{test_username}%',))
user_results = cursor.fetchall()

print(f"\n=== Entries for user '{test_username}' ===")
if user_results:
    print(f"Found {len(user_results)} entries for this user:")
    for result in user_results:
        print(f"  {result}")
else:
    print("No entries found for this specific user.")

conn.close()