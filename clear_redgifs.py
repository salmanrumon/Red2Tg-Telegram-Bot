#!/usr/bin/env python3
import sqlite3

# Connect to the database
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

print("=== Before cleanup ===")
cursor.execute("SELECT COUNT(*) FROM shared_posts WHERE post_id LIKE 'redgifs_%';")
count_before = cursor.fetchone()[0]
print(f"RedGifs entries: {count_before}")

# Clear any existing RedGifs entries (both api and other formats)
cursor.execute("DELETE FROM shared_posts WHERE post_id LIKE 'redgifs_%';")
deleted_count = cursor.rowcount
conn.commit()

print(f"\n=== Cleanup complete ===")
print(f"Deleted {deleted_count} RedGifs entries")

cursor.execute("SELECT COUNT(*) FROM shared_posts WHERE post_id LIKE 'redgifs_%';")
count_after = cursor.fetchone()[0]
print(f"RedGifs entries remaining: {count_after}")

conn.close()
print("\nRedGifs entries cleared. You can now try scraping again.")