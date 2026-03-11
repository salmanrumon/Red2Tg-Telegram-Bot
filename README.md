# Reddit to Telegram Bot

A Python Telegram bot that monitors Reddit subreddits and automatically forwards new posts to Telegram chats.

## Features

- 🤖 **Telegram Bot Integration**: Built with aiogram v3 for reliable Telegram API interaction
- 🔄 **Automatic Monitoring**: Polls subscribed subreddits every 60 seconds for new posts
- 📱 **Multi-Content Support**: Handles text posts, images, videos, and galleries
- 🔞 **NSFW Detection**: Automatically labels NSFW content
- 🛡️ **Admin Controls**: Secure admin-only commands for managing subscriptions
- 💾 **Persistent Storage**: SQLite database for tracking subreddits and post history
- 📊 **Rich Formatting**: HTML-formatted messages with proper links and metadata

## Quick Start

### 1. Get Your API Keys

**Telegram Bot Token:**
1. Open Telegram and message [@BotFather](https://t.me/botfather)
2. Send `/newbot` and follow the instructions
3. Save the bot token you receive

**Reddit API Credentials:**
1. Go to [Reddit Apps](https://www.reddit.com/prefs/apps)
2. Click "Create App" or "Create Another App"
3. Choose "script" as the app type
4. Save the Client ID and Client Secret

**Your Telegram User ID:**
1. Message [@userinfobot](https://t.me/userinfobot) on Telegram
2. Save the User ID number it sends you

### 2. Setup Environment Variables

Create a `.env` file (copy from `.env.template`) and add:
- `TELEGRAM_TOKEN` - Your bot token from BotFather
- `REDDIT_CLIENT_ID` - Your Reddit app client ID
- `REDDIT_CLIENT_SECRET` - Your Reddit app client secret  
- `ADMIN_ID` - Your Telegram user ID (numbers only)

### 3. Run the Bot

```bash
python main.py
```

### 4. One-Click Operation (Local)

For local installations, you can run the bot with one click using the provided scripts:

1. Double-click `run_bot.bat` to start the bot
2. Or run `Create_Desktop_Shortcut.ps1` once to create a desktop shortcut for true one-click operation

See `README_ONE_CLICK.md` for detailed instructions.

## Bot Commands

Once your bot is running, send these commands in Telegram:

- `/start` or `/help` - Show help message
- `/add <subreddit>` - Subscribe to a subreddit (e.g., `/add python`)
- `/remove <subreddit>` - Unsubscribe from a subreddit
- `/list` - Show all subscribed subreddits

**Note:** Only the admin (user with the ADMIN_ID) can use these commands.

## How It Works

The bot continuously monitors your subscribed subreddits every 60 seconds. When it finds new posts, it sends them to your Telegram chat with:

- 📝 **Post title and content** (text posts are truncated to 500 characters)
- 🖼️ **Images and galleries** (sent as photo messages)
- 🎥 **Videos** (with direct links when available)
- 🔗 **External links** (for other content types)
- 🕒 **Timestamp** and subreddit information
- 🔞 **NSFW warning** for adult content

## Installation (Local Development)

1. **Install required Python packages**:
   ```bash
   pip install aiogram praw python-dotenv
   ```

2. **Create environment file**:
   ```bash
   cp .env.template .env
   ```

3. **Fill in your API credentials in the .env file**

4. **Run the bot**:
   ```bash
   python main.py
   ```

## Technical Details

- **Framework**: aiogram v3 (Telegram Bot API)
- **Reddit API**: PRAW (Python Reddit API Wrapper)
- **Database**: SQLite (local file storage)
- **Polling Interval**: 60 seconds
- **Rate Limiting**: Built-in delays between API calls
- **Content Types**: Text, images, videos, galleries, external links
- **Error Handling**: Comprehensive logging and graceful error recovery

## Project Structure

```
├── main.py              # Main bot application
├── .env.template        # Environment variables template
├── README.md            # This file
└── data.db              # SQLite database (created automatically)
```

## Troubleshooting

**Bot not responding to commands:**
- Check that your ADMIN_ID is correct (message @userinfobot)
- Verify your Telegram bot token is valid
- Ensure the bot is running without errors

**Reddit API errors:**
- Verify your Reddit client ID and secret are correct
- Check that you selected "script" as the app type
- Make sure your Reddit user agent is descriptive

**Missing posts:**
- The bot only shows new posts after you subscribe
- Check the bot logs for any API rate limiting messages
- Verify the subreddit name is spelled correctly

## License

This project is open source and available under the MIT License.
   