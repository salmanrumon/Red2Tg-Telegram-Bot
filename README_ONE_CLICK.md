# One-Click Bot Runner

This folder contains scripts to run the Reddit to Telegram bot with a single click.

## Files

- `run_bot.bat` - Windows batch script (recommended for most users)
- `run_bot.ps1` - PowerShell script (alternative for PowerShell users)

## How to Run the Bot with One Click

### Method 1: Using the Batch File (Recommended)

1. Double-click on `run_bot.bat`
2. The bot will start automatically

### Method 2: Using the PowerShell Script

1. Right-click on `run_bot.ps1`
2. Select "Run with PowerShell"
3. The bot will start automatically

### Method 3: Desktop Shortcut (One-time setup)

1. Run `Create_Desktop_Shortcut.ps1` once to create a desktop shortcut
2. After that, you can run the bot by double-clicking the "Reddit Bot" shortcut on your desktop

## Requirements

- Windows operating system
- Python 3.11 or higher
- All required dependencies (already installed in the virtual environment)

## Troubleshooting

If you encounter any issues:

1. Make sure the `.env` file is properly configured with your:
   - Telegram Bot Token
   - Reddit API credentials
   - Admin ID

2. Ensure the virtual environment (`..\reddit_bot_env`) exists and is properly set up

3. If you get permission errors with the PowerShell script, you may need to run:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

## Stopping the Bot

To stop the bot, simply close the command window or press `Ctrl+C` in the window.