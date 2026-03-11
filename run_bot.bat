@echo off
title Reddit to Telegram Bot

echo ========================================
echo  Reddit to Telegram Bot - Starting...
echo ========================================

REM Change to the script's directory
cd /d "%~dp0"

REM Activate the virtual environment
echo Activating virtual environment...
call ..\reddit_bot_env\Scripts\activate.bat

REM Run the bot
echo Starting the bot...
python main.py

REM Pause to see any error messages
echo.
echo Bot has stopped. Press any key to close this window.
pause >nul