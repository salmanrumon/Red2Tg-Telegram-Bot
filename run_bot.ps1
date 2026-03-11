# Reddit to Telegram Bot - PowerShell Script

Write-Host "========================================" -ForegroundColor Green
Write-Host " Reddit to Telegram Bot - Starting..." -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Change to the script's directory
Set-Location $PSScriptRoot

# Activate the virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
& ..\reddit_bot_env\Scripts\Activate.ps1

# Run the bot
Write-Host "Starting the bot..." -ForegroundColor Yellow
python main.py

# Pause to see any error messages
Write-Host ""
Write-Host "Bot has stopped. Press any key to close this window." -ForegroundColor Cyan
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")