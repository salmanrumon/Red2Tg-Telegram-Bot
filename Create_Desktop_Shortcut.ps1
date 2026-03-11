# Create Desktop Shortcut for Reddit to Telegram Bot

# Get the path to the current script's directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path

# Path to the batch file
$batchFile = Join-Path $scriptPath "run_bot.bat"

# Path to the desktop
$desktop = [Environment]::GetFolderPath("Desktop")

# Path for the shortcut
$shortcutPath = Join-Path $desktop "Reddit Bot.lnk"

# Create the shortcut
$WshShell = New-Object -ComObject WScript.Shell
$shortcut = $WshShell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $batchFile
$shortcut.WorkingDirectory = $scriptPath
$shortcut.IconLocation = "shell32.dll,13"
$shortcut.Save()

Write-Host "Desktop shortcut created successfully!" -ForegroundColor Green
Write-Host "You can now run the bot by double-clicking 'Reddit Bot' on your desktop." -ForegroundColor Yellow
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")