import os

def update_env_file():
    """Helper script to update the .env file with your credentials"""
    
    # Read the current .env file
    try:
        with open('.env', 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("Error: .env file not found!")
        return
    
    print("Reddit to Telegram Bot - Environment Configuration")
    print("=" * 50)
    
    # Get user input for each required field
    telegram_token = input("Enter your Telegram Bot Token (from @BotFather): ").strip()
    reddit_client_id = input("Enter your Reddit Client ID: ").strip()
    reddit_client_secret = input("Enter your Reddit Client Secret: ").strip()
    admin_id = input("Enter your Telegram User ID (from @userinfobot): ").strip()
    
    # Update the lines with user input
    updated_lines = []
    for line in lines:
        if line.startswith('TELEGRAM_TOKEN='):
            updated_lines.append(f'TELEGRAM_TOKEN={telegram_token}\n')
        elif line.startswith('REDDIT_CLIENT_ID='):
            updated_lines.append(f'REDDIT_CLIENT_ID={reddit_client_id}\n')
        elif line.startswith('REDDIT_CLIENT_SECRET='):
            updated_lines.append(f'REDDIT_CLIENT_SECRET={reddit_client_secret}\n')
        elif line.startswith('ADMIN_ID='):
            updated_lines.append(f'ADMIN_ID={admin_id}\n')
        else:
            updated_lines.append(line)
    
    # Write the updated content back to the file
    with open('.env', 'w') as f:
        f.writelines(updated_lines)
    
    print("\n.env file has been updated successfully!")
    print("You can now run the bot with: python main.py")

if __name__ == "__main__":
    update_env_file()