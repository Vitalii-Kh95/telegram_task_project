API_ID = 20752661
API_HASH = "f76ddf320dac0fa2681006e35e8e8081"
CHAT_IDENTIFIER = "Python"  # Fallback group identifier


from telethon import TelegramClient

client = TelegramClient("session", API_ID, API_HASH)  # For easy import =)
