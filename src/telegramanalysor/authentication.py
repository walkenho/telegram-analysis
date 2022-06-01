from pathlib import Path
import configparser
from telethon.sync import TelegramClient


def create_client():
    # Reading Configs
    config = configparser.ConfigParser()
    config.read(Path.cwd().parent / ".env")

    # Setting configuration values
    api_id = config['Telegram']['api_id']
    api_hash = str(config['Telegram']['api_hash'])

    # phone = config['Telegram']['phone']
    username = config['Telegram']['username']

    return TelegramClient(username, api_id, api_hash)
