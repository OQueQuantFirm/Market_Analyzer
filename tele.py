from telethon import TelegramClient, utils, errors

class TelegramHandler:
    def __init__(self, api_id, api_hash, bot_token, group_username, session_name):
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.group_username = group_username
        self.session_name = session_name

    async def send_message(self, message):
        try:
            client = TelegramClient(self.session_name, self.api_id, self.api_hash)
            await client.start(bot_token=self.bot_token)

            entity = await client.get_entity(self.group_username)
            await client.send_message(entity, message)

        except Exception as e:
            print(f"Error sending message: {e}")

        finally:
            await client.disconnect()
