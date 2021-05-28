from aiogram import Bot, Dispatcher, executor
import asyncio
from config import TG_TOKEN

loop = asyncio.get_event_loop()
bot = Bot(token=TG_TOKEN, parse_mode='HTML')
dp = Dispatcher(bot=bot, loop=loop)

if __name__ == '__main__':
    from handlers import dp, send_to_admin
    executor.start_polling(dispatcher=dp, on_startup=send_to_admin)