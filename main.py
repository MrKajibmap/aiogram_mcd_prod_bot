from aiogram import Bot, Dispatcher, executor
import asyncio

from aiogram.types import InlineKeyboardButton

from config import TG_TOKEN, DATABASELINK_POSTGRES
import dj_database_url
import psycopg2

loop = asyncio.get_event_loop()
bot = Bot(token=TG_TOKEN, parse_mode='HTML')
dp = Dispatcher(bot=bot, loop=loop)

db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
connection = psycopg2.connect(database=db_info.get('NAME'),
                              user=db_info.get('USER'),
                              password=db_info.get('PASSWORD'),
                              host=db_info.get('HOST'),
                              port=db_info.get('PORT'))

if __name__ == '__main__':
    # from handlers import dp, send_to_admin
    #
    # executor.start_polling(dispatcher=dp, on_startup=send_to_admin)
    import time
    def get_modules_list(l: list) -> list:
        for obj in l:
            print(obj)
        return l
    get_modules_list([1,2])
    pr='1_23'
    print(pr)
    if pr.find('+')!=-1:
        print(f"result={pr.find('+')}")
    elif call_data.split('+')[1] == 'single_resource':
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(InlineKeyboardButton('Запустить', callback_data=f"{call_data.split('+')[0]}+single_res_act_rerun"), (
            InlineKeyboardButton('Узнать статус', callback_data=f"{call_data.split('+')[0]}+single_res_act_status")),
            InlineKeyboardButton('Закрыть', callback_data=f"{call_data.split('+')[0]}+single_res_act_close"),
            InlineKeyboardButton('Пропустить', callback_data=f"{call_data.split('+')[0]}+single_res_act_skip"))
        # заряжаем новую
        await bot.edit_message_text(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            text=f"Как обработать ресурс {call_data.split('+')[0]}?\n",
            reply_markup=markup
        )
