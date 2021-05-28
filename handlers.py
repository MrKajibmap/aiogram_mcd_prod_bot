from main import bot, dp
from config import ADMIN_ID, button_help_text, button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, DATABASELINK_POSTGRES
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.dispatcher.filters import Text
import dj_database_url
import psycopg2

reply_markup = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=button_help_text),
            KeyboardButton(text=button_request_errors_text),
            KeyboardButton(text=button_request_status_text),
            KeyboardButton(text=button_request_rtp_text),
            KeyboardButton(text=button_request_vf_text),
        ],
    ],
    resize_keyboard=True,
)


async def send_to_admin(dp):
    await bot.send_message(chat_id=ADMIN_ID, text='Бот запущен', reply_markup=reply_markup)


async def button_help_handler(bot: bot):
    txt = '<b>HELP:</b>\n'
    txt += '<b>/start</b> Ну че, народ, погнали?!\n'
    txt += '<b>/request_rtp</b> Показать последнюю сводку по краткосрочному прогнозу\n'
    txt += '<b>/request_vf</b> Показать последюю сводку по долгосрочному прогнозу\n'
    txt += '<b>/request</b> Показать ошибки с начала цикла (17:00)\n'
    txt += '<b>/request_etl</b> Отображение общей сводки в системе\n'
    txt += '<b> INFO </b> Бот создан для получения запросов к базе данных Postgresql проекта Macdonalds\n'
    bot.send_message(ADMIN_ID, txt, parse_mode='HTML')


@dp.message_handler(Text(equals=button_help_text))
async def show_help(message: Message):
    txt = '<b>HELP:</b>\n'
    txt += '<b>/start</b> Ну че, народ, погнали?!\n'
    txt += '<b>/request_rtp</b> Показать последнюю сводку по краткосрочному прогнозу\n'
    txt += '<b>/request_vf</b> Показать последюю сводку по долгосрочному прогнозу\n'
    txt += '<b>/request</b> Показать ошибки с начала цикла (17:00)\n'
    txt += '<b>/request_etl</b> Отображение общей сводки в системе\n'
    txt += '<b> INFO </b> Бот создан для получения запросов к базе данных Postgresql проекта Macdonalds\n'
    await message.answer(text=txt)


@dp.message_handler(Text(equals=button_request_errors_text))
async def show_errors(message: Message):
    db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
    connection = psycopg2.connect(database=db_info.get('NAME'),
                                  user=db_info.get('USER'),
                                  password=db_info.get('PASSWORD'),
                                  host=db_info.get('HOST'),
                                  port=db_info.get('PORT'))
    cursor = connection.cursor()
    await message.answer(text='Я отправил реквест в базу данных для извлечения статистики ошибок в системе за '
                              'сегодня, ждите!')
    cursor.execute(""" select t1.* from 
        (
        select cast(lower(process_nm) as varchar(32)) as process_nm,
        				case when status_description='' and end_dttm is null then 'In progress' 
    					when status_description='' and end_dttm is not null then 'Success' 
        				else cast(status_description as varchar(50)) 
        				end as status_description
        				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
        				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
        				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
        					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
        					end as exec_tm
        				, cast(user_nm as varchar(15)) as user
        				from etl_cfg.cfg_log_event 
        				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
        				and status_cd=1
        				order by start_dttm desc 
        				limit 40
        				) t1 
    			order by start_dttm
        				""")

    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_status_text))
async def show_errors(message: Message):
    db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
    connection = psycopg2.connect(database=db_info.get('NAME'),
                                  user=db_info.get('USER'),
                                  password=db_info.get('PASSWORD'),
                                  host=db_info.get('HOST'),
                                  port=db_info.get('PORT'))
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики в системе за сегодня (ограничение = '
             '30 записей), ждите!')

    cursor.execute(""" select t1.* from 
           (
           select cast(lower(process_nm) as varchar(32)) as process_nm,
           				case when status_description='' and end_dttm is null then 'In progress' 
       					when status_description='' and end_dttm is not null then 'Success' 
           				else cast(status_description as varchar(50)) 
           				end as status_description
           				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
           				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
           				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
           					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
           					end as exec_tm
           				, cast(user_nm as varchar(15)) as user
           				from etl_cfg.cfg_log_event 
           				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
           				order by start_dttm desc 
           				limit 40
           				) t1 
       			order by start_dttm
           				""")
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_rtp_text))
async def show_errors(message: Message):
    db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
    connection = psycopg2.connect(database=db_info.get('NAME'),
                                  user=db_info.get('USER'),
                                  password=db_info.get('PASSWORD'),
                                  host=db_info.get('HOST'),
                                  port=db_info.get('PORT'))
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')
    cursor.execute("""select t1.* from
    (
    select cast(lower(process_nm) as varchar(32)) as process_nm,
        				case when status_description='' and end_dttm is null then 'In progress' 
    					when status_description='' and end_dttm is not null then 'Success' 
        				else cast(status_description as varchar(50)) 
        				end as status_description
        				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
        				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
        				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
        					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
        					end as exec_tm
        				, cast(user_nm as varchar(15)) as user
        				from etl_cfg.cfg_log_event 
        				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
     					and lower(process_nm) like 'rtp_%'
        				order by start_dttm desc 
        				limit 30
    	) t1 order by start_dttm """)
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_vf_text))
async def show_errors(message: Message):
    db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
    connection = psycopg2.connect(database=db_info.get('NAME'),
                                  user=db_info.get('USER'),
                                  password=db_info.get('PASSWORD'),
                                  host=db_info.get('HOST'),
                                  port=db_info.get('PORT'))
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')
    cursor.execute("""select t1.* from
(
select cast(lower(process_nm) as varchar(32)) as process_nm,
    				case when status_description='' and end_dttm is null then 'In progress' 
					when status_description='' and end_dttm is not null then 'Success' 
    				else cast(status_description as varchar(50)) 
    				end as status_description
    				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
    				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
    				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
    					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
    					end as exec_tm
    				, cast(user_nm as varchar(15)) as user
    				from etl_cfg.cfg_log_event 
    				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
 					and lower(process_nm) like 'vf_%'
    				order by start_dttm desc 
    				limit 30
	) t1 order by start_dttm """)
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )
