from typing import List
from aiogram.types import InlineKeyboardMarkup
import dj_database_url
import psycopg2

from config import DATABASELINK_POSTGRES

db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
connection = psycopg2.connect(database=db_info.get('NAME'),
                              user=db_info.get('USER'),
                              password=db_info.get('PASSWORD'),
                              host=db_info.get('HOST'),
                              port=db_info.get('PORT'))

def get_errors_list_nms() -> List[str]:
    cursor = connection.cursor()
    cursor.execute("select resource_nm\n"
                   "from etl_cfg.cfg_status_table\n"
                   "where status_cd='P'\n")
    query_results = cursor.fetchall()
    list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    return list_results


def cnt_list_obj(list):
    count_of_objects = 0
    for obj in list:
        count_of_objects = count_of_objects + 1

    return count_of_objects


def errors_keyboard():
    markup = InlineKeyboardMarkup()
    err_processes = get_errors_list_nms()
    print(err_processes)
    if cnt_list_obj(get_errors_list_nms()) > 0:
        for resource in err_processes:
            button_text = resource
            print(button_text)

        markup.insert(InlineKeyboardMarkup(text=button_text))
    return markup
