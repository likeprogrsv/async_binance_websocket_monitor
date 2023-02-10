import asyncio
import asyncpg
import datetime
from aio_binance.futures.usdt import WsClient
import os


SYMBOL = 'XRPUSDT'

# Запрос в БД для вставки новой строки с данными
db_insert = '''
                INSERT INTO crypto_monitor(pair_name, time, price)
                VALUES($1, $2, $3)
            '''

# Запрос в БД для получения максимального значения цены
# в скользящем интервале который равен 1 часу.
db_max_price_1h = '''
                    SELECT MAX(price) FROM crypto_monitor
                    WHERE time > current_timestamp - interval '1' hour
                  '''

HOST = os.environ.get('DB_HOST')
DATABASE = 'crypto_test'
USER = 'postgres'
PASSWORD = 'postgres'

max_price_1h_roll_window: float = 0

# Наше целевое значение процента при котором будет выведено сообщение
percent: float = 1
# Добавим шаг изменения процента для вывода сообщения в консоли
percent_step: float = 1

# Коды для изменения цвета сообщений в консоли
RED_TEXT = '\033[91m'
END_COLOR = '\033[0m'
BLUE_TEXT = '\033[94m'


async def db_connect_create_if_not_exists(host, user, password, database,
                                          port=5432,):
    try:
        conn = await asyncpg.connect(
                host=host, user=user, password=password,
                database=database, port=port,)

    except (asyncpg.exceptions.InvalidCatalogNameError,
            asyncpg.exceptions.ConnectionDoesNotExistError):
        # Database does not exist, create it.
        sys_conn = await asyncpg.connect(
            host=host,
            database='postgres',
            user='postgres',
            password=password,
        )
        await sys_conn.execute(
            f'CREATE DATABASE "{database}" OWNER "{user}"'
        )
        await sys_conn.close()
        # Connect to the newly created database.
        conn = await asyncpg.connect(host=host,
                                     user=user,
                                     database=database,
                                     password=password)
        await conn.execute('''
            CREATE TABLE crypto_monitor(
                id serial PRIMARY KEY,
                pair_name text,
                time timestamp,
                price float
                )
            ''')
    return conn


async def make_db_pool():
    return await asyncpg.create_pool(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                )


async def make_db_request():
    # Wait for websocket connection
    await asyncio.sleep(5)
    global max_price_1h_roll_window
    while True:
        if event:
            time = datetime.datetime.now()
            await db_pool.fetch(db_insert, SYMBOL, time, float(event.get('p')))
            # print('\nCREATE A NEW ROW IN DB\n')
        max_price_1h_roll_window = await db_pool.fetchval(db_max_price_1h)
        await asyncio.sleep(1)


async def event_agg_trade(data: dict):
    global max_price_1h_roll_window, percent, percent_step
    curr_price = float(data['p'])
    if curr_price > max_price_1h_roll_window:
        max_price_1h_roll_window = curr_price
    price_delta = float(100 - (curr_price / max_price_1h_roll_window * 100))
    if price_delta >= percent:
        print(RED_TEXT + f'{datetime.datetime.now()} - '
              f'Цена фьючерса {SYMBOL} снизилась за последний час '
              f'от максимального значения на {round(price_delta, 2)} %'
              + END_COLOR)
        percent += percent_step
    elif price_delta < percent - percent_step:
        percent -= percent_step


async def adapter_event(data: dict):
    global event
    event = data
    if event.get('e') == 'aggTrade':
        await event_agg_trade(data)


async def wait_low_price():
    await asyncio.sleep(5)
    while True:
        print(f'\n{datetime.datetime.now()} '
              f'- Current max price in rolling 1h window ' +
              BLUE_TEXT + f'- {max_price_1h_roll_window}\n' + END_COLOR)
        print(f'{datetime.datetime.now()} '
              f'- Waiting for {round(percent, 2)}% price changing...')
        await asyncio.sleep(5)


async def main():
    print('Ждём инициализацию БД в контейнере Docker...')

    await db_connect_create_if_not_exists(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD)
    # Ждём инициализацию БД в контейнере Docker
    await asyncio.sleep(3)
    global db_pool
    db_pool = await make_db_pool()

    tasks = [
        asyncio.create_task(
            WsClient().stream_agg_trade(
                SYMBOL,
                callback_event=adapter_event)),
        asyncio.create_task(
            make_db_request()
        ),
        asyncio.create_task(
            wait_low_price()),
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
