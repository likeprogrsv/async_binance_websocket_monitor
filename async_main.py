import asyncio
import asyncpg
import datetime
# from time import sleep
from aio_binance.futures.usdt import WsClient


SYMBOL = 'XRPUSDT'
db_insert = '''
                INSERT INTO crypto_monitor(pair_name, time, price)
                VALUES($1, $2, $3)
            '''

# С параметрами для БД через переменные среды почему-то
# не захотело работать, поэтому пропишем руками ниже
HOST = 'localhost'
DATABASE = 'crypto_test'
USER = 'postgres'
PASSWORD = 'likeprogdbpass'


async def connect_create_if_not_exists(host, user, password, database,
                                       port=5432,):
    try:
        conn = await asyncpg.connect(
                host=host, user=user, password=password,
                database=database, port=port,)

    except (asyncpg.InvalidCatalogNameError,
            asyncpg.exceptions.ConnectionDoesNotExistError):
        # Database does not exist, create it.
        sys_conn = await asyncpg.connect(
            database='template1',
            user='postgres',
            password=password,
        )
        await sys_conn.execute(
            f'CREATE DATABASE "{database}" OWNER "{user}"'
        )
        await sys_conn.close()

        # Connect to the newly created database.
        conn = await asyncpg.connect(
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
    while True:
        if event:
            time = datetime.datetime.fromtimestamp(int(event.get('E')) / 1000)
            await db_pool.fetch(db_insert, SYMBOL, time, float(event.get('p')))
            print('\nCREATE A NEW ROW IN DB\n')
        await asyncio.sleep(1)


async def event_kline(data: dict):
    print(data)
    await asyncio.sleep(5)


async def event_agg_trade(data: dict):
    print(data)


async def adapter_event(data: dict):
    global event
    event = data
    # if event == 'kline':
    #     await event_kline(data)
    if event.get('e') == 'aggTrade':
        await event_agg_trade(data)
        # await make_db_request(data=data)
    # else:
    #     print(data)


async def main():
    await connect_create_if_not_exists(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD)
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
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
