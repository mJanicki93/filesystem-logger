import time
import clickhouse_connect
import os
import datetime
import re
import configparser


def create_connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    clickhouse_client = clickhouse_connect.get_client(host=config['CLICKHOUSE']['Host'],
                                                      username=config['CLICKHOUSE']['User'],
                                                      password=config['CLICKHOUSE']['Password'])
    clickhouse_client.command('''CREATE TABLE IF NOT EXISTS filesystem
                                                (name String,
                                                size String,
                                                used String,
                                                availible String,
                                                used_percent String,
                                                mounted String,
                                                time DateTime) 
                            ENGINE MergeTree
                            ORDER BY time''')
    return clickhouse_client


def check_filesystem(client):
    while True:
        print('.')
        filesystem = os.popen('df -h').read()
        now = datetime.datetime.now().utcnow()

        sf = filesystem.split('\n')
        sf = sf[1:]
        data = []

        for i in sf:
            try:
                if i[0][0] != '/':
                    continue

                i = re.sub(r"\s+", ",", i)
                i = i.split(',')
                row = [i[0], i[1], i[2], i[3], i[4], i[5], now]
                data.append(row)
            except IndexError:
                continue
        client.insert('filesystem', data, column_names=['name', 'size', 'used', 'availible', 'used_percent', 'mounted', 'time'])
        time.sleep(2)


check_filesystem(create_connection())
