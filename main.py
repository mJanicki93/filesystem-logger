import time
import clickhouse_connect
import os
import datetime
import re
import configparser
import psutil

config = configparser.ConfigParser()
config.read('config.ini')

def convert_to_megabytes(val):
    if 'Ki' in val:
        val = val.split('Ki')[0]
        val = float(val) * 0.001
    elif 'Mi' in val:
        val = val.split('Mi')[0]
        val = float(val)
    elif 'Gi' in val:
        val = val.split('Gi')[0]
        val = float(val) * 1000
    else:
        val = 0.01
    return val


def create_connection():
    
    
    clickhouse_client = clickhouse_connect.get_client(host=config['CLICKHOUSE']['Host'],
                                                      username=config['CLICKHOUSE']['User'],
                                                      password=config['CLICKHOUSE']['Password'])
    clickhouse_client.command('''CREATE TABLE IF NOT EXISTS vm_filesystem
                                                (vm String,
                                                device String,
                                                size Float64,
                                                used Float64,
                                                available Float64,
                                                used_percent UInt32,
                                                mounted String,
                                                time DateTime) 
                            ENGINE MergeTree
                            ORDER BY time''')

    clickhouse_client.command('''CREATE TABLE IF NOT EXISTS vm_usage
                                                (vm String,
                                                cpu_usage Float64,
                                                swap_mem Float64,
                                                v_mem Float64,
                                                time DateTime) 
                            ENGINE MergeTree
                            ORDER BY time''')

    return clickhouse_client


def check_filesystem(client):
    
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

            size = convert_to_megabytes(i[1])
            used = convert_to_megabytes(i[2])
            availible = convert_to_megabytes(i[3])
            

            used_percent = i[4].split('%')[0] # Remove % sign from used_percent

            row = [config['MACHINE']['NAME'], i[0], size, used, availible, int(used_percent), i[5], now]
            print(row)
            data.append(row)
        except IndexError:
            continue
    client.insert('vm_filesystem', data, column_names=['vm', 'device', 'size', 'used', 'available', 'used_percent', 'mounted', 'time'])
    time.sleep(2)

def check_usage(client):
    
    now = datetime.datetime.now().utcnow()
    data = []

    cpu = psutil.cpu_percent(4)
    swap_mem = psutil.swap_memory().percent
    v_mem = psutil.virtual_memory().percent

    row = [config['MACHINE']['NAME'], cpu, swap_mem, v_mem, now]
    print(row)
    data.append(row)

    client.insert('vm_usage', data, column_names=['vm', 'cpu_usage', 'swap_mem', 'v_mem', 'time'])
    time.sleep(2)

client = create_connection()

while True:
    check_filesystem(client)
    check_usage(client)
