import time
import clickhouse_connect
import os
import datetime
import re
import configparser
import psutil

statuses = {
    'active (running)': 0,
    'active (exited)': 1,
    'active (waiting)': 2,
    'inactive': 3,
    'enabled': 4,
    'disabled': 5,
    'static': 6,
    'masked': 7,
    'alias': 8,
    'linked': 9
}

config = configparser.ConfigParser()
config.read('/var/scripts/system_logger/config.ini')

def convert_to_megabytes(val):
    if 'K' in val:
        val = val.split('K')[0]
        val = float(val) * 0.001
    elif 'M' in val:
        val = val.split('M')[0]
        val = float(val)
    elif 'G' in val:
        val = val.split('G')[0]
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

    clickhouse_client.command('''CREATE TABLE IF NOT EXISTS vm_healthcheck
                                                (vm String,
                                                service String,
                                                active UInt32,
                                                container UInt32,
                                                time DateTime) 
                            ENGINE MergeTree
                            ORDER BY time''')

    return clickhouse_client


def check_filesystem(client):
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
            data.append(row)
        except IndexError:
            continue
    client.insert('vm_filesystem', data, column_names=['vm', 'device', 'size', 'used', 'available', 'used_percent', 'mounted', 'time'])


def check_usage(client):
    now = datetime.datetime.now().utcnow()
    data = []

    cpu = psutil.cpu_percent(4)
    swap_mem = psutil.swap_memory().percent
    v_mem = psutil.virtual_memory().percent

    row = [config['MACHINE']['NAME'], cpu, swap_mem, v_mem, now]
    data.append(row)

    client.insert('vm_usage', data, column_names=['vm', 'cpu_usage', 'swap_mem', 'v_mem', 'time'])

def check_service_status(machine, service, client):
    now = now = datetime.datetime.now().utcnow()
    data = []

    command_active = f'machinectl shell {machine} /usr/bin/systemctl status {service} & exit'
    # command_status = f'machinectl shell {machine} /usr/bin/systemctl status {service} | grep Status'
    command_container = f'systemctl status systemd-nspawn@{machine}.service'
    active_output = os.popen(command_active).read()
    container_output = os.popen(command_container).read()
    # status_output = os.popen(command_status).read()

    for i in statuses:
        if i in container_output:
            container = statuses[i]
            break
        else:
            container = 404
    
    for i in statuses:
        if i in active_output:
            active = statuses[i]
            break
        else:
            active = 404


    row = [config['MACHINE']['NAME'], machine, active, container, now]
    data.append(row)

    client.insert('vm_healthcheck', data, column_names=['vm', 'service', 'active', 'container', 'time'])

def check_machines(client):
    machines = {
        'redis': 'redis',
        'redis-cache': 'redis',
        'redis3': 'redis-server',
        'rabbitmq': 'rabbitmq-server'
    }

    for i in machines:
        check_service_status(i, machines[i], client)


client = create_connection()


check_filesystem(client)
check_usage(client)
check_machines(client)

client.close()
