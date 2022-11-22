import psutil
import os

cpu = psutil.cpu_percent(4)
swap_mem = psutil.swap_memory()
mem = psutil.virtual_memory()

print(cpu)
print(swap_mem.percent)
print(mem.percent)

total_memory = os.popen('free -t -m').readlines()

print(total_memory)