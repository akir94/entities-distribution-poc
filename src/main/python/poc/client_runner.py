import random
import subprocess
import sys
import time
import os

#  as generated by the generator, all in degrees
generation_long_range = 10
generation_lat_range = 10
generation_min_long = 30
generation_min_lat = 30

seeder_url = 'http://192.168.0.53:7003'
python_exe = str(sys.executable)
trigger_script = 'trigger.py'

node_exe = 'node'
js_script = str(os.path.join('..', '..', '..', '..', 'deepstream_client', 'client.js'))
print(js_script)


def main():
    total_entities = int(sys.argv[1])
    client_amount = int(sys.argv[2])
    seed_entities_amount = sys.argv[3]

    processes = []
    for i in range(0, client_amount):
        area = generate_area(total_entities)
        processes.append(run_trigger(seed_entities_amount, area))
        processes.append(run_listener(i, area))

    time.sleep(3)  # otherwise next line might block the other processes
    input("Press enter to terminate")
    for process in processes:
        #if process is not None:
        process.kill()
        print(process.communicate())


def generate_area(total_entities):
    entity_density = total_entities / (generation_long_range * generation_lat_range)  # generator dimensions
    lat_size = 1  # degrees
    long_size = 100 / (entity_density * lat_size)  # ensure 100 entities on average

    min_lat = random.uniform(30, 40 - lat_size)
    min_long = random.uniform(30, 40 - long_size)
    max_lat = min_lat + lat_size
    max_long = min_long + long_size

    return min_long, max_long, min_lat, max_lat


def run_trigger(seed_entities_amount, area):
    command_and_params = [python_exe, trigger_script, seeder_url, seed_entities_amount,
                          str(area[0]), str(area[1]), str(area[2]), str(area[3])]
    return subprocess.Popen(command_and_params, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def run_listener(client_index, area):
    command_and_params = [node_exe, js_script, str(client_index),
                          str(area[0]), str(area[1]), str(area[2]), str(area[3])]
    return subprocess.Popen(command_and_params, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


if __name__ == "__main__":
    main()
