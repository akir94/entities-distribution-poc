import sys
import subprocess

#  as generated by the generator, all in degrees
generation_long_range = 10
generation_lat_range = 10
generation_min_long = 30
generation_min_lat = 30

seeder_url = 'localhost:7003'


def main():
    total_entities = sys.argv[1]
    client_index = sys.argv[2]
    seed_entities_amount = sys.argv[3]

    run_trigger(total_entities, client_index, seed_entities_amount)
    #  run client


def run_trigger(total_entities, client_index, seed_entities_amount):
    entity_density = total_entities / (generation_long_range * generation_lat_range)  # generator dimensions
    window_lat_size = 0.1  # degrees
    window_long_size = 100 / (entity_density * window_lat_size)  # ensure 100 entities on average

    lat_slots = generation_lat_range / window_lat_size
    long_slots = generation_long_range / window_long_size
    chosen_lat_slot = client_index % lat_slots
    chosen_long_slot = client_index / lat_slots

    min_lat = chosen_lat_slot * window_lat_size + generation_min_lat
    max_lat = min_lat + window_lat_size
    min_long = chosen_long_slot * window_long_size + generation_min_long
    max_long = min_long + window_long_size

    subprocess.call(['python', 'trigger.py', seeder_url, seed_entities_amount, min_long, max_long, min_lat, max_lat])


if __name__ == "__main__":
    main()
