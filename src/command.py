#!/usr/bin/python

import json
import sys, getopt

from consumer import CitusConsumer


def main(argv):
    workers_config_file = None
    base_config_file = None

    try:
        opts, args = getopt.getopt(argv,"hc:w:",["config=","config-worker="])
    except getopt.GetoptError:
        print('command.py -c <config> -w <config-worker>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('command.py -c <configfile> -w <configworkerfile>')
            sys.exit()
        elif opt in ("-c", "--config"):
            base_config_file = arg
        elif opt in ("-w", "--config-worker"):
            workers_config_file = arg
            print('Base Config File "', base_config_file)
            print('Worker config file "', workers_config_file)


    if not workers_config_file or not base_config_file:
        print('Config files are missing')
        sys.exit()


    config = None
    with open(base_config_file) as json_data:
        config = json.load(json_data)

    workers_config = None
    with open(workers_config_file) as json_data:
        workers_config = json.load(json_data)


    consumers = []
    for topic in config.get('topics', []):
        consumers.append(CitusConsumer(
            config,
            workers_config,
            topic,auto_offset_reset='earliest',
            bootstrap_servers=config['bootstrap_servers'],
            api_version=(0,10),
            consumer_timeout_ms=config['consumer_timeout']))


    for consumer in consumers:
        consumer.consume_messages()


if __name__ == "__main__":
   main(sys.argv[1:])
