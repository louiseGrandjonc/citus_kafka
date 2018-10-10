import json

from kafka.consumer import KafkaConsumer


class CitusConsumer(KafkaConsumer):
    def __init__(self, base_config, workers,  *args, **kwargs):
        return super(CitusConsumer, self).__init__(*args, **kwargs)
        self.database = config.get('database', None)
        self.password = config.get('password', None)
        self.username = config.get('username', None)
        self.schema = config.get('schema', None)
        self.table = config.get('table', None)
        self.columns = config.get('columns', None)
        self.sharding_key = config.get('sharding_key', None)

        self.coordinator = workers.get('coordinator')
        self.workers = workers.get('workers')


    def consume_messages(self):
        for msg in self:
            if msg:
                try:
                    record = json.loads(msg.value.decode('utf8').replace("'", '"'))
                    print(record)
                except:
                    print(msg.value)
                    pass


    def parse_msg(self, message):
        # TODO
        # load json, validate format compared to columns
        # raise error if column in json not in columns
        pass

    def find_shard(self, message):
        # Use the cloud.config to find which column is the sharding key
        # Then find the right shard
        # Then find host
        # return host + shard
        pass

    def insert_into_table(self, message):
        # parse msg
        # find shard
        # insert into shard
        pass
