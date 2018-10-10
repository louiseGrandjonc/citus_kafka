import json

from kafka.consumer import KafkaConsumer

from utils import hash_bi


class CitusConsumer(KafkaConsumer):
    def __init__(self, config, workers,  *args, **kwargs):
        super(CitusConsumer, self).__init__(*args, **kwargs)
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
        per_shard = {}

        for msg in self:
            if msg:
                values = self.parse_msg(msg)

                if not values:
                    continue

                shard = self.find_shard(values[self.sharding_key])

                if shard['shard_id'] not in per_shard:
                    per_shard[shard['shard_id']] = {
                        'host': shard['host'],
                        'port': shard['port'],
                        'records': [values, ]
                    }
                else:
                    per_shard[shard['shard_id']]['records'].append(values)

        for shard_id in per_shard:
            self.insert_into_table(shard_id, per_shard[shard_id]['host'],
                                   per_shard[shard_id]['port'],
                                   per_shard[shard_id]['records'])

    def parse_msg(self, message):
        # load json, validate format compared to columns
        # raise error if column in json not in columns
        try:
            print(message.value)
            record = json.loads(message.value.decode('utf8').replace("'", '"'))
        except:
            return None


        for key in record.keys():
            if key not in self.columns:
                return None

        return record

    def find_shard(self, shard_key):
        # Use the cloud.config to find which column is the sharding key
        # Then find the right shard
        # Then find host
        # return host + shard
        hashed_value = hash_bi(shard_key)

        for shard in self.workers:
            if hashed_value >= shard['min_hash'] and hashed_value <= shard['max_hash']:
                return shard

    def insert_into_table(self, shard_id, host, port, records):
        # insert into shard
        table_name = '%s_%d' % (self.table, shard_id)

        column_list = records[0].keys()
        columns = ', '.join(column_list)


        query = 'INSERT INTO {} ({}) VALUES'.format(table_name, columns)


        values = []
        for record in records:
            for column in column_list:
                values.append(record[column])
