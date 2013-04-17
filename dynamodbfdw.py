from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING
import boto.dynamodb
import json


class DynamoFdw(ForeignDataWrapper):
    """
    A DynamoDB foreign data wrapper.

    """

    def __init__(self, options, columns):
         super(DynamoFdw, self).__init__(options, columns)
         self.columns = columns
         self.aws_access_key_id = options['aws_access_key_id']
         self.aws_secret_access_key = options['aws_secret_access_key']
         self.aws_region = options['aws_region']
         self.remote_table = options['remote_table']
         log_to_postgres(json.dumps(options))


    def conn(self):
        conn = boto.dynamodb.connect_to_region(self.aws_region,aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key)
        table = conn.get_table(self.remote_table)
        key = 'hola'
        item = table.get_item(key)
        return item

    def execute(self, quals, columns):
        item = self.conn()
        yield item


