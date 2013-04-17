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
         try:
            self.aws_access_key_id = options['aws_access_key_id']
            self.aws_secret_access_key = options['aws_secret_access_key']
            self.aws_region = options['aws_region']
            self.remote_table = options['remote_table']
         except KeyError:
            log_to_postgres("You must specify these options :\n\naws_access_key_id\naws_secret_access_key\naws_region\nremote_table\n",ERROR)


    def get_table(self):
        conn = boto.dynamodb.connect_to_region(self.aws_region,aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key)
        table = conn.get_table(self.remote_table)
        #log_to_postgres(json.dumps(conn.describe_table(self.remote_table)),WARNING)
        return table

    def execute(self, quals, columns):
        table = self.get_table()
        for item in table.scan():
           log_to_postgres(json.dumps(item),WARNING)
           yield item


