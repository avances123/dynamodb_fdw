from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
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
            log_to_postgres("You must specify these options when creating the FDW: aws_access_key_id,aws_secret_access_key,aws_region,remote_table",ERROR)


    def get_table(self):
        conn = boto.dynamodb.connect_to_region(self.aws_region,aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key)
        table = conn.get_table(self.remote_table)
        log_to_postgres(json.dumps(conn.describe_table(self.remote_table)),DEBUG)
        return table


    def filter_condition(self,quals):
        for qual in quals:
            if qual.field_name == 'customer' and qual.operator == '=':
                return qual.value
        return None

    def execute(self, quals, columns):
        table = self.get_table()
        #result = table.scan()

        customer = self.filter_condition(quals)

        try:
            log_to_postgres('Asking dynamodb for this columns: ' + json.dumps(list(columns)),DEBUG)
            result = table.query(customer,attributes_to_get=list(columns))
        except:
            # TODO Dangerous query, replace to Error message
            log_to_postgres('Performing table.scan()')
            result = table.scan()
            
        for item in result:
           log_to_postgres(json.dumps(item),WARNING)
           yield item


