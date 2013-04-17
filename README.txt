============================================
DynamoDB Foreign Data Wrapper for postgresql
============================================

This data wrapper allows to do 'select *' queries on a DynamoDB database (NoSQL)


============================================
Create Foreign Data Wrapper
============================================

CREATE SERVER multicorn_dynamo FOREIGN DATA WRAPPER multicorn
options (
	wrapper 'dynamodbfdw.dynamodbfdw.DynamoFdw'
);



============================================
Create Foreign Data Wrapper
============================================

You have to replace this example fileds from yours, fill the region where you 
have your DynamoDB, and the name of your remote table

Example:

CREATE FOREIGN TABLE test (
	remote_filed1  character varying,
	remote_field2  integer
) server multicorn_dynamo options(
	aws_region  'YOUR AWS REGION HERE',
	remote_table 'remote_table'
);




============================================
Create Foreign Data Wrapper
============================================

Store your aws credentials into a postgresql user mapping.

Example:

CREATE USER MAPPING FOR my_pg_user SERVER multicorn_dynamo OPTIONS (aws_access_key_id  'XXXXXXXXXXXXXXX',aws_secret_access_key  'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX');


============================================
Perform queries
============================================

SELECT * from test;

