#!/usr/bin/env python
#
# Snowflake Connector for Python Sample Program
#

# Logging
import logging
logging.basicConfig(
    filename='/tmp/snowflake_python_connector.log',
    level=logging.INFO)

import snowflake.connector

# Set your account and login information (replace the variables with
# the necessary values).
ACCOUNT = '<account_identifier>'
USER = '<login_name>'
PASSWORD = '<password>'

import os

# Only required if you copy data from your S3 bucket
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Connecting to Snowflake
con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Creating a database, schema, and warehouse if none exists
con.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse")
con.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb")
con.cursor().execute("USE DATABASE testdb")
con.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema")

# Using the database, schema and warehouse
con.cursor().execute("USE WAREHOUSE tiny_warehouse")
con.cursor().execute("USE SCHEMA testdb.testschema")

# Creating a table and inserting data
con.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "testtable(col1 integer, col2 string)")
con.cursor().execute(
    "INSERT INTO testtable(col1, col2) "
    "VALUES(123, 'test string1'),(456, 'test string2')")

# Copying data from internal stage (for testtable table)
con.cursor().execute("PUT file:///tmp/data0/file* @%testtable")
con.cursor().execute("COPY INTO testtable")

# Copying data from external stage (S3 bucket -
# replace <s3_bucket> with the name of your bucket)
con.cursor().execute("""
COPY INTO testtable FROM s3://<s3_bucket>/data/
     STORAGE_INTEGRATION = myint
     FILE_FORMAT=(field_delimiter=',')
""".format(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY))

# Querying data
cur = con.cursor()
try:
    cur.execute("SELECT col1, col2 FROM testtable")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
finally:
    cur.close()

# Binding data
con.cursor().execute(
    "INSERT INTO testtable(col1, col2) "
    "VALUES(%(col1)s, %(col2)s)", {
        'col1': 789,
        'col2': 'test string3',
        })

# Retrieving column names
cur = con.cursor()
cur.execute("SELECT * FROM testtable")
print(','.join([col[0] for col in cur.description]))

# Catching syntax errors
cur = con.cursor()
try:
    cur.execute("SELECT * FROM testtable")
except snowflake.connector.errors.ProgrammingError as e:
    # default error message
    print(e)
    # user error message
    print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
finally:
    cur.close()

# Retrieving the Snowflake query ID
cur = con.cursor()
cur.execute("SELECT * FROM testtable")
print(cur.sfqid)

# Closing the connection
con.close()