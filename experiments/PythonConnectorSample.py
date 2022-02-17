import snowflake.connector
# Connection string
#PASSWORD = os.getenv('SNOWSQL_PWD')
conn = snowflake.connector.connect(
                user='geoniece',
                password='ImLoving!t2022',
                account='pd81480.us-east-2.aws',
                warehouse='COMPUTE_WH',
                database='SNOWFLAKE_SAMPLE_DATA',
                schema='public'
                )

# Create cursor
cur = conn.cursor()

# Execute SQL statement
cur.execute("select current_date;")

# Fetch result
print(cur.fetchone()[0])
