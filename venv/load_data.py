import pandas as pd
import psycopg2
from psycopg2 import Error
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from variables import *
from datetime import date

# Connect to postgres database

try:
    conn = psycopg2.connect(dbname=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST)
    cur = conn.cursor()

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)


# SQL query that will be sent to Postgres to extract data
sql = '''
SELECT c.customer_id, c.first_name, c.last_name,p.product_id,p.product_type,   
p.model,ROUND(s.sales_amount::numeric,2) , s.sales_transaction_date
FROM customers c 
left join sales s
ON c.customer_id = s.customer_id
inner join products p
ON s.product_id = p.product_id
GROUP BY c.customer_id, c.first_name, c.last_name,p.product_id,
p.product_type, p.model,
s.sales_amount, s.sales_transaction_date;'''

# Send query to Postgres and return results as a tuple
cur.execute(sql)
tuples = cur.fetchall()

# Create custom column names
col_names = ['Customer_id', 'First_Name', 'Last_Name', 'Product_id',
             'Product_Type', 'Model', 'Sale_Amount', 'Sales_Transaction_Date']


# Create pandas dataframe using tuple and custom headings
df = pd.DataFrame(tuples, columns=col_names)

print(df.head())


def fix_date_cols(tz='UTC'):
    """Add timezone to date"""
    cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in cols:
        df[col] = df[col].dt.tz_localize(tz)


fix_date_cols()


# Close postgres connection
conn.commit()
cur.close()
conn.close()



# Connect to Snowflake Data Warehouse
ctx = snowflake.connector.connect(
        user=USER,
        password=DWH_PASS,
        account=ACCOUNT,
        warehouse=WAREHOUSE
    )
cs = ctx.cursor()


def create_snow_tables():
    """Create snowflake tables"""
    cs.execute('USE ROLE ACCOUNTADMIN')
    cs.execute('USE DATABASE DB_TEST')
    cs.execute('USE WAREHOUSE COMPUTE_DWH;')
    cs.execute('DROP TABLE IF EXISTS "sales.customers"')
    query = '''CREATE TABLE "sales.customers" ("Customer_id" number, 
            "First_Name" varchar, "Last_Name" varchar,"Product_id" number,
            "Product_Type" varchar,"Model" varchar, "Sale_Amount" decimal, 
            "Sales_Transaction_Date" timestamp_tz)'''
    cs.execute(query)
    ctx.commit()
    print("Table Created Successfully")


create_snow_tables()


# Send data to Snowflake
write_pandas(ctx, df, table_name='sales.customers')




# Close snowflake connection
cs.close()
ctx.close()