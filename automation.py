# Import libraries required for connecting to MySQL
import mysql.connector

# Import libraries required for connecting to PostgreSQL
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='MzAyNDctd2Fsa2Vy', host='127.0.0.1', database='sales')

# Connect to PostgreSQL
dsn_hostname = '127.0.0.1'
dsn_user = 'postgres'        # e.g. "abc12345"
dsn_pwd = 'MzE3NjEtd2Fsa2Vy'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "5432"                # e.g. "50000" 
dsn_database = "postgres"           # i.e. "BLUDB"

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port=dsn_port
)

# Find out the last rowid from PostgreSQL data warehouse
def get_last_rowid():
    cur = conn.cursor()
    SQL = '''SELECT MAX(rowid) FROM sales_data'''
    cur.execute(SQL)
    last_rowid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return last_rowid

last_row_id = get_last_rowid()
print("Last row id on production data warehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the data warehouse
def get_latest_records(rowid):
    cur = connection.cursor()
    SQL = f"SELECT * FROM sales_data WHERE rowid > {rowid}"
    cur.execute(SQL)
    records = cur.fetchall()
    connection.commit()
    cur.close()
    return records

new_records = get_latest_records(last_row_id)
print("New rows on staging data warehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSQL data warehouse
def insert_records(records):
    cur = conn.cursor()
    for row in records:
        SQL = '''INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)'''
        cur.execute(SQL, row)
    conn.commit()
    cur.close()

insert_records(new_records)
print("New rows inserted into production data warehouse = ", len(new_records))

# Disconnect from MySQL warehouse
connection.close()

# Disconnect from PostgreSQL data warehouse
conn.close()

# End of program