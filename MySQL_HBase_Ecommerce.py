import mysql.connector
import happybase

# MySQL connection details
mysql_config = {
    'host': 'phpdemo03.kcspl.in',
    'user': 'admin',
    'password': 'Krish@123',
    'database': 'parth_poc'
}

# HBase connection details
hbase_host = '172.16.2.11'
hbase_table_name = 'ecommerce'
hbase_column_family = {'order_details': dict(),
 'product_details': dict(),
 'user_details': dict(),
 }
# MySQL query to fetch data in batches
query = "SELECT * FROM ecommerce limit 50000"

# Batch size and starting offset
batch_size = 100
offset = 0

# Connect to MySQL database
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# Connect to HBase
hbase_conn = happybase.Connection(hbase_host)
hbase_conn.open()
hbase_table = hbase_conn.table(hbase_table_name)


table_names = [name.decode() for name in hbase_conn.tables()]
if hbase_table_name not in table_names:
    # Create HBase table with column family
    hbase_conn.create_table(hbase_table_name, hbase_column_family)

# Fetch data in batches from MySQL and load into HBase
while True:
    # Execute MySQL query to fetch data in batches
    mysql_cursor.execute(query)
    rows = mysql_cursor.fetchall()
    if not rows:
        break

    # Load batch data into HBase
    with hbase_table.batch(batch_size=batch_size) as batch:
        for row in rows:
            # Get row key
            row_key = str(row[0])

            # Create column data dictionary
            column_data = {
                f'user_details:user_name': row[1],
                f'order_details:event_time': str(row[2].strftime('%Y-%m-%d %H:%M:%S')),
                f'order_details:event_type': row[3],
                f'product_details:brand': row[4],
                f'product_details:category_id': row[5],
                f'product_details:category_code': row[6],
                f'product_details:product_id': str(row[7]).encode('utf-8'),
                f'product_details:price':str(row[8]).encode('utf-8'),
                f'order_details:order_status': row[9],
                f'order_details:country': row[10],
                f'order_details:city': row[11],
                f'order_details:ship_postal_code': row[12],
                f'order_details:Order_ID': row[13],
                f'user_details:mobile_no': str(row[14]).encode('utf-8'),
                f'user_details:email_id': row[15],
                # add more columns as needed
            }

            # Add row to batch for HBase insert
            batch.put(row_key, column_data)

    # Update offset for next batch
    offset += batch_size

# Close MySQL and HBase connections
mysql_cursor.close()
mysql_conn.close()
hbase_conn.close()
