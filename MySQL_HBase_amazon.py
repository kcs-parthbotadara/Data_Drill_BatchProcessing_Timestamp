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
hbase_table_name = 'amazon'
hbase_column_family = {'order_details': dict(),
 'product_details': dict(),
 'user_details': dict(),
 }
# MySQL query to fetch data in batches
query = "SELECT * FROM amazon"

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
                f'order_details:date': str(row[1].strftime('%Y-%m-%d %H:%M:%S')),
                f'user_details:user_name': row[2],
                f'order_details:status': row[3],
                f'order_details:fulfilment': row[4],
                f'order_details:Sales_Channel': row[5],
                f'order_details:ship-service-level': row[6],
                f'product_details:style': row[7],
                f'product_details:sku': row[8],
                f'product_details:asin': row[9],
                f'order_details:courier_status': row[10],
                f'order_details:qty': str(row[11]).encode('utf-8'),
                f'order_details:currency': row[12],
                f'order_details:amount': row[13],
                f'order_details:City_name': row[14],
                f'order_details:ship_postal_code': row[15],
                f'order_details:ship_country': row[16],
                f'order_details:promotion_ids': row[17],
                f'order_details:b2b': row[18],
                f'order_details:fulfilled_by': row[19],
                f'user_details:mobile_number': row[20],
                f'user_details:email_id': row[21],
                f'product_details:Product_Name': row[22],
                f'product_details:Product_Category': row[23],
                f'user_details:user_id': row[24],
                f'product_details:product_id': row[25]
            }

            # Add row to batch for HBase insert
            batch.put(row_key, column_data)

    # Update offset for next batch
    offset += batch_size

# Close MySQL and HBase connections
mysql_cursor.close()
mysql_conn.close()
hbase_conn.close()
