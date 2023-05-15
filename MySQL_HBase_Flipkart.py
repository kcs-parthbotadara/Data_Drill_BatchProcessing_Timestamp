import mysql.connector
import happybase

# MySQL connection details
mysql_config = {
    'host': 'phpdemo03.kcspl.in',
    'user': 'admin',
    'password': 'Krish@123',
    'database': 'parth_poc',
}

# HBase connection details
hbase_host = '172.16.2.11'
hbase_table_name = 'flipkart'
hbase_column_family = {'order_details': dict(),
 'product_details': dict(),
 'user_details': dict(),
 }
# MySQL query to fetch data in batches
query = "select order_id ,date_ ,city,cart_id ,dim_customer_key,procured_quantity ,unit_selling_price,total_discount_amount ,product_id ,total_weighted_landing_price ,Order_Status ,mobile_no ,user_name, email_id ,product_name ,unit ,product_type ,brand_name ,manufacturer_name ,l0_category ,l1_category ,l2_category ,l0_category_id ,l1_category_id ,l2_category_id  from flipkart"

# Batch size and starting offset
batch_size = 100
# offset = 0

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
                f'order_details:date_': str(row[1].strftime('%Y-%m-%d %H:%M:%S')),
                f'order_details:city': row[2],
                f'order_details:cart_id': str(row[3]).encode('utf-8'),
                f'order_details:dim_customer_key': str(row[4]).encode('utf-8'),
                f'order_details:procured_quantity': str(row[5]).encode('utf-8'),
                f'product_details:unit_selling_price': str(row[6]).encode('utf-8'),
                f'order_details:total_discount_amount': str(row[7]).encode('utf-8'),
                f'product_details:product_id': str(row[8]).encode('utf-8'),
                f'order_details:total_weighted_landing_price': str(row[9]).encode('utf-8'),
                f'order_details:Order_Status': row[10],
                f'user_details:mobile_no': str(row[11]).encode('utf-8'),
                f'user_details:user_name': row[12],
                f'user_details:email_id': row[13],
                f'product_details:product_name': row[14],
                f'product_details:unit': row[15],
                f'product_details:product_type': row[16],
                f'product_details:brand_name': row[17],
                f'product_details:manufacturer_name': row[18],
                f'product_details:l0_category': row[19],
                f'product_details:l1_category': row[20],
                f'product_details:l2_category': row[21],
                f'product_details:l0_category_id': str(row[22]).encode('utf-8'),
                f'product_details:l1_category_id': str(row[23]).encode('utf-8'),
                f'product_details:l2_category_id': str(row[24]).encode('utf-8'),
                # add more columns as needed
            }

            # Add row to batch for HBase insert
            batch.put(row_key, column_data)
        batch.send()
    # Update offset for next batch
    # offset += batch_size

# Close MySQL and HBase connections
mysql_cursor.close()
mysql_conn.close()
hbase_conn.close()
