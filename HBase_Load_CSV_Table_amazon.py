import happybase
import csv


# Connect to HBase
connection = happybase.Connection('172.16.2.11', port=9090)
table_name = 'amazon'

# Create table
connection.create_table(
    table_name,
    {'order_details': dict(),
     'product_details': dict(),
     'user_details': dict(),
     }
)

# Open table
table = connection.table(table_name)

# Read CSV data
with open('amazon_250000 (2).csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Insert data into HBase
        table.put(
            row['user_id'],
            {
                'order_details:order ID': row['order ID'],
                'order_details:date': row['date'],
                'order_details:status': row['status'],
                'order_details:fulfilment': row['fulfilment'],
                'order_details:sales Channel': row['sales Channel'],
                'order_details:ship-service-level': row['ship-service-level'],
                'product_details:style': row['style'],
                'product_details:sku': row['sku'],
                'product_details:asin': row['asin'],
                'order_details:courier_status': row['courier_status'],
                'order_details:qty': row['qty'],
                'order_details:currency': row['currency'],
                'order_details:amount': row['amount'],
                'order_details:City_name': row['City_name'],
                'order_details:ship_postal_code': row['ship_postal_code'],
                'order_details:ship_country': row['ship_country'],
                'order_details:promotion_ids': row['promotion_ids'],
                'order_details:b2b': row['b2b'],
                'order_details:fulfilled_by': row['fulfilled_by'],
                'user_details:name': row['name'],
                'user_details:mobile_number': row['mobile_number'],
                'user_details:email_id': row['email_id'],
                'product_details:Product_Name': row['Product_Name'],
                'product_details:Product_Category': row['Product_Category'],
                'user_details:user_id': row['user_id'],
                'user_details:product_id': row['product_id'],

            }
        )

# Close connection
connection.close()
