import happybase
import csv

# Define batch size
BATCH_SIZE = 100

# Connect to HBase
connection = happybase.Connection('172.16.1.5', port=9090)
table_name = 'my_table'

# Create table
connection.create_table(
    table_name,
    {'order_details': dict(),
     'product_details': dict(),
     'user_details': dict()}
)

# Open table
table = connection.table(table_name)

# Read CSV data
with open('AmazonDataset_new.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    batch_count = 0
    with table.batch(batch_size=BATCH_SIZE) as batch:
        for row in reader:
            # Insert data into batch
            batch.put(
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
                    'product_details:category': row['category'],
                    'product_details:size': row['size'],
                    'product_details:asin': row['asin'],
                    'order_details:courier_status': row['courier_status'],
                    'order_details:qty': row['qty'],
                    'order_details:currency': row['currency'],
                    'order_details:amount': row['amount'],
                    'order_details:ship_city': row['ship_city'],
                    'order_details:ship_state': row['ship_state'],
                    'order_details:ship_postal_code': row['ship_postal_code'],
                    'order_details:ship_country': row['ship_country'],
                    'order_details:promotion_ids': row['promotion_ids'],
                    'order_details:b2b': row['b2b'],
                    'order_details:fulfilled_by': row['fulfilled_by'],
                    'user_details:name': row['name'],
                    'user_details:mobile_number': row['mobile_number'],
                    'user_details:user_id': row['user_id'],
                    'user_details:product_id': row['product_id'],
                    'user_details:email_id': row['email_id'],
                }
            )
            batch_count += 1

            # Flush batch after BATCH_SIZE records
            if batch_count % BATCH_SIZE == 0:
                batch.send()

        # Flush any remaining records in the batch
        if batch_count % BATCH_SIZE != 0:
            batch.send()

# Close connection
connection.close()
