import os 
import optparse

parser = optparse.OptionParser()
parser.add_option("-d", "--dir", help="refresh data directory")
parser.add_option("-r", "--refresh", help="refresh number")
parser.add_option("-D", "--database", help="database name")


(options, args) = parser.parse_args()

if not (options.dir and options.refresh):
    parser.print_help()
    exit(1)

data_dir = options.dir
db_name = options.database
refresh_num = options.refresh

# TODO: Method 1 Implementation

# Method 2 Implementation
dm_queries = []
with open(data_dir+"/delete_"+str(refresh_num)+".dat",'r') as f:
    rows = f.read().split('\n')[:-1] #slice to -1 to throw away empty item at the end
    for row in rows:
        start_date, end_date = tuple(row.split('|'))
        catalog_return_delete_query = "delete catalog_returns from catalog_returns join catalog_sales on cr_item_sk = cs_item_sk and cr_order_number=cs_order_number join date_dim on cs_sold_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        catalog_sales_delete_query = "delete catalog_sales from catalog_sales join date_dim on cs_sold_date_sk=d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        store_return_delete_query = "delete store_returns from store_returns join store_sales on sr_item_sk=ss_item_sk and sr_ticket_number=ss_ticket_number join date_dim on ss_sold_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        store_sales_delete_query = "delete store_sales from store_sales join date_dim on ss_sold_date_sk=d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        web_return_delete_query = "delete web_returns from web_returns join web_sales on wr_item_sk = ws_item_sk and wr_order_number = ws_order_number join date_dim on ws_sold_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        web_sales_delete_query = "delete web_sales from web_sales join date_dim on ws_sold_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        
        dm_queries.append(catalog_return_delete_query+"\n")
        dm_queries.append(catalog_sales_delete_query+"\n")
        dm_queries.append(store_return_delete_query+"\n")
        dm_queries.append(store_sales_delete_query+"\n")
        dm_queries.append(web_return_delete_query+"\n")
        dm_queries.append(web_sales_delete_query+"\n")

# Method 3 Implementation
with open(data_dir+"/inventory_delete_"+str(refresh_num)+".dat",'r') as f:
    rows = f.read().split('\n')[:-1] #slice to -1 to throw away empty item at the end
    for row in rows:
        start_date, end_date = tuple(row.split('|'))
        inventory_deletetion_query = "delete inventory from inventory join date_dim on inv_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        dm_queries.append(inventory_deletetion_query+"\n")

# Execute Data Modification Queries
fname = "tpcds_data_modification.sql"
with open("/tmp/"+fname,"w") as f:
    log_start_query = "INSERT INTO log (log_test_name, log_start_time) VALUES ('dm_"+str(refresh_num)+"', UTC_TIMESTAMP());\n"
    f.write("%s\n"%log_start_query)
    for query in dm_queries:
        f.write("%s\n"%query)
    log_end_time = "UPDATE log SET log_end_time = UTC_TIMESTAMP() WHERE log_id = last_insert_id();\n"
    f.write("%s\n"%log_end_time)
    
exec_query_command = "memsql -D "+db_name+" < /tmp/"+fname
delete_file_command = "rm /tmp/"+fname
os.system(exec_query_command)
os.system(delete_file_command)