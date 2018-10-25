import os 
import optparse

parser = optparse.OptionParser()
parser.add_option("-d", "--dir", help="refresh data directory")
parser.add_option("-r", "--refresh", help="refresh number")
parser.add_option("-o", "--output", help="output directory")


(options, args) = parser.parse_args()

if not (options.dir and options.output and options.refresh):
    parser.print_help()
    exit(1)

data_dir = options.dir
out_dir = options.output
refresh_num = options.refresh

# Method 2 Implementation
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
        
        with open(out_dir+"/inventory_"+str(refresh_num)+".sql", "a") as o:
            o.write(catalog_return_delete_query+"\n")
            o.write(catalog_sales_delete_query+"\n")
            o.write(store_return_delete_query+"\n")
            o.write(store_sales_delete_query+"\n")
            o.write(web_return_delete_query+"\n")
            o.write(web_sales_delete_query+"\n")

# Method 3 Implementation
with open(data_dir+"/inventory_delete_"+str(refresh_num)+".dat",'r') as f:
    rows = f.read().split('\n')[:-1] #slice to -1 to throw away empty item at the end
    for row in rows:
        start_date, end_date = tuple(row.split('|'))
        inventory_deletetion_query = "delete inventory from inventory join date_dim on inv_date_sk = d_date_sk where d_date between '"+start_date+"' and '"+end_date+"';"
        with open(out_dir+"/delete_inventory_"+str(refresh_num)+".sql", "a") as o:
            o.write(inventory_deletetion_query+"\n")