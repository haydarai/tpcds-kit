import os
import re


rule = '-- end query .* in stream . using template.query.*.tpl'

def generate_timed_query(query_num):
    with open("../sql/query_"+str(query_num)+".sql") as f:
        queries = f.read()
        query_ids = [q.split(' ')[9].split('.tpl')[0].split('query')[1] for q in re.findall(rule,queries)]
        queries = re.compile(rule).split(queries)[:-1]

    ## Create condition for testname
    if (int(query_num) == 0):
        test_name = 'power_test'
    elif (int(query_num) <= (len(os.listdir("../sql"))-1)/2):
        test_name = 'throughput_1'
    else:
        test_name = 'throughput_2'
    modified_queries = ''
    for i in range(99):
        modified_queries+="INSERT INTO log (log_query_num, log_test_name, log_start_time) VALUES ("+str(query_ids[i])+",'"+test_name+"', UTC_TIMESTAMP());\n"
        query = queries[i]
        modified_queries+=query
        modified_queries+="UPDATE log SET log_end_time = UTC_TIMESTAMP() WHERE log_id = last_insert_id();\n"

    with open("../sql_timed/query_"+str(query_num)+"_timed.sql", 'w') as f:
        f.write(modified_queries)

if __name__ == "__main__":
    for query_file in os.listdir("../sql/"):
        generate_timed_query(query_file.split("_")[1].split(".")[0])
        ## TODO: Add stream identifier to the log