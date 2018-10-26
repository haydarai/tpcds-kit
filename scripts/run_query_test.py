import os
import re
import optparse
import multiprocessing

parser = optparse.OptionParser()
parser.add_option("-i", "--input", help="directory of input queries")
parser.add_option("-t", "--test", help="tesname to execute [power|throughput]")
parser.add_option("-D", "--database", help="database name to test")
parser.add_option("-s", "--session", help="Session number for throughput test [1|2]")


(options, args) = parser.parse_args()
if not (options.database and options.input and options.test and options.test in ["power","throughput"]):
    parser.print_help()
    exit(1)

input_dir = options.input
exec_test = options.test
db_name = options.database

rule = '-- end query .* in stream . using template.query.*.tpl'

def generate_timed_query(query_num):
    with open(input_dir+"/query_"+str(query_num)+".sql") as f:
        queries = f.read()
        query_ids = [q.split(' ')[9].split('.tpl')[0].split('query')[1] for q in re.findall(rule,queries)]
        queries = re.compile(rule).split(queries)[:-1]

    n_test = int((len(os.listdir(input_dir+"/"))-1)/2)
    ## Create condition for testname
    if (int(query_num) == 0):
        test_name = 'power_test'
    elif (int(query_num) <= n_test):
        test_name = 'throughput_1'
    else:
        test_name = 'throughput_2'

    ## Create condition for stream_num
    stream_num = query_num%n_test
    if stream_num == 0:
        stream_num = n_test
    if test_name == 'power_test':
        stream_num = 0
    modified_queries = ''
    for i in range(99):
        modified_queries+="INSERT INTO log (log_stream_num, log_query_num, log_test_name, log_start_time) VALUES ("+str(stream_num)+","+str(query_ids[i])+",'"+test_name+"', UTC_TIMESTAMP(6));\n"
        query = queries[i]
        modified_queries+=query
        modified_queries+="UPDATE log SET log_end_time = UTC_TIMESTAMP(6) WHERE log_id = last_insert_id();\n"


    return modified_queries

def execute_single_query(query_num):
    fname = exec_test+"_"+str(query_num)+"_tpcds.sql"
    with open("/tmp/"+fname, 'w') as f:
        f.write(generate_timed_query(query_num)) 
    exec_query_command = "memsql -D "+db_name+" < /tmp/"+fname
    delete_file_command = "rm /tmp/"+fname
    os.system(exec_query_command)
    os.system(delete_file_command)

if __name__ == "__main__":
    if exec_test == "power":
        execute_single_query(0)
    elif exec_test == "throughput":
        if not (options.session and options.session in ["1","2"]):
            parser.print_help()
            exit(1)
        session = options.session
        if session == "1":
            q_from = 1
            q_to = int((len(os.listdir(input_dir+"/"))+1)/2)
        else:
            q_from = int((len(os.listdir(input_dir+"/"))+1)/2)
            q_to = len(os.listdir(input_dir+"/"))

        query_pool = list(range(q_from,q_to))
        pool = multiprocessing.Pool()
        for q_num in query_pool:
            pool.apply_async(execute_single_query, args=(q_num,))
        pool.close()
        pool.join()