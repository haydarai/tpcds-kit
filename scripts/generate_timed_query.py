import os
import re
import optparse

parser = optparse.OptionParser()
parser.add_option("-o", "--output", help="output directory")
parser.add_option("-i", "--input", help="directory of input queries")


(options, args) = parser.parse_args()
if not (options.output and options.input):
    parser.print_help()
    exit(1)

output_dir = options.output
input_dir = options.input

rule = '-- end query .* in stream . using template.query.*.tpl'

def generate_timed_query(query_num):
    with open(output_dir+"/query_"+str(query_num)+".sql") as f:
        queries = f.read()
        query_ids = [q.split(' ')[9].split('.tpl')[0].split('query')[1] for q in re.findall(rule,queries)]
        queries = re.compile(rule).split(queries)[:-1]

    n_test = (len(os.listdir(input_dir+"/"))-1)/2

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
        modified_queries+="INSERT INTO log (log_stream_num, log_query_num, log_test_name, log_start_time) VALUES ("+str(stream_num)+","+str(query_ids[i])+",'"+test_name+"', UTC_TIMESTAMP());\n"
        query = queries[i]
        modified_queries+=query
        modified_queries+="UPDATE log SET log_end_time = UTC_TIMESTAMP() WHERE log_id = last_insert_id();\n"

    with open(output_dir+"/query_"+str(query_num)+"_timed.sql", 'w') as f:
        f.write(modified_queries)

if __name__ == "__main__":
    for query_file in os.listdir(input_dir+"/"):
        generate_timed_query(query_file.split("_")[1].split(".")[0])
