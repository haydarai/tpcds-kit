import multiprocessing
import os
import re
import subprocess
import optparse

parser = optparse.OptionParser()
parser.add_option("-d", "--dir", help="data directory")
parser.add_option("-D", "--Database", help="database name")
parser.add_option("-l", "--logdir", help="directory to store logs")
(options, args) = parser.parse_args()

if not (options.dir and options.Database and options.logdir):
   parser.print_help()
   exit(1)


data_dir = options.dir
db_name = options.Database
log_dir = options.logdir

rule = re.compile("([^0-9 ]+)") # Assume that paralellism is always used
def load_to_db(fname):
   load_data_query="LOAD DATA LOCAL INFILE '"+data_dir+fname+"' INTO TABLE "+rule.search(fname).group(0)[:-1]+" COLUMNS TERMINATED BY '|';"
   memsql_load_command = "memsql --local-infile=1 -D "+db_name+" -e \""+load_data_query+"\""
#    os.system(memsql_load_command)
   print(memsql_load_command)
   os.system("echo END loading database "+db_name+" : >> "+log_dir+"/load_data.log")
   os.system("date >> "+log_dir+"/load_data.log")

if __name__ == '__main__':
   os.system("echo START loading database "+db_name+" : >> "+log_dir+"/load_data.log")
   os.system("date >> "+log_dir+"/load_data.log")
   pool = multiprocessing.Pool(16)
   for fname in os.listdir(data_dir):
       pool.apply_async(load_to_db, args=(fname,))
   pool.close()
   pool.join()