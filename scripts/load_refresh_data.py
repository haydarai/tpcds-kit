import multiprocessing
import os
import re
import subprocess
import optparse

parser = optparse.OptionParser()
parser.add_option("-d", "--dir", help="data directory")
parser.add_option("-D", "--Database", help="database name")
parser.add_option("-r", "--refresh", help="refresh number")
parser.add_option("-l", "--logdir", help="directory to store logs")



(options, args) = parser.parse_args()

if not (options.dir and options.Database and options.refresh):
   parser.print_help()
   exit(1)

db_name = options.Database
refresh_num = options.refresh
log_dir = options.logdir

data_dir = options.dir
rule = re.compile("([^0-9 ]+)")
def load_to_db(fname):
   load_data_query="LOAD DATA LOCAL INFILE '"+data_dir+"/"+fname.split(".dat")[0][:-1]+refresh_num+".dat"+"' INTO TABLE "+rule.search(fname).group(0)[:-1]+" COLUMNS TERMINATED BY '|';"
   memsql_load_command = "memsql --local-infile=1 -D "+db_name+" -e \""+load_data_query+"\""
#    print(memsql_load_command)
   os.system(memsql_load_command)
   os.system("echo END loading database "+db_name+" : >> "+log_dir+"/load_refresh_data.log")
   os.system("date >> "+log_dir+"/load_refresh_data.log")   

if __name__ == '__main__':
   os.system("echo END loading database "+db_name+" : >> "+log_dir+"/load_refresh_data.log")
   os.system("date >> "+log_dir+"/load_refresh_data.log")
   pool = multiprocessing.Pool(48)
   for fname in [fname for fname in os.listdir(data_dir) if (fname[:2]=="s_" and str(refresh_num) in fname)]:
       pool.apply_async(load_to_db, args=(fname,))
   pool.close()
   pool.join()