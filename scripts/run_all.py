import os
import optparse

parser = optparse.OptionParser()

parser.add_option("-D", "--database", help="Database name")
parser.add_option("-l", "--logdir", help="Directory to save the log files")
parser.add_option("r","--refreshdir", help="Refresh data dir")
parser.add_option("q","--querydir", help="Query dir")
parser.add_option("-b","--basedir", help="Base data dir")

(options, args) = parser.parse_args()
if not (options.database and options.logidr):
    parser.print_help()
    exit(1)

load_base_data = "python load_base_data.py -D "+options.database+" -d "+options.basedir+"/ -l "+options.logdir
throughput_test_1 = "python run_query_test.py -D "+options.database+" -i "+options.querydir+"/ -t throughput -s 1"
data_maintenance_1 = "python run_data_maintenance.py -D "+options.database+" -r 1 -d "+options.refreshdir+"/"
throughput_test_2 = "python run_query_test.py -D "+options.database+" -i "+options.querydir+"/ -t throughput -s 2"
data_maintenance_2 = "python run_data_maintenance.py -D "+options.database+" -r 1 -d "+options.refreshdir+"/"

os.system(load_base_data)
os.system(throughput_test_1)
os.system(data_maintenance_1)
os.system(throughput_test_2)
os.system(data_maintenance_2)