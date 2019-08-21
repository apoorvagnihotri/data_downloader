import sys
import json
import time
import argparse
import os

import multiprocessing as mp
from subprocess import Popen, PIPE


from datetime import timedelta, date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


parser = argparse.ArgumentParser(
    description='Parallel data downloader.')
parser.add_argument(
    '--base', metavar='href_base', dest='href', default='https://openaq-data.s3.amazonaws.com/',
    help="Base url to use to download data (notice the '/' at the end).",
)
parser.add_argument(
    '-t', metavar='seconds', dest='time', default=10,
    help="How much to sleep when calling subprocs", type=int
)
parser.add_argument(
    '-n', metavar='# of concurrent downloads', dest='downs', default=32,
    help="How many subproceses should run concurrently", type=int
)
parser.add_argument(
    '-j', metavar='# of connections per download', dest='jobs', default=8,
    help="How many connections per download to be alloted (aria2c stuff) (1-16) valid", type=int
)
parser.add_argument(
    '-s', metavar='start date', dest='start_date', default='2018.1.1',
    help="Download to start from this date (inclusive).", type=str
)
parser.add_argument(
    '-e', metavar='end date', dest='end_date', default='2018.12.31',
    help="Download to end on this date (inclusive).", type=str
)
parser.add_argument(
    '-d', metavar='DIR', dest='down_dir', default='down_data',
    help="Directory to store the files",
)

args = parser.parse_args()

# getting dates
start_date = date(*list(map(int, args.start_date.split('.'))))
end_date = date(*list(map(int, args.end_date.split('.'))))

fnames = []
for single_date in daterange(start_date, end_date):
    fnames.append(args.href + (single_date.strftime("%Y-%m-%d")) + '.csv')

count_fnames = len(fnames)
n = args.downs # number of processes to run parallely
final = [fnames[i * n:(i + 1) * n] for i in range((count_fnames + n - 1) // n )]

for j, fnames_sel in enumerate(final):
    sub_count = len(fnames_sel)
    procs = [] # processes
    for i, fname_sel in enumerate(fnames_sel):
        # telling python pfile to run ith test
        cmd = f'aria2c -d {args.down_dir} -j {args.jobs} -x {args.jobs} {fname_sel}'
        # print (cmd)
        cmd = cmd.split()
        print (f'Downloading {j*n + i}/{count_fnames}!')
        procs.append(Popen(cmd, stdout=PIPE))

    finished = [False] * len(procs)

    while True:
        time.sleep(args.time)
        allclose = True
        for i, p in enumerate(procs):
            rc = p.poll()
            if rc is None: # running
                print ("Running...")
                allclose = False
            
            elif rc == 0: # successfully terminated
                if not finished[i]:
                    stdout = str(p.stdout.read())
                    stdout = stdout.replace('\\n', '\n')
                    print (f'Downloaded {j*n + i}/{count_fnames}!')
                    base_path = 'logs'
                    if not os.path.exists(base_path):
                        os.makedirs(base_path)
                    with open(f"{base_path}/{j*n + i}.txt","w") as f:
                        f.write(stdout)
                finished[i] = True
            
            else:
                print (f"Link: '{fname_sel}' gave the error code of {rc}, check: logs/{j*n + i}.txt")
        if allclose:
            break

sys.exit()
