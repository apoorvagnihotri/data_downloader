import pandas as pd
import time
import os

def get_df(fname):
    cluster_1 = pd.read_csv(fname, index_col=0)
    cluster_1.drop(columns=['cluster_id'], inplace=True)

    df_1 = cluster_1.drop(columns=['PM25_Concentration','station_id', 'time_met'])
    df_1 = df_1 - df_1.min()
    df_1 = df_1/df_1.max()
    df_1['PM2.5']= cluster_1['PM25_Concentration']
    df_1['station_id'] = cluster_1['station_id']
    df_1['time_met'] = cluster_1['time_met']
    return df_1

def write_log(proc, saving_loc):
    '''helpful for writing logs'''
    stdout = str(proc.stdout.read())
    stderr = str(proc.stderr.read())
    stdout = stdout.replace('\\n', '\n')
    stderr = stderr.replace('\\n', '\n')
    if not os.path.exists(saving_loc):
        os.makedirs(saving_loc)
    with open(os.path.join(saving_loc, "out.txt"),"w") as f:
        f.write(stdout)
    with open(os.path.join(saving_loc, "err.txt"),"w") as f:
        f.write(stderr)

class ProcBucket:
    """helpful for haing `num` number of subprocesses alive and computing"""
    def __init__(self, num, sleep_time):
        self.total = 0
        self.finished = 0
        self.failed = 0
        self.num = num
        self.sleep_time = sleep_time
        self.procs = {i: None for i in range(num)}
        self.saving_locs = {i: None for i in range(num)}
        
    def _check_free(self):
        '''Returns the index of free slot else, returns None'''
        for i in range(self.num):
            if self.procs[i] is None: # empty
#jjj                print("slot retured beacause empty", i)
                return i
            else: # pro alloted
                rc = self.procs[i].poll()
                if rc is None: # running
   #                 print ('processing')
                    continue
                else: # finished
                    if rc == 0: # finished successfully
                        self.finished += 1
                        write_log(self.procs[i], self.saving_locs[i])
                    else: # had recived some error
                        self.failed += 1
                        write_log(self.procs[i], self.saving_locs[i])
                    self.procs[i] = None
                    self.saving_locs[i] = None
                    return i
        return None # none empty
                
        
    def add_queue(self, fn, fnargs, saving_loc=None):
        '''Adds procs to queue and blocks if already we are busy'''
        self.total += 1
   #     print ('total', self.total)
        while True:
            rtrn_str = "Running...\n" \
                        + f"Successful: {self.finished}/{self.total}\n"\
                        + f"Failed: {self.failed}/{self.total}\n"
            ix = self._check_free()
            if ix is None: # all are busy
                time.sleep(self.sleep_time)
            else:
                self.procs[ix] = fn(*fnargs) # run the function that returns proc
                self.saving_locs[ix] = saving_loc
                return rtrn_str
            
    def finalize(self):
        while True:
            for i in range(self.num):
                if self.procs[i] is not None: # filled
                    rc = self.procs[i]
                    if rc is not None:
                        write_log(self.procs[i], self.saving_locs[i])
                        self.procs[i] = None
                        self.saving_locs[i] = None
                    else:
                        time.sleep(self.sleep_time)
                else: # dont care slot empty
                    pass
            break
        return "Finished..." \
            + f"Successful: {self.finished}/{self.total}\n"\
            + f"Failed: {self.failed}/{self.total}\n"
            
