

import pandas as pd
import numpy as np 
from datetime import datetime
from multiprocessing import Pool, freeze_support
import logging
logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import multiprocessing as mp

from ksql_data_helpers import return_det_counts_data, return_recent_data_ts,ts_str_to_unix, ts_unix_to_ts, return_det_counts_data_v2
from helpers import return_det_waveform

class LabelEOI():
    
    
    
    def __init__(self, detector_id, start_time, end_time):
        super().__init__()
        self.no_of_cores = 30
        self.aggregation_level = 300
        self.min_prev_data = 2
        self.window_size = 4
        self.detector_id =str(detector_id)
        self.start_time= start_time
        self.end_time = end_time-1 


        # ALL THRESHOLDS  AND CONSTANTS
        self.DET_HISTORY_VOLUME_THRESHOLD = 10
        self.time_start = '07:00'
        self.time_end = '21:00'
        self.prev_days_to_consider = [7,14,21]
        self.current_weight = 2
        self.history_weight = 1

        
    def compute_historic_baseline(self, row):

        """
        Computes historic baseline traffic for a given time interval
        Parameters
        ----------
        row : dataframe row
            More info to be displayed (default is None)

        Returns
        -------
        value: float
            traffic volume baseline
        """
        all_prev = ['MA_prev_7','MA_prev_14','MA_prev_21' ]
        value = 0
        count = self.current_weight
        value += self.current_weight*row['MA_curr']
        for prev in all_prev:
            if(prev in row and row[prev]>self.DET_HISTORY_VOLUME_THRESHOLD):
                value+=self.history_weight*row[prev]
                count+=self.history_weight
                
        return value/count
    
    def get_EOI(self):
        flag, hist_df = self.return_hist_df()
        if(flag):
            last_row = hist_df.iloc[-1]
            curr_vol = last_row.Count
            curr_ma = last_row.MA_curr
            reduction =  (last_row.baseline- curr_vol)/(last_row.baseline)
            end_time_str = pd.to_datetime(ts_unix_to_ts(self.end_time)).strftime("%Y-%m-%d %H:%M:%S")
            start_time_str = (pd.to_datetime(ts_unix_to_ts(self.end_time)) - pd.Timedelta(seconds=300)).strftime("%Y-%m-%d %H:%M:%S")
            reducton_dict = {'detector_id':self.detector_id, 'start_time':start_time_str, 'end_time':end_time_str, 'curr_volume':curr_vol, 'curr_ma':curr_ma,'baseline':last_row.baseline, 'reduction':reduction}
            if(reduction>0):
                return True, reducton_dict
            else:
                return False, 0
        return False, 0
            
    
    
    def return_hist_df(self):
        # current data
        # count_df = return_det_counts_data(self.detector_id , self.start_time, self.end_time)
        count_df = return_det_counts_data_v2(self.detector_id , self.start_time, self.end_time)
#         count_df.rename(columns={'EventCode': 'Count_curr'}, inplace=True)

        count_df.rename(columns={'COUNT': 'Count'}, inplace=True)
        ll = f"{self.aggregation_level}s"
        count_df= count_df.resample(ll).sum()
        count_df['MA_curr'] = count_df.rolling(window=self.window_size ).mean()
        count_df.fillna(method='bfill', inplace = True)
        count_df['time'] = count_df.index.time
        
        
        # history from previous weeks


        prev_count ={}
        all_prev = [7,14, 21]
        for i in  all_prev:
            start_time_prev =  (pd.to_datetime(ts_unix_to_ts(self.start_time)) - pd.Timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
            end_time_prev =  (pd.to_datetime(ts_unix_to_ts(self.end_time)) - pd.Timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
            # print(f"time prev {time_prev}")

            count_df_prev = return_det_waveform(self.detector_id ,start_time_prev, end_time_prev)
#             print(f"count_df_prev shape {count_df_prev.shape}")
            if(count_df_prev.shape[0]<5):
                pass
            else:
        #         count_df_prev.drop(columns = ['SignalID','EventParam'], inplace = True)
        #         count_df_prev.rename(columns={'EventCode': 'Count'}, inplace=True)
                ll = f"{self.aggregation_level}s"
                count_df_prev= count_df_prev.resample(ll).sum()
                count_df_prev[f'MA_prev_{i}'] = count_df_prev.rolling(window=self.window_size).mean()
                count_df_prev[f'time'] =  count_df_prev.index.time
                count_df_prev.fillna(method='bfill', inplace = True)
                prev_count[i]  = count_df_prev  
                
                
        all_merged_df =   count_df.copy() 
        all_merged_df['timestamp'] = all_merged_df.index
        #         print(f"Yash DEBUG   all_merged_df {all_merged_df.head(5)}" )
        
#         print(f"prev count {len(prev_count)}")
        if(len(prev_count) > self.min_prev_data):
            for k,each_prev in   prev_count.items():
                all_merged_df = pd.merge(all_merged_df,each_prev, on = 'time')
                
            all_merged_df['baseline'] = all_merged_df.apply(lambda x: self.compute_historic_baseline(x), axis = 1) 
            all_merged_df['DetectorID'] = self.detector_id
            all_merged_df.set_index('timestamp', inplace = True)
            
            return True, all_merged_df
        else:
            return False, None

    
    
        
    
