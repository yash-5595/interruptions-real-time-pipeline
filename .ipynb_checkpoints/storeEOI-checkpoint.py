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

from ksql_data_helpers import return_det_counts_data, return_recent_data_ts,ts_str_to_unix, ts_unix_to_ts
from helpers import return_det_waveform
from labelEOI import LabelEOI

class IscEoiStore():
    
    def __init__(self):
        self.all_isc_store = {}
        self.isc_last_processed = {}
        self.map_file = pd.read_csv('MAPPINGS_DET_INFO_OCT_2022.csv')
        self.reduction_threshold = 0.01
        self.volume_threshold = 1
        self.no_of_cores = 30
        
    def process_logic(self,signalid, window_end):
        if(signalid not in self.isc_last_processed):
            return 1
        elif(window_end > self.isc_last_processed[signalid]):
            return 1
        return 0
            
        
    def get_recent_status(self):
        df = return_recent_data_ts(look_back_time = 15)
        
        df_filtered = df[df.SUMFLAG==25].copy()
        df_filtered['processed'] = df_filtered.apply(lambda x : self.process_logic(x.SIGNALID, x.WINDOWEND), axis =1 )
        df_filtered = df_filtered[df_filtered['processed']==1]
        df_filtered = df_filtered.drop_duplicates(['SIGNALID'], keep='first')
        
        return df_filtered
    
    def get_detector_ids(self, signalid):
        all_dets = self.map_file[self.map_file.ATSPM_ID.isin([signalid])]
        dets_of_interest = all_dets[(all_dets.phase.isin(['2','6'])) & (all_dets.distanceToStopbar >150) ]
        # storing all the detector ID's. Detector ID is SignalID + Channel no.
        detector_ids = []
        for i, each_row in dets_of_interest.iterrows():
            each_signal = each_row['ATSPM_ID']
            channel = each_row['channel']
            detector_ids.append(int(str(each_signal)+ str(channel)))
        return detector_ids

    def process_single_isc(self, row):
        isc_result = {}
        
        all_dets = self.get_detector_ids(row.SIGNALID)
        
        for each_det in all_dets:
            le = LabelEOI(each_det, row.WINDOWSTART, row.WINDOWEND)
            reduction_flag, reduction_dict = le.get_EOI()
            if(reduction_flag):
                print(f"redu {reduction_dict}")
                if((reduction_dict['reduction']>self.reduction_threshold) and(reduction_dict['curr_ma']>self.volume_threshold)):
                    isc_result[each_det]= reduction_dict
            else:
                isc_result[each_det] = 'None'
        return row.SIGNALID, isc_result
                    
    def start_processing(self):
#         get current data
        df_recent = self.get_recent_status()
        print(df_recent)
#     filter if next timestamp is available
        all_rows = []
    
        for i, each_row in df_recent.iterrows():
            if(each_row.SIGNALID not in self.isc_last_processed ):
                all_rows.append(each_row)
                self.isc_last_processed[each_row.SIGNALID] = each_row.WINDOWEND
            else:
                if(each_row.WINDOWEND>self.isc_last_processed[each_row.SIGNALID]):
#                     new timestamp available
                    all_rows.append(each_row)
                    self.isc_last_processed[each_row.SIGNALID] = each_row.WINDOWEND
#         return all_rows
#     parallel process all rows
        print(f"allrows")
        print(all_rows)
        p = mp.Pool(self.no_of_cores)
        results = p.map(self.process_single_isc, all_rows)
        p.close()
        
        for isc_id, each_result in results:
            if(isc_id not in self.all_isc_store ): # isc is not yet in store,
                self.all_isc_store[isc_id]={} # dict for storing all dets
                for det_id, det_value in each_result.items():
                    if(det_value != 'None'): # if reduction exist
                        self.all_isc_store[isc_id][det_id]=[det_value]
                    else:
                        self.all_isc_store[isc_id][det_id]=[]
                        
            else:
                for det_id, det_value in each_result.items():
                    if(det_id not in self.all_isc_store[isc_id]):
                        if(det_value != 'None'):
                            self.all_isc_store[isc_id][det_id]=[det_value]
                        else:
                            self.all_isc_store[isc_id][det_id] = []
                    else:
                        if(det_value != 'None'):
                            self.all_isc_store[isc_id][det_id].append(det_value)
                        else:
                            self.all_isc_store[isc_id][det_id] = []
                            
                
            
        
        return results


if __name__ == "__main__":
    isc_eoi = IscEoiStore()
    all_rows = isc_eoi.start_processing()
    print(f"all_isc_store")     
    print(isc_eoi.all_isc_store)
