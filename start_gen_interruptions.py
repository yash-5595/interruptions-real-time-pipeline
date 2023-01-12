import pandas as pd
import numpy as np
from storeEOI import IscEoiStore


from ksql import KSQLAPI
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import time
import logging
logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.ERROR,
            datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.ERROR)


client = KSQLAPI('http://localhost:8088')
def insert_to_ksql_stream():
    all_res = []
    for each_signal, all_dets in  isc_eoi.all_isc_store.items():
        for each_det, interruptions in all_dets.items():
            if(interruptions):
                prev_timestamp = None
                base_vol = 0
                curr_vol = 0
                curr_ma = 0
                reduction = 0
                count =0
                for each_interruption in interruptions[::-1]:


                    if((prev_timestamp is None) ):

                        base_vol += each_interruption['baseline']
                        curr_vol += each_interruption['curr_volume']
                        reduction += each_interruption['reduction']
                        prev_timestamp = each_interruption['end_time']
                        count +=1
                    else:
                        time_diff = (pd.to_datetime(prev_timestamp) - pd.to_datetime(each_interruption['end_time'])).total_seconds()
                        print(f"det id {each_det} time diff {time_diff}")
                        if(time_diff == 300):
                            base_vol += each_interruption['baseline']
                            curr_vol += each_interruption['curr_volume']
                            reduction += each_interruption['reduction']
                            prev_timestamp = each_interruption['end_time']
                            count +=1

                res_dict ={}
                res_dict['SIGNALID'] = each_signal
                res_dict['REDUCTION'] = (reduction/count)*100
                res_dict['DURATION'] =300*count

                res_dict['STARTVOLUME'] = float(interruptions[0]['curr_volume'])
                res_dict['STARTBASELINE'] = interruptions[0]['baseline']
                res_dict['STARTMA'] = interruptions[0]['curr_ma']

                res_dict['TOTALVOLUME'] = float(curr_vol)
                res_dict['TOTALBASELINE'] = base_vol

                res_dict['TIMEEND'] = interruptions[0]['end_time']

                res_dict['DETECTORID'] = str(each_det)



                all_res.append(res_dict)
                client.inserts_stream('interruptionsdetector', [res_dict])
            else:
                res_dict = {}
                res_dict['DETECTORID'] = str(each_det)
                client.inserts_stream('interruptionsdetector', [res_dict])
    #             no interruptions for this detector. insert tombstone record

isc_eoi = IscEoiStore()


while True:
    try:
        all_rows = isc_eoi.start_processing()
        if(all_rows):
            insert_to_ksql_stream()
    except Exception as e:
        print(f"FAILURE DUE TO {e}")
        print(f"RETRYING IN 10 ........")
    time.sleep(10)

