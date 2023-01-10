import json
import socket
import sys

from dateutil.parser import parse
from confluent_kafka import Producer
import csv
import pandas as pd
import datetime
import time

class MessageProducer:
    
    def __init__(self):
        self.file_path = None
        self.topic = 'topic_ATSPM_RAW_v1'
        

        self.conf = {'bootstrap.servers': "localhost:9092",
                'client.id': socket.gethostname()}
        self.producer = Producer(self.conf)
        
        
    def acked(self,err, msg):
        print(f"called {msg} {err}")
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
        else:
            print(f"Message produced: {msg.value()}")
            
    def generate_message_bit_mask(self, isc_id, last_ts, curr_ts):
        result = {}
        
        
        curr_str_time = curr_ts.strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.datetime.strptime(curr_str_time, "%Y-%m-%d %H:%M:%S")
        unix_time =  int(time.mktime(dt.timetuple()) * 1000)
        
        result['ts']= unix_time
        result['SignalID']=isc_id
        result['flag']  = 1
        jresult = json.dumps(result)
        print(f"PRODUCE OUT ISC {isc_id} json {result}")
        self.producer.produce('atspm_bit_mask' , key=isc_id, value=jresult, callback=self.acked)
        
        if((last_ts is not None)and (curr_ts>last_ts)):
            all_ts = pd.date_range(start=last_ts,end=curr_ts, freq= '1min')
            if(len(all_ts)>2):
                for each_ts in all_ts[1:-1]:
                    result = {}
                    
                    curr_str_time = each_ts.strftime("%Y-%m-%d %H:%M:%S")
                    dt = datetime.datetime.strptime(curr_str_time, "%Y-%m-%d %H:%M:%S")
                    unix_time =  int(time.mktime(dt.timetuple()) * 1000)
                    
                    result['ts']=unix_time
                    result['SignalID']=isc_id
                    result['flag']  = 0
                    jresult = json.dumps(result)
                    self.producer.produce('atspm_bit_mask' , key=isc_id, value=jresult, callback=self.acked)
                    print(f"PRODUCE IN ISC {isc_id} json {result}")
            
            
            
            
    def generate_messages(self,isc_id, path):
        df_file = pd.read_csv(path,sep=',',skiprows = 6,header=None,names = ['Timestamp','EventCode','EventParam'] )
        
#         print(f"df_file {df_file.shape}")
        for i,each_row  in df_file.iterrows():
            result = {}
            result["SignalID"] = isc_id
            dt = datetime.datetime.strptime(each_row['Timestamp'], '%m/%d/%Y %H:%M:%S.%f')
            unix_time =  int(time.mktime(dt.timetuple()) * 1000)
            str_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
            result["ts"] = unix_time
            result["EventCode"] = each_row['EventCode']
            result["EventParam"] = each_row['EventParam']

            # Convert dict to json as message format
            jresult = json.dumps(result)
#             print(f"{jresult}")
            self.producer.produce(self.topic , key=isc_id, value=jresult, callback=self.acked)
    
        self.producer.flush()
        
            
        
    