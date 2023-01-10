import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
import sys
import datetime
import json
import pandas as pd
import os
import glob
from ISC_PRODUCER import MessageProducer
from multiprocessing import Process

class Watcher:

    def __init__(self, directory=".", handler=FileSystemEventHandler()):
        self.observer = Observer()
        self.handler = handler
        self.directory = directory

    def run(self):
        self.observer.schedule(
            self.handler, self.directory, recursive=True)
        self.observer.start()
        print("\nWatcher Running in {}/\n".format(self.directory))
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
        self.observer.join()
        print("\nWatcher Terminated\n")


class MyHandler(FileSystemEventHandler):


    def __init__(self,isc_id):
        self.root = 'Orlando_converted'
        self.decoder_path = 'PurdueDecoder.exe'
        self.produce = MessageProducer()
        self.isc_id = isc_id
        self.last_timestamp = None

            
    def on_moved(self, event):
#         print(f"Indi - {event}")
        try:
            path_dest = event.dest_path
            all_split = path_dest.split('/')
            city, year, month, day, isc, filename  = all_split[0], all_split[1], all_split[2], all_split[3], all_split[4],all_split[5] 
            self.check_destination_directory(year, month, day, isc)
            out_file = f"{self.root}/{year}/{month}/{day}/{isc}/{filename}.txt"
            temp = out_file.split('/')[-1].split('_')
            curr_ts = pd.to_datetime(f"{temp[2]}-{temp[3]}-{temp[4]} {temp[5][:2]}:{temp[5][2:4]}:00")
            last_ts  = self.last_timestamp
            
            print(f"Signal {isc} curr_ts {curr_ts} last_ts {last_ts}")
            
            
            process = subprocess.Popen(["wine", f"{self.decoder_path }", f"{path_dest}", f"{out_file}"])
            (output, err) = process.communicate()  

            #This makes the wait possible
            p_status = process.wait()
            self.produce.generate_messages(isc, out_file)
            
#             produce message from last to current timestamp
            self.produce.generate_message_bit_mask(isc, last_ts, curr_ts)


            if(last_ts is None):
                self.last_timestamp = curr_ts
            if((last_ts is not None)and (curr_ts>last_ts)):
                self.last_timestamp= curr_ts
            

        except Exception as e:
            print(f"deocde failed for event {event} due to {e}")
        
#         subprocess.Popen(["cp",f"{path_dest}",f"{self.root}/{year}/{month}/{day}/{isc}"])
        
        
        
    def check_destination_directory(self, year, month, day, isc):
        root = self.root
        
        
        if not os.path.exists(f"{root}/{year}"):
            os.makedirs(f"{root}/{year}")
            
        if not os.path.exists(f"{root}/{year}/{month}"):
            os.makedirs(f"{root}/{year}/{month}")
            
        if not os.path.exists(f"{root}/{year}/{month}/{day}"):
            os.makedirs(f"{root}/{year}/{month}/{day}")
            
        if not os.path.exists(f"{root}/{year}/{month}/{day}/{isc}"):
            os.makedirs(f"{root}/{year}/{month}/{day}/{isc}")

class ISCWatcher:
    def __init__(self, directory=".", handler=FileSystemEventHandler()):
        self.observer = Observer()
        self.handler = handler
        self.directory = directory
        

    def run(self):
#         watchers for already created isc folders

        self.handler.initial_init(self.directory)
        
#         self.observer.schedule(
#             self.handler, self.directory, recursive=False)
#         self.observer.start()
#         print("\nWatcher Running in {}/\n".format(self.directory))
#         try:
#             while True:
#                 time.sleep(1)
#         except:
#             self.observer.stop()
#         self.observer.join()
#         print("\nWatcher Terminated\n")
        
class ISChandler(FileSystemEventHandler):
    def __init__(self):
        self.root = 'Orlando_converted'
        self.decoder_path = 'PurdueDecoder.exe'
        self.all_isc_dict = {}
        
    def on_created(self, event):
        print(event.is_directory)
        if(event.is_directory):
            isc_path = event.src_path
            isc_id = event.src_path.split('/')[-1]
            if(isc_id not in self.all_isc_dict):
                print(f"start monitoring {isc_id} at path {isc_path}")
                print(event) # Your code here
                p = Process(target=self.start_process, args=(isc_path,isc_id))
                p.start()
                self.all_isc_dict[isc_id]=1
#                 p.join()
#                 start new process and join 

    def start_process(self,dest,isc_id):
        watcher = Watcher(dest, handler = MyHandler(isc_id))
        watcher.run()
        
    def initial_init(self, path_to):
        print(f"Yash: DOING THE INITIAL INIT **** ")
        all_isc_folders = glob.glob(f"{path_to}/*")
        for each_folder in all_isc_folders[:5]:
            isc_id = each_folder.split('/')[-1]
            print(f"INTIAL INIT for SignalID {isc_id}")
            p = Process(target=self.start_process, args=(each_folder,isc_id))
            p.start()
            self.all_isc_dict[isc_id]=1
        
        
                
                
CURRENT_DAY_PATH = 'Orlando/2022/12/15'
watcher = ISCWatcher(CURRENT_DAY_PATH, handler = ISChandler())
watcher.run()