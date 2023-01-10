from ksql import KSQLAPI
import socket
import json
import re
import pandas as pd
import time
from datetime import datetime

client = KSQLAPI('http://localhost:8088')
streamProperties = {
    "ksql.streams.auto.offset.reset": "earliest", 
    "ksql.query.pull.table.scan.enabled": "true"
}



def parse_columns(columns_str):
    regex = r"(?<!\<)`(?P<name>[A-Z_]+)` (?P<type>[A-z]+)[\<, \"](?!\>)"
    result = []

    matches = re.finditer(regex, columns_str)
    for matchNum, match in enumerate(matches, start=1):
        result.append({"name": match.group("name"), "type": match.group("type")})

    return result


def process_row(row, column_names):
    row = row.replace(",\n", "").replace("]\n", "").rstrip("]")
    row_obj = json.loads(row)
    if "finalMessage" in row_obj:
        return None
    column_values = row_obj["row"]["columns"]
    index = 0
    result = {}
    for column in column_values:
        result[column_names[index]["name"]] = column
        index += 1

    return result

def process_query_result(results, return_objects=None):
    if return_objects is None:
        yield from results

    # parse rows into objects
    try:
        header = next(results)
    except StopIteration:
        return
    columns = parse_columns(header)

    for result in results:
        row_obj = process_row(result, columns)
        if row_obj is None:
            return
        yield row_obj
        
def return_query_df(ksql_string):
    query = client.query(ksql_string, stream_properties=streamProperties)
    tspm = list(process_query_result(query, return_objects=True))
    return pd.DataFrame(tspm)
 

def ts_unix_to_ts(time_unix):
    dt = datetime.fromtimestamp(time_unix/1000)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')    
    
def ts_str_to_unix(time_str):
    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    return int(time.mktime(dt.timetuple()) * 1000)

# FUNCTION TO RETURN MOST RECENT BIT MASK FOR ALL INTERSECTIONS
def return_recent_data_ts(look_back_time = 15):
    '''
    look_back_time: time in min 
    '''
    current_time_ms= round(time.time()*1000)
    look_time = current_time_ms - look_back_time*60*1000
    TABLE_NAME = 'ATSPM_MASK_1500SEC' #THIS IS A TABLE
    ksql_string = f'SELECT *  from {TABLE_NAME} where WINDOWEND >=' + f'{look_time}' 
    # print(ksql_string)
    df = return_query_df(ksql_string)
    return df


# FUNCTION TO RETURN COUNTS FOR A DETECTOR BETWEEN START AND END TIME. 5 sec aggregation
def return_det_counts_data(detector_id, time_start, time_end):
    '''
    detector_id: detector_id string (signalID + channel value)
    start_time: unix time in ms (INT)
    end_time: unix time in ms(INT)
    '''
    TABLE_NAME = 'COUNTS_RAW_STREAM_V1_AGG_5SEC' #Detector ID to counts  #THIS IS A TABLE
    ksql_string = f'SELECT * FROM {TABLE_NAME} where DETECTORID = ' +  "'" + detector_id + "'"   + f' AND WINDOWSTART>={time_start} AND WINDOWSTART < {time_end}'
    # print(ksql_string)
    temp_df = return_query_df(ksql_string)
    
    temp_df.drop(columns = ['DETECTORID', 'WINDOWEND'], inplace = True)
    new_row = pd.Series(data={'WINDOWSTART':time_start,'COUNT':0}, name=0)
    temp_df = temp_df.append(new_row, ignore_index=False)
    new_row = pd.Series(data={'WINDOWSTART':time_end,'COUNT':0}, name=0)
    temp_df = temp_df.append(new_row, ignore_index=False)
    temp_df['timestamp']  = pd.to_datetime(temp_df['WINDOWSTART'], unit = 'ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    temp_df.set_index('timestamp', inplace = True)
    temp_df.drop(columns = ['WINDOWSTART'], inplace = True)
    temp_df.sort_index(inplace = True)

    final_df = temp_df.resample('5s').sum()
    
    
    return final_df


