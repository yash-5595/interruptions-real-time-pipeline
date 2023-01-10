import numpy as np
import pandas as pd
from ksql import KSQLAPI


client = KSQLAPI('http://localhost:8088')

mappings_file = pd.read_csv('MAPPINGS_DET_INFO_OCT_2022.csv')

cols_of_interest = ['ATSPM_ID', 'channel', 'phase']

adv_detectors = mappings_file[mappings_file.distanceToStopbar>=120][cols_of_interest]
adv_detectors.rename({'ATSPM_ID': 'SignalID'}, axis=1, inplace=True)
adv_detectors = adv_detectors[adv_detectors.phase.isin(['2','6'])]
adv_detectors['channel_str'] = [str(x) for x in adv_detectors['channel'] ]

adv_detectors['DetectorID'] = [str(x) + y for x,y in zip(adv_detectors['SignalID'],  adv_detectors['channel_str'])]     
adv_detectors['SignalID'] = [str(x) for x in adv_detectors['SignalID']]
adv_detectors.drop(['channel_str'],axis = 1, inplace = True)
adv_detectors["phase"] = pd.to_numeric(adv_detectors["phase"])
adv_detectors['DetType'] = 'stp'

client.inserts_stream('detectorid', adv_detectors.to_dict('records'))