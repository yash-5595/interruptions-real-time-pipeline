read year mon date <<<$(date  +'%Y %m %d')
date1=02
echo Orlando/$year/$mon/$date
#cd /data/atspm_raw_data

if [ ! -d "Orlando" ]; then
    mkdir Orlando
fi
if [ ! -d "Orlando/$year" ]; then
    mkdir Orlando/$year
fi
if [ ! -d "Orlando/$year/$mon" ]; then
    mkdir Orlando/$year/$mon
fi
if [ ! -d "Orlando/$year/$mon/$date" ]; then
    echo $date
    mkdir Orlando/$year/$mon/$date
fi
python3 manage_scripts/s3sync/s3sync.py --config  manage_scripts/s3sync/config.yaml -v DEBUG pull --s3path s3://intersection-atspm/Orlando/$year/$mon/$date --localpath Orlando/$year/$mon/$date --interval 5


