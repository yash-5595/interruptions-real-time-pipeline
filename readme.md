
## Check this for complete documentation

https://docs.google.com/document/d/1d7dXlSkUq_joeQXNaEIJwnMU3CQ9jwByMeqSz3V8dcw/edit?usp=sharing

### Sync from s3 to local - starts syncing data for the current day 

 ./sync_data.sh

### file system event watchers, Convert to txt file, Kafka producer - Python watchdog looks for new downloaded files from S3, triggers a process to convert dat file to txt file and start generating Kafka messages 

python ISC_WATCHER.py   


### Online EOI computation 


python start_gen_interruptions.py

### Use the docker-compose.yml file to setup various docker containers for the overall system. More info -

 https://docs.google.com/document/d/1d7dXlSkUq_joeQXNaEIJwnMU3CQ9jwByMeqSz3V8dcw/edit#bookmark=id.id3xp0306ulr