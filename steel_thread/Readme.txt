after cloning, copy the steel_thread folder to your working directory
cd into the copy of the steel_thread folder
bash remove_headers.sh to format the historical weather and outage CSVs
bash setup_virtual_environment.sh installs Anaconda to /data and starts the virtual environment for the steel thread

do a clean startup of hadoop, postgres, and hive metastore:
1) mount HDFS volume
2) /root/start-hadoop.sh
3) /data/start_postgres.sh
4) /data/start_metastore.sh

then run spark-submit run_steel_thread_v3.py

To connect to Tableau:
1) As root user, enter "hive --service hiveserver2 &"
2) Open Tableau Desktop and connect to Cloudera Hadoop. Enter in server information and sign in.
3) Set Schema as "default"
4) Select table "ri_outage_table"
