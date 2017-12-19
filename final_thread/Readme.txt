Demo Video: https://drive.google.com/open?id=1hs661E9WI_TOuGKafBCBjclYgPr6VjKW

About the Instance:
We have created an AMI, "UCB W205 Fall 2017 Power Outage Project" with AMI ID "ami-c35927b9" with our github repo cloned and conda environment installed. After connecting to the instance, do a clean startup of Hadoop, postgres, Hive metastore and HiveServer2:
1) mount HDFS volume
2) /root/start-hadoop.sh
3) /data/start_postgres.sh
4) /data/start_metastore.sh
5) hive --service hiveserver2 &

Steps for Running the program:
1. Switch to user w205 with: "su - w205"
2. Change into the directory called final_thread: "cd final_thread"
2. Before running the bash scripts, you may need to change the permissions on the file with the following:
  - chmod u+x,g+x remove_headers.sh
  - chmod u+x,g+x setup_virtual_environment.sh
  - chmod u+x,g+x run_bash.sh
3. (Optional) If you need to format the historical weather and outage CSVs: "./remove_headers.sh"
  - This step has already been completed for you in this AMI.
4. (Optional) If conda environment is not installed: "./setup_virtual_environment.sh" to install Anaconda
  - This step has already been completed for you in this AMI.
5. To run the program, activate the conda environment: "source activate steel_thread"
6. Once in the conda environment, run the script: "spark-submit run_final_thread_v1.py"

To connect to Tableau (assuming hiveserver2 is started):
1) Open Tableau Desktop and connect to Cloudera Hadoop. Enter in server information and sign in.
2) Set Schema as "default"
3) Select table "ri_outage_table"

Notes on scheduled cron jobs:
At this time, we have cron jobs scheduled every hour to run "run_bash.sh" which pulls forecast data from Weather Underground API and runs our run_final_thread_v1.py script. To check if the cron jobs are correctly scheduled and running, follow these steps:
1. First, check the timezone you're in by entering "date" at the command line. Take note of the MIN and HR as you will need to check the cront job script with up-to-date MIN and HR. 
2. Enter "crontab -e" at the command line
3. Insert this script: "57 * * * * /home/w205/final_thread/run_bash.sh"
4. Adapt the first two fields accordingly. (i.e. If the current time is 13:00 and you are schedule the cron job for 13:01, the script would be "01 13 * * * /home/w205/final_thread/run_bash.sh"
