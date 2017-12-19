#!/bin/bash
PATH=/data/anaconda3/envs/steel_thread/bin:/data/spark15/bin:/data/anaconda3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/jdk1.7.0_79/bin:/usr/lib/spark/bin:/usr/lib/hadoop/bin:/home/w205/bin
cd /home/w205/final_thread
source activate steel_thread
/data/spark15/bin/spark-submit run_final_thread_v1.py >> results.txt
source deactivate
echo "end" >> end.txt
