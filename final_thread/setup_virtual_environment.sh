#!/bin/bash

# save my current directory
MY_CWD=$(pwd)

cd /data
wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh
bash Anaconda3-5.0.1-Linux-x86_64.sh
conda create -n steel_thread numpy scipy scikit-learn pandas
source activate steel_thread

# change directory back to the original
cd $MY_CWD

# clean exit
#exit
