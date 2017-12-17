#!/bin/bash

# save my current directory
#MY_CWD=$(pwd)

# remove first line of files and rename

OLD_FILE="1130585.csv"
NEW_FILE="weather_history_Rhode_Island.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE

OLD_FILE="1154483.csv"
NEW_FILE="weather_history_Massachusetts.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE

OLD_FILE="Table_B_1.csv"
NEW_FILE="outage_history.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE

# change directory back to the original
#cd $MY_CWD

# clean exit
#exit
