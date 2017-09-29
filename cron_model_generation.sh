#!/bin/bash
echo "RUNNING JOB"
/Path/to/spark-submit /Path/to/model_generation.py

# 0 4 * * *  /Path/To/cron_model_generation.sh --add this to crontab to execute batch job at 4AM every night