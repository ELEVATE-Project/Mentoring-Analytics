#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date

echo "RUNNING JOB"

echo ""
echo "$(date)"
echo "====================================="
echo "MentorEd - Daily User and Session Reports Execution == Started"
. /opt/analytics/spark_venv/bin/activate && /opt/analytics/spark_venv/lib/python3.7/site-packages/pyspark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.7 /opt/analytics/Mentoring-Analytics/reports/pyspark_user_session_batch.py
echo "MentorEd - Daily User and Session Reports Execution == Completed"
echo "*************************************"

echo "COMPLETED"
