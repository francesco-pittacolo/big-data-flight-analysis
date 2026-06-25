#!/bin/bash

# CONFIGURAZIONE
SIZE="100"
TASK="3_1"
CLUSTER=false

# DA CAMBIARE IN BASE AI PROPRI PATH
S3_BUCKET="big-data-2026-project"
LOCAL_HIVE_CMD="/home/ubuntu/apache-hive-2.3.9-bin/bin/hive"
CLUSTER_HIVE_CMD="/usr/bin/hive"

while getopts "s:ct:" opt; do
    case $opt in
        s) SIZE="$OPTARG" ;;
        c) CLUSTER=true ;;
        t) TASK="$OPTARG" ;;
    esac
done


SCRIPT_DIR=$( cd "$(dirname "$0")" && pwd )
PROJECT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)



# PATH INPUT
if [ "$CLUSTER" = true ]; then
    HIVE_SCRIPT="s3://${S3_BUCKET}/src/analysis/hive/${TASK}_hive.sql"
    HIVE_CMD="$CLUSTER_HIVE_CMD"
    BASE="s3a://${S3_BUCKET}"
    LOG_DIR="s3://${S3_BUCKET}/logs/hive/${TASK}/${SIZE}"

else
    HIVE_SCRIPT="${PROJECT_DIR}/src/analysis/hive/${TASK}_hive.sql"
    HIVE_CMD="$LOCAL_HIVE_CMD"
    BASE="hdfs:///big_data"
    LOG_DIR="${SCRIPT_DIR}/../logs/hive/${TASK}/${SIZE}"

    mkdir -p ${LOG_DIR}

fi

INPUT_DIR="${BASE}/data/processed/flights_cleaned_${SIZE}.parquet"


# LOG FILE
LOG_FILE="${LOG_DIR}/${TASK}_${SIZE}_$(date +%s).log"


echo "============================================================="
echo "Running Hive task: ${TASK}"
echo "SIZE: ${SIZE}"
echo "INPUT: ${INPUT_DIR}"
echo "SCRIPT: ${HIVE_SCRIPT}"
echo "============================================================="


# START TIMER
START_NS=$(date +%s%N)


# RUN HIVE
LOG_OUTPUT=$(${HIVE_CMD} \
    --hivevar INPUT_DIR=${INPUT_DIR} \
    --hivevar SIZE=${SIZE} \
    -f ${HIVE_SCRIPT} 2>&1)

HIVE_EXIT=$?


# END TIMER
END_NS=$(date +%s%N)

TOTAL_SEC=$(awk "BEGIN {printf \"%.0f\", ($END_NS - $START_NS)/1000000000}")


# SALVA LOG
if [ "$CLUSTER" = true ]; then
    echo "$LOG_OUTPUT" | aws s3 cp - ${LOG_FILE}
else
    echo "$LOG_OUTPUT" > ${LOG_FILE}
fi

echo "Log saved to: ${LOG_FILE}"


# CHECK ERRORI
if [ $HIVE_EXIT -ne 0 ] || echo "$LOG_OUTPUT" | grep -q "FAILED\|Error during job"; then

    echo "ERROR: Hive job failed! See log: ${LOG_FILE}"
    exit 1

fi

# OUTPUT
echo ""
echo "TOTAL EXECUTION TIME: ${TOTAL_SEC} sec"


echo ""
echo "Per-query times:"

echo "$LOG_OUTPUT" | grep "Time taken" | awk -F': ' '{
    gsub(" seconds","",$2);
    print " - " $2 " sec"
}'


SUM=$(echo "$LOG_OUTPUT" | grep "Time taken" | awk -F': ' '{
    gsub(" seconds","",$2);
    sum+=$2
} END {print sum}')


echo ""
echo "SUM OF HIVE INTERNAL TIMES: ${SUM} sec"