#!/bin/bash

# CONFIG TO CHANGE
S3_BUCKET="big-data-2026-project"
LOCAL_HIVE_CMD="/home/ubuntu/apache-hive-2.3.9-bin/bin/hive"
CLUSTER_HIVE_CMD="/usr/bin/hive"

# ------
TASKS=("3_1" "3_2" "3_3")
SIZES=(25 50 75 100 200 400)


CLUSTER=false
while getopts "c" opt; do
  case $opt in
    c) CLUSTER=true ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

if [ "$CLUSTER" = true ]; then
  HIVE_CMD="$CLUSTER_HIVE_CMD"
  BASE="s3a://${S3_BUCKET}"
  LOG_DIR="s3://${S3_BUCKET}/logs/hive"
else
  HIVE_CMD="$LOCAL_HIVE_CMD"
  BASE="hdfs:///big_data"
  LOG_DIR="${PROJECT_DIR}/logs/hive"
  mkdir -p "$LOG_DIR"
fi

run_hive() {
  local TASK=$1
  local SIZE=$2

  local INPUT_DIR="${BASE}/data/processed/flights_cleaned_${SIZE}.parquet"

  if [ "$CLUSTER" = true ]; then
    HIVE_SCRIPT="s3://${S3_BUCKET}/src/analysis/hive/${TASK}_hive.sql"
    LOG_FILE="${LOG_DIR}/${TASK}/${SIZE}/${TASK}_size${SIZE}_$(date +%Y%m%d_%H%M%S).log"
  else
    HIVE_SCRIPT="${PROJECT_DIR}/src/analysis/hive/${TASK}_hive.sql"
    mkdir -p "${LOG_DIR}/${TASK}/${SIZE}"
    LOG_FILE="${LOG_DIR}/${TASK}/${SIZE}/${TASK}_${SIZE}_$(date +%s).log"
  fi

  echo "============================================================="
  echo "Running Hive task: ${TASK}"
  echo "SIZE: ${SIZE}"
  echo "INPUT: ${INPUT_DIR}"
  echo "SCRIPT: ${HIVE_SCRIPT}"
  echo "============================================================="

  START_NS=$(date +%s%N)

  LOG_OUTPUT=$(${HIVE_CMD} \
    --hivevar INPUT_DIR=${INPUT_DIR} \
    --hivevar SIZE=${SIZE} \
    -f ${HIVE_SCRIPT} 2>&1)

  HIVE_EXIT=$?

  END_NS=$(date +%s%N)
  TOTAL_SEC=$(awk "BEGIN {printf \"%.0f\", ($END_NS-$START_NS)/1000000000}")

  if [ "$CLUSTER" = true ]; then
    echo "$LOG_OUTPUT" | aws s3 cp - "$LOG_FILE"
  else
    echo "$LOG_OUTPUT" > "$LOG_FILE"
  fi

  echo "Log saved: $LOG_FILE"

  if [ $HIVE_EXIT -ne 0 ] || echo "$LOG_OUTPUT" | grep -q "FAILED\|Error during job"; then
    echo "ERROR: Hive job failed!"
    return 1
  fi

  echo "TOTAL EXECUTION TIME: ${TOTAL_SEC} sec"

  echo "Per-query times:"
  echo "$LOG_OUTPUT" | grep "Time taken" | awk -F': ' '{
    gsub(" seconds","",$2);
    print " - "$2" sec"
  }'
}

for TASK in "${TASKS[@]}"; do
  for SIZE in "${SIZES[@]}"; do
    run_hive "$TASK" "$SIZE"
  done
done