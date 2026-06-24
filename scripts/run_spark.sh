#!/bin/bash

MODE=""
IMPL="spark_core"

while getopts "cm:" opt; do
  case $opt in
    c) MODE="-c" ;;
    m) IMPL="$OPTARG" ;;
  esac
done


S3_DIR="big-data-2026-project"

TASKS=("3_1" "3_2" "3_3")
SIZES=(25 50 75 100 200 400)

EXECUTOR="spark-submit"


SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)


if [ "$MODE" = "-c" ]; then

  BASE_SCRIPT_DIR="s3://${S3_DIR}/src/analysis/spark"
  LOG_DIR="s3://${S3_DIR}/logs/spark"

else

  BASE_SCRIPT_DIR="${PROJECT_DIR}/src/analysis/spark"
  LOG_DIR="${PROJECT_DIR}/logs/spark"

  mkdir -p "$LOG_DIR"

fi


for TASK in "${TASKS[@]}"; do

  SCRIPT="${BASE_SCRIPT_DIR}/task_${TASK}/${TASK}_${IMPL}.py"


  echo "=================================================="
  echo "TASK $TASK | IMPL $IMPL"
  echo "SCRIPT $SCRIPT"
  echo "=================================================="


  for SIZE in "${SIZES[@]}"; do


    if [ "$MODE" != "-c" ]; then
      mkdir -p "${LOG_DIR}/${TASK}/${IMPL}/${SIZE}"
    fi

    LOG_FILE="${LOG_DIR}/${TASK}/${IMPL}/${SIZE}/${TASK}_${IMPL}_${SIZE}_$(date +%s).log"

    echo "RUN TASK=$TASK SIZE=$SIZE"

    if [ "$MODE" = "-c" ]; then

      $EXECUTOR \
        "$SCRIPT" \
        -s "$SIZE" \
        $MODE \
        2>&1 | aws s3 cp - "$LOG_FILE"

    else

      /usr/bin/time -f "TIME=%e sec" \
        $EXECUTOR \
        "$SCRIPT" \
        -s "$SIZE" \
        $MODE \
        > "$LOG_FILE" 2>&1

    fi

    echo "LOG SAVED: $LOG_FILE"
    echo ""

  done

done