#!/bin/bash

base_path="/mnt/c/Users/PaulTitto/Documents/Dicoding/dbtProject/dbt-mini_order/dbt_mini_order"

python_script="${base_path}/elt_pipeline.py"

python "$python_script" >> "${base_path}/logs/luigi_process.log" 2>&1

dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi started at ${dt}" >> "${base_path}/logs/luigi_info.log"