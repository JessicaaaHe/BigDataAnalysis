filename="$1"
mkdir report_date_level_out
mkdir location_date_level_out

spark-submit test.py "$filename"
hadoop fs -getmerge "report_date_level.out" "report_date_level.txt"
hadoop fs -getmerge "location_date_level.out" "location_date_level.txt"
mv "eport_date_level.txt" report_date_level_out/
mv "location_date_level.txt" location_date_level/

hadoop fs -rm -r "report_date_level.out"
hadoop fs -rm -r "location_date_level.out"
