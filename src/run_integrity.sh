filename="$1"

mkdir integrity_out

spark-submit Integrity.py "$filename"

hadoop fs -getmerge "integrity.out" "integrity.txt"
mv "integrity.txt" integrity_out/
hadoop fs -rm -r "integrity.out"
