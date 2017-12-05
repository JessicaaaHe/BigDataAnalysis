filename="$1"

mkdir validate_out

spark-submit column_validate.py "$filename"

for((i=0; i<24; i++))
do
	hadoop fs -getmerge "validate_$i.out" "validate_$i.txt"
	mv "validate_$i.txt" validate_out/
	hadoop fs -rm -r "validate_$i.out"
done
