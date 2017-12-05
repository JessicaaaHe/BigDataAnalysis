filename="$1"

mkdir nullvalue_out

spark-submit null_value.py "$filename"
for ((i=0; i<24; i++))
do
	hadoop fs -getmerge "nullvalue_$i.out" "nullvalue_$i.txt"
	mv "nullvalue_$i.txt" nullvalue_out/
	hadoop fs -rm -r "nullvalue_$i.out"
done
