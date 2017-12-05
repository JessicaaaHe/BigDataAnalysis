filename="$1"
mkdir statistic_out

spark-submit column_statistic.py "$filename"
for((i=0; i<24; i++))
do
	hadoop fs -getmerge "statistic_$i.out" "statistic_$i.txt"
	mv "statistic_$i.txt" statistic_out/
	hadoop fs -rm -r "statistic_$i.out"
done
