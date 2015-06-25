# sparkmulticount
##Translates input data in Spark:

###input CSV with fomat id,ip,type_of_event
```
1, 64.236.4.133, view
1, 64.236.4.133, view
1, 64.236.4.133, impression
2, 54.72.128.142, click
```
###to CSV output with fomat id,country_from_ip,number_of_view,number_of_click,number_of_impression
```
1,US,2,0,1
2,PL,0,1,0
```
###run test
sbt test

###build and run with spark 1.4
```
sbt assembly
$YOUR_SPARK_HOME/bin/spark-submit   --class "pl.abicz.sparkmulticount.Main"  --master local[4]   target/scala-2.10/multicounter.jar `pwd`/data/ result.csv src/test/resources/GeoLiteCity.dat
```
