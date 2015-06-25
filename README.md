# sparkmulticount
Translates input data in Spark:

input CSV:
id,ip,type_of_event
1, 64.236.4.133, view
1, 64.236.4.133, view
1, 64.236.4.133, impression
2, 54.72.128.142, click

to output 
id,country_from_ip,number_of_view,number_of_click,number_of_impression
1,US,2,0,1
2,PL,0,1,0
