hbase
http://www.apache.org/dist/phoenix/apache-phoenix-4.14.0-cdh5.12.2/parcels/
phoenix-sqlline.py

hbase 查询
查看表 list
scan 'test_yuan'
scan 'test_yuan' ,{LIMIT=>5}  limit必须是大写
get 'test_yuan','row1'     根据rowkey查询
scan 'test_yuan',{STARTROW=>'row1',STOPROW=>'row3'}  rowkey 大于等于row1，小于row3
scan 'test_yuan',{COLUMNS=>'base:name'}   查询指定列名
scan 'test_yuan',{COLUMNS=>['base:name','base:famm']}   查询多列


scan 'test_yuan', FILTER=>"ValueFilter(=,'binary:value 1')"  值等于 value 1
scan 'test_yuan', FILTER=>"ValueFilter(=,'substring:1')"   值包含了1
更多可以查看https://blog.csdn.net/huangxia73/article/details/50514931
scan 'test_yuan', FILTER=>"ColumnPrefixFilter('name') AND ValueFilter(=,'binary:value 1')"   查询某一列的值以及这列的值等于。


phoenix建表
create table test (mykey integer not null primary key, mycolumn varchar);
upsert into test values(1,'Hello');
upsert into test values(2,'World!');
select * from test;



从hbase 导入到phoenix 
https://blog.csdn.net/Yuan_CSDF/article/details/97264624

!table   查看表
!quit


