--csv第一行是title可以通过
--sed -i '1d' aisles.csv 将第一行去掉再导入到hive


-- 产品的具体信息
create table test.products(
product_id string,
product_name string,
aisle_id string,  --  所属小类别
department_id string  --  所属大类别
)ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties(
 "skip.header.line.count"="1")
;

load data local inpath 
'/mnt/hgfs/share_fold2/products.csv' 
into table test.products;

--
create table test.orders
(order_id string,   -- 订单id
user_id string,   -- 用户id
eval_set string,
order_number string,  --  用户对应订单的顺序
order_dow string,  -- 星期一到星期天
order_hour_of_day string,  -- 订单的   hour
days_since_prior_order string  --  距离上一订单的天数
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 
'/mnt/hgfs/share_fold2/orders.csv'
into table test.orders;

create table test.aisles(aisle_id string,aisle string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 
'/mnt/hgfs/share_fold2/aisles.csv'
into table test.aisles;

create table test.departments(
department_id string,department string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 
'/mnt/hgfs/share_fold2/departments.csv'
into table test.departments;

-- 行为数据
create table test.priors(
order_id string,   --订单id
product_id string,  --产品id
add_to_cart_order string,  -- 加到购物车的顺序
reordered string    -- 是否再次购买 0 1
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 
'/mnt/hgfs/share_fold2/order_products_prior.csv'
into table test.priors;

-- 订单数据 一般从数据库或者hbase过来  购买记录   行为数据
create table test.trains(
order_id string,     --订单id
product_id string,       --产品id
add_to_cart_order int,  -- 加到购物车的顺序
reordered int  -- 是否再次购买 0 1
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
load data local inpath 
'/mnt/hgfs/share_fold2/order_products_train.csv'
into table test.trains;
