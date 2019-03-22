SET mapred.job.name='festive care';
SET hive.exec.compress.output=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapreduce.reduce.shuffle.input.buffer.percent =0.6;
SET hive.exec.max.created.files=655350;
SET hive.exec.max.dynamic.partitions=10000000;
SET hive.exec.max.dynamic.partitions.pernode=10000000;
set hive.stats.autogather=false;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task = 512000000;
set hive.support.concurrency=false;

add jar  hdfs://nameservice1/user/event/resource/jar/commons-codec-1.10.jar;
add jar  hdfs://nameservice1/user/hive/resource/jar/crypt-1.0.16.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/metrics-core-2.2.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/gson-2.8.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/scala-library-2.10.4.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka_2.10-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka-clients-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/plt-hive-udf-1.0-SNAPSHOT.jar;

create temporary function decrypt as 'com.shuyun.plt.hive.udf.Decrypt';
create temporary function make_rfm as 'com.shuyun.plt.hive.udf.shoprepurchase.MakeRfm';
create temporary function check_care_time as 'com.shuyun.plt.hive.udf.festivecare.CheckCareTime';
create temporary function send_kafka as 'com.shuyun.plt.hive.udf.festivecare.SendKafka';

set CURRENT_YEAR=substr('${stat_date}',1,4);

create table IF NOT EXISTS event.festive_care_shop (
shopId string,
config string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/event/festive_care/festive_care_shop/';

drop table IF EXISTS event.festive_care_shop_distinct;
create table IF NOT EXISTS event.festive_care_shop_distinct (
shopId string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/event/festive_care/festive_care_shop_distinct/';

insert overwrite table event.festive_care_shop_distinct
select shopId from event.festive_care_shop
where check_care_time(config,'${stat_date}')=1 group by shopId;


drop table IF EXISTS event.festive_care_effective_member;
create table IF NOT EXISTS event.festive_care_effective_member (
buyer_nick string,
dp_id string,
status string,
grade bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.festive_care_effective_member
select
m.buyer_nick as buyer_nick,
m.dp_id as dp_id,
m.status as status,
m.grade as grade
from event.festive_care_shop_distinct as s right outer join
(select buyer_nick,dp_id,status,grade from dw_base.b_top_member) as m
on s.shopId=m.dp_id
where s.shopId is not null;

drop table IF EXISTS event.festive_care_effective_member_customer;
create table IF NOT EXISTS event.festive_care_effective_member_customer (
buyer_nick string,
dp_id string,
status string,
grade bigint,
full_name string,
mobile string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.festive_care_effective_member_customer
select
member.buyer_nick as buyer_nick,
member.dp_id as dp_id,
member.status as status,
member.grade as grade,
customer.full_name as full_name,
customer.mobile as mobile
from event.festive_care_effective_member as member
left outer join (select buyer_nick,full_name,mobile from dw_base.b_top_customer) as customer on(member.buyer_nick=customer.buyer_nick)
where customer.mobile is not null and customer.mobile!='' and customer.full_name is not null and customer.full_name!='';

drop table IF EXISTS event.festive_care_rfm_tmp;
create table IF NOT EXISTS event.festive_care_rfm_tmp (
buyer_nick string,
dp_id string,
status string,
grade bigint,
full_name string,
mobile string,
--rfm字段
period bigint,
trade_count bigint,
trade_tidcount bigint,
trade_amount double,
trade_item_num bigint,
trade_refund_count bigint,
trade_refund_amount double,
trade_last_time string,
trade_last_amount double,
trade_last_interval bigint,
trade_first_time string,
trade_first_amount double,
trade_first_interval bigint,
trade_avg_amount double,
trade_avg_item_num double,
trade_avg_buy_interval double,
trade_avg_confirm_interval double,
trade_max_amount double,
trade_discount_fee double,
trade_order_discount_fee double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;
drop table IF EXISTS event.festive_care_rfm_distinct;
create table IF NOT EXISTS event.festive_care_rfm_distinct (
buyer_nick string,
dp_id string,
status string,
grade bigint,
full_name string,
mobile string,
--rfm字段
rfm ARRAY<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.festive_care_rfm_tmp
select
rfm.buyer_nick as buyer_nick,
mc.dp_id as dp_id,
mc.status as status,
mc.grade as grade,
nvl(decrypt(mc.full_name, 'receiver_name'), '') as full_name,
nvl(decrypt(mc.mobile, 'phone'), '') as mobile,
--rfm字段
rfm.period as period,
rfm.trade_count as trade_count,
rfm.trade_tidcount as trade_tidcount,
rfm.trade_amount as trade_amount,
rfm.trade_item_num as trade_item_num,
rfm.trade_refund_count as trade_refund_count,
rfm.trade_refund_amount as trade_refund_amount,
rfm.trade_last_time as trade_last_time,
rfm.trade_last_amount as trade_last_amount,
rfm.trade_last_interval as trade_last_interval,
rfm.trade_first_time as trade_first_time,
rfm.trade_first_amount as trade_first_amount,
rfm.trade_first_interval as trade_first_interval,
rfm.trade_avg_amount as trade_avg_amount,
rfm.trade_avg_item_num as trade_avg_item_num,
rfm.trade_avg_buy_interval as trade_avg_buy_interval,
rfm.trade_avg_confirm_interval as trade_avg_confirm_interval,
rfm.trade_max_amount as trade_max_amount,
rfm.trade_discount_fee as trade_discount_fee,
rfm.trade_order_discount_fee as trade_order_discount_fee
from event.festive_care_effective_member_customer mc
left outer join (select
	dp_id,
	buyer_nick,
	period,
	trade_count,
	trade_tidcount,
	trade_amount,
	trade_item_num,
	trade_refund_count,
	trade_refund_amount,
	trade_last_time,
	trade_last_amount,
	trade_last_interval,
	trade_first_time,
	trade_first_amount,
	trade_first_interval,
	trade_avg_amount,
	trade_avg_item_num,
	trade_avg_buy_interval,
	trade_avg_confirm_interval,
	trade_max_amount,
	trade_discount_fee,
	trade_order_discount_fee
	from label.label_rfm_event where period=6) as rfm
on (mc.dp_id=rfm.dp_id and decrypt(mc.buyer_nick,'nick')=rfm.buyer_nick)
where rfm.period is not null;

insert overwrite table event.festive_care_rfm_distinct
select
buyer_nick,
dp_id,
status,
grade,
full_name,
mobile,
--rfm array<string>
make_rfm(collect_list(period),collect_list(trade_count),collect_list(trade_tidcount),collect_list(trade_amount),collect_list(trade_item_num),collect_list(trade_refund_count),collect_list(trade_refund_amount),collect_list(trade_last_time),collect_list(trade_last_amount),collect_list(trade_last_interval),collect_list(trade_first_time),collect_list(trade_first_amount),collect_list(trade_first_interval),collect_list(trade_avg_amount),collect_list(trade_avg_item_num),collect_list(trade_avg_buy_interval),collect_list(trade_avg_confirm_interval),collect_list(trade_max_amount),collect_list(trade_discount_fee),collect_list(trade_order_discount_fee)) as rfm
from event.festive_care_rfm_tmp
group by buyer_nick,dp_id,status,grade,full_name,mobile;

--创建临时表，保存发送结果
create table IF NOT EXISTS event.festive_care_send_kafka_result (
buyer_nick string,
result bigint
)
PARTITIONED BY(part STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.festive_care_send_kafka_result partition(part='${stat_date}')
select
buyer_nick,
send_kafka('holiday_member','10.153.205.127:9092,10.153.205.93:9092,10.153.205.112:9092',
buyer_nick,
dp_id,
status,
grade,
full_name,
mobile,
rfm
) as result
from event.festive_care_rfm_distinct;
