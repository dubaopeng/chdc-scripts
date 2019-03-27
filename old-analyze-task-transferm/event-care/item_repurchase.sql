SET mapred.job.name='make item repurchase event trade';
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
set mapred.job.reuse.jvm.num.tasks=20;

add jar  hdfs://nameservice1/user/hive/resource/jar/crypt-1.0.16.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/metrics-core-2.2.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/gson-2.8.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/scala-library-2.10.4.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka_2.10-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka-clients-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/plt-hive-udf-1.0-SNAPSHOT.jar;

create temporary function decrypt as 'com.shuyun.plt.hive.udf.Decrypt';
create temporary function make_rfm as 'com.shuyun.plt.hive.udf.shoprepurchase.MakeRfm';
create temporary function filter_flag as 'com.shuyun.plt.hive.udf.itemrepurchase.FilterFlag';
create temporary function send_kafka as 'com.shuyun.plt.hive.udf.itemrepurchase.SendKafka';

set EARLY_DAY=substr(date_sub('${stat_date}',180),1,10);
set EARLY_MONTH=substr(date_sub(${hiveconf:EARLY_DAY},0),1,7);

--事件关怀配置表
create table IF NOT EXISTS event.item_repurchase_config (
	shopId string,
	itemId string,
	skuId string,
	period int,
	exclude_flag string,
	exclude_customer string,
	trigger_status string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/event/item_repurchase/item_repurchase_config/';

drop table IF EXISTS event.item_repurchase_order_config_tmp;
create table IF NOT EXISTS event.item_repurchase_order_config_tmp (
	dp_id string,
	cid string,
	num bigint,
	num_iid string,
	sku_id string,
	status string,
	oid string,
	tid string,
	created string,
	buyer_nick string,
	config_period int,
	config_exclude_flag string
)
PARTITIONED BY(config_exclude_customer int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

--关联订单和子订单
insert overwrite table event.item_repurchase_order_config_tmp partition(config_exclude_customer)
select
	o.dp_id as dp_id,
	o.cid as cid,
	o.num as num,
	o.num_iid as num_iid,
	o.sku_id as sku_id,
	o.status as status,
	o.oid as oid,
	o.tid as tid,
	o.created as created,
	o.buyer_nick as buyer_nick,
	config.period * o.num as config_period,
	config.exclude_flag as config_exclude_flag,
	config.exclude_customer as config_exclude_customer
from event.item_repurchase_config as config
left outer join
(select
	dp_id,
	cid,
	num,
	num_iid,
	status,
	oid,
	tid,
	buyer_nick,
	sku_id,
	created,
	pay_time,
	end_time from dw_base.b_top_order where (part='active' or part>=${hiveconf:EARLY_MONTH}) and substr(created,1,10)>=${hiveconf:EARLY_DAY} and (status='TRADE_FINISHED' or (pay_time!='NULL' and pay_time regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9\:\.]*$'))
) as o
on o.dp_id=config.shopId and o.num_iid=config.itemId
where ((config.trigger_status='0' and o.status='TRADE_FINISHED' and date_add(end_time, config.period * cast(o.num as int))=from_unixtime(unix_timestamp(),'yyyy-MM-dd')) or (config.trigger_status='1' and pay_time!='NULL' and pay_time regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9\:\.]*$' and date_add(pay_time, config.period * cast(o.num as int))=from_unixtime(unix_timestamp(),'yyyy-MM-dd')))
and (config.skuId is null or config.skuId='null' or config.skuId='NULL' or config.skuId='' or config.skuId=o.sku_id);


drop table IF EXISTS event.item_repurchase_effective_order_tmp;
create table IF NOT EXISTS event.item_repurchase_effective_order_tmp(
	dp_id string,
	cid string,
	num bigint,
	num_iid string,
	sku_id string,
	status string,
	oid string,
	tid string,
	created string,
	buyer_nick string,
	config_period int,
	config_exclude_flag string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert into table event.item_repurchase_effective_order_tmp
select
	dp_id,
	cid,
	num,
	num_iid,
	sku_id,
	status,
	oid,
	tid,
	created,
	buyer_nick,
	config_period,
	config_exclude_flag
from event.item_repurchase_order_config_tmp where config_exclude_customer='0';

--优化
drop table IF EXISTS event.item_repurchase_join_info_tmp;
create table IF NOT EXISTS event.item_repurchase_join_info_tmp(
	dp_id string,
	cid string,
	num bigint,
	num_iid string,
	sku_id string,
	status string,
	oid string,
	tid string,
	created string,
	buyer_nick string,
	config_period int,
	config_exclude_flag string,
	order_oid string,
	order_tid string,
	order_created string,
	order_num_iid string,
	order_cid string
)
PARTITIONED BY(config_exclude_customer int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.item_repurchase_join_info_tmp partition(config_exclude_customer)
select
	oct.dp_id,
	oct.cid,
	oct.num,
	oct.num_iid,
	oct.sku_id,
	oct.status,
	oct.oid,
	oct.tid,
	oct.created,
	oct.buyer_nick,
	oct.config_period,
	oct.config_exclude_flag,
	o.oid as order_oid,
	o.tid as order_tid,
	o.created as order_created,
	o.num_iid as order_num_iid,
	o.cid as order_cid,
	oct.config_exclude_customer
from event.item_repurchase_order_config_tmp as oct
left outer join (select dp_id,buyer_nick,created,oid,tid,num_iid,cid from dw_base.b_top_order where (part='active' or part>=${hiveconf:EARLY_MONTH}) and substr(created,1,10)>=${hiveconf:EARLY_DAY} and pay_time!='NULL' and pay_time regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9\:\.]*$') as o
on oct.dp_id=o.dp_id and oct.buyer_nick=o.buyer_nick
where oct.config_exclude_customer>'0' and oct.tid!=o.tid;

--排除同店铺已购买
insert into table event.item_repurchase_effective_order_tmp
select
	oct.dp_id,
	oct.cid,
	oct.num,
	oct.num_iid,
	oct.sku_id,
	oct.status,
	oct.oid,
	oct.tid,
	oct.created,
	oct.buyer_nick,
	oct.config_period,
	oct.config_exclude_flag
	from event.item_repurchase_order_config_tmp as oct
left outer join (
	select
	oid
	from event.item_repurchase_join_info_tmp as oc
	where oc.config_exclude_customer='1' and oc.tid is not null and oc.order_created>oc.created and (unix_timestamp(oc.created) + oc.config_period*3600*24)>=unix_timestamp(oc.order_created)
) as excludeOrder
on oct.oid=excludeOrder.oid where oct.config_exclude_customer='1' and excludeOrder.oid is null;

--排除同类目已购买
insert into table event.item_repurchase_effective_order_tmp
select
	oct.dp_id,
	oct.cid,
	oct.num,
	oct.num_iid,
	oct.sku_id,
	oct.status,
	oct.oid,
	oct.tid,
	oct.created,
	oct.buyer_nick,
	oct.config_period,
	oct.config_exclude_flag
from event.item_repurchase_order_config_tmp as oct
left outer join (
	select
	oid
	from event.item_repurchase_join_info_tmp as oc
	where oc.config_exclude_customer='2' and oc.cid=oc.order_cid and oc.tid is not null and oc.order_created>oc.created and (unix_timestamp(oc.created) + oc.config_period*3600*24)>=unix_timestamp(oc.order_created)
) as excludeOrder
on oct.oid=excludeOrder.oid where oct.config_exclude_customer='2' and excludeOrder.oid is null;

--排除同商品已购买
insert into table event.item_repurchase_effective_order_tmp
select
	oct.dp_id,
	oct.cid,
	oct.num,
	oct.num_iid,
	oct.sku_id,
	oct.status,
	oct.oid,
	oct.tid,
	oct.created,
	oct.buyer_nick,
	oct.config_period,
	oct.config_exclude_flag
from event.item_repurchase_order_config_tmp as oct
left outer join (
select
	oid
	from event.item_repurchase_join_info_tmp as oc
	where oc.config_exclude_customer='3' and oc.num_iid=oc.order_num_iid and oc.tid is not null and oc.order_created>oc.created and (unix_timestamp(oc.created) + oc.config_period*3600*24)>=unix_timestamp(oc.order_created)
) as excludeOrder
on oct.oid=excludeOrder.oid where oct.config_exclude_customer='3' and excludeOrder.oid is null;

--关联订单
drop table IF EXISTS  event.item_repurchase_allinfo_tmp;
create table IF NOT EXISTS  event.item_repurchase_allinfo_tmp (
	--子订单
	dp_id string,
	cid string,
	num bigint,
	num_iid string,
	sku_id string,
	order_status string,
	oid string,
	tid string,
	created string,
	buyer_nick string,
	--订单
	status string,
	receiver_name string,
	trade_from string,
	receiver_city string,
	receiver_district string,
	receiver_state string,
	receiver_address string,
	receiver_mobile string,
	receiver_phone string,
	pay_time string,
	payment double,
	discount_fee double,
	post_fee double,
	buyer_alipay_no string,
	end_time string,
	`type` string,
	seller_flag string,
	--member
	member_status string,
	member_grade bigint,
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

drop table IF EXISTS  event.item_repurchase_allinfo_distinct;
create table IF NOT EXISTS  event.item_repurchase_allinfo_distinct (
	--子订单
	dp_id string,
	cid string,
	num bigint,
	num_iid string,
	sku_id string,
	order_status string,
	oid string,
	tid string,
	created string,
	buyer_nick string,
	--订单
	status string,
	receiver_name string,
	trade_from string,
	receiver_city string,
	receiver_district string,
	receiver_state string,
	receiver_address string,
	receiver_mobile string,
	receiver_phone string,
	pay_time string,
	payment double,
	discount_fee double,
	post_fee double,
	buyer_alipay_no string,
	end_time string,
	`type` string,
	seller_flag string,
	--member
	member_status string,
	member_grade bigint,
	--rfm字段
	rfm ARRAY<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.item_repurchase_allinfo_tmp
select
	--子订单
	oc.dp_id as dp_id,
	oc.cid as cid,
	oc.num as num,
	oc.num_iid as num_iid,
	oc.sku_id as sku_id,
	oc.status as order_status,
	oc.oid as oid,
	oc.tid as tid,
	oc.created as created,
	rfm.buyer_nick as buyer_nick,
	--订单
	t.status as status,
	nvl(decrypt(t.receiver_name, 'receiver_name'),'') as receiver_name,
	t.trade_from as trade_from,
	t.receiver_city as receiver_city,
	t.receiver_district as receiver_district,
	t.receiver_state as receiver_state,
	nvl(decrypt(t.receiver_address, 'simple'), '') as receiver_address,
	nvl(decrypt(t.receiver_mobile, 'phone'), '') as receiver_mobile,
	nvl(decrypt(t.receiver_phone, 'phone'), '') as receiver_phone,
	t.pay_time as pay_time,
	t.payment as payment,
	t.discount_fee as discount_fee,
	t.post_fee as post_fee,
	nvl(decrypt(t.buyer_alipay_no, 'simple'), '') as buyer_alipay_no,
	t.end_time as end_time,
	t.`type` as `type`,
	t.seller_flag as seller_flag,
	--member
	m.status as member_status,
	m.grade as member_grade,
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
from event.item_repurchase_effective_order_tmp as oc
left outer join 
(select
	tid,
	status,
	receiver_name,
	trade_from,
	receiver_city,
	receiver_district,
	receiver_state,
	receiver_address,
	receiver_mobile,
	receiver_phone,
	pay_time,
	payment,
	discount_fee,
	post_fee,
	buyer_alipay_no,
	end_time,
	`type`,
	seller_flag
	 from dw_base.b_top_trade where part='active' or part>=${hiveconf:EARLY_MONTH}) as t on oc.tid=t.tid
	left outer join (select dp_id,buyer_nick,status,grade from dw_base.b_top_member) as m on oc.dp_id=m.dp_id and oc.buyer_nick=m.buyer_nick
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
	from label.label_rfm_event where part>=${hiveconf:EARLY_DAY} and period=6) as rfm on oc.dp_id=rfm.dp_id and decrypt(oc.buyer_nick,'nick')=rfm.buyer_nick
where rfm.period is not null and filter_flag(oc.config_exclude_flag, t.seller_flag)=false;

insert overwrite table event.item_repurchase_allinfo_distinct
select
	--子订单
	dp_id,
	cid,
	num,
	num_iid,
	sku_id,
	order_status,
	oid,
	tid,
	created,
	buyer_nick,
	--订单
	status,
	receiver_name,
	trade_from,
	receiver_city,
	receiver_district,
	receiver_state,
	receiver_address,
	receiver_mobile,
	receiver_phone,
	pay_time,
	payment,
	discount_fee,
	post_fee,
	buyer_alipay_no,
	end_time,
	`type`,
	seller_flag,
	--member
	member_status,
	member_grade,
	make_rfm(collect_list(period),collect_list(trade_count),collect_list(trade_tidcount),collect_list(trade_amount),collect_list(trade_item_num),collect_list(trade_refund_count),collect_list(trade_refund_amount),collect_list(trade_last_time),collect_list(trade_last_amount),collect_list(trade_last_interval),collect_list(trade_first_time),collect_list(trade_first_amount),collect_list(trade_first_interval),collect_list(trade_avg_amount),collect_list(trade_avg_item_num),collect_list(trade_avg_buy_interval),collect_list(trade_avg_confirm_interval),collect_list(trade_max_amount),collect_list(trade_discount_fee),collect_list(trade_order_discount_fee)) as rfm
from (
select
	--子订单
	dp_id,
	cid,
	num,
	num_iid,
	sku_id,
	order_status,
	oid,
	tid,
	created,
	buyer_nick,
	--订单
	status,
	receiver_name,
	trade_from,
	receiver_city,
	receiver_district,
	receiver_state,
	receiver_address,
	receiver_mobile,
	receiver_phone,
	pay_time,
	payment,
	discount_fee,
	post_fee,
	buyer_alipay_no,
	end_time,
	`type`,
	seller_flag,
	--member
	member_status,
	member_grade,
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
	trade_order_discount_fee,
	row_number() over(partition by oid) rank
from event.item_repurchase_allinfo_tmp
) as t1 
where t1.rank=1
group by
dp_id,
cid,
num,
num_iid,
sku_id,
order_status,
oid,
tid,
created,
buyer_nick,
--订单
status,
receiver_name,
trade_from,
receiver_city,
receiver_district,
receiver_state,
receiver_address,
receiver_mobile,
receiver_phone,
pay_time,
payment,
discount_fee,
post_fee,
buyer_alipay_no,
end_time,
`type`,
seller_flag,
--member
member_status,
member_grade;

--往kafka推送消息
create table IF NOT EXISTS event.item_repurchase_send_kafka_result (
	tid string,
	num_iid string,
	result bigint
)
PARTITIONED BY(part STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

insert overwrite table event.item_repurchase_send_kafka_result partition(part='${stat_date}')
select
	tid,
	num_iid,
	send_kafka('product_repurchase_order','10.153.205.127:9092,10.153.205.93:9092,10.153.205.112:9092',
	dp_id,
	cid,
	num,
	num_iid,
	sku_id,
	order_status,
	oid,
	tid,
	created,
	buyer_nick,
	--订单
	status,
	receiver_name,
	trade_from,
	receiver_city,
	receiver_district,
	receiver_state,
	receiver_address,
	receiver_mobile,
	receiver_phone,
	pay_time,
	payment,
	discount_fee,
	post_fee,
	buyer_alipay_no,
	end_time,
	`type`,
	seller_flag,
	--member
	member_status,
	member_grade,
	rfm
) as result
from event.item_repurchase_allinfo_distinct;