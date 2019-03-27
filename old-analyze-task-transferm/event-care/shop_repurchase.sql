SET mapred.job.name='make shop repurchase event trade';
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


--加载依赖jar包
add jar  hdfs://nameservice1/user/hive/resource/jar/crypt-1.0.16.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/metrics-core-2.2.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/gson-2.8.0.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/scala-library-2.10.4.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka_2.10-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/kafka-clients-0.8.2.2.jar;
add jar  hdfs://nameservice1/user/event/resource/jar/plt-hive-udf-1.0-SNAPSHOT.jar;

--加载udf函数
create temporary function decrypt as 'com.shuyun.plt.hive.udf.Decrypt';
create temporary function hbase_rowkey as 'com.shuyun.plt.hive.udf.HbaseRowkey';
create temporary function make_rfm as 'com.shuyun.plt.hive.udf.shoprepurchase.MakeRfm';
create temporary function make_order as 'com.shuyun.plt.hive.udf.shoprepurchase.MakeOrder';
create temporary function send_kafka as 'com.shuyun.plt.hive.udf.shoprepurchase.SendKafka';
create temporary function check_shop_repurchase as 'com.shuyun.plt.hive.udf.shoprepurchase.CheckShopRepurchase';

set EARLY_DAY=substr(date_sub('${stat_date}',180),1,10);
set EARLY_MONTH=substr(date_sub(${hiveconf:EARLY_DAY},0),1,7);

--hbase创建表,做预分区
--create 'shop_repurchase_event_record', {NAME=>'f1', TTL=>'15552000',COMPRESSION=>'snappy'},{SPLITS=>['d1ca3aaf52b41acd68ebb3bf69079bd1','5eceadafba9f7df05d245049d9d2de4e','e8d212ab1136e5aa6c15f6df68f5f5ff','d46bf94e9eb1d22281a71504685082ac','8bf95db53b52762745b2190b9b211b17','613569eed46c33a2749bcdf06013a161','8c80456a9e2ea7f460d61701612b7021','c457bc8dbdcd7f5be129d40f9d7e12d6','6a6534106f21563d07691889dd7f7fce','4999644a5eb7bd56311478a71d156106','2fd6b77fd56be004843b699a43638429','e9d335e45a9e772331a4eddae718755b','d8c3614bf71c13f85046b04deb88ede7','744e3ef237b814d0fcbe6b35c61fc509','1baa60574366a3798c0e3c02f93962db']}
--rowkey(基于tid生成)
--f1:tid,f1:shopId,f1:createTime

--创建事件排重表
CREATE EXTERNAL TABLE IF NOT EXISTS event.shop_repurchase_event_record(
	id string,
	tid string,
	shopId string,
	createTime string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,f1:tid,f1:shopId,f1:createTime")
TBLPROPERTIES ("hbase.table.name" = "shop_repurchase_event_record");

--创建店铺订购表(订购店铺复购的店铺)

--  sql如下：
--  SELECT shop_id FROM `t_ea_event_subscribe` WHERE started =1 AND scene_id = '-10022';
--  sqoop命令如下：
--  import --connect jdbc:mysql://rm-vy1dff92wpbsj738a.mysql.rds.aliyuncs.com:3306/ebm --username u_ebm --password dM2lmhntaf 
--  --query "SELECT shop_id,variables FROM `t_ea_event_subscribe` WHERE started =1 AND scene_id = '-10022' AND $CONDITIONS GROUP BY shop_id" --split-by shop_id
--  -m 1 --delete-target-dir --target-dir /user/event/shop_repurchase/shop_repurchase_shop/
create table IF NOT EXISTS event.shop_repurchase_shop (
	shopId string,
	config string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/event/shop_repurchase/shop_repurchase_shop/';


--创建临时订单表，存放最近90天的订单数据（已经根据店铺和nick排重，并且已经剔除了已经生成了事件的订单）
drop table event.shop_repurchase_event_trade_90_tmp;
create table IF NOT EXISTS event.shop_repurchase_event_trade_90_tmp (
	tid string,
	status string,
	dp_id string,
	created string,
	buyer_nick string,
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
	end_time bigint,
	`type` string,
	seller_flag string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

--set mapreduce.map.java.opts=-Xmx3g;
--set mapreduce.reduce.java.opts=-Xmx4g;
--set mapreduce.map.memory.mb=3500;
--set mapreduce.reduce.memory.mb=4500;

--一个店铺下的一个用户只保留最近一条订单信息，并且排除掉已经生成事件的订单
insert overwrite table event.shop_repurchase_event_trade_90_tmp
select 
	tid,
	status,
	dp_id,
	created,
	nvl(decrypt(buyer_nick, 'nick'),'') as buyer_nick,
	nvl(decrypt(receiver_name, 'receiver_name'),'') as receiver_name,
	trade_from,
	receiver_city,
	receiver_district,
	receiver_state,
	nvl(decrypt(receiver_address, 'simple'), '') as receiver_address,
	nvl(decrypt(receiver_mobile, 'phone'), '') as receiver_mobile,
	nvl(decrypt(receiver_phone, 'phone'), '') as receiver_phone,
	pay_time,
	payment,
	discount_fee,
	post_fee,
	nvl(decrypt(buyer_alipay_no, 'simple'), '') as buyer_alipay_no,
	case when end_time is null or end_time='null' or end_time='NULL' then 0 else unix_timestamp(end_time) * 1000 end as end_time,
	`type`,
	seller_flag
from (
	select 
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
	row_number() over(partition by dp_id,buyer_nick order by created desc) rank2
from (
	select 
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
	config,
	rank() over(partition by dp_id,buyer_nick order by created desc) rank
from (
	select
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
	from dw_base.b_top_trade 
	where (part='active' or part>=${hiveconf:EARLY_MONTH}) 
		and substr(created,1,10)>=${hiveconf:EARLY_DAY} 
		and (status='TRADE_FINISHED' or (pay_time!='NULL' and pay_time regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9\:\.]*$'))
  ) tmpB 
  left outer join (select shopId,config from event.shop_repurchase_shop) srs on tmpB.dp_id=srs.shopId where srs.shopId is not null
) t 
	where t.rank=1 and (check_shop_repurchase(t.pay_time,t.config)=1 or check_shop_repurchase(t.end_time,t.config)=1)
) dist_t 
where dist_t.rank2=1;


--创建临时表，保存trade-member-rfm join后的数据
drop table event.trade_member_rfm_tmp;
create table IF NOT EXISTS event.trade_member_rfm_tmp (
	--trade
	tid string,
	status string,
	dp_id string,
	created string,
	buyer_nick string,
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
	end_time bigint,
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

--创建临时表，保存trade-member-rfm数据（同一个订单rfm聚合成一条）
drop table trade_member_rfm_distinct;
create table IF NOT EXISTS event.trade_member_rfm_distinct (
	--trade
	tid string,
	status string,
	dp_id string,
	created string,
	buyer_nick string,
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
	end_time bigint,
	`type` string,
	seller_flag string,
	--member
	member_status string,
	member_grade bigint,
	rfm ARRAY<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

--trade-member-rfm数据进行join
insert overwrite table event.trade_member_rfm_tmp
select 
	--trade
	t.tid as tid,
	t.status as status,
	t.dp_id as dp_id,
	t.created as created,
	t.buyer_nick as buyer_nick,
	t.receiver_name as receiver_name,
	t.trade_from as trade_from,
	t.receiver_city as receiver_city,
	t.receiver_district as receiver_district,
	t.receiver_state as receiver_state,
	t.receiver_address as receiver_address,
	t.receiver_mobile as receiver_mobile,
	t.receiver_phone as receiver_phone,
	t.pay_time as pay_time,
	t.payment as payment,
	t.discount_fee as discount_fee,
	t.post_fee as post_fee,
	t.buyer_alipay_no as buyer_alipay_no,
	t.end_time as end_time,
	t.type as `type`,
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
from 
(select 
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
from event.shop_repurchase_event_trade_90_tmp) t 
left outer join (select dp_id,buyer_nick,status,grade from dw_base.b_top_member) m on t.dp_id=m.dp_id and t.buyer_nick=decrypt(m.buyer_nick, 'nick')
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
	from label.label_rfm_event where part>=${hiveconf:EARLY_DAY} and period=6) rfm on t.dp_id=rfm.dp_id and t.buyer_nick=rfm.buyer_nick
where rfm.period is not null;

--将join后的数据，根据订单，把rfm进行聚合处理
insert overwrite table event.trade_member_rfm_distinct
select
--订单字段
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
--member字段
	member_status,
	member_grade,
--rfm array<string>
	make_rfm(collect_list(period),collect_list(trade_count),collect_list(trade_tidcount),collect_list(trade_amount),collect_list(trade_item_num),collect_list(trade_refund_count),collect_list(trade_refund_amount),collect_list(trade_last_time),collect_list(trade_last_amount),collect_list(trade_last_interval),collect_list(trade_first_time),collect_list(trade_first_amount),collect_list(trade_first_interval),collect_list(trade_avg_amount),collect_list(trade_avg_item_num),collect_list(trade_avg_buy_interval),collect_list(trade_avg_confirm_interval),collect_list(trade_max_amount),collect_list(trade_discount_fee),collect_list(trade_order_discount_fee)) as rfm
from event.trade_member_rfm_tmp
group by tid,status,dp_id,created,buyer_nick,receiver_name,trade_from,receiver_city,receiver_district,receiver_state,receiver_address,receiver_mobile,receiver_phone,pay_time,payment,discount_fee,post_fee,buyer_alipay_no,end_time,`type`,seller_flag,member_status,member_grade;


--创建临时表，保存和子订单的join信息
drop table event.trade_member_rfm_order_tmp;
create table IF NOT EXISTS event.trade_member_rfm_order_tmp (
	--trade
	tid string,
	status string,
	dp_id string,
	created string,
	buyer_nick string,
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
	end_time bigint,
	`type` string,
	seller_flag string,
	--member
	member_status string,
	member_grade bigint,
	rfm ARRAY<string>,
	order_cid string,
	order_num bigint,
	order_num_iid string,
	order_status string,
	order_oid string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

--创建临时表，保存和子订单的join信息(根据订单聚合order)
drop table event.trade_member_rfm_order_distinct;
create table IF NOT EXISTS event.trade_member_rfm_order_distinct (
	--trade
	tid string,
	status string,
	dp_id string,
	created string,
	buyer_nick string,
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
	end_time bigint,
	`type` string,
	seller_flag string,
	--member
	member_status string,
	member_grade bigint,
	rfm ARRAY<string>,
	orders ARRAY<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;


--trade-member-rfm数据和order进行join
insert overwrite table event.trade_member_rfm_order_tmp
select 
	tmrd.tid as tid,
	tmrd.status as status,
	dp_id,
	created,
	buyer_nick,
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
--member字段
	member_status,
	member_grade,
	rfm,
	o.cid as order_cid,
	o.num as order_num,
	o.num_iid as order_num_iid,
	o.status as order_status,
	o.oid as order_oid
from event.trade_member_rfm_distinct tmrd left outer join 
(select tid,cid,num,num_iid,status,oid from dw_base.b_top_order where (part='active' or part>=${hiveconf:EARLY_MONTH}) and substr(created,1,10)>=${hiveconf:EARLY_DAY}) o 
on tmrd.tid=o.tid where o.tid is not null;

--聚合order信息
--将join后的数据，根据订单，把rfm进行聚合处理
insert overwrite table event.trade_member_rfm_order_distinct
select
--订单字段
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
--member字段
	member_status,
	member_grade,
	rfm,
--order array<string>
	make_order(collect_list(order_cid),collect_list(order_num),collect_list(order_num_iid),collect_list(order_status),collect_list(order_oid))
from event.trade_member_rfm_order_tmp
group by tid,status,dp_id,created,buyer_nick,receiver_name,trade_from,receiver_city,receiver_district,receiver_state,receiver_address,receiver_mobile,receiver_phone,pay_time,payment,discount_fee,post_fee,buyer_alipay_no,end_time,`type`,seller_flag,member_status,member_grade,rfm;


--创建临时表，保存发送结果
create table IF NOT EXISTS event.shop_repurchase_send_kafka_result (
	dp_id string,
	tid string,
	result bigint
)
PARTITIONED BY(part STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

--组装信息，通过udf发送信息到kafka
insert overwrite table event.shop_repurchase_send_kafka_result partition(part='${stat_date}')
select
	dp_id,
	tid,
	send_kafka('shop_repurchase_trade', '10.153.205.127:9092,10.153.205.93:9092,10.153.205.112:9092',
	--订单字段
	tid,
	status,
	dp_id,
	created,
	buyer_nick,
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
--member字段
	member_status,
	member_grade,
--rfm array<stirng>
	rfm,
--order array<string>
	orders
) as result
from event.trade_member_rfm_order_distinct;
