SET mapred.job.name='offline-member-consume-rate-analyze';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=4915;
set tez.runtime.unordered.output.buffer.size-mb=1640;
set tez.runtime.io.sort.mb=6553;
set hive.cbo.enable=true;
SET hive.exec.compress.output=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
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


set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 平台店铺级的总金额及次数计算结果
create table if not exists dw_rfm.b_shop_sale_total_statics_temp(
	card_plan_id string,
	plat_code string,
	shop_id string,
	uni_shop_id string,
	date_type int,
	total_payment double,
	total_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 计算卡、平台、店铺的总统计指标
insert overwrite table dw_rfm.b_shop_sale_total_statics_temp partition(part='${stat_date}')
select t.card_plan_id,t.plat_code,t.shop_id,t.uni_shop_id,t.date_type,t.total_payment,t.total_times
from(
	select card_plan_id,plat_code,shop_id,uni_shop_id,1 as date_type,
		sum(receive_payment) as total_payment,
		count(created) as total_times
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and yestoday=1
	group by card_plan_id,plat_code,shop_id,uni_shop_id
	union all
	select card_plan_id,plat_code,shop_id,uni_shop_id,2 as date_type,
		sum(receive_payment) as total_payment,
		count(created) as total_times
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and thisweek=1
	group by card_plan_id,plat_code,shop_id,uni_shop_id
	union all
	select card_plan_id,plat_code,shop_id,uni_shop_id,3 as date_type,
		sum(receive_payment) as total_payment,
		count(created) as total_times
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and thismonth=1
	group by card_plan_id,plat_code,shop_id,uni_shop_id
	union all
	select card_plan_id,plat_code,shop_id,uni_shop_id,4 as date_type,
		sum(receive_payment) as total_payment,
		count(created) as total_times
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and last7day=1
	group by card_plan_id,plat_code,shop_id,uni_shop_id
	union all
	select card_plan_id,plat_code,shop_id,uni_shop_id,5 as date_type,
		sum(receive_payment) as total_payment,
		count(created) as total_times
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and last30day=1
	group by card_plan_id,plat_code,shop_id,uni_shop_id
) t;

-- 计算平台级的指标计算
create table if not exists dw_rfm.b_plat_sale_total_statics_temp(
	card_plan_id string,
	plat_code string,
	date_type int,
	total_payment double,
	total_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_plat_sale_total_statics_temp partition(part='${stat_date}')
select card_plan_id,plat_code,date_type,
	sum(total_payment) as total_payment,
	sum(total_times) as total_times
from dw_rfm.b_shop_sale_total_statics_temp 
where part='${stat_date}'
group by card_plan_id,plat_code,date_type;


-- 计算会员平台会员的总指标
create table if not exists dw_rfm.b_shop_member_total_statics_temp(
	card_plan_id string,
	plat_code string,
	shop_id string,
	date_type int,
	mtotal_payment double,
	mtotal_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_shop_member_total_statics_temp partition(part='${stat_date}')
select t.card_plan_id,t.plat_code,t.shop_id,t.date_type,t.mtotal_payment,t.mtotal_times
from(
	select card_plan_id,plat_code,shop_id,1 as date_type,
		sum(receive_payment) mtotal_payment,
		count(created) mtotal_times
	from dw_rfm.b_offline_last30day_member_trade_temp
	where part='${stat_date}' and  yestoday=1
	group by card_plan_id,plat_code,shop_id
	union all
	select card_plan_id,plat_code,shop_id,2 as date_type,
		sum(receive_payment) mtotal_payment,
		count(created) mtotal_times
	from dw_rfm.b_offline_last30day_member_trade_temp
	where part='${stat_date}' and  thisweek=1
	group by card_plan_id,plat_code,shop_id
	union all
	select card_plan_id,plat_code,shop_id,3 as date_type,
		sum(receive_payment) mtotal_payment,
		count(created) mtotal_times
	from dw_rfm.b_offline_last30day_member_trade_temp
	where part='${stat_date}' and  thismonth=1
	group by card_plan_id,plat_code,shop_id
	union all
	select card_plan_id,plat_code,shop_id,4 as date_type,
		sum(receive_payment) mtotal_payment,
		count(created) mtotal_times
	from dw_rfm.b_offline_last30day_member_trade_temp
	where part='${stat_date}' and  last7day=1
	group by card_plan_id,plat_code,shop_id
	union all
	select card_plan_id,plat_code,shop_id,5 as date_type,
		sum(receive_payment) mtotal_payment,
		count(created) mtotal_times
	from dw_rfm.b_offline_last30day_member_trade_temp
	where part='${stat_date}' and  last30day=1
	group by card_plan_id,plat_code,shop_id
) t;


-- 计算会员平台会员的总指标
create table if not exists dw_rfm.b_plat_member_total_statics_temp(
	card_plan_id string,
	plat_code string,
	date_type int,
	mtotal_payment double,
	mtotal_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_plat_member_total_statics_temp partition(part='${stat_date}')
select card_plan_id,plat_code,date_type,
	sum(mtotal_payment) as mtotal_payment,
	sum(mtotal_times) as mtotal_times
from dw_rfm.b_shop_member_total_statics_temp
where part='${stat_date}' 
group by card_plan_id,plat_code,date_type;


-- 计算各个等级的会员销售数据
create table if not exists dw_rfm.b_shop_grade_total_statics_temp(
	card_plan_id string,
	plat_code string,
	shop_id string,
	grade int,
	date_type int,
	mtotal_payment double,
	mtotal_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_shop_grade_total_statics_temp partition(part='${stat_date}')
select card_plan_id,plat_code,shop_id,grade,1 as date_type,
	sum(receive_payment) mtotal_payment,
	count(created) mtotal_times
from dw_rfm.b_offline_last30day_member_trade_temp
where part='${stat_date}' and yestoday=1
group by card_plan_id,plat_code,shop_id,grade
union all
select card_plan_id,plat_code,shop_id,grade,2 as date_type,
	sum(receive_payment) mtotal_payment,
	count(created) mtotal_times
from dw_rfm.b_offline_last30day_member_trade_temp
where part='${stat_date}' and thisweek=1
group by card_plan_id,plat_code,shop_id,grade
union all
select card_plan_id,plat_code,shop_id,grade,3 as date_type,
	sum(receive_payment) mtotal_payment,
	count(created) mtotal_times
from dw_rfm.b_offline_last30day_member_trade_temp
where part='${stat_date}' and thismonth=1
group by card_plan_id,plat_code,shop_id,grade
union all
select card_plan_id,plat_code,shop_id,grade,4 as date_type,
	sum(receive_payment) mtotal_payment,
	count(created) mtotal_times
from dw_rfm.b_offline_last30day_member_trade_temp
where part='${stat_date}' and last7day=1
group by card_plan_id,plat_code,shop_id,grade
union all
select card_plan_id,plat_code,shop_id,grade,5 as date_type,
	sum(receive_payment) mtotal_payment,
	count(created) mtotal_times
from dw_rfm.b_offline_last30day_member_trade_temp
where part='${stat_date}' and last30day=1
group by card_plan_id,plat_code,shop_id,grade;

-- 计算店铺各个时段的会员销售综合统计数据
insert into table dw_rfm.b_shop_grade_total_statics_temp partition(part='${stat_date}')
select card_plan_id,plat_code,shop_id,99 as grade,date_type,
	   sum(mtotal_payment) mtotal_payment,
	   sum(mtotal_times) mtotal_times
from dw_rfm.b_shop_grade_total_statics_temp
where part='${stat_date}'
group by card_plan_id,plat_code,shop_id,date_type;


-- 计算会员平台会员的总指标
create table if not exists dw_rfm.b_plat_grade_total_statics_temp(
	card_plan_id string,
	plat_code string,
	grade int,
	date_type int,
	mtotal_payment double,
	mtotal_times bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_plat_grade_total_statics_temp partition(part='${stat_date}')
select card_plan_id,plat_code,grade,date_type,
	sum(mtotal_payment) as mtotal_payment,
	sum(mtotal_times) as mtotal_times
from dw_rfm.b_shop_grade_total_statics_temp
where part='${stat_date}'
group by card_plan_id,plat_code,grade,date_type;


-- 接下来需要把平台级、各个等级的关联起来，算比例,99表示不限等级
create table if not exists dw_rfm.cix_offline_plat_member_sale_rate(
	card_plan_id string,
	plat_code string,	
	date_type int,
	total_payment double,
	mtotal_payment double,
	sale_rate double,
	total_times bigint,
	mtotal_times bigint,
	times_rate double,
	grade int,
	stat_date string,
	modified string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_offline_plat_member_sale_rate partition(part='${stat_date}')
select t1.card_plan_id,t1.plat_code,t1.date_type,t1.total_payment,t1.mtotal_payment,
	   case when t1.total_payment=0 then -1 else round(t1.mtotal_payment/t1.total_payment,4) end as sale_rate,
	   t1.total_times,t1.mtotal_times,
	   case when t1.total_times=0 then -1 else round(t1.mtotal_times/t1.total_times,4) end as times_rate,
	   t1.grade,
	    '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from (
	select a.card_plan_id,a.plat_code,a.date_type,a.total_payment,a.total_times,
		   if(b.mtotal_payment is null,0,b.mtotal_payment) as mtotal_payment,
		   if(b.mtotal_times is null,0,b.mtotal_times) as mtotal_times,
		   b.grade
	from (select card_plan_id,plat_code,date_type,total_payment,total_times from dw_rfm.b_plat_sale_total_statics_temp where part='${stat_date}')a
	left join 
	(select card_plan_id,plat_code,date_type,mtotal_payment,mtotal_times,grade from dw_rfm.b_plat_grade_total_statics_temp where part='${stat_date}') b
	on a.card_plan_id=b.card_plan_id and a.plat_code=b.plat_code and a.date_type=b.date_type
	where b.grade is not null 
) t1;

-- 将有销售记录的店铺和各个时间段里面,各等级的数据关联起来
create table if not exists dw_rfm.cix_offline_sale_shops_of_card(
	card_plan_id string,
	plat_code string,
	uni_shop_id string,
	shop_name string,
	date_type int,
	grade int,
	total_payment double,
	mtotal_payment double,
	sale_rate double,
	total_times bigint,
	mtotal_times bigint,
	times_rate double,
	stat_date string,
	modified string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_offline_sale_shops_of_card partition(part='${stat_date}')
select  t.card_plan_id,t.plat_code,t.uni_shop_id,t.shop_name,t.date_type,
		if(c.grade is null,99,c.grade) as grade,
		if(t.total_payment is null,0,t.total_payment) as total_payment,
		if(c.mtotal_payment is null,0,c.mtotal_payment) as mtotal_payment,
		case when t.total_payment=0 then -1 when c.mtotal_payment is null then 0 else c.mtotal_payment/t.total_payment end as sale_rate,
		if(t.total_times is null,0,t.total_times) as total_times,
		if(c.mtotal_times is null,0,c.mtotal_times) as mtotal_times,
		case when t.total_times=0 then -1 when c.mtotal_times is null then 0 else c.mtotal_times/t.total_times end as times_rate,
		'${stat_date}' as stat_date,
	    ${hiveconf:submitTime} as modified
from(
	select a.card_plan_id,a.plat_code,a.uni_shop_id,a.shop_id,a.date_type,a.total_payment,a.total_times,b.shop_name
	from (
		select card_plan_id,plat_code,uni_shop_id,shop_id,date_type,total_payment,total_times 
			from dw_rfm.b_shop_sale_total_statics_temp where part='${stat_date}') a
	left join 
	(
		select plat_code,shop_id,shop_name 
		from dw_base.b_std_tenant_shop where plat_code='OFFLINE' or plat_code='TAOBAO'
		group by plat_code,shop_id,shop_name
	)b
	on a.plat_code=b.plat_code and a.shop_id=b.shop_id
) t
left join 
(select card_plan_id,plat_code,shop_id,date_type,grade,mtotal_payment,mtotal_times from dw_rfm.b_shop_grade_total_statics_temp where part='${stat_date}') c
on t.card_plan_id=c.card_plan_id and t.plat_code=c.plat_code and t.shop_id=c.shop_id and t.date_type=c.date_type;


ALTER TABLE dw_rfm.b_plat_sale_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_shop_member_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_plat_member_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_shop_grade_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_plat_grade_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_shop_sale_total_statics_temp DROP IF EXISTS PARTITION (part='${stat_date}');


--需要将 cix_offline_plat_member_sale_rate,cix_offline_sale_shops_of_card的结果同步到
