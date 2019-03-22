SET mapred.job.name='offline-member-consume-statistic-analyze';
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

-- 平台线下门店的数据
drop table if exists dw_rfm.b_offline_shop_last30day_trade;
create table if not exists dw_rfm.b_offline_shop_last30day_trade(
	card_plan_id string,
	plat_code string,
	shop_id string,
	uni_shop_id string,
	receive_payment double,
	created string,
	yestoday int,
	thisweek int,
	thismonth int,
	last7day int,
	last30day int,
	member_id string,
	grade int,
	open_shop_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 计算线下门店的消费数据,识别会员在门店的记录
insert overwrite table dw_rfm.b_offline_shop_last30day_trade
select card_plan_id,plat_code,shop_id,uni_shop_id,receive_payment,created,
	   yestoday,thisweek,thismonth,last7day,last30day,member_id,grade,open_shop_id
from dw_rfm.b_offline_last30day_member_trade_temp
where plat_code='OFFLINE';

-- 计算开卡门店为消费店铺的会员消费统计(在开卡门店的消费会员数、总金额、人均购买金额、总次数,平均次数、客单价)
drop table if exists dw_rfm.b_opencard_shop_member_consume_statics;
create table if not exists dw_rfm.b_opencard_shop_member_consume_statics(
	card_plan_id string,
	plat_code string,
	shop_id string,
	uni_shop_id string,
	date_type int,
	grade int,
	members bigint,
	total_payment double,
	avg_price double,
	total_times bigint,
	avg_times double,
	single_price double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 计算各个等级在指定时段内的会员消费统计
insert overwrite table dw_rfm.b_opencard_shop_member_consume_statics
select t.card_plan_id,t.plat_code,t.shop_id,t.uni_shop_id,t.date_type,grade,
	   t.members,
	   t.total_payment,
	   (t.total_payment/t.members) as avg_price, 
	   t.total_times,
	   (t.total_times/t.members) as avg_times, 
	   (t.total_payment/t.total_times) as single_price  
from(
	select card_plan_id,plat_code,shop_id,uni_shop_id,grade,1 as date_type,
	   count(distinct member_id) as members,
	   sum(receive_payment) as total_payment,
	   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where yestoday=1 and shop_id=open_shop_id
	group by card_plan_id,plat_code,shop_id,uni_shop_id,grade
	union all 
	select card_plan_id,plat_code,shop_id,uni_shop_id,grade,2 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where thisweek=1 and shop_id=open_shop_id
	group by card_plan_id,plat_code,shop_id,uni_shop_id,grade
	union all 
	select card_plan_id,plat_code,shop_id,uni_shop_id,grade,3 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where thismonth=1 and shop_id=open_shop_id
	group by card_plan_id,plat_code,shop_id,uni_shop_id,grade
	union all 
	select card_plan_id,plat_code,shop_id,uni_shop_id,grade,4 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where last7day=1 and shop_id=open_shop_id
	group by card_plan_id,plat_code,shop_id,uni_shop_id,grade
	union all
	select card_plan_id,plat_code,shop_id,uni_shop_id,grade,5 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where last30day=1 and shop_id=open_shop_id
	group by card_plan_id,plat_code,shop_id,uni_shop_id,grade
)t;

-- 开卡店铺内会员在该店铺消费汇总，不限等级
insert into table dw_rfm.b_opencard_shop_member_consume_statics
select t.card_plan_id,t.plat_code,t.shop_id,t.uni_shop_id,t.date_type,99 as grade,
	   t.members,
	   t.total_payment,
	   (t.total_payment/t.members) as avg_price, 
	   t.total_times,
	   (t.total_times/t.members) as avg_times, 
	   (t.total_payment/t.total_times) as single_price  
from(
	select card_plan_id,plat_code,shop_id,uni_shop_id,date_type,
	   sum(members) as members,
	   sum(total_payment) as total_payment,
	   sum(total_times) as total_times
	from dw_rfm.b_opencard_shop_member_consume_statics
	group by card_plan_id,plat_code,shop_id,uni_shop_id,date_type
)t;

-- 计算各个店铺中的会员在所有店铺中的消费统计
drop table if exists dw_rfm.b_shop_all_member_consume_statics;
create table if not exists dw_rfm.b_shop_all_member_consume_statics(
	card_plan_id string,
	plat_code string,
	open_shop_id string,
	date_type int,
	grade int,
	members bigint,
	total_payment double,
	avg_price double,
	total_times bigint,
	avg_times double,
	single_price double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_shop_all_member_consume_statics
select t.card_plan_id,t.plat_code,t.open_shop_id,t.date_type,t.grade,
	   t.members,
	   t.total_payment,
	   (t.total_payment/t.members) as avg_price, 
	   t.total_times,
	   (t.total_times/t.members) as avg_times, 
	   (t.total_payment/t.total_times) as single_price
from (
	select card_plan_id,plat_code,open_shop_id,grade,1 as date_type,
	   count(distinct member_id) as members,
	   sum(receive_payment) as total_payment,
	   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where yestoday=1
	group by card_plan_id,plat_code,open_shop_id,grade
	union all 
	select card_plan_id,plat_code,open_shop_id,grade,2 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where thisweek=1
	group by card_plan_id,plat_code,open_shop_id,grade
	union all 
	select card_plan_id,plat_code,open_shop_id,grade,3 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where thismonth=1
	group by card_plan_id,plat_code,open_shop_id,grade
	union all 
	select card_plan_id,plat_code,open_shop_id,grade,4 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where last7day=1
	group by card_plan_id,plat_code,open_shop_id,grade
	union all
	select card_plan_id,plat_code,open_shop_id,grade,5 as date_type,
		   count(distinct member_id) as members,
		   sum(receive_payment) as total_payment,
		   count(created) as total_times
	from dw_rfm.b_offline_shop_last30day_trade
	where last30day=1
	group by card_plan_id,plat_code,open_shop_id,grade
) t;

--按日期汇总的店铺所有会员消费数据统计
insert into table dw_rfm.b_shop_all_member_consume_statics
select a.card_plan_id,a.plat_code,a.open_shop_id,a.date_type,99 as grade,
		a.members,
		a.total_payment,
		(a.total_payment/a.members) as avg_price,
		a.total_times,
		(a.total_times/a.members) as avg_times, 
	    (a.total_payment/a.total_times) as single_price
from (
	select t.card_plan_id,t.plat_code,t.open_shop_id,t.date_type,
		   sum(t.members) as members,
		   sum(t.total_payment) as total_payment,
		   sum(t.total_times) as total_times 
	from dw_rfm.b_shop_all_member_consume_statics t
	group by t.card_plan_id,t.plat_code,t.open_shop_id,t.date_type
) a;

-- 店铺在各时段内各等级对应会员总数
drop table if exists dw_rfm.b_offline_shop_members_count_temp;
create table if not exists dw_rfm.b_offline_shop_members_count_temp(
	card_plan_id string,
	plat_code string,
	uni_shop_id string,
	shop_id string,
	shop_name string,
	grade int,
	members bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 计算店铺在各个时段不限等级和各等级对应的会员人数
insert overwrite table dw_rfm.b_offline_shop_members_count_temp
select t.card_plan_id,t.plat_code,t.uni_shop_id,t1.shop_id,t1.shop_name,t.grade,t.members
from (
	select card_plan_id,plat_code,uni_shop_id,grade,count(member_id) as members
	from dw_business.b_std_member_base_info where plat_code='OFFLINE'
	group by card_plan_id,plat_code,uni_shop_id,grade
) t
left join 
(
	select plat_code,shop_id,concat(plat_code,'|',shop_id) as uni_shop_id,shop_name 
	from dw_base.b_std_tenant_shop where plat_code='OFFLINE'
) t1
on t.plat_code=t1.plat_code and t.uni_shop_id=t1.uni_shop_id;


-- 不限等级时店铺的会员存量
insert into dw_rfm.b_offline_shop_members_count_temp
select card_plan_id,plat_code,uni_shop_id,shop_id,shop_name,99 as grade,sum(members) as members
from dw_rfm.b_offline_shop_members_count_temp
group by card_plan_id,plat_code,uni_shop_id,shop_id,shop_name;


-- 基于该数据和其他各个等级的数据进行合并，输出结果表
drop table if exists dw_rfm.cix_offline_member_consume_statistic;
create table if not exists dw_rfm.cix_offline_member_consume_statistic(
	card_plan_id string,
	uni_shop_id string,
	shop_name string,
	date_type int,
	grade int,
	store_members bigint,
	shop_members bigint,
	shop_payment double,
	shop_avg_price double,
	shop_avg_times double,
	shop_single_price double,
	all_members bigint,
	all_payment double,
	all_avg_price double,
	all_avg_times double,
	all_single_price double,
	payment_rate double,
	stat_date string,
	modified string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_offline_member_consume_statistic
select a.card_plan_id,a.uni_shop_id,a.shop_name,b.date_type,a.grade,
	   a.members as store_members,
	   if(b.members is null,0,b.members) as shop_members,
	   if(b.total_payment is null,0,b.total_payment) as shop_payment,
	   if(b.avg_price is null,0,b.avg_price) as shop_avg_price,
	   if(b.avg_times is null,0,b.avg_times) as shop_avg_times,
	   if(b.single_price is null,0,b.single_price) as shop_single_price,
	   if(c.members is null,0,c.members) as all_members,
	   if(c.total_payment is null,0,c.total_payment) as all_payment,
	   if(c.avg_price is null,0,c.avg_price) as all_avg_price,
	   if(c.avg_times is null,0,c.avg_times) as all_avg_times,
	   if(c.single_price is null,0,c.single_price) as all_single_price,
	   case when c.total_payment is null then -1 when b.total_payment is null then 0 else b.total_payment/c.total_payment end as payment_rate,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from dw_rfm.b_offline_shop_members_count_temp a
left join dw_rfm.b_opencard_shop_member_consume_statics b
on a.card_plan_id=b.card_plan_id and a.uni_shop_id=b.uni_shop_id and a.grade=b.grade
left join dw_rfm.b_shop_all_member_consume_statics c
on a.card_plan_id=c.card_plan_id and a.plat_code=c.plat_code and a.shop_id=c.open_shop_id and a.grade=c.grade and b.date_type=c.date_type;


--需要将 cix_offline_member_consume_statistic 的结果同步到
