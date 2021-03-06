SET mapred.job.name='b_shop_rfm_history 店铺RFM月底数据分析';
--set hive.execution.engine=mr;
set hive.tez.container.size=6144;
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

-- 注:以下数据依赖于店铺级客户RFM宽表 b_qqd_shop_rfm
-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--设置M的间隔值
set mType1=50;
set mType2=100;
set mType3=200;
set mType4=500;
set mType5=1000;
set mType6=2000;

-- 计算前13月月底日期
set pre1MonthEnd=date_sub(concat(substr('${stat_date}',0,7),'-01'),1);
set pre2MonthEnd=add_months(${hiveconf:pre1MonthEnd},-1);
set pre3MonthEnd=add_months(${hiveconf:pre1MonthEnd},-2);
set pre4MonthEnd=add_months(${hiveconf:pre1MonthEnd},-3);
set pre5MonthEnd=add_months(${hiveconf:pre1MonthEnd},-4);
set pre6MonthEnd=add_months(${hiveconf:pre1MonthEnd},-5);
set pre7MonthEnd=add_months(${hiveconf:pre1MonthEnd},-6);
set pre8MonthEnd=add_months(${hiveconf:pre1MonthEnd},-7);
set pre9MonthEnd=add_months(${hiveconf:pre1MonthEnd},-8);
set pre10MonthEnd=add_months(${hiveconf:pre1MonthEnd},-9);
set pre11MonthEnd=add_months(${hiveconf:pre1MonthEnd},-10);
set pre12MonthEnd=add_months(${hiveconf:pre1MonthEnd},-11);
set pre13MonthEnd=add_months(${hiveconf:pre1MonthEnd},-12);

-- 第一步：对RFM数据进行数据打标，记录每行数据所属RFM的坐标

drop table if exists dw_rfm.b_shop_history_rfm_temp;
create table dw_rfm.b_shop_history_rfm_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,t.year_payment,t.year_buy_times,t.year_buy_num,t.stat_date,
--R维度打标
case when datediff(t.stat_date,t.year_last_time)<=30 then 1
	when datediff(t.stat_date,t.year_last_time)>30 and datediff(t.stat_date,t.year_last_time)<=90 then 2
	when datediff(t.stat_date,t.year_last_time)>90 and datediff(t.stat_date,t.year_last_time)<=180 then 3
	when datediff(t.stat_date,t.year_last_time)>180 and datediff(t.stat_date,t.year_last_time)<=365 then 4 end as recency,	
--F维度打标
case when t.year_buy_times=1 then 1 
	when t.year_buy_times=2  then 2
    when t.year_buy_times=3 then 3  
	when t.year_buy_times=4  then 4
	when t.year_buy_times>=5 then 5 end as frequency,
-- M维度打标
case when t.year_payment<=${hiveconf:mType1} then 1
	when t.year_payment>${hiveconf:mType1} and t.year_payment <= (${hiveconf:mType1}*2) then 2
	when t.year_payment>(${hiveconf:mType1}*2) and t.year_payment <= (${hiveconf:mType1}*3) then 3
	when t.year_payment>(${hiveconf:mType1}*3) and t.year_payment <= (${hiveconf:mType1}*4) then 4
	when t.year_payment>(${hiveconf:mType1}*4) and t.year_payment <= (${hiveconf:mType1}*5) then 5
	when t.year_payment>(${hiveconf:mType1}*5) and t.year_payment <= (${hiveconf:mType1}*6) then 6
	when t.year_payment>(${hiveconf:mType1}*6) and t.year_payment <= (${hiveconf:mType1}*7) then 7
	when t.year_payment>(${hiveconf:mType1}*7) and t.year_payment <= (${hiveconf:mType1}*8) then 8
	when t.year_payment>(${hiveconf:mType1}*8) and t.year_payment <= (${hiveconf:mType1}*9) then 9
	when t.year_payment>(${hiveconf:mType1}*9) and t.year_payment <= (${hiveconf:mType1}*10) then 10
	when t.year_payment>(${hiveconf:mType1}*10) then 11 end as monetary1,
case when t.year_payment<=${hiveconf:mType2} then 1
	when t.year_payment>${hiveconf:mType2} and t.year_payment <= (${hiveconf:mType2}*2) then 2
	when t.year_payment>(${hiveconf:mType2}*2) and t.year_payment <= (${hiveconf:mType2}*3) then 3
	when t.year_payment>(${hiveconf:mType2}*3) and t.year_payment <= (${hiveconf:mType2}*4) then 4
	when t.year_payment>(${hiveconf:mType2}*4) and t.year_payment <= (${hiveconf:mType2}*5) then 5
	when t.year_payment>(${hiveconf:mType2}*5) and t.year_payment <= (${hiveconf:mType2}*6) then 6
	when t.year_payment>(${hiveconf:mType2}*6) and t.year_payment <= (${hiveconf:mType2}*7) then 7
	when t.year_payment>(${hiveconf:mType2}*7) and t.year_payment <= (${hiveconf:mType2}*8) then 8
	when t.year_payment>(${hiveconf:mType2}*8) and t.year_payment <= (${hiveconf:mType2}*9) then 9
	when t.year_payment>(${hiveconf:mType2}*9) and t.year_payment <= (${hiveconf:mType2}*10) then 10
	when t.year_payment>(${hiveconf:mType2}*10) then 11 end as monetary2,
case when t.year_payment<=${hiveconf:mType3} then 1
	when t.year_payment>${hiveconf:mType3} and t.year_payment <= (${hiveconf:mType3}*2) then 2
	when t.year_payment>(${hiveconf:mType3}*2) and t.year_payment <= (${hiveconf:mType3}*3) then 3
	when t.year_payment>(${hiveconf:mType3}*3) and t.year_payment <= (${hiveconf:mType3}*4) then 4
	when t.year_payment>(${hiveconf:mType3}*4) and t.year_payment <= (${hiveconf:mType3}*5) then 5
	when t.year_payment>(${hiveconf:mType3}*5) and t.year_payment <= (${hiveconf:mType3}*6) then 6
	when t.year_payment>(${hiveconf:mType3}*6) and t.year_payment <= (${hiveconf:mType3}*7) then 7
	when t.year_payment>(${hiveconf:mType3}*7) and t.year_payment <= (${hiveconf:mType3}*8) then 8
	when t.year_payment>(${hiveconf:mType3}*8) and t.year_payment <= (${hiveconf:mType3}*9) then 9
	when t.year_payment>(${hiveconf:mType3}*9) and t.year_payment <= (${hiveconf:mType3}*10) then 10
	when t.year_payment>(${hiveconf:mType3}*10) then 11 end as monetary3,
case when t.year_payment<=${hiveconf:mType4} then 1
	when t.year_payment>${hiveconf:mType4} and t.year_payment <= (${hiveconf:mType4}*2) then 2
	when t.year_payment>(${hiveconf:mType4}*2) and t.year_payment <= (${hiveconf:mType4}*3) then 3
	when t.year_payment>(${hiveconf:mType4}*3) and t.year_payment <= (${hiveconf:mType4}*4) then 4
	when t.year_payment>(${hiveconf:mType4}*4) and t.year_payment <= (${hiveconf:mType4}*5) then 5
	when t.year_payment>(${hiveconf:mType4}*5) and t.year_payment <= (${hiveconf:mType4}*6) then 6
	when t.year_payment>(${hiveconf:mType4}*6) and t.year_payment <= (${hiveconf:mType4}*7) then 7
	when t.year_payment>(${hiveconf:mType4}*7) and t.year_payment <= (${hiveconf:mType4}*8) then 8
	when t.year_payment>(${hiveconf:mType4}*8) and t.year_payment <= (${hiveconf:mType4}*9) then 9
	when t.year_payment>(${hiveconf:mType4}*9) and t.year_payment <= (${hiveconf:mType4}*10) then 10
	when t.year_payment>(${hiveconf:mType4}*10) then 11 end as monetary4,
case when t.year_payment<=${hiveconf:mType5} then 1
	when t.year_payment>${hiveconf:mType5} and t.year_payment <= (${hiveconf:mType5}*2) then 2
	when t.year_payment>(${hiveconf:mType5}*2) and t.year_payment <= (${hiveconf:mType5}*3) then 3
	when t.year_payment>(${hiveconf:mType5}*3) and t.year_payment <= (${hiveconf:mType5}*4) then 4
	when t.year_payment>(${hiveconf:mType5}*4) and t.year_payment <= (${hiveconf:mType5}*5) then 5
	when t.year_payment>(${hiveconf:mType5}*5) and t.year_payment <= (${hiveconf:mType5}*6) then 6
	when t.year_payment>(${hiveconf:mType5}*6) and t.year_payment <= (${hiveconf:mType5}*7) then 7
	when t.year_payment>(${hiveconf:mType5}*7) and t.year_payment <= (${hiveconf:mType5}*8) then 8
	when t.year_payment>(${hiveconf:mType5}*8) and t.year_payment <= (${hiveconf:mType5}*9) then 9
	when t.year_payment>(${hiveconf:mType5}*9) and t.year_payment <= (${hiveconf:mType5}*10) then 10
	when t.year_payment>(${hiveconf:mType5}*10) then 11 end as monetary5,
case when t.year_payment<=${hiveconf:mType6} then 1
	when t.year_payment>${hiveconf:mType6} and t.year_payment <= (${hiveconf:mType6}*2) then 2
	when t.year_payment>(${hiveconf:mType6}*2) and t.year_payment <= (${hiveconf:mType6}*3) then 3
	when t.year_payment>(${hiveconf:mType6}*3) and t.year_payment <= (${hiveconf:mType6}*4) then 4
	when t.year_payment>(${hiveconf:mType6}*4) and t.year_payment <= (${hiveconf:mType6}*5) then 5
	when t.year_payment>(${hiveconf:mType6}*5) and t.year_payment <= (${hiveconf:mType6}*6) then 6
	when t.year_payment>(${hiveconf:mType6}*6) and t.year_payment <= (${hiveconf:mType6}*7) then 7
	when t.year_payment>(${hiveconf:mType6}*7) and t.year_payment <= (${hiveconf:mType6}*8) then 8
	when t.year_payment>(${hiveconf:mType6}*8) and t.year_payment <= (${hiveconf:mType6}*9) then 9
	when t.year_payment>(${hiveconf:mType6}*9) and t.year_payment <= (${hiveconf:mType6}*10) then 10
	when t.year_payment>(${hiveconf:mType6}*10) then 11 end as monetary6
from dw_rfm.b_qqd_shop_rfm t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd}) 
and t.plat_code is not null
and t.uni_shop_id is not null
and t.year_last_time is not null 
and t.year_buy_times is not null 
and t.year_payment is not null;


-- 1、计算店铺级的RF指标
-- 创建活跃客户的RFM的RF指标表
CREATE TABLE IF NOT EXISTS dw_rfm.`b_active_customer_rf_history`(
	`tenant` string,
    `plat_code` string,
    `uni_shop_id` string,
	`recency` int,
	`frequency` int,
	`customer_num` bigint,
	`customer_rate` double,
	`avg_payment` double,
    `avg_guest_pay` double,
	`avg_guest_item` double, 
	`avg_item_pay` double,
	`type` int,
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS RCFILE;

-- 店铺级的RF统计临时表
drop table if exists dw_rfm.b_shop_history_rf_temp;
create table dw_rfm.b_shop_history_rf_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.frequency,t.stat_date,
		count(t.uni_id) customer_num,
		sum(t.year_payment) total_payment,
		sum(t.year_buy_times) total_times,
		sum(t.year_buy_num) total_num
		from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.frequency,t.stat_date;

-- 店铺级的RF+两个维度合计的数据
drop table if exists dw_rfm.b_shop_history_rf_result;
create table dw_rfm.b_shop_history_rf_result as
select r.tenant,r.plat_code,r.uni_shop_id,r.recency,r.frequency,r.stat_date,r.customer_num,
	r.total_payment/r.customer_num as avg_payment,
	r.total_payment/r.total_times as avg_guest_pay,
	r.total_num/r.total_times as avg_guest_item,
	r.total_payment/r.total_num as avg_item_pay
from (
    select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.frequency,t.stat_date,t.customer_num,t.total_payment,t.total_times,t.total_num from dw_rfm.b_shop_history_rf_temp t
    union all
    select t1.tenant,t1.plat_code,t1.uni_shop_id,99 as recency,t1.frequency,t1.stat_date,sum(t1.customer_num) customer_num,sum(t1.total_payment) total_payment,sum(t1.total_times) total_times,sum(t1.total_num) total_num
    from dw_rfm.b_shop_history_rf_temp t1
    group by t1.tenant,t1.plat_code,t1.uni_shop_id,t1.frequency,t1.stat_date
    union all
    select t2.tenant,t2.plat_code,t2.uni_shop_id,t2.recency,99 as frequency,t2.stat_date,sum(t2.customer_num) customer_num,sum(t2.total_payment) total_payment,sum(t2.total_times) total_times,sum(t2.total_num) total_num
    from dw_rfm.b_shop_history_rf_temp t2
    group by t2.tenant,t2.plat_code,t2.uni_shop_id,t2.recency,t2.stat_date
    union all 
    select t3.tenant,t3.plat_code,t3.uni_shop_id,99 as recency,99 as frequency,t3.stat_date,sum(t3.customer_num) customer_num,sum(t3.total_payment) total_payment ,sum(t3.total_times) total_times,sum(t3.total_num)total_num
    from dw_rfm.b_shop_history_rf_temp t3
    group by t3.tenant,t3.plat_code,t3.uni_shop_id,t3.stat_date
) r;

-- 计算RF中客户占比，数据入目标表
insert into table dw_rfm.b_active_customer_rf_history partition(part='shop')
select r.tenant,r.plat_code,r.uni_shop_id,r.recency,r.frequency,r.customer_num,
	case r.bcusnum when 0 then -1 else r.customer_num/r.bcusnum end as customer_rate,
	r.avg_payment,r.avg_guest_pay,r.avg_guest_item,r.avg_item_pay,
	3 as type,
	r.stat_date,
	${hiveconf:submitTime} as modified
from
(
    select t.*,
	case when b.customer_num is null then 0 else b.customer_num end as bcusnum
	from dw_rfm.b_shop_history_rf_result t
    left outer join 
	(select tenant,uni_shop_id,customer_num,stat_date from dw_rfm.b_shop_history_rf_result where recency =99 and frequency=99) b
    on t.tenant = b.tenant and t.uni_shop_id=b.uni_shop_id and t.stat_date=b.stat_date
) r;

-- 删除店铺级的RF临时表
drop table if exists dw_rfm.b_shop_history_rf_temp;
drop table if exists dw_rfm.b_shop_history_rf_result;


-- 2、计算店铺级的RM指标

-- 创建活跃客户的RM指标表
CREATE TABLE IF NOT EXISTS dw_rfm.`b_active_customer_rm_history`(
	`tenant` string,
    `plat_code` string,
    `uni_shop_id` string,
	`recency` int,
	`monetary` int,
	`customer_num` bigint,
	`customer_rate` double,
	`avg_payment` double,
    `avg_guest_pay` double,
	`avg_guest_item` double, 
	`avg_item_pay` double,
	`interval_type` int,
	`type` int,
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS RCFILE;

-- 创建店铺的RM临时表
drop table if exists dw_rfm.b_shop_history_rm_temp;
create table dw_rfm.b_shop_history_rm_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary1 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	1 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary1,t.stat_date
union all
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary2 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	2 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary2,t.stat_date
union all
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary3 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	3 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary3,t.stat_date
union all
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary4 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	4 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary4,t.stat_date
union all
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary5 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	5 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary5,t.stat_date
union all
select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary6 as monetary,t.stat_date,
	count(t.uni_id) customer_num,
	sum(t.year_payment) total_payment,
	sum(t.year_buy_times) total_times,
	sum(t.year_buy_num) total_num,
	6 as interval_type
	from dw_rfm.b_shop_history_rfm_temp t
group by t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary6,t.stat_date;


-- 创建RM+合计列的临时结果
drop table if exists dw_rfm.b_shop_history_rm_result;
create table dw_rfm.b_shop_history_rm_result as
select r.tenant,r.plat_code,r.uni_shop_id,r.recency,r.monetary,r.stat_date,r.interval_type,
	r.customer_num,
	r.total_payment/r.customer_num as avg_payment,
	r.total_payment/r.total_times as avg_guest_pay,
	r.total_num/r.total_times as avg_guest_item,
	r.total_payment/r.total_num as avg_item_pay
from (
    select t.tenant,t.plat_code,t.uni_shop_id,t.recency,t.monetary,t.interval_type,t.stat_date,t.customer_num,t.total_payment,t.total_times,t.total_num from dw_rfm.b_shop_history_rm_temp t
    union all
    select t1.tenant,t1.plat_code,t1.uni_shop_id,99 as recency,t1.monetary,t1.interval_type,t1.stat_date,sum(t1.customer_num) customer_num,sum(t1.total_payment) total_payment,sum(t1.total_times) total_times,sum(t1.total_num) total_num
    from dw_rfm.b_shop_history_rm_temp t1
    group by t1.tenant,t1.plat_code,t1.uni_shop_id,t1.monetary,t1.stat_date,t1.interval_type
    union all
    select t2.tenant,t2.plat_code,t2.uni_shop_id,t2.recency,99 as monetary,t2.interval_type,t2.stat_date,sum(t2.customer_num) customer_num,sum(t2.total_payment) total_payment,sum(t2.total_times) total_times,sum(t2.total_num) total_num
    from dw_rfm.b_shop_history_rm_temp t2
    group by t2.tenant,t2.plat_code,t2.uni_shop_id,t2.recency,t2.stat_date,t2.interval_type
    union all 
    select t3.tenant,t3.plat_code,t3.uni_shop_id,99 as recency,99 as monetary,t3.interval_type,t3.stat_date,sum(t3.customer_num) customer_num,sum(t3.total_payment) total_payment ,sum(t3.total_times) total_times,sum(t3.total_num)total_num
    from dw_rfm.b_shop_history_rm_temp t3
    group by t3.tenant,t3.plat_code,t3.uni_shop_id,t3.stat_date,t3.interval_type
) r;

-- 计算客户占比后，插入RM统计结果表，同步给业务
insert into table dw_rfm.b_active_customer_rm_history partition(part='shop')
select r.tenant,r.plat_code,r.uni_shop_id,r.recency,r.monetary,r.customer_num,
	case r.bcusnum when 0 then -1 else r.customer_num/r.bcusnum end as customer_rate,
	r.avg_payment,r.avg_guest_pay,r.avg_guest_item,r.avg_item_pay,
	r.interval_type,
	3 as type,
	r.stat_date,
	${hiveconf:submitTime} as modified
from
(
    select t.*,
	case when b.customer_num is null then 0 else b.customer_num end as bcusnum 
	from dw_rfm.b_shop_history_rm_result t
    left outer join 
	(select tenant,uni_shop_id,customer_num,stat_date,interval_type from dw_rfm.b_shop_history_rm_result where recency =99 and monetary=99) b
    on t.tenant = b.tenant and t.uni_shop_id=b.uni_shop_id and t.stat_date=b.stat_date and t.interval_type=b.interval_type
) r;

-- 删除中间临时表
drop table if exists dw_rfm.b_shop_history_rm_temp;
drop table if exists dw_rfm.b_shop_history_rm_result;
drop table if exists dw_rfm.b_shop_history_rfm_temp;

