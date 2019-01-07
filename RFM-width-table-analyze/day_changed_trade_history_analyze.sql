SET mapred.job.name='new_tenant_history_trade_analyze 新订购租户历史数据计算';
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

-- 计算今日增加的数据，增量表中数据，该表在上一步由每日计算脚本生成，此处不需要重算
create table if not exists dw_rfm.b_trade_day_changed_all(	
	tenant string,
	plat_code string,
	shop_id string,
	uni_shop_id string,
	uni_id string,
	receive_payment double,
	product_num int,
    created string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

CREATE TABLE IF NOT EXISTS dw_rfm.`b_first_buy_day_temp`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`first_buy_time` string,
	`first_payment` double,
	`second_buy_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 设定前13个月月底的日期，用于时间比对
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

-- 从上面的结果打标每条数据所属的时间范围，对其进行后续的计算
drop table if exists dw_rfm.b_day_history_monthend_all;
create table dw_rfm.b_day_history_monthend_all as
select t.tenant,t.plat_code,t.shop_id,t.uni_shop_id,t.uni_id,
    t.receive_payment,t.product_num,t.created,
	case when (t.created >= date_add(add_months(${hiveconf:pre1MonthEnd},-12),1) and t.created <= ${hiveconf:pre1MonthEnd}) then 1 else 0 end as m1_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre1MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre1MonthEnd},-12),1)) then 1 else 0 end as m1_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre1MonthEnd},-24),1) then 1 else 0 end as m1_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre2MonthEnd},-12),1) and t.created <= ${hiveconf:pre2MonthEnd}) then 1 else 0 end as m2_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre2MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre2MonthEnd},-12),1)) then 1 else 0 end as m2_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre2MonthEnd},-24),1) then 1 else 0 end as m2_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre3MonthEnd},-12),1) and t.created <= ${hiveconf:pre3MonthEnd}) then 1 else 0 end as m3_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre3MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre3MonthEnd},-12),1)) then 1 else 0 end as m3_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre3MonthEnd},-24),1) then 1 else 0 end as m3_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre4MonthEnd},-12),1) and t.created <= ${hiveconf:pre4MonthEnd}) then 1 else 0 end as m4_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre4MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre4MonthEnd},-12),1)) then 1 else 0 end as m4_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre4MonthEnd},-24),1) then 1 else 0 end as m4_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre5MonthEnd},-12),1) and t.created <= ${hiveconf:pre5MonthEnd}) then 1 else 0 end as m5_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre5MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre5MonthEnd},-12),1)) then 1 else 0 end as m5_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre5MonthEnd},-24),1) then 1 else 0 end as m5_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre6MonthEnd},-12),1) and t.created <= ${hiveconf:pre6MonthEnd}) then 1 else 0 end as m6_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre6MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre6MonthEnd},-12),1)) then 1 else 0 end as m6_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre6MonthEnd},-24),1) then 1 else 0 end as m6_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre7MonthEnd},-12),1) and t.created <= ${hiveconf:pre7MonthEnd}) then 1 else 0 end as m7_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre7MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre7MonthEnd},-12),1)) then 1 else 0 end as m7_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre7MonthEnd},-24),1) then 1 else 0 end as m7_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre8MonthEnd},-12),1) and t.created <= ${hiveconf:pre8MonthEnd}) then 1 else 0 end as m8_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre8MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre8MonthEnd},-12),1)) then 1 else 0 end as m8_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre8MonthEnd},-24),1) then 1 else 0 end as m8_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre9MonthEnd},-12),1) and t.created <= ${hiveconf:pre9MonthEnd}) then 1 else 0 end as m9_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre9MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre9MonthEnd},-12),1)) then 1 else 0 end as m9_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre9MonthEnd},-24),1) then 1 else 0 end as m9_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre10MonthEnd},-12),1) and t.created <= ${hiveconf:pre10MonthEnd}) then 1 else 0 end as m10_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre10MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre10MonthEnd},-12),1)) then 1 else 0 end as m10_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre10MonthEnd},-24),1) then 1 else 0 end as m10_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre11MonthEnd},-12),1) and t.created <= ${hiveconf:pre11MonthEnd}) then 1 else 0 end as m11_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre11MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre11MonthEnd},-12),1)) then 1 else 0 end as m11_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre11MonthEnd},-24),1) then 1 else 0 end as m11_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre12MonthEnd},-12),1) and t.created <= ${hiveconf:pre12MonthEnd}) then 1 else 0 end as m12_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre12MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre12MonthEnd},-12),1)) then 1 else 0 end as m12_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre12MonthEnd},-24),1) then 1 else 0 end as m12_btyear,
	
	case when (t.created >= date_add(add_months(${hiveconf:pre13MonthEnd},-12),1) and t.created <= ${hiveconf:pre13MonthEnd}) then 1 else 0 end as m13_year,
	case when (t.created >= date_add(add_months(${hiveconf:pre13MonthEnd},-24),1) and t.created < date_add(add_months(${hiveconf:pre13MonthEnd},-12),1)) then 1 else 0 end as m13_tyear,
	case when t.created < date_add(add_months(${hiveconf:pre13MonthEnd},-24),1) then 1 else 0 end as m13_btyear
from dw_rfm.b_trade_day_changed_all t;


-- 计算各个月底的近一年的数据,后面需要和历史数据进行合并
drop table if exists dw_rfm.b_day_month13end_lastyear_temp;
create table dw_rfm.b_day_month13end_lastyear_temp as
select t.* from (
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre1MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m1_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre2MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m2_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre3MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m3_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre4MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m4_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre5MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m5_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre6MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m6_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre7MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m7_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre8MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m8_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre9MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m9_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre10MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m10_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre11MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m11_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre12MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m12_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as year_payment,
		count(created) as year_buy_times,
		sum(product_num) as year_buy_num,
		min(created) as year_first_time,
		max(created) as year_last_time,
		${hiveconf:pre13MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m13_year=1
	group by tenant,plat_code,uni_shop_id,uni_id
) t;

-- 计算前13个月各个月底近两年的数据,后面需要和历史数据进行合并
drop table if exists dw_rfm.b_day_month13end_last_twoyear_temp;
create table dw_rfm.b_day_month13end_last_twoyear_temp as
select t.* from (
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre1MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m1_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre2MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m2_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre3MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m3_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre4MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m4_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre5MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m5_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre6MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m6_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre7MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m7_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre8MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m8_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre9MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m9_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre10MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m10_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre11MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m11_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre12MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m12_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as tyear_payment,
		count(created) as tyear_buy_times,
		sum(product_num) as tyear_buy_num,
		${hiveconf:pre13MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m13_tyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
) t;

-- 计算前13月各月底对应的两年前的统计数据,后面需要和历史数据进行合并
drop table if exists dw_rfm.b_day_month13end_before_twoyear_temp;
create table dw_rfm.b_day_month13end_before_twoyear_temp as
select t.* from (
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre1MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m1_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre2MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m2_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre3MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m3_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre4MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m4_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre5MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m5_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre6MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m6_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre7MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m7_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre8MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m8_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre9MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m9_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre10MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m10_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre11MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m11_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre12MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m12_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
	union all
	select tenant,plat_code,uni_shop_id,uni_id,
		sum(receive_payment) as btyear_payment,
		count(created) as btyear_buy_times,
		sum(product_num) as btyear_buy_num,
		${hiveconf:pre13MonthEnd} as day
	from dw_rfm.b_day_history_monthend_all where m13_btyear=1
	group by tenant,plat_code,uni_shop_id,uni_id
) t;


-- 对本次13个月各月底的统计结果进行合并
-- 以客户第一次和第二次的那个表作为主表进行关联，确保今日新增的数据能够全部关联到

-- 计算变化的用户各月底近一年的数据，与原历史数据合并
drop table if exists dw_rfm.b_all_monthend_last_year_temp;
create table dw_rfm.b_all_monthend_last_year_temp as
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,a.first_buy_time,a.first_payment,a.second_buy_time,
	   b.year_payment,b.year_buy_times,b.year_buy_num,b.year_first_time,b.year_last_time,b.day
from dw_rfm.b_first_buy_day_temp a
left outer join
dw_rfm.b_day_month13end_lastyear_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id;

-- 计算变化的用户各月底近两年的数据，与原历史数据合并
drop table if exists dw_rfm.b_all_monthend_last_twoyear_temp;
create table dw_rfm.b_all_monthend_last_twoyear_temp as
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,a.first_buy_time,a.first_payment,a.second_buy_time,
	   b.tyear_payment,b.tyear_buy_times,b.tyear_buy_num,b.day
from dw_rfm.b_first_buy_day_temp a
left outer join
dw_rfm.b_day_month13end_last_twoyear_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id;

-- 计算变化的用户各月两年前的数据，与原历史数据合并
drop table if exists dw_rfm.b_all_monthend_before_twoyear_temp;
create table dw_rfm.b_all_monthend_before_twoyear_temp as
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,a.first_buy_time,a.first_payment,a.second_buy_time,
	   b.btyear_payment,b.btyear_buy_times,b.btyear_buy_num,b.day
from dw_rfm.b_first_buy_day_temp a
left outer join
dw_rfm.b_day_month13end_before_twoyear_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id;


-- 对上面三个数据进行合并后，和对应的月底分区的数据进行合并，相同用户的数据合并，新增的用户的数据直接加入
drop table if exists dw_rfm.b_day_month13end_all_temp;
CREATE table dw_rfm.b_day_month13end_all_temp as
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
	   a.first_buy_time as earliest_time,
	   a.first_buy_time,
	   if(a.first_payment is null,0,a.first_payment) first_payment,
	   a.second_buy_time,
	   if(a.year_payment is null,0,a.year_payment) year_payment,
	   if(a.year_buy_times is null,0,a.year_buy_times) year_buy_times,
	   if(a.year_buy_num is null,0,a.year_buy_num) year_buy_num,
	   a.year_first_time,
	   a.year_last_time,
	   if(b.tyear_payment is null,0,b.tyear_payment) tyear_payment,
	   if(b.tyear_buy_times is null,0,b.tyear_buy_times) tyear_buy_times,
	   if(b.tyear_buy_num is null,0,b.tyear_buy_num) tyear_buy_num,
	   if(c.btyear_payment is null,0,c.btyear_payment) btyear_payment,
	   if(c.btyear_buy_times is null,0,c.btyear_buy_times) btyear_buy_times,
	   if(c.btyear_buy_num is null,0,c.btyear_buy_num) btyear_buy_num,
	   a.day
from 
dw_rfm.b_all_monthend_last_year_temp a
left join 
dw_rfm.b_all_monthend_last_twoyear_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id and a.day=b.day
left join 
dw_rfm.b_all_monthend_before_twoyear_temp c
on a.tenant=c.tenant and a.plat_code=c.plat_code and a.uni_shop_id=c.uni_shop_id and a.uni_id=c.uni_id and a.day=c.day;


-- 计算完全新增的记录，以今天变化的数据做主表，计算出今日新增的部分记录，后面直接合并
drop table if exists dw_rfm.b_day_month13end_increment_rfm;
create table dw_rfm.b_day_month13end_increment_rfm as
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
	   case when t.first_buy_time is null or t.first_buy_time > c.modified then c.modified else t.first_buy_time end as earliest_time,
	   case when t.first_buy_time is not null and t.first_buy_time < t.day then t.first_buy_time else null end as first_buy_time,
	   t.first_payment,
	   case when t.second_buy_time is not null and t.second_buy_time < t.day then t.second_buy_time else null end as second_buy_time,
	   t.year_payment,
	   t.year_buy_times,
	   t.year_buy_num,
	   t.year_first_time,
	   t.year_last_time,
	   t.tyear_payment,
	   t.tyear_buy_times,
	   t.tyear_buy_num,
	   t.btyear_payment,
	   t.btyear_buy_times,
	   t.btyear_buy_num,
	   t.day as stat_date
from(
	select a.* from dw_rfm.b_day_month13end_all_temp a
	left join 
	(select * from dw_rfm.b_qqd_shop_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
		${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
		${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	)b
	on a.day=b.part and a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id
	where b.tenant is null and b.plat_code is null
)t
left join dw_base.b_std_customer c
on t.tenant=c.tenant and t.uni_id =c.uni_id
where t.first_buy_time is not null and t.first_buy_time < t.day;

-- 计算发生变化的用户记录，以今天的表作为主表进行计算，计算两边都存在用户进行合并后，与
insert overwrite table dw_rfm.b_qqd_shop_rfm partition(part)
select r.*,r.stat_date as part from(
	select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
		case when tmp.earliest_time is null or t.earliest_time < tmp.earliest_time then t.earliest_time else tmp.earliest_time end as earliest_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_buy_time else tmp.first_buy_time end as first_buy_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_payment else tmp.first_payment end as first_payment,
		case when tmp.second_buy_time is null or t.second_buy_time < tmp.second_buy_time then t.second_buy_time else tmp.second_buy_time end as second_buy_time,
		(t.year_payment+ if(tmp.year_payment is null,0,tmp.year_payment)) year_payment,
		(t.year_buy_times+ if(tmp.year_buy_times is null,0,tmp.year_buy_times)) year_buy_times,
		(t.year_buy_num+ if(tmp.year_buy_num is null,0,tmp.year_buy_num)) year_buy_num,
		case when tmp.year_first_time is null or t.year_first_time < tmp.year_first_time then t.year_first_time else tmp.year_first_time end as year_first_time,
		case when tmp.year_last_time is null or t.year_last_time > tmp.year_last_time then t.year_last_time else tmp.year_last_time end as year_last_time,
		(t.tyear_payment+ if(tmp.tyear_payment is null,0,tmp.tyear_payment)) tyear_payment,
		(t.tyear_buy_times+ if(tmp.tyear_buy_times is null,0,tmp.tyear_buy_times)) tyear_buy_times,
		(t.tyear_buy_num+ if(tmp.tyear_buy_num is null,0,tmp.tyear_buy_num)) tyear_buy_num,	
		(t.btyear_payment+ if(tmp.btyear_payment is null,0,tmp.btyear_payment)) btyear_payment,
		(t.btyear_buy_times+ if(tmp.btyear_buy_times is null,0,tmp.btyear_buy_times)) btyear_buy_times,
		(t.btyear_buy_num+ if(tmp.btyear_buy_num is null,0,tmp.btyear_buy_num)) btyear_buy_num,
		t.stat_date
	from (
		select * from dw_rfm.b_qqd_shop_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
			${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
			${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	) t
	left outer join dw_rfm.b_day_month13end_all_temp tmp
	on t.tenant = tmp.tenant and t.plat_code=tmp.plat_code and t.uni_shop_id = tmp.uni_shop_id and t.uni_id=tmp.uni_id and t.part=tmp.day
	union all
	select b.tenant,b.plat_code,b.uni_shop_id,b.uni_id,
		   b.earliest_time,
		   b.first_buy_time,
		   b.first_payment,
		   b.second_buy_time,
		   b.year_payment,
		   b.year_buy_times,
		   b.year_buy_num,
		   b.year_first_time,
		   b.year_last_time,
		   b.tyear_payment,
		   b.tyear_buy_times,
		   b.tyear_buy_num,
		   b.btyear_payment,
		   b.btyear_buy_times,
		   b.btyear_buy_num,
		   b.stat_date
    from
	dw_rfm.b_day_month13end_increment_rfm b
) r
distribute by r.stat_date;


-- 对今日计算的13个月月底的数据进行去重计算平台级数据
-- 去重获取平台级的最早购买时间
drop table if exists dw_rfm.b_day_month13end_plat_first_buy;
create table dw_rfm.b_day_month13end_plat_first_buy as
select r.tenant,r.plat_code,r.uni_id,r.day,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  else concat_ws('',collect_set(r.second_buy_time)) end as second_buy_time
from(
	select t.tenant,t.plat_code,t.uni_id,t.day,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,plat_code,uni_id,day order by first_buy_time asc) as rank 
		from dw_rfm.b_day_month13end_all_temp
	) t where t.rank <= 2
) r
group by r.tenant,r.plat_code,r.uni_id,r.day;

-- 从店铺级数据中统计出租户级历史月底数据的统计结果
drop table if exists dw_rfm.b_day_month13end_plat_statisc_temp;
create table dw_rfm.b_day_month13end_plat_statisc_temp as
select t.tenant,t.plat_code,t.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	r.day
from 
	dw_rfm.b_day_month13end_plat_first_buy t
left outer join
(
	select a.tenant,a.plat_code,a.uni_id,a.day,
		min(a.earliest_time) earliest_time,
		sum(a.year_payment) year_payment,
		sum(a.year_buy_times) year_buy_times,
		sum(a.year_buy_num) year_buy_num,
		min(a.year_first_time) year_first_time,
		max(a.year_last_time) year_last_time,
		sum(a.tyear_payment) tyear_payment,
		sum(a.tyear_buy_times) tyear_buy_times,
		sum(a.tyear_buy_num) tyear_buy_num,
		sum(a.btyear_payment) btyear_payment,
		sum(a.btyear_buy_times) btyear_buy_times,
		sum(a.btyear_buy_num) btyear_buy_num
	from dw_rfm.b_day_month13end_all_temp a 
	group by a.tenant,a.plat_code,a.uni_id,a.day
) r
on t.tenant=r.tenant and t.plat_code=r.plat_code and t.uni_id=r.uni_id and t.day=r.day;


-- 计算今日历史数据中完全新增的平台级统计数据
drop table if exists dw_rfm.b_day_month13end_plat_increment_rfm;
create table dw_rfm.b_day_month13end_plat_increment_rfm as
select t.tenant,t.plat_code,t.uni_id,
	   t.earliest_time,
	   t.first_buy_time,
	   t.first_payment,
	   t.second_buy_time,
	   t.year_payment,
	   t.year_buy_times,
	   t.year_buy_num,
	   t.year_first_time,
	   t.year_last_time,
	   t.tyear_payment,
	   t.tyear_buy_times,
	   t.tyear_buy_num,
	   t.btyear_payment,
	   t.btyear_buy_times,
	   t.btyear_buy_num,
	   t.day as stat_date
from(
	select a.* from dw_rfm.b_day_month13end_plat_statisc_temp a
	left join 
	(select * from dw_rfm.b_qqd_plat_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
		${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
		${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	)b
	on a.day=b.part and a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_id=b.uni_id
	where b.tenant is null and b.plat_code is null
)t
where t.first_buy_time is not null and t.first_buy_time < t.day;


-- 计算发生变化的平台用户，重新生成平台级的月底数据
insert overwrite table dw_rfm.b_qqd_plat_rfm partition(part)
select r.*,r.stat_date as part from(
	select t.tenant,t.plat_code,t.uni_id,
		case when tmp.earliest_time is null or t.earliest_time < tmp.earliest_time then t.earliest_time else tmp.earliest_time end as earliest_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_buy_time else tmp.first_buy_time end as first_buy_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_payment else tmp.first_payment end as first_payment,
		case when tmp.second_buy_time is null or t.second_buy_time < tmp.second_buy_time then t.second_buy_time else tmp.second_buy_time end as second_buy_time,
		(t.year_payment+ if(tmp.year_payment is null,0,tmp.year_payment)) year_payment,
		(t.year_buy_times+ if(tmp.year_buy_times is null,0,tmp.year_buy_times)) year_buy_times,
		(t.year_buy_num+ if(tmp.year_buy_num is null,0,tmp.year_buy_num)) year_buy_num,
		case when tmp.year_first_time is null or t.year_first_time < tmp.year_first_time then t.year_first_time else tmp.year_first_time end as year_first_time,
		case when tmp.year_last_time is null or t.year_last_time > tmp.year_last_time then t.year_last_time else tmp.year_last_time end as year_last_time,
		(t.tyear_payment+ if(tmp.tyear_payment is null,0,tmp.tyear_payment)) tyear_payment,
		(t.tyear_buy_times+ if(tmp.tyear_buy_times is null,0,tmp.tyear_buy_times)) tyear_buy_times,
		(t.tyear_buy_num+ if(tmp.tyear_buy_num is null,0,tmp.tyear_buy_num)) tyear_buy_num,	
		(t.btyear_payment+ if(tmp.btyear_payment is null,0,tmp.btyear_payment)) btyear_payment,
		(t.btyear_buy_times+ if(tmp.btyear_buy_times is null,0,tmp.btyear_buy_times)) btyear_buy_times,
		(t.btyear_buy_num+ if(tmp.btyear_buy_num is null,0,tmp.btyear_buy_num)) btyear_buy_num,
		t.stat_date
	from (
		select * from dw_rfm.b_qqd_plat_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
			${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
			${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	) t
	left outer join dw_rfm.b_day_month13end_plat_statisc_temp tmp
	on t.tenant = tmp.tenant and t.plat_code=tmp.plat_code and t.uni_id=tmp.uni_id and t.part=tmp.day
	union all
	select b.tenant,b.plat_code,b.uni_id,
		   b.earliest_time,
		   b.first_buy_time,
		   b.first_payment,
		   b.second_buy_time,
		   b.year_payment,
		   b.year_buy_times,
		   b.year_buy_num,
		   b.year_first_time,
		   b.year_last_time,
		   b.tyear_payment,
		   b.tyear_buy_times,
		   b.tyear_buy_num,
		   b.btyear_payment,
		   b.btyear_buy_times,
		   b.btyear_buy_num,
		   b.stat_date
    from dw_rfm.b_day_month13end_plat_increment_rfm b
) r
distribute by r.stat_date;

-- 从平台级月底变化数据，计算租户级的用户第一次和第二次购买记录
drop table if exists dw_rfm.b_day_month13end_tenant_first_buy;
create table dw_rfm.b_day_month13end_tenant_first_buy as
select r.tenant,r.uni_id,r.day,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  else concat_ws('',collect_set(r.second_buy_time)) end as second_buy_time
from(
	select t.tenant,t.uni_id,t.day,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,uni_id,day order by first_buy_time asc) as rank 
		from dw_rfm.b_day_month13end_plat_statisc_temp
	) t where t.rank <= 2
) r
group by r.tenant,r.uni_id,r.day;

-- 从平台级的统计结果中计算租户级的历史月底统计数据
drop table if exists dw_rfm.b_day_month13end_tenant_statisc_temp;
create table dw_rfm.b_day_month13end_tenant_statisc_temp as
select t.tenant,t.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	r.day
from 
	dw_rfm.b_day_month13end_tenant_first_buy t
left outer join
(
	select a.tenant,a.uni_id,a.day,
		min(a.earliest_time) earliest_time,
		sum(a.year_payment) year_payment,
		sum(a.year_buy_times) year_buy_times,
		sum(a.year_buy_num) year_buy_num,
		min(a.year_first_time) year_first_time,
		max(a.year_last_time) year_last_time,
		sum(a.tyear_payment) tyear_payment,
		sum(a.tyear_buy_times) tyear_buy_times,
		sum(a.tyear_buy_num) tyear_buy_num,
		sum(a.btyear_payment) btyear_payment,
		sum(a.btyear_buy_times) btyear_buy_times,
		sum(a.btyear_buy_num) btyear_buy_num
	from dw_rfm.b_day_month13end_plat_statisc_temp a 
	group by a.tenant,a.uni_id,a.day
) r
on t.tenant=r.tenant and t.uni_id=r.uni_id and t.day=r.day;

-- 计算完全新增的租户级的月底数据
drop table if exists dw_rfm.b_day_month13end_tenant_increment_rfm;
create table dw_rfm.b_day_month13end_tenant_increment_rfm as
select t.tenant,t.uni_id,
	   t.earliest_time,
	   t.first_buy_time,
	   t.first_payment,
	   t.second_buy_time,
	   t.year_payment,
	   t.year_buy_times,
	   t.year_buy_num,
	   t.year_first_time,
	   t.year_last_time,
	   t.tyear_payment,
	   t.tyear_buy_times,
	   t.tyear_buy_num,
	   t.btyear_payment,
	   t.btyear_buy_times,
	   t.btyear_buy_num,
	   t.day as stat_date
from(
	select a.* from dw_rfm.b_day_month13end_tenant_statisc_temp a
	left join 
	(select * from dw_rfm.b_qqd_tenant_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
		${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
		${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	)b
	on a.day=b.part and a.tenant=b.tenant and a.uni_id=b.uni_id
	where b.tenant is null
)t
where t.first_buy_time is not null and t.first_buy_time < t.day;


-- 计算发生变化的租户级数据，重新生成前13月月底租户级RFM数据
insert overwrite table dw_rfm.b_qqd_tenant_rfm partition(part)
select r.*,r.stat_date as part from(
	select t.tenant,t.uni_id,
		case when tmp.earliest_time is null or t.earliest_time < tmp.earliest_time then t.earliest_time else tmp.earliest_time end as earliest_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_buy_time else tmp.first_buy_time end as first_buy_time,
		case when tmp.first_buy_time is null or t.first_buy_time < tmp.first_buy_time then t.first_payment else tmp.first_payment end as first_payment,
		case when tmp.second_buy_time is null or t.second_buy_time < tmp.second_buy_time then t.second_buy_time else tmp.second_buy_time end as second_buy_time,
		(t.year_payment+ if(tmp.year_payment is null,0,tmp.year_payment)) year_payment,
		(t.year_buy_times+ if(tmp.year_buy_times is null,0,tmp.year_buy_times)) year_buy_times,
		(t.year_buy_num+ if(tmp.year_buy_num is null,0,tmp.year_buy_num)) year_buy_num,
		case when tmp.year_first_time is null or t.year_first_time < tmp.year_first_time then t.year_first_time else tmp.year_first_time end as year_first_time,
		case when tmp.year_last_time is null or t.year_last_time > tmp.year_last_time then t.year_last_time else tmp.year_last_time end as year_last_time,
		(t.tyear_payment+ if(tmp.tyear_payment is null,0,tmp.tyear_payment)) tyear_payment,
		(t.tyear_buy_times+ if(tmp.tyear_buy_times is null,0,tmp.tyear_buy_times)) tyear_buy_times,
		(t.tyear_buy_num+ if(tmp.tyear_buy_num is null,0,tmp.tyear_buy_num)) tyear_buy_num,	
		(t.btyear_payment+ if(tmp.btyear_payment is null,0,tmp.btyear_payment)) btyear_payment,
		(t.btyear_buy_times+ if(tmp.btyear_buy_times is null,0,tmp.btyear_buy_times)) btyear_buy_times,
		(t.btyear_buy_num+ if(tmp.btyear_buy_num is null,0,tmp.btyear_buy_num)) btyear_buy_num,
		t.stat_date
	from (
		select * from dw_rfm.b_qqd_tenant_rfm where part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},
			${hiveconf:pre4MonthEnd},${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},
			${hiveconf:pre9MonthEnd},${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
	) t
	left outer join dw_rfm.b_day_month13end_tenant_statisc_temp tmp
	on t.tenant = tmp.tenant and t.uni_id=tmp.uni_id and t.part=tmp.day
	union all
	select b.tenant,b.uni_id,
		   b.earliest_time,
		   b.first_buy_time,
		   b.first_payment,
		   b.second_buy_time,
		   b.year_payment,
		   b.year_buy_times,
		   b.year_buy_num,
		   b.year_first_time,
		   b.year_last_time,
		   b.tyear_payment,
		   b.tyear_buy_times,
		   b.tyear_buy_num,
		   b.btyear_payment,
		   b.btyear_buy_times,
		   b.btyear_buy_num,
		   b.stat_date
	from dw_rfm.b_day_month13end_tenant_increment_rfm b
) r
distribute by r.stat_date;

-- 对以上重新生成的记录，需要重新计算RFM后同步给业务库






