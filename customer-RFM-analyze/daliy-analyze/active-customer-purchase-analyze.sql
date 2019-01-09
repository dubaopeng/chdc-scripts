SET mapred.job.name='active_customer_purchase_analyze-活跃客户复购间隔分析';
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

-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 注：以下数据依赖于三个层级的RFM宽表
-- 第一步：对RFM数据进行数据打标，记录每行数据所属RFM的坐标

-- 创建复购客户购买间隔分析结果表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_customer_purchase_interval`(
	`tenant` string,
    `plat_code` string,
    `uni_shop_id` string,
	`customer_type` int,
	`interval_days` int,
	`customer_num` bigint,
	`type` int,
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

-- 租户级复购客平均购买间隔
insert overwrite table dw_rfm.cix_online_customer_purchase_interval partition(part='${stat_date}')

-- 复购客复购间隔分析
select a.tenant,a.plat_code,a.uni_shop_id,a.customer_type,a.interval_days,a.customer_num,a.type,
	'${stat_date}' as stat_date,${hiveconf:submitTime} as modified
from(
	select t.tenant,null as plat_code,null as uni_shop_id,1 as customer_type,
	ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1)) interval_days,
	count(t.uni_id) customer_num,
	1 as type
	from dw_rfm.b_qqd_tenant_rfm t
	where t.part='${stat_date}' and t.year_buy_times >= 2 
	group by t.tenant,ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1))

	--平台级复购客购买间隔分析
	union all
	select t.tenant,t.plat_code,null as uni_shop_id,1 as customer_type,
	ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1)) interval_days,
	count(t.uni_id) customer_num,
	2 as type
	from dw_rfm.b_qqd_plat_rfm t
	where t.part='${stat_date}' and t.year_buy_times >= 2 
	group by t.tenant,t.plat_code,ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1))

	-- 店铺级复购客购买间隔分析
	union all
	select t.tenant,t.plat_code,t.uni_shop_id,1 as customer_type,
	ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1)) interval_days,
	count(t.uni_id) customer_num,
	3 as type
	from dw_rfm.b_qqd_shop_rfm t
	where t.part='${stat_date}' and t.year_buy_times >= 2 
	group by t.tenant,t.plat_code,t.uni_shop_id,
	ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1))
) a
union all

-- 新客的复购间隔分析
select b.tenant,b.plat_code,b.uni_shop_id,b.customer_type,b.interval_days,b.customer_num,b.type,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from(
	select t.tenant,null as plat_code,null as uni_shop_id,
		2 as customer_type,
		datediff(t.second_buy_time,t.first_buy_time) interval_days,
		count(t.uni_id) customer_num,
		1 as type
		from dw_rfm.b_qqd_tenant_rfm t
	where t.part ='${stat_date}'
		and t.first_buy_time > add_months('${stat_date}',-12)
		and t.second_buy_time is not null
		and t.year_buy_times > 1
	group by t.tenant,datediff(t.second_buy_time,t.first_buy_time)
	union all
	select t.tenant,t.plat_code,null as uni_shop_id,
		2 as customer_type,
		datediff(t.second_buy_time,t.first_buy_time) interval_days,
		count(t.uni_id) customer_num,
		2 as type
		from dw_rfm.b_qqd_plat_rfm t
	where t.part ='${stat_date}'
		and t.first_buy_time > add_months('${stat_date}',-12)
		and t.second_buy_time is not null
		and t.year_buy_times > 1
	group by t.tenant,t.plat_code,datediff(t.second_buy_time,t.first_buy_time)
	union all
	select t.tenant,t.plat_code,t.uni_shop_id,
		2 as customer_type,
		datediff(t.second_buy_time,t.first_buy_time) interval_days,
		count(t.uni_id) customer_num,
		3 as type
		from dw_rfm.b_qqd_shop_rfm t
	where t.part ='${stat_date}'
		and t.first_buy_time > add_months('${stat_date}',-12)
		and t.second_buy_time is not null
		and t.year_buy_times > 1
	group by t.tenant,t.plat_code,t.uni_shop_id,datediff(t.second_buy_time,t.first_buy_time)
) b;





