SET mapred.job.name='cix_online_customer_retention_analyze-客户保持率分析';
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

-- 注：以下数据依赖于客户RFM宽表，需要造数据进行场景和结果验证

-- 客户保持率分析结果表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_customer_retention_analyze`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`shop_name` string,
	`prev_year_num` bigint,
    `last_year_num` bigint, 
    `rate` double,
    `type` int,
	`end_month` int,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 设置变量记录统计当天是否为月末的那天
set isMonthEnd=if(date_sub(concat(substr(add_months('${stat_date}',1),0,7),'-01'),1)='${stat_date}',1,0);
-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 租户级客户保持数据分析
insert into table dw_rfm.cix_online_customer_retention_analyze
select r.tenant,null as plat_code,null as uni_shop_id,null as shop_name,
    sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num,
    case sum(r.prev_year_num) when 0 then -1 else sum(r.last_year_num)/sum(r.prev_year_num) end as rate,
    1 as type,
	${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from (
    select t.tenant,
    if(t.tyear_buy_times >=1,1,0) prev_year_num,
    if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
    from dw_rfm.b_qqd_tenant_rfm t
    where part='${stat_date}'
) r
group by r.tenant

union all
-- 平台级客户保持率计算
select r.tenant,r.plat_code,null as uni_shop_id,null as shop_name,
    sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num,
    case sum(r.prev_year_num) when 0 then -1 else sum(r.last_year_num)/sum(r.prev_year_num) end as rate,
    2 as type,
    ${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from (
    select t.tenant,t.plat_code,
    if(t.tyear_buy_times >=1,1,0) prev_year_num,
    if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
    from dw_rfm.b_qqd_plat_rfm t
    where part='${stat_date}'
) r
group by r.tenant,r.plat_code

union all
-- 店铺级客户保持率计算
select re.tenant,re.plat_code,re.uni_shop_id,dp.shop_name,
	re.prev_year_num,re.last_year_num,
	case re.prev_year_num when 0 then -1 else re.last_year_num/re.prev_year_num end as rate,
	3 as type,
	${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from(
    select r.tenant,r.plat_code,r.uni_shop_id,sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num
    from (
        select t.tenant,t.plat_code,t.uni_shop_id,
        if(t.tyear_buy_times >=1,1,0) prev_year_num,
        if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
        from dw_rfm.b_qqd_shop_rfm t
        where part='${stat_date}'
    ) r
    group by r.tenant,r.plat_code,r.uni_shop_id
) re
left outer join dw_base.b_std_tenant_shop db
	on re.tenant=db.tenant and re.plat_code=db.plat_code and re.uni_shop_id =concat(db.plat_code,'|',db.shop_id)
where db.tenant is not null;