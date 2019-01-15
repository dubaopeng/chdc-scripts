SET mapred.job.name='cix_online_customer_repurchase_anlyze-客户复购分析';
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

-- 活跃客复购分析数据存储表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_customer_repurchase_anlyze`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`shop_name` string,
	`active` bigint,
    `repurchase` bigint, 
    `rate` double,
	`active_new` bigint,
	`new_repurchase` bigint,
    `new_rate` double,
	`active_old` bigint,
	`old_repurchase` bigint,
    `old_rate` double,	
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
-- 设置一年前的日期，减少后续计算量
set beforeAYear = add_months('${stat_date}',-12);

-- 租户级数据客户类型识别及统计
insert into table dw_rfm.cix_online_customer_repurchase_anlyze
select r.tenant,null as plat_code,null as uni_shop_id,null as shop_name,
	sum(r.active) activeNum,sum(r.repurchase) repurNum,
	case sum(r.active) when 0 then -1 else sum(r.repurchase)/sum(r.active) end as rate,
	sum(r.active_new) activeNewNum,sum(r.new_repurchase) newrepurNum,
	case sum(r.active_new) when 0 then -1 else sum(r.new_repurchase)/sum(r.active_new) end as new_rate,
	sum(r.active_old) activeOldNum,sum(r.old_repurchase) oldrepurNum,
	case sum(r.active_old) when 0 then -1 else sum(r.old_repurchase)/sum(r.active_old) end as old_rate,
	1 as type,
	${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from (
	select t.tenant,
	if(t.year_buy_times>=1,1,0) active, 
	if(t.year_buy_times >= 2,1,0) repurchase, 
	if((t.year_buy_times >=1 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) active_new,
	if((t.year_buy_times >=2 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) new_repurchase,
	if((t.year_buy_times >=1 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) active_old,
	if((t.year_buy_times >=2 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) old_repurchase
	from dw_rfm.b_qqd_tenant_rfm t
	where part='${stat_date}'
) r
group by r.tenant

union all
-- 平台级客户复购率计算
select r.tenant,r.plat_code,null as uni_shop_id,null as shop_name,
    sum(r.active) activeNum,sum(r.repurchase) repurNum,
    case sum(r.active) when 0 then -1 else sum(r.repurchase)/sum(r.active) end as rate,
    sum(r.active_new) activeNewNum,sum(r.new_repurchase) newrepurNum,
    case sum(r.active_new) when 0 then -1 else sum(r.new_repurchase)/sum(r.active_new) end as new_rate,
    sum(r.active_old) activeOldNum,sum(r.old_repurchase) oldrepurNum,
    case sum(r.active_old) when 0 then -1 else sum(r.old_repurchase)/sum(r.active_old) end as old_rate,
    2 as type,
	${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from (
    select t.tenant,t.plat_code,
    if(t.year_buy_times>=1,1,0) active, 
    if(t.year_buy_times >= 2,1,0) repurchase, 
    if((t.year_buy_times >=1 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) active_new,
    if((t.year_buy_times >=2 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) new_repurchase,
    if((t.year_buy_times >=1 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) active_old,
    if((t.year_buy_times >=2 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) old_repurchase
    from dw_rfm.b_qqd_plat_rfm t
    where part='${stat_date}'
) r
group by r.tenant,r.plat_code

union all
-- 店铺级客户复购率计算
select re.tenant,re.plat_code,re.uni_shop_id,dp.shop_name,
re.activeNum,re.repurNum,case re.activeNum when 0 then -1 else re.repurNum/re.activeNum end as rate,
re.activeNewNum,re.newrepurNum,case re.activeNewNum when 0 then -1 else re.newrepurNum/re.activeNewNum end as new_rate,
re.activeOldNum,re.oldrepurNum,case re.activeOldNum when 0 then -1 else re.oldrepurNum/re.activeOldNum end as old_rate,
3 as type,
${hiveconf:isMonthEnd} as end_month,
'${stat_date}' as stat_date,
${hiveconf:submitTime} as modified
from(
    select r.tenant,r.plat_code,r.uni_shop_id,
    sum(r.active) activeNum,sum(r.repurchase) repurNum,
    sum(r.active_new) activeNewNum,sum(r.new_repurchase) newrepurNum,
    sum(r.active_old) activeOldNum,sum(r.old_repurchase) oldrepurNum
    from (
        select t.tenant,t.plat_code,t.uni_shop_id,
        if(t.year_buy_times>=1,1,0) active, 
        if(t.year_buy_times >= 2,1,0) repurchase, 
        if((t.year_buy_times >=1 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) active_new,
        if((t.year_buy_times >=2 and t.first_buy_time >= ${hiveconf:beforeAYear}),1,0) new_repurchase,
        if((t.year_buy_times >=1 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) active_old,
        if((t.year_buy_times >=2 and t.first_buy_time < ${hiveconf:beforeAYear}),1,0) old_repurchase
        from dw_rfm.b_qqd_shop_rfm t
        where part='${stat_date}'
    ) r
    group by r.tenant,r.plat_code,r.uni_shop_id
) re
left outer join dw_base.b_std_tenant_shop db
	on re.tenant=db.tenant and re.plat_code=db.plat_code and re.uni_shop_id =concat(db.plat_code,'|',db.shop_id)
where db.tenant is not null;
