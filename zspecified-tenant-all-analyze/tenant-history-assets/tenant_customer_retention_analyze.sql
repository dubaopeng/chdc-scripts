SET mapred.job.name='tenant_customer_retention_analyze-客户保持率历史月底分析';
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

-- 注：以下数据依赖于客户RFM宽表，需要造数据进行场景和结果验证

-- 客户保持率分析结果表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`tenant_customer_retention_analyze_history`(
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
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

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

-- 租户级客户保持数据分析
insert overwrite table dw_rfm.tenant_customer_retention_analyze_history partition(part='${tenant}')
select r.tenant,null as plat_code,null as uni_shop_id,null as shop_name,
    sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num,
    case sum(r.prev_year_num) when 0 then -1 else sum(r.last_year_num)/sum(r.prev_year_num) end as rate,
    1 as type,
	1 as end_month,
	r.stat_date,
	${hiveconf:submitTime} as modified
from (
    select t.tenant,t.stat_date,
    if(t.tyear_buy_times >=1,1,0) prev_year_num,
    if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
    from dw_rfm.b_qqd_tenant_rfm_tenants t
    where t.part = '${tenant}'
		and t.stat_date in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
) r
group by r.tenant,r.stat_date;


-- 平台级客户保持率计算
insert into table dw_rfm.tenant_customer_retention_analyze_history partition(part='${tenant}')
select r.tenant,r.plat_code,null as uni_shop_id,null as shop_name,
    sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num,
    case sum(r.prev_year_num) when 0 then -1 else sum(r.last_year_num)/sum(r.prev_year_num) end as rate,
    2 as type,
    1 as end_month,
	r.stat_date,
	${hiveconf:submitTime} as modified
from (
    select t.tenant,t.plat_code,t.stat_date,
    if(t.tyear_buy_times >=1,1,0) prev_year_num,
    if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
    from dw_rfm.b_qqd_plat_rfm_tenants t
    where t.part = '${tenant}'
		and t.stat_date in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
) r
group by r.tenant,r.plat_code,r.stat_date;

-- 店铺级客户保持率计算
insert into table dw_rfm.tenant_customer_retention_analyze_history partition(part='${tenant}')
select re.tenant,re.plat_code,re.uni_shop_id,db.shop_name,
	re.prev_year_num,re.last_year_num,
	case re.prev_year_num when 0 then -1 else re.last_year_num/re.prev_year_num end as rate,
	3 as type,
	1 as end_month,
	re.stat_date,
	${hiveconf:submitTime} as modified
from(
    select r.tenant,r.plat_code,r.uni_shop_id,r.stat_date,sum(r.prev_year_num) prev_year_num,sum(r.last_year_num) last_year_num
    from (
        select t.tenant,t.plat_code,t.uni_shop_id,t.stat_date,
        if(t.tyear_buy_times >=1,1,0) prev_year_num,
        if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
        from dw_rfm.b_qqd_shop_rfm_tenants t
        where t.part = '${tenant}'
		and t.stat_date in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
    ) r
    group by r.tenant,r.plat_code,r.uni_shop_id,r.stat_date
) re
left outer join dw_base.b_std_tenant_shop db
	on re.tenant=db.tenant and re.plat_code=db.plat_code and re.uni_shop_id =concat(db.plat_code,'|',db.shop_id)
where db.tenant is not null;

--需要对 tenant_customer_retention_analyze_history 下租户分区的数据进行同步