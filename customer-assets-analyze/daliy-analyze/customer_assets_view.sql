SET mapred.job.name='cix_online_customer_assets_view-客户资产概览';
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

-- 客户资产概览结果表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_customer_assets_view`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`shop_name` string,
	`prospective_num` bigint,
    `active_num` bigint, 
    `silent_num` bigint,
	`loss_num` bigint,
    `whole_num` bigint, 
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

-- 对租户级、平台级、店铺级的统计分析结果合并入统计分析结果表
insert overwrite table dw_rfm.cix_online_customer_assets_view
select re.tenant,re.plat_code,re.uni_shop_id,re.shop_name,re.prospective,re.active,
re.silent,re.loss,re.whole,re.type,
${hiveconf:isMonthEnd} as end_month,
'${stat_date}' as stat_date,
${hiveconf:submitTime} as modified
from (
	-- 租户级别的客户资产统计分析
	select c.tenant,null as plat_code,null as uni_shop_id,null as shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	1 as type
	from (
	select b.tenant, cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
		  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
		  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
		  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
		  sum(b.num) whole
		from (
			select a.tenant,a.type,count(a.type) num from(
				select t.tenant,t.uni_id,
				case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'prospective' 
				 when t.year_buy_times >= 1 then 'active'
				 when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
				 when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
				from dw_rfm.b_qqd_tenant_rfm t
				where part='${stat_date}'
			) a
			group by a.tenant,a.type
		) b
		group by b.tenant
	) c
	union all
	-- 平台级别的客户资产统计分析
	select c.tenant,c.plat_code,null as uni_shop_id,null as shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	2 as type
	from (
	select b.tenant,b.plat_code, 
		  cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
		  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
		  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
		  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
		  sum(b.num) whole
		from (
			select a.tenant,a.plat_code,a.type,count(a.type) num from(
				select t.tenant,t.plat_code,t.uni_id,
				case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'prospective' 
				 when t.year_buy_times >= 1 then 'active'
				 when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
				 when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
				from dw_rfm.b_qqd_plat_rfm t
				where part='${stat_date}'
			) a
			group by a.tenant,a.plat_code,a.type
		) b
		group by b.tenant,b.plat_code
	) c
	union all
	-- 店铺级别的客户资产统计分析
	select c.tenant,c.plat_code,c.uni_shop_id,db.shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	3 as type
	from (
		select b.tenant,b.plat_code,b.uni_shop_id,
		  cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
		  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
		  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
		  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
		  sum(b.num) whole
		from (
			select a.tenant,a.plat_code,a.uni_shop_id,a.type,count(a.type) num from(
				select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
				case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'prospective' 
				when t.year_buy_times >= 1 then 'active'
				when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
				when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
				from dw_rfm.b_qqd_shop_rfm t
				where part='${stat_date}'
			) a
			group by a.tenant,a.plat_code,a.uni_shop_id,a.type
		) b
		group by b.tenant,b.plat_code,b.uni_shop_id
	) c
	left outer join dw_rfm.b_std_shop db
	on c.uni_shop_id=db.uni_shop_id
	where db.uni_shop_id is not null
)re;
