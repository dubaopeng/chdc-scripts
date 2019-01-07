SET mapred.job.name='history_customer_assets_view-客户资产概览前13月底数据计算';
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

-- 客户资产概览历史数据计算结果表定义，不做分区
DROP TABLE IF EXISTS dw_rfm.`customer_assets_view_history`;
CREATE TABLE IF NOT EXISTS dw_rfm.`customer_assets_view_history`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`shop_name` string,
	`prospective_num` bigint,
    `active_num` bigint, 
    `silent_num` bigint,
	`loss_num` bigint,
    `whole_num` bigint, 
    `type` string,
	`end_month` int,
	`stat_date` string,
	`modified` string
)
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

-- 对租户级的历史进行计算
insert overwrite table dw_rfm.customer_assets_view_history
select c.tenant,null as plat_code,null as uni_shop_id,null as shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	1 as type,
	1 as end_month,
	c.stat_date,
	${hiveconf:submitTime} as modified
from (
	select b.tenant,b.stat_date, cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
	  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
	  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
	  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
	  sum(b.num) whole
	from (
		select a.tenant,a.type,a.stat_date,count(a.type) num from(
			select t.tenant,t.uni_id, t.stat_date,
			case when (t.earliest_time <= t.stat_date and t.first_buy_time is null) or t.first_buy_time > t.stat_date then 'prospective' 
			 when t.year_buy_times >= 1 then 'active'
			 when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
			 when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
			from dw_rfm.b_qqd_tenant_rfm t
			where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
		) a
		group by a.tenant,a.type,a.stat_date
	) b
	group by b.tenant,b.stat_date
) c;


-- 平台级别的历史月底数据客户资产统计分析
insert into table dw_rfm.customer_assets_view_history
select c.tenant,c.plat_code,null as uni_shop_id,null as shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	2 as type,
	1 as end_month,
	c.stat_date,
	${hiveconf:submitTime} as modified
from (
	select b.tenant,b.plat_code,b.stat_date,
	  cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
	  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
	  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
	  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
	  sum(b.num) whole
	from (
		select a.tenant,a.plat_code,a.type,a.stat_date,count(a.type) num from(
			select t.tenant,t.plat_code,t.uni_id,t.stat_date,
			case when (t.earliest_time <= t.stat_date and t.first_buy_time is null) or t.first_buy_time > t.stat_date then 'prospective' 
			 when t.year_buy_times >= 1 then 'active'
			 when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
			 when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
			from dw_rfm.b_qqd_plat_rfm t
			where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
		) a
		group by a.tenant,a.plat_code,a.type,a.stat_date
	) b
	group by b.tenant,b.plat_code,b.stat_date
) c;


-- 店铺级别的历史月底客户资产统计分析
insert into table dw_rfm.customer_assets_view_history
select c.tenant,c.plat_code,c.uni_shop_id,db.shop_name,
	case when c.prospective is null then 0 else c.prospective end prospective,
	case when c.active is null then 0 else c.active end active,
	case when c.silent is null then 0 else c.silent end silent,
	case when c.loss is null then 0 else c.loss end loss,
	c.whole,
	3 as type,
	1 as end_month,
	c.stat_date,
	${hiveconf:submitTime} as modified
from (
	select b.tenant,b.plat_code,b.uni_shop_id,b.stat_date,
	  cast(concat_ws('',collect_set(if(b.type='prospective',b.num,''))) as bigint) prospective,
	  cast(concat_ws('',collect_set(if(b.type='active',b.num,''))) as bigint) active,
	  cast(concat_ws('',collect_set(if(b.type='silent',b.num,''))) as bigint) silent,
	  cast(concat_ws('',collect_set(if(b.type='loss',b.num,''))) as bigint) loss,
	  sum(b.num) whole
	from (
		select a.tenant,a.plat_code,a.uni_shop_id,a.type,a.stat_date,count(a.type) num from(
			select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,t.stat_date,
			case when (t.earliest_time <= t.stat_date and t.first_buy_time is null) or t.first_buy_time > t.stat_date then 'prospective' 
			when t.year_buy_times >= 1 then 'active'
			when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
			when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as type
			from dw_rfm.b_qqd_shop_rfm t
			where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd})
		) a
		group by a.tenant,a.plat_code,a.uni_shop_id,a.type,a.stat_date
	) b
	group by b.tenant,b.plat_code,b.uni_shop_id,b.stat_date
) c
left outer join dw_rfm.b_std_shop db
on c.uni_shop_id=db.uni_shop_id
where db.uni_shop_id is not null;
