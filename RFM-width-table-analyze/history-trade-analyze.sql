SET mapred.job.name='history-trade-analyze 历史订单分析计算';
--set hive.execution.engine=mr;

set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
--set tez.am.resource.memory.mb=16384;
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


-- 需要先运行trade-history_base-init.sql脚本后，再执行该脚本，才能计算数据
-- 基于历史数据初始化结果表进行，各月份月底数据，上线前一天数据计算

-- 首次购买时间，首次购买金额和第二次购买时间计算
CREATE TABLE IF NOT EXISTS dw_rfm.`b_first_buy_history_base`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`first_buy_time` string,
	`first_payment` double,
	`second_buy_time` string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 去重排序取前两条记录，获取到首次时间,首次金额和第二次时间
insert overwrite table dw_rfm.`b_first_buy_history_base` partition(part = '${stat_date}')
select r.tenant,r.plat_code,r.uni_shop_id,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 then NULL else concat_ws('',collect_set(r.second_buy_time))
	  end as second_buy_time
from(
	select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
	   case t.rank when 1 then t.created else '' end as first_buy_time,
	   case t.rank when 1 then t.receive_payment else '' end as first_payment,
	   case t.rank when 2 then t.created else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,plat_code,uni_shop_id,uni_id order by created asc) as rank 
		from dw_rfm.b_rfm_history_base_trade
		where substr(created,1,10) <= '${stat_date}'
	) t where t.rank <= 2
) r
group by r.tenant,r.plat_code,r.uni_shop_id,r.uni_id;


-- 最近一年的购买指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last_year_history_base`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`year_payment` double,
	`year_buy_times` int,
	`year_buy_num` int,
	`year_first_time` string,
	`year_last_time` string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_last_year_history_base` partition(part = '${stat_date}')
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as year_payment,
	count(created) as year_buy_times,
	sum(product_num) as year_buy_num,
	min(created) as year_first_time,
	max(created) as year_last_time
from dw_rfm.b_rfm_history_base_trade
where created is not NULL
    and (created >= date_add(add_months('${stat_date}',-12),1) and created <= '${stat_date}')
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
-- 最近两年的购买记录指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last_tyear_history_base`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`tyear_payment` double,
	`tyear_buy_times` int,
	`tyear_buy_num` int
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_last_tyear_history_base` partition(part = '${stat_date}')
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as tyear_payment,
	count(created) as tyear_buy_times,
	sum(product_num) as tyear_buy_num
from dw_rfm.b_rfm_history_base_trade
where created is not NULL
	and (created >= date_add(add_months('${stat_date}',-24),1) and created < date_add(add_months('${stat_date}',-12),1))
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
-- 两年前的购买记录指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_before_tyear_history_base`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`btyear_payment` double,
	`btyear_buy_times` int,
	`btyear_buy_num` int
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_before_tyear_history_base` partition(part = '${stat_date}')
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as btyear_payment,
	count(created) as btyear_buy_times,
	sum(product_num) as btyear_buy_num
from dw_rfm.b_rfm_history_base_trade
where created is not NULL and created < date_add(add_months('${stat_date}',-24),1)
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;

-- 店铺级的数据表定义 
CREATE TABLE IF NOT EXISTS dw_rfm.`b_qqd_shop_rfm`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
    `earliest_time` string,
	`first_buy_time` string,
	`first_payment` double,
    `second_buy_time` string, 
    `year_payment` double,
	`year_buy_times` int,
    `year_buy_num` int, 
    `year_first_time` string,
	`year_last_time` string,
	`tyear_payment` double,
    `tyear_buy_times` int, 
    `tyear_buy_num` int,
	`btyear_payment` double,
	`btyear_buy_times` int,
    `btyear_buy_num` int,
	`stat_date` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 店铺级RFM指标合并
insert overwrite table dw_rfm.`b_qqd_shop_rfm` partition(part='${stat_date}')
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
	   case when r.earliest_time is null or r.earliest_time > t.modified then t.modified else r.earliest_time end as earliest_time,
	   r.first_buy_time,
	   case when r.first_payment is null then 0 else r.first_payment end as first_payment,
	   r.second_buy_time,
	   case when r.year_payment is null then 0 else r.year_payment end as year_payment,
	   case when r.year_buy_times is null then 0 else r.year_buy_times end as year_buy_times,
	   case when r.year_buy_num is null then 0 else r.year_buy_num end as year_buy_num,
	   r.year_first_time,
	   r.year_last_time,
	   case when r.tyear_payment is null then 0 else r.tyear_payment end as tyear_payment,
	   case when r.tyear_buy_times is null then 0 else r.tyear_buy_times end as tyear_buy_times,
	   case when r.tyear_buy_num is null then 0 else r.tyear_buy_num end as tyear_buy_num,
	   case when r.btyear_payment is null then 0 else r.btyear_payment end as btyear_payment,
	   case when r.btyear_buy_times is null then 0 else r.btyear_buy_times end as btyear_buy_times,
	   case when r.btyear_buy_num is null then 0 else r.btyear_buy_num end as btyear_buy_num,
	   '${stat_date}' as stat_date
from 
(
	select r.tenant,r.plat_code,r.shop_id,r1.uni_shop_id,r1.uni_id,r1.modified
	from dw_base.b_std_tenant_shop r
	left join (
		select c1.tenant,c2.plat_code,c2.uni_shop_id,c2.shop_id,c1.uni_id,c1.modified 
		from dw_base.b_std_customer c1
		left join dw_base.b_std_shop_customer_rel c2
		on c1.uni_id = c2.uni_id
		where c2.plat_code is not null
	) r1
	on r.tenant=r1.tenant and r.plat_code=r1.plat_code and r.shop_id=r1.shop_id
	where r1.tenant is not null
)t
left outer join
(
	select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
		a.first_buy_time as earliest_time,
		a.first_buy_time,a.first_payment,a.second_buy_time,
		b.year_payment,b.year_buy_times,b.year_buy_num,b.year_first_time,b.year_last_time,
		c.tyear_payment,c.tyear_buy_times,c.tyear_buy_num,
		d.btyear_payment,d.btyear_buy_times,d.btyear_buy_num
	from 
	 (select * from dw_rfm.`b_first_buy_history_base` where part='${stat_date}') a
	 left join
	 (select * from  dw_rfm.`b_last_year_history_base` where part='${stat_date}') b
	 on a.tenant=b.tenant and a.plat_code = b.plat_code and a.uni_shop_id = b.uni_shop_id and a.uni_id = b.uni_id
	 left join
	 (select * from dw_rfm.`b_last_tyear_history_base` where part='${stat_date}') c
	 on a.tenant=c.tenant and a.plat_code = c.plat_code and a.uni_shop_id = c.uni_shop_id and a.uni_id = c.uni_id
	 left join
	 (select * from dw_rfm.`b_before_tyear_history_base` where part='${stat_date}') d
	 on a.tenant=d.tenant and a.plat_code = d.plat_code and a.uni_shop_id = d.uni_shop_id and a.uni_id = d.uni_id
) r
on t.tenant = r.tenant and t.plat_code=r.plat_code and t.uni_shop_id=r.uni_shop_id and t.uni_id = r.uni_id;

-- 去重获取平台级的最早购买时间
drop table if exists dw_rfm.plat_first_buy_history_temp;
create table dw_rfm.plat_first_buy_history_temp as
select r.tenant,r.plat_code,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  when concat_ws('',collect_set(r.second_buy_time)) < concat_ws('',collect_set(r.second_buy_time_mid)) then concat_ws('',collect_set(r.second_buy_time))
	  else NULL end as second_buy_time
from(
	select t.tenant,t.plat_code,t.uni_id,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,plat_code,uni_id order by first_buy_time asc) as rank 
		from dw_rfm.b_first_buy_history_base
		where part='${stat_date}'
	) t where t.rank <= 2
) r
group by r.tenant,r.plat_code,r.uni_id;
 
--全渠道平台级用户RFM指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_qqd_plat_rfm`(
	`tenant` string,
	`plat_code` string,
	`uni_id` string,
    `earliest_time` string,
	`first_buy_time` string,
	`first_payment` double,
    `second_buy_time` string, 
    `year_payment` double,
	`year_buy_times` int,
    `year_buy_num` int, 
    `year_first_time` string,
	`year_last_time` string,
	`tyear_payment` double,
    `tyear_buy_times` int, 
    `tyear_buy_num` int,
	`btyear_payment` double,
	`btyear_buy_times` int,
    `btyear_buy_num` int,
	`stat_date` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_qqd_plat_rfm` partition(part='${stat_date}')
select r.tenant,r.plat_code,r.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	'${stat_date}' as stat_date
from (
	select a.tenant,a.plat_code,a.uni_id,
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
	from dw_rfm.`b_qqd_shop_rfm` a 
	where a.part='${stat_date}'
	group by a.tenant,a.plat_code,a.uni_id
) r
left outer join 
	dw_rfm.plat_first_buy_history_temp t
	on r.tenant = t.tenant and r.plat_code=t.plat_code and r.uni_id=t.uni_id;

-- 去重排序，获取租户级的首次购买时间
drop table if exists dw_rfm.tenant_first_buy_history_temp;
create table dw_rfm.tenant_first_buy_history_temp as
select r.tenant,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  when concat_ws('',collect_set(r.second_buy_time)) < concat_ws('',collect_set(r.second_buy_time_mid)) then concat_ws('',collect_set(r.second_buy_time))
	  else NULL end as second_buy_time
from(
	select t.tenant,t.uni_id,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,uni_id order by first_buy_time asc) as rank 
		from dw_rfm.plat_first_buy_history_temp
	) t where t.rank <= 2
) r
group by r.tenant,r.uni_id;

-- 全渠道租户级客户RMF指标
CREATE EXTERNAL TABLE IF NOT EXISTS dw_rfm.`b_qqd_tenant_rfm`(
	`tenant` string,
	`uni_id` string,
    `earliest_time` string,
	`first_buy_time` string,
	`first_payment` double,
    `second_buy_time` string, 
    `year_payment` double,
	`year_buy_times` int,
    `year_buy_num` int, 
    `year_first_time` string,
	`year_last_time` string,
	`tyear_payment` double,
    `tyear_buy_times` int, 
    `tyear_buy_num` int,
	`btyear_payment` double,
	`btyear_buy_times` int,
    `btyear_buy_num` int,
	`stat_date` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_qqd_tenant_rfm` partition(part='${stat_date}')
select r.tenant,r.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	'${stat_date}' as stat_date
from (
	select a.tenant,a.uni_id,
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
	from dw_rfm.`b_qqd_plat_rfm` a 
	where a.part='${stat_date}'
	group by a.tenant,a.uni_id
) r
left outer join 
	dw_rfm.tenant_first_buy_history_temp t
	on r.tenant = t.tenant and r.uni_id=t.uni_id;
