SET mapred.job.name='b_interaction_statistics_result-用户参与互动记录统计';
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

-- 1、创建互动统计结果基础表，避免没有表后续报错
CREATE TABLE IF NOT EXISTS dw_business.`b_interaction_statistics_base`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 2、创建临时表，存放今日统计结果
DROP TABLE IF exists dw_source.`s_interaction_statistics_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_interaction_statistics_temp`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 3、对时间区间两头的记录统计后合并取总数，然后使用临时表和基础统计表进行关联，找出变化（减少或者增加，对于0的部分不用管）的部分，同步给业务
insert into table dw_source.`s_interaction_statistics_temp`
select re.tenant,re.uni_id,sum(re.totalnum) as totalnum,max(re.lasttime) as lasttime from (
	select t.tenant,t.uni_id,count(t.uni_id) totalnum,max(t.actiontime) as lasttime from
	(
		 select a.plat_code,a.plat_account,a.shop_id,a.interaction_time as actiontime,b.uni_id,c.tenant 
		 from(
			 select plat_code,plat_account,shop_id,interaction_time from dw_business.b_interaction_history where day = date_sub('${stat_date}',1)
		 ) a
		 LEFT OUTER JOIN
		 dw_base.b_std_shop_customer_rel b
		 on( a.plat_code = b.plat_code and a.plat_account = b.plat_account and a.shop_id = b.shop_id)
		 LEFT OUTER JOIN
		 dw_base.b_std_tenant_shop c
		 on (a.plat_code = c.plat_code and a.shop_id = c.shop_id)
		 where c.tenant is not null
	) t
	group by t.tenant,t.uni_id
	union all 
	select t.tenant,t.uni_id,-count(t.uni_id) totalnum,max(t.actiontime) as lasttime from
	(
		 select a.plat_code,a.plat_account,a.shop_id,a.interaction_time as actiontime,b.uni_id,c.tenant 
		 from(
			 select plat_code,plat_account,shop_id,interaction_time from dw_business.b_interaction_history where day = date_sub('${stat_date}',${daynum}-1)
		 ) a
		 LEFT OUTER JOIN
		 dw_base.b_std_shop_customer_rel b
		 on( a.plat_code = b.plat_code and a.plat_account = b.plat_account and a.shop_id = b.shop_id)
		 LEFT OUTER JOIN
		 dw_base.b_std_tenant_shop c
		 on (a.plat_code = c.plat_code and a.shop_id = c.shop_id)
		 where c.tenant is not null
	) t
	group by t.tenant,t.uni_id
) re
GROUP BY re.tenant,re.uni_id;


-- 4、创建每日统计结果变化的表
CREATE TABLE IF NOT EXISTS dw_source.`s_interaction_statistics_result`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string,
	`stdate` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' lines terminated by '\n'
STORED AS TEXTFILE;

-- 5、插入每日统计结果，同步程序对该表进行同步
insert overwrite table dw_source.`s_interaction_statistics_result`
select t.tenant,t.uni_id,(t.oldnum+t.changenum) as totalnum, t.lasttime,'${stat_date}' as stdate 
from (
	select a.tenant,a.uni_id,a.total_num as changenum,a.last_time as changetime,
		case when b.total_num is null then 0 else b.total_num end as oldnum,
		case when b.last_time is null then a.last_time 
		when b.last_time < a.last_time then a.last_time
		else b.last_time end as lasttime 
	from dw_source.s_interaction_statistics_temp a
	left outer join 
		dw_business.b_interaction_statistics_base b
	on a.tenant = b.tenant and a.uni_id = b.uni_id
) t;


-- 6、合并更改后的数据和未变化的数据到基础统计表
insert overwrite table dw_business.b_interaction_statistics_base partition(part)
select a.tenant,a.uni_id,a.total_num,a.last_time,a.tenant as part from dw_business.b_interaction_statistics_base  a
left outer join
(select tenant,uni_id,total_num,last_time from dw_source.`s_interaction_statistics_result`)  b
on (a.tenant = b.tenant and a.uni_id = b.uni_id)
where b.tenant is null
union all
select t.tenant,t.uni_id,t.total_num,t.last_time,t.tenant as part  
from dw_source.`s_interaction_statistics_result` t;

-- 7、删除临时表
DROP TABLE IF exists dw_source.`s_interaction_statistics_temp`;





