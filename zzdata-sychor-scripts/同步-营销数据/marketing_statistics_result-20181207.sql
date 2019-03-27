SET mapred.job.name='b_marketing_statistics_result-用户营销数据增量统计';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
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

-- 1、创建营销统计结果基础表，避免没有表后续报错
CREATE TABLE IF NOT EXISTS dw_business.`b_marketing_statistics_base`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 2、创建临时表，存放今日统计结果
drop table if exists dw_source.`s_marketing_statistics_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_marketing_statistics_temp`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 3、对时间区间两头的记录统计后合并取总数，然后使用临时表和基础统计表进行关联，找出变化（减少或者增加，对于0的部分不用管）的部分，同步给业务
insert into table dw_source.`s_marketing_statistics_temp`
select re.tenant,re.uni_id,sum(re.totalnum) as totalnum,max(re.lasttime) as lasttime from (
	select t.tenant,t.uni_id,count(t.uni_id) totalnum,max(t.marketing_time) as lasttime
	from dw_business.`b_marketing_history` t
	where t.day = '${stat_date}' and t.tenant is not null and t.uni_id is not null
	group by t.tenant,t.uni_id
	union all
	select t1.tenant,t1.uni_id,count(t1.uni_id) totalnum,max(t1.marketing_time) as lasttime
	from dw_business.`b_marketing_history` t1
	where t1.day = date_sub('${stat_date}',${daynum}) and t1.tenant is not null and t1.uni_id is not null
	group by t1.tenant,t1.uni_id
) re
GROUP BY re.tenant,re.uni_id;


-- 4、创建每日统计结果变化的表
CREATE TABLE IF NOT EXISTS dw_source.`s_marketing_statistics_result`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string,
	`stdate` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;


-- 6、插入每日统计结果，同步程序对该表进行同步
insert overwrite table dw_source.`s_marketing_statistics_result`
select t.tenant,t.uni_id,(t.oldnum+t.changenum) as totalnum, t.lasttime,current_date as stdate
from (
	select a.tenant,a.uni_id,a.total_num as changenum,a.last_time as changetime,
		case when b.total_num is null then 0 else b.total_num end as oldnum,
		case when b.last_time is null then a.last_time 
		when b.last_time < a.last_time then a.last_time
		else b.last_time end as lasttime 
	from dw_source.s_marketing_statistics_temp a
	left outer join 
	dw_business.b_marketing_statistics_base b
	on a.tenant = b.tenant and a.uni_id = b.uni_id
) t;

-- 7、合并更改后的数据和未变化的数据到基础统计表
insert overwrite table dw_business.b_marketing_statistics_base
select a.tenant,a.uni_id,a.total_num,a.last_time 
	from dw_business.b_marketing_statistics_base  a
	left outer join
	(select tenant,uni_id,total_num,last_time from dw_source.s_marketing_statistics_result )  b
	on a.tenant = b.tenant and a.uni_id = b.uni_id
	where b.tenant is null
union all
	select t.tenant,t.uni_id,t.total_num,t.last_time  
	from dw_source.s_marketing_statistics_result t;


drop table if exists dw_source.s_marketing_statistics_temp;





