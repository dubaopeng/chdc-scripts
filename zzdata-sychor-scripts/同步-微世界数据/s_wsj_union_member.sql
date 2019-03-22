SET mapred.job.name='s_wsj_union_member-微世界租户级用户表同步';
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

-- 微世界租户级用户表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_wsj_union_member`(
	`company_id` string,
	`uni_customer_id` string,
    `nickname` string,
	`headimgurl` string,
    `sex` int, 
	`country` string,
	`province` string,
    `city` string,
	`mobile` string,
	`status` int,
	`create_time` string,
	`update_time` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/wsjscrm/union_member';

-- 批量重设分区
msck repair table dw_source.`s_wsj_union_member`;

-- 创建租户级用户基础表
CREATE TABLE IF NOT EXISTS dw_wsj.`b_wsj_union_member`(
	`uni_customer_id` string,
    `nickname` string,
	`headimgurl` string,
    `sex` int, 
	`country` string,
	`province` string,
    `city` string,
	`mobile` string,
	`status` int,
	`create_time` string,
	`update_time` string
)
partitioned by(`company` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 合并数据插入用户基础表
insert overwrite table dw_wsj.b_wsj_union_member partition(company)
select t.uni_customer_id,t.nickname,t.headimgurl,t.sex,t.country,t.province,t.city,
		  t.mobile,t.status,t.create_time,t.update_time,t.company_id as company
 from (
	select re.company_id,re.uni_customer_id,re.nickname,re.headimgurl,re.sex,re.country,re.province,re.city,
		  re.mobile,re.status,re.create_time,re.update_time,
		row_number() over(distribute by re.company_id,re.uni_customer_id sort by re.update_time desc) as num
	from (
		select t1.company as company_id,t1.uni_customer_id,t1.nickname,t1.headimgurl,t1.sex,t1.country,t1.province,t1.city,
			t1.mobile,t1.status,t1.create_time,t1.update_time
			from dw_wsj.b_wsj_union_member t1
		union all 
		select 
			t2.company_id,t2.uni_customer_id,t2.nickname,t2.headimgurl,t2.sex,t2.country,t2.province,t2.city,
			t2.mobile,t2.status,t2.create_time,t2.update_time
			from dw_source.s_wsj_union_member t2 
			where t2.dt='${stat_date}'
	)re
) t 
where t.num =1
distribute by company;
