SET mapred.job.name='wsj-company-customer-label-analyze';
set hive.tez.container.size=16144;
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


--依赖租户级用户表 dw_wsj.b_wsj_union_member

--创建租户级用户标签分析结果表
drop table if exists dw_wsj.b_company_member_label;
create table if not exists dw_wsj.b_company_member_label(
	`company_id` string,
	`uni_customer_id` string,
	`customer_type` string,
    `sex` int,
	`province` string,
    `city` string,
	`influence` double,
	`activity` double,
	`communication` double,
	`first_cognitive_time` string,
	`first_cognitive_scene` string,
	`last_active_time` string,
	`last_active_scene` string,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 基于店铺级的分析结果，计算租户级的结果，然后再和租户级客户关联后的结果，同步给业务
insert overwrite table dw_wsj.b_company_member_label
select a.company,a.uni_customer_id,b.customer_type,
	   if(a.sex is null,0,a.sex) as sex,
	   a.province,a.city,
	   b.influence,b.activity,b.communication,
	   b.first_cognitive_time,b.first_cognitive_scene,b.last_active_time,b.last_active_scene,
	   '${stat_date}' as stat_date
from dw_wsj.b_wsj_union_member a
left join dw_wsj.b_shop_customer_label_info b
on a.company=b.company and a.uni_customer_id=b.uni_customer_id;


