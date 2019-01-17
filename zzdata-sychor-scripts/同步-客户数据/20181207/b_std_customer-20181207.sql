SET mapred.job.name='b_std_customer-全渠道客户数据同步作业';
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

-- 创建全渠道客户数据增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_customer`( 
    `uni_id` string,
	`tenant` string,
    `full_name` string, 
    `first_name` string,
    `last_name` string,
    `gender` string,
    `birth_year` int,
	`birth_month` int,
	`birthday_type` int,
	`birthday` bigint,
	`constellation` int,
    `identity_card` string, 
    `email_suffix` string,
    `is_valid_email` int,
    `email` string,
    `email_markting` int,
	`mobile_zone` string,
	`mobile` string,
    `is_valid_mobile` string, 
    `used_mobile` string,
    `register_mobile` string,
    `custom_mobile` string,
    `bindcard_mobile` string,
	`mobile_markting` int,
	`zip` string,
    `country` string, 
    `state` string,
    `city` string,
    `district` string,
    `town` string,
	`address` string,
	`insert_time` string,
    `most_consumption_shop_id` string,
    `last_consumption_shop_id` string,
    `modified` string,
	`hone_type` int,
    `customer_source` string,
	`customer_from_plat` string,
	`customer_from_shop` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/customer';

-- 批量重设分区
msck repair table dw_source.`s_std_customer`;

-- 全渠道客户数据表，以租户id做分区
CREATE TABLE IF NOT EXISTS dw_base.`b_std_customer`( 
    `uni_id` string,
	`tenant` string,
    `full_name` string, 
    `first_name` string,
    `last_name` string,
    `gender` string,
    `birth_year` int,
	`birth_month` int,
	`birthday_type` int,
	`birthday` bigint,
	`constellation` int,
    `identity_card` string, 
    `email_suffix` string,
    `is_valid_email` int,
    `email` string,
    `email_markting` int,
	`mobile_zone` string,
	`mobile` string,
    `is_valid_mobile` string, 
    `used_mobile` string,
    `register_mobile` string,
    `custom_mobile` string,
    `bindcard_mobile` string,
	`mobile_markting` int,
	`zip` string,
    `country` string, 
    `state` string,
    `city` string,
    `district` string,
    `town` string,
	`address` string,
	`insert_time` string,
    `most_consumption_shop_id` string,
    `last_consumption_shop_id` string,
    `modified` string,
	`hone_type` int,
    `customer_source` string,
	`customer_from_plat` string,
	`customer_from_shop` string
)
partitioned by(tenantid string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 合并并去重插入结果
insert overwrite table dw_base.`b_std_customer` partition(tenantid)
select t.uni_id,t.tenant,t.full_name,t.first_name,t.last_name,t.gender,t.birth_year,t.birth_month,
	   t.birthday_type,t.birthday,t.constellation,t.identity_card,t.email_suffix,t.is_valid_email,t.email,
	   t.email_markting,t.mobile_zone,t.mobile,t.is_valid_mobile,t.used_mobile,t.register_mobile,t.custom_mobile,
	   t.bindcard_mobile,t.mobile_markting,t.zip,t.country,t.state,t.city,t.district,t.town,t.address,
	   t.insert_time,t.most_consumption_shop_id,t.last_consumption_shop_id,t.modified,t.hone_type,t.customer_source,
	   t.customer_from_plat,t.customer_from_shop,t.tenant as tenantid
 from (
	select re.*,row_number() over(distribute by re.uni_id,re.tenant SORT BY re.modified desc) as num
	from (
		select t1.uni_id,t1.tenant,t1.full_name,t1.first_name,t1.last_name,t1.gender,t1.birth_year,t1.birth_month,
			   t1.birthday_type,t1.birthday,t1.constellation,t1.identity_card,t1.email_suffix,t1.is_valid_email,t1.email,
			   t1.email_markting,t1.mobile_zone,t1.mobile,t1.is_valid_mobile,t1.used_mobile,t1.register_mobile,t1.custom_mobile,
			   t1.bindcard_mobile,t1.mobile_markting,t1.zip,t1.country,t1.state,t1.city,t1.district,t1.town,t1.address,
			   t1.insert_time,t1.most_consumption_shop_id,t1.last_consumption_shop_id,t1.modified,t1.hone_type,t1.customer_source,
			   t1.customer_from_plat,t1.customer_from_shop
			from dw_base.`b_std_customer` t1
		union all 
		select t2.uni_id,t2.tenant,t2.full_name,t2.first_name,t2.last_name,t2.gender,t2.birth_year,t2.birth_month,
			   t2.birthday_type,t2.birthday,t2.constellation,t2.identity_card,t2.email_suffix,t2.is_valid_email,t2.email,
			   t2.email_markting,t2.mobile_zone,t2.mobile,t2.is_valid_mobile,t2.used_mobile,t2.register_mobile,t2.custom_mobile,
			   t2.bindcard_mobile,t2.mobile_markting,t2.zip,t2.country,t2.state,t2.city,t2.district,t2.town,t2.address,
			   t2.insert_time,t2.most_consumption_shop_id,t2.last_consumption_shop_id,t2.modified,t2.hone_type,t2.customer_source,
			   t2.customer_from_plat,t2.customer_from_shop
			from dw_source.`s_std_customer` t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1
distribute by t.tenant;
