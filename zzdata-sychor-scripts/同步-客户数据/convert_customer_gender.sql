SET mapred.job.name='convert-customer-gender-清洗客户性别';
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

insert overwrite table dw_base.`b_std_customer` partition(tenantid)
select t.uni_id,t.tenant,t.full_name,t.first_name,t.last_name,
	   case when  t.gender='男' then 'M' when t.gender='女' then 'F' else t.gender end  as gender,
	   t.birth_year,t.birth_month,
	   t.birthday_type,t.birthday,t.constellation,t.identity_card,t.email_suffix,t.is_valid_email,t.email,
	   t.email_markting,t.mobile_zone,t.mobile,t.is_valid_mobile,t.used_mobile,t.register_mobile,t.custom_mobile,
	   t.bindcard_mobile,t.mobile_markting,t.zip,t.country,t.state,t.city,t.district,t.town,t.address,
	   t.insert_time,t.most_consumption_shop_id,t.last_consumption_shop_id,t.modified,t.hone_type,t.customer_source,
	   t.customer_from_plat,t.customer_from_shop,t.tenant as tenantid
 from (
	select t1.uni_id,t1.tenant,t1.full_name,t1.first_name,t1.last_name,t1.gender,t1.birth_year,t1.birth_month,
	   t1.birthday_type,t1.birthday,t1.constellation,t1.identity_card,t1.email_suffix,t1.is_valid_email,t1.email,
	   t1.email_markting,t1.mobile_zone,t1.mobile,t1.is_valid_mobile,t1.used_mobile,t1.register_mobile,t1.custom_mobile,
	   t1.bindcard_mobile,t1.mobile_markting,t1.zip,t1.country,t1.state,t1.city,t1.district,t1.town,t1.address,
	   t1.insert_time,t1.most_consumption_shop_id,t1.last_consumption_shop_id,t1.modified,t1.hone_type,t1.customer_source,
	   t1.customer_from_plat,t1.customer_from_shop
	from dw_base.`b_std_customer` t1
) t
distribute by t.tenant;



