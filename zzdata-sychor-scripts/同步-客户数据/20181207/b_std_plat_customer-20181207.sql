SET mapred.job.name='b_std_plat_customer-平台与客户数据同步作业';
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


-- 全渠道客户与平台数据每日增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_plat_customer`( 
    `uni_id` string, 
    `plat_code` string, 
    `plat_account` string,
    `plat_nick` string,
    `partner` string,
    `tenant` string,
    `plat_avatar` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/plat_customer';

-- 批量重设分区
msck repair table dw_source.`s_std_plat_customer`;


-- 全渠道平台客户表，以租户和平台进行分区
CREATE TABLE IF NOT EXISTS dw_base.`b_std_plat_customer_rel`( 
    `uni_id` string, 
    `plat_code` string, 
    `plat_account` string,
    `plat_nick` string,
    `partner` string,
    `tenant` string,
    `plat_avatar` string
)
partitioned by(tenantid string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 合并并去重插入结果
insert overwrite table dw_base.`b_std_plat_customer_rel` partition(tenantid)
select t.uni_id,t.plat_code,t.plat_account,t.plat_nick,t.partner,t.tenant,t.plat_avatar,t.tenant as tenantid
 from (
	select re.*,row_number() over(partition by re.plat_code,re.plat_account,re.partner,re.tenant) as num
	from (
		select t1.uni_id,t1.plat_code,t1.plat_account,t1.plat_nick,t1.partner,t1.tenant,t1.plat_avatar 
			from dw_base.`b_std_plat_customer_rel` t1
		union all 
		select t2.uni_id,t2.plat_code,t2.plat_account,t2.plat_nick,t2.partner,t2.tenant,t2.plat_avatar
			from dw_source.`s_std_plat_customer` t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1
distribute by t.tenant;