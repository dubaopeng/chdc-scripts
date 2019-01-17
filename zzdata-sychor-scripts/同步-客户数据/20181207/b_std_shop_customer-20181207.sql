SET mapred.job.name='b_std_shop_customer-店铺与客户关系数据同步作业';
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

--add jar hdfs://master01.bigdata.shuyun.com:8020/user/hive/jar/plat-hive-udf-1.0.0.jar;
add jar hdfs://standard-cluster/user/hive/jar/plat-hive-udf-1.0.0.jar;
create temporary function shopid_hash as 'com.shuyun.plat.hive.udf.ShopIdHashUDF';

-- 全渠道平台店铺与客户关系数据增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_shop_customer`( 
    `uni_id` string,
	`uni_shop_id` string,
	`shop_id` string,
    `plat_code` string, 
    `plat_account` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/shop_customer';

-- 批量重设分区
msck repair table dw_source.`s_std_shop_customer`;

-- 平台店铺客户正式表
CREATE TABLE IF NOT EXISTS dw_base.`b_std_shop_customer_rel`( 
    `uni_id` string,
	`uni_shop_id` string,
	`shop_id` string,
    `plat_code` string, 
    `plat_account` string
)
partitioned by(`dp` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 合并并去重插入结果
insert overwrite table dw_base.`b_std_shop_customer_rel` partition(dp)
select t.uni_id,t.uni_shop_id,t.shop_id,t.plat_code,t.plat_account,shopid_hash(t.uni_shop_id) as dp
 from (
	select re.*,row_number() over(partition by re.uni_id,re.uni_shop_id,re.shop_id,re.plat_code) as num
	from (
		select t1.uni_id,t1.uni_shop_id,t1.shop_id,t1.plat_code,t1.plat_account from dw_base.`b_std_shop_customer_rel` t1
		union all 
		select t2.uni_id,t2.uni_shop_id,t2.shop_id,t2.plat_code,t2.plat_account
			from dw_source.`s_std_shop_customer` t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1
distribute by shopid_hash(t.uni_shop_id);