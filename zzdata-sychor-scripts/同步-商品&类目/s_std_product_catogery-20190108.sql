SET mapred.job.name='s_plat_shop_category-平台商品类目数据同步';
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

-- 创建商品每天同步的商品数据表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_plat_shop_category`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
    `shop_id` string, 
    `category_id` string,
	`parent_category_id` string,
    `category_name` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/product/catogery';

-- 批量重设分区
msck repair table dw_source.`s_plat_shop_category`;

-- 创建商品历史信息表
CREATE TABLE IF NOT EXISTS dw_business.`b_plat_shop_category`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
    `shop_id` string, 
    `category_id` string,
	`parent_category_id` string,
    `category_name` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 类目全量覆盖原有数据
insert overwrite table dw_business.`b_plat_shop_category`
select t.data_from,t.partner,t.plat_code,t.shop_id,t.category_id,t.parent_category_id,t.category_name
 from (
	select t1.*,row_number() over(partition by t1.partner,t1.plat_code,t1.shop_id,t1.category_id) as num
	from dw_source.s_plat_shop_category t1 
	where t1.dt='${stat_date}'
) t 
where t.num =1;







