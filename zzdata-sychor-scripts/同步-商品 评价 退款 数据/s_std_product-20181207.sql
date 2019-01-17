SET mapred.job.name='s_std_item-商品数据同步';
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

-- 创建商品每天同步数据表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_product_increm`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
	`product_id` string,
	`uni_shop_id` string,
    `shop_id` string, 
    `category_id` string,
	`type` string,
	`stock` bigint,
    `product_name` string, 
	`price` double,
	`detail_url` string,
    `pic_url` string, 
    `modified` string,
	`online_time` string,
	`offline_time` string,
    `outer_product_id` string,
	`status` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/product';

-- 批量重设分区
msck repair table dw_source.`s_std_product_increm`;

-- 创建商品历史信息表
CREATE TABLE IF NOT EXISTS dw_business.`s_std_product`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
	`product_id` string,
	`uni_shop_id` string,
    `shop_id` string, 
    `category_id` string,
	`type` string,
	`stock` bigint,
    `product_name` string, 
	`price` double,
	`detail_url` string,
    `pic_url` string, 
    `modified` string,
	`online_time` string,
	`offline_time` string,
    `outer_product_id` string,
	`status` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 合并并去重插入结果表
insert overwrite table dw_business.s_std_product
select t.data_from,t.partner,t.plat_code,t.product_id,t.uni_shop_id,t.shop_id,t.category_id,t.type,t.stock, t.product_name, 
	   t.price,t.detail_url,t.pic_url,t.modified,t.online_time,t.offline_time,t.outer_product_id,t.status
 from (
	select re.*,row_number() over(distribute by re.partner,re.plat_code,re.product_id sort by re.modified desc) as num
	from (
		select t1.* from dw_business.s_std_product t1
		union all 
		select t2.data_from,t2.partner,t2.plat_code,t2.product_id,t2.uni_shop_id,t2.shop_id,t2.category_id,t2.type,t2.stock,t2.product_name, 
			   t2.price,t2.detail_url,t2.pic_url,t2.modified,t2.online_time,t2.offline_time,t2.outer_product_id,t2.status
			from dw_source.s_std_product_increm t2 where t2.dt='${stat_date}'
	)re
) t 
where t.num =1;







