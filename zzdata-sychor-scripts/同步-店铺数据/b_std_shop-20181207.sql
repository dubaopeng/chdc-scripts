SET mapred.job.name='b_std_shop-店铺信息数据';
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

-- 创建店铺每天同步数据表
-- DROP TABLE IF EXISTS dw_source.`s_std_shop`;
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_shop`(
	`data_from` string,
	`partner` string,
    `uni_shop_id` string,
	`plat_code` string,
	`shop_id` string,
    `shop_name` string, 
    `shop_desc` string,
	`shop_logo` string,
	`category_id` string,
    `country` string, 
    `state` string,
	`city` string,
	`district` string,
    `town` string, 
    `address` string,
	`is_online_shop` int,
	`status` int,
    `open_date` string,
	`modified` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/shop';

-- 批量重设分区
msck repair table dw_source.`s_std_shop`;

-- 创建店铺信息表
CREATE TABLE IF NOT EXISTS dw_base.`b_std_shop`(
	`data_from` string,
	`partner` string,
    `uni_shop_id` string,
	`plat_code` string,
	`shop_id` string,
    `shop_name` string, 
    `shop_desc` string,
	`shop_logo` string,
	`category_id` string,
    `country` string, 
    `state` string,
	`city` string,
	`district` string,
    `town` string, 
    `address` string,
	`is_online_shop` int,
	`status` int,
    `open_date` string,
	`modified` string
)
partitioned by(`plat` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS RCFILE;


-- 去重后覆盖原有数据插入
insert overwrite table dw_base.`b_std_shop` partition(plat)
select a.data_from,a.partner,a.uni_shop_id,a.plat_code,a.shop_id,a.shop_name,a.shop_desc,a.shop_logo,a.category_id,
a.country,a.state,a.city,a.district,a.town,a.address,a.is_online_shop,a.status,a.open_date,a.modified,a.plat_code as plat
from (
	select *,row_number() over (distribute by uni_shop_id sort by modified desc) as num 
	from dw_source.s_std_shop where dt='${stat_date}'
) a 
where a.num = 1
distribute by a.plat_code;







