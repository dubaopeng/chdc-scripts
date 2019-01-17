SET mapred.job.name='b_std_tenant_shop-租户和店铺关系';
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

-- 创建租户和店铺关系每日同步数据表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_tenant_shop`(
	`app_id` string,
    `tenant` string,
    `plat_code` string,
	`shop_id` string,
	`shop_name` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/tenant_shop';

-- 批量重设分区
msck repair table dw_source.`s_std_tenant_shop`;

-- 创建租户和店铺数据表
CREATE TABLE IF NOT EXISTS dw_base.`b_std_tenant_shop`(
	`app_id` string,
    `tenant` string,
    `plat_code` string,
	`shop_id` string,
	`shop_name` string
)
partitioned by(`plat` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 覆盖原有租户和店铺关系数据
insert overwrite table dw_base.b_std_tenant_shop partition(plat)
select a.app_id,a.tenant,a.plat_code,a.shop_id,a.shop_name,a.plat_code as plat
from (
	select *,row_number() over (partition by app_id,tenant,plat_code,shop_id) as num 
	from dw_source.s_std_tenant_shop where dt='${stat_date}'
) a 
where a.num = 1
distribute by a.plat_code;






