SET mapred.job.name='s_std_refund-退款数据同步';
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

-- 创建退款每天同步数据表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_refund_increm`(
	`data_from` int,
	`partner` string,
    `uni_refund_id` string,
	`refund_id` string,
	`created` string,
    `uni_order_item_id` string, 
    `uni_order_id` string,
	`plat_code` string,
	`order_item_id` string,
    `order_id` string, 
	`uni_shop_id` string,
	`shop_id` string,
	`sku_id` string,
    `product_id` string, 
    `refund_fee` double,
	`refund_reason` string,
	`good_return` string,
    `uni_refund_status` string, 
	`refund_status` string,
	`modified` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/refund';

-- 批量重设分区
msck repair table dw_source.`s_std_refund_increm`;

-- 创建退款历史记录表
CREATE TABLE IF NOT EXISTS dw_business.`s_std_refund`(
	`data_from` int,
	`partner` string,
    `uni_refund_id` string,
	`refund_id` string,
	`created` string,
    `uni_order_item_id` string, 
    `uni_order_id` string,
	`plat_code` string,
	`order_item_id` string,
    `order_id` string, 
	`uni_shop_id` string,
	`shop_id` string,
	`sku_id` string,
    `product_id` string, 
    `refund_fee` double,
	`refund_reason` string,
	`good_return` string,
    `uni_refund_status` string, 
	`refund_status` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 合并并去重插入结果表
insert overwrite table dw_business.`s_std_refund`
select t.data_from,t.partner,t.uni_refund_id,t.refund_id,t.created,t.uni_order_item_id,t.uni_order_id,t.plat_code,t.order_item_id,t.order_id,
	   t.uni_shop_id,t.shop_id,t.sku_id,t.product_id,t.refund_fee,t.refund_reason,t.good_return,t.uni_refund_status,t.refund_status,t.modified
 from (
	select re.*,row_number() over (distribute by re.uni_refund_id sort by re.modified desc) as num
	from (
		select t1.* from dw_business.`s_std_refund` t1
		union all 
		select t2.data_from,t2.partner,t2.uni_refund_id,t2.refund_id,t2.created,t2.uni_order_item_id,t2.uni_order_id,t2.plat_code,t2.order_item_id,t2.order_id,
			t2.uni_shop_id,t2.shop_id,t2.sku_id,t2.product_id,t2.refund_fee,t2.refund_reason,t2.good_return,t2.uni_refund_status,t2.refund_status,t2.modified
			from dw_source.`s_std_refund_increm` t2 where t2.dt='${stat_date}'
	) re
) t 
where t.num =1;

