SET mapred.job.name='merge-cix-offline-member-analysis-notify';
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

-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 将两个表的记录插入通知表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_offline_member_analysis_notify`(
	`table_name` string,
    `available` string,
    `modified` string,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_offline_member_analysis_notify 
select a.tablename,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date 
from (
	select explode(split(concat_ws(',',
	   'cix_offline_shop_member_change_statistic',
	   'cix_offline_plat_member_sale_rate',
	   'cix_offline_sale_shops_of_card',
	   'cix_offline_member_consume_statistic'),',')) as tablename
) a;

--最后清除订单临时表分区
ALTER TABLE dw_rfm.b_offline_last30day_trade_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_offline_last30day_member_trade_temp DROP IF EXISTS PARTITION (part='${stat_date}');

-- TODO 接下来使用sqoop导出上面几个表的数据到mysql表中





