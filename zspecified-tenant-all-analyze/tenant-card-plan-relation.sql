SET mapred.job.name='b_tenant_card_plan_relation';
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

--计算出租户的卡ID的关系表

CREATE TABLE IF NOT EXISTS dw_rfm.`b_tenant_card_plan_relation`(
	`card_plan_id` string
)
PARTITIONED BY(tenant string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_tenant_card_plan_relation partition(tenant='${tenant}')
select b.card_plan_id
from (
	select tenant,plat_code,shop_id from dw_base.b_std_tenant_shop where tenant='${tenant}'
) a
left join dw_business.b_card_shop_rel b
on a.plat_code=b.plat_code and a.shop_id=b.shop_id
where b.card_plan_id is not null
group by b.card_plan_id;