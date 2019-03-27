SET mapred.job.name='merge-tenant-plat-shop-customer';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=4915;
set tez.runtime.unordered.output.buffer.size-mb=1640;
set tez.runtime.io.sort.mb=6553;
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

-- 租户平台店铺客户宽表
CREATE TABLE IF NOT EXISTS dw_base.`b_tenant_plat_shop_customer`( 
    `plat_code` string, 
    `shop_id` string,
	`uni_shop_id` string,
	`shop_name` string,
	`uni_id` string,
	`modified` string
)
partitioned by(`tenant` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY",'orc.bloom.filter.columns'='uni_shop_id');

insert overwrite table dw_base.b_tenant_plat_shop_customer partition(tenant)
select r.plat_code,r.shop_id,r.uni_shop_id,r.shop_name,t.uni_id,t.modified,t.tenant as tenant
from dw_base.b_std_customer t
left join(
	select a.uni_id,a.plat_code,a.shop_id,a.uni_shop_id,b.tenant,b.shop_name
	from dw_base.b_std_shop_customer_rel a
	left join dw_base.b_std_tenant_shop b
	on a.plat_code=b.plat_code and a.shop_id=b.shop_id
) r
on t.tenant=r.tenant and t.uni_id=r.uni_id
where r.tenant is not null and r.uni_id is not null;