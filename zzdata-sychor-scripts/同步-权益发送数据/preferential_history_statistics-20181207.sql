SET mapred.job.name='b_preferential_statistics_base-权益发送历史统计作业';
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

-- 1、创建互动统计结果存储表，设置统计分区（按租户分区）
CREATE TABLE IF NOT EXISTS dw_business.`b_preferential_statistics_base`(
	`tenant` string,
    `uni_id` string,
    `total_num` int,
	`last_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 2、直接对互动记录进行近90天的数据统计，插入统计基础表中
insert into table dw_business.`b_preferential_statistics_base`
select t.tenant,t.uni_id,count(t.uni_id) totalnum,max(t.actiontime) as lasttime
from
(
 select a.plat_code,a.plat_account,a.shop_id,a.send_time as actiontime,b.uni_id,c.tenant 
 from(
    select plat_code,plat_account,shop_id,send_time 
	from dw_business.`b_preferential_send_history`
    where day > date_sub('${stat_date}',${daynum}) 
 ) a
 left outer join
	dw_base.b_std_shop_customer_rel b
	on( a.plat_code = b.plat_code and a.plat_account = b.plat_account and a.shop_id = b.shop_id)
 LEFT OUTER JOIN
	dw_base.b_std_tenant_shop c
 on (a.plat_code = c.plat_code and a.shop_id = c.shop_id)
	where c.tenant is not null and b.uni_id is not null
) t
group by t.tenant,t.uni_id;




