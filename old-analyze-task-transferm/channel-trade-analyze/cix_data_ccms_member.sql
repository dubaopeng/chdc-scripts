SET mapred.job.name='dw_channel_cix_data_ccms_member';
SET hive.exec.compress.output=true;
set hive.cbo.enable=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output.codec=org.apache.hadoop.io.compress.SnappyCodec;
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
set MODIFIED = substr(current_timestamp,1,19);
------------------------
--获取每个租户店铺客人数
------------------------
create table if not exists dw_channel.cix_data_ccms_member(
  tenant_id           string,
  cal_number          bigint,
  created             string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert OVERWRITE TABLE dw_channel.cix_data_ccms_member
  SELECT bcs.tenant as tenant_id, 
         count(distinct btm.buyer_nick) as cal_number,
         ${hiveconf:MODIFIED} as created
  from common_base.b_common_shops bcs
    left join dw_base.b_top_member btm ON bcs.plat_shop_id = btm.dp_id
  GROUP BY bcs.tenant;