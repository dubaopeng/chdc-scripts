SET mapred.job.name='dw_channel_cix_data_ccms_trade';
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


----------------------------------------------------------------------------------
--获取每个租户最近365天成功交易订单数，成功交易客人数（重复的客人需要排重），成功交易金额
----------------------------------------------------------------------------------
create table if not exists dw_channel.cix_data_ccms_trade(
  tenant_id           string,
  period              bigint,
  cal_trade_number    bigint,
  cal_customer_number bigint,
  cal_payment         double,
  created             string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

--一年365天统计
insert overwrite table dw_channel.cix_data_ccms_trade
select a.tenant,
      '365' as period,
       count(a.tid) as cal_trade_number,
       count(distinct a.buyer_nick) as cal_customer_number,
       cast(sum(case when a.payment is null or a.payment = 'NULL' or a.payment = 'null' or a.payment is NULL or a.payment = '' then 0 else coalesce(a.payment, 0) end) as decimal(20,2)) as cal_payment,
       ${hiveconf:MODIFIED} as created
from (
  select bcs.tenant, btt.tid, btt.buyer_nick, btt.payment
    from common_base.b_common_shops bcs
   left join (
                select dp_id, tid, buyer_nick, payment
                from dw_base.b_top_trade
                WHERE (part >= substr(DATE_SUB('${stat_date}', 365), 1, 7) or part = 'active') and status = 'TRADE_FINISHED'
                and substr(created, 1, 10) >= DATE_SUB('${stat_date}', 365)
             ) btt 
       on bcs.plat_shop_id = btt.dp_id
  group by bcs.tenant, btt.tid, btt.buyer_nick, btt.payment
) a
group by a.tenant;

-- 最近三十天订单统计
insert into table dw_channel.cix_data_ccms_trade
select a.tenant,
      '30' as period,
       count(a.tid) as cal_trade_number,
       count(distinct a.buyer_nick) as cal_customer_number,
       cast(sum(case when a.payment is null or a.payment = 'NULL' or a.payment = 'null' or a.payment is NULL or a.payment = '' then 0 else coalesce(a.payment, 0) end) as decimal(20,2)) as cal_payment,
       ${hiveconf:MODIFIED} as created
from (
  select bcs.tenant, btt.tid, btt.buyer_nick, btt.payment
    from common_base.b_common_shops bcs
   left join (
                select dp_id, tid, buyer_nick, payment
                from dw_base.b_top_trade
                WHERE (part >= substr(DATE_SUB('${stat_date}', 30), 1, 7) or part = 'active') and status = 'TRADE_FINISHED'
                and substr(created, 1, 10) >= DATE_SUB('${stat_date}', 30)
             ) btt 
       on bcs.plat_shop_id = btt.dp_id
  group by bcs.tenant, btt.tid, btt.buyer_nick, btt.payment
) a
group by a.tenant;



