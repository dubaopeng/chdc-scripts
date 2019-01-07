SET mapred.job.name='tarde-history-base-init 历史订单基础数据初始化';
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

-- 初始化历史订单数据，取需要的字段进行存储，需要在上线前执行该脚本作为订单基础表
drop table if exists dw_rfm.b_rfm_history_base_trade;
create table if not exists dw_rfm.b_rfm_history_base_trade(
	tenant string,
	plat_code string,
	shop_id string,
	uni_shop_id string,
	uni_id string,
	receive_payment double,
	product_num int,
    created string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 订单数据关联出租户信息
insert overwrite table dw_rfm.b_rfm_history_base_trade
select
	a.tenant,
	a.plat_code,
	a.shop_id,
    b.uni_shop_id, 
	b.uni_id,
    b.receive_payment,
	b.product_num,
	b.created
from dw_base.b_std_tenant_shop a
left join
(
	select
		plat_code,
		uni_id,
		uni_shop_id,
		shop_id,
		case when refund_fee = 'NULL' or refund_fee is NULL then payment else (payment - refund_fee) end as receive_payment,
		case when product_num is null then 1 else product_num end as product_num,
		case when lower(trade_type) = 'step' and pay_time is not null then pay_time else created end as created
	from dw_base.b_std_trade
	where
	  part < '${stat_date}'  
	  and (created is not NULL  and substr(created,1,10) <= '${stat_date}')
	  and uni_id is not NULL 
	  and payment is not NULL
	  and order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
) b
on a.plat_code = b.plat_code and a.shop_id = b.shop_id
where b.uni_id is not null and b.uni_shop_id is not null;


