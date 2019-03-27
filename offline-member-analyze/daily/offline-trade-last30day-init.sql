SET mapred.job.name='offline-trade-last30day-init';

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

-- 依赖表：订单表
-- 会员卡与店铺关系：dw_business.b_card_shop_rel
-- 客户与会员关系：dw_business.b_customer_member_relation

-- 计算各个时间边界
set yestoday='${stat_date}';
set weekfirst=date_sub('${stat_date}',pmod(datediff('${stat_date}', concat(year('${stat_date}'),'-01-01'))-6,7));
set monthfirst=date_sub('${stat_date}',dayofmonth('${stat_date}')-1);
set last7day=date_sub('${stat_date}',7);
set last30day=date_sub('${stat_date}',30);


-- 查询近30天的订单数据,标识订单区间
create table if not exists dw_rfm.b_offline_last30day_trade_temp(
	card_plan_id string,
	shop_id string,
	uni_shop_id string,
	uni_id string,
	receive_payment double,
    created string,
	yestoday int,
	thisweek int,
	thismonth int,
	last7day int,
	last30day int,
	member_id string
)
PARTITIONED BY(part string,plat_code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 只对淘宝和线下平台的数据
insert overwrite table dw_rfm.b_offline_last30day_trade_temp partition(part='${stat_date}',plat_code)
select r.card_plan_id,r.shop_id,r.uni_shop_id,r.uni_id,r.receive_payment,r.created,
	   r.yestoday,r.thisweek,r.thismonth,r.last7day,r.last30day,
	   r1.member_id,
	   r.plat_code as plat_code
from (
	select a.card_plan_id,a.plat_code,a.shop_id,b.uni_shop_id,b.uni_id,
		   b.receive_payment,b.created,
		   case when substr(b.created,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
		   case when substr(b.created,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
		   case when substr(b.created,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
		   case when substr(b.created,1,10) > ${hiveconf:last7day} then 1 else 0 end as last7day,
		   case when substr(b.created,1,10) > ${hiveconf:last30day} then 1 else 0 end as last30day
	from dw_business.b_card_shop_rel a
	left join (
		select plat_code,uni_shop_id,shop_id,uni_id,
			case when lower(refund_fee) = 'null' or refund_fee is NULL then payment else (payment - refund_fee) end as receive_payment,
			case when lower(trade_type) = 'step' and pay_time is not null then pay_time else created end as created
		from dw_base.b_std_trade
		where
		  part >= substr(date_sub('${stat_date}',30),1,7)
		  and (plat_code='TAOBAO' or plat_code='OFFLINE')
		  and (created is not NULL and substr(created,1,10) > date_sub('${stat_date}',30) and substr(created,1,10) <= '${stat_date}')
		  and uni_id is not NULL 
		  and payment is not NULL
		  and order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
	) b
	on a.plat_code=b.plat_code and a.shop_id=b.shop_id
	where b.uni_shop_id is not null
) r
left join dw_business.b_customer_member_relation r1
on r.card_plan_id=r1.card_plan_id and r.uni_id=r1.uni_id;


--近30天所有会员的订单
create table if not exists dw_rfm.b_offline_last30day_member_trade_temp(
	card_plan_id string,
	shop_id string,
	uni_shop_id string,
	receive_payment double,
    created string,
	yestoday int,
	thisweek int,
	thismonth int,
	last7day int,
	last30day int,
	member_id string,
	grade int,
	open_shop_id string
)
PARTITIONED BY(part string,plat_code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_offline_last30day_member_trade_temp partition(part='${stat_date}',plat_code)
select t.card_plan_id,t.shop_id,t.uni_shop_id,t.receive_payment,t.created,
	   t.yestoday,t.thisweek,t.thismonth,t.last7day,t.last30day,
	   t.member_id,
	   t1.grade,
	   t1.shop_id as open_shop_id,
	   t.plat_code plat_code
from (
	select card_plan_id,shop_id,uni_shop_id,uni_id,receive_payment,created,
	   yestoday,
	   thisweek,
	   thismonth,
	   last7day,
	   last30day,
	   member_id,
	   plat_code
	from dw_rfm.b_offline_last30day_trade_temp
	where part='${stat_date}' and member_id is not null
) t
left join dw_business.b_std_member_base_info t1
on t.card_plan_id=t1.card_plan_id and t.plat_code=t1.plat_code and t.uni_shop_id=t1.uni_shop_id and t.member_id=t1.member_id
where t1.grade is not null and t1.shop_id is not null;


