SET mapred.job.name='day-changed-trade-analyze 每天变化订单数据分析';
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

-- 需要计算出前一天的数据，在前一天结果数据的基础上进行增量数据和变化边界数据合并、处理

-- 计算今日增加的数据，增量表中数据
drop table if exists dw_rfm.b_trade_day_changed_all;
create table if not exists dw_rfm.b_trade_day_changed_all(	
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

-- 每天变化的所有订单数据
insert overwrite table dw_rfm.b_trade_day_changed_all
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
	from dw_source.s_std_trade
	where part = '${stat_date}'
	  and created is not NULL and uni_id is not NULL and payment is not NULL
	  and order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
) b
on a.plat_code = b.plat_code and a.shop_id = b.shop_id
where b.uni_id is not null and b.uni_shop_id is not null;


-- 每天对变化的数据进行计算，然后基于前一天的分析结果进行合并，减掉时间边界之前的，加上当天的数据

-- 首次购买时间，首次购买金额和第二次购买时间计算
CREATE TABLE IF NOT EXISTS dw_rfm.`b_first_buy_day_temp`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`first_buy_time` string,
	`first_payment` double,
	`second_buy_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 去重排序取前两条记录，获取到首次时间,首次金额和第二次时间
insert overwrite table dw_rfm.`b_first_buy_day_temp`
select r.tenant,r.plat_code,r.uni_shop_id,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 then NULL else concat_ws('',collect_set(r.second_buy_time))
	  end as second_buy_time
from(
	select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
	   case t.rank when 1 then t.created else '' end as first_buy_time,
	   case t.rank when 1 then t.receive_payment else '' end as first_payment,
	   case t.rank when 2 then t.created else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,plat_code,uni_shop_id,uni_id order by created asc) as rank 
		from dw_rfm.b_trade_day_changed_all
	) t where t.rank <= 2
) r
group by r.tenant,r.plat_code,r.uni_shop_id,r.uni_id;

-- 计算今日新增订单中最近一年的购买指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last_year_day_temp`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`year_payment` double,
	`year_buy_times` int,
	`year_buy_num` int,
	`year_first_time` string,
	`year_last_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_last_year_day_temp`
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as year_payment,
	count(created) as year_buy_times,
	sum(product_num) as year_buy_num,
	min(created) as year_first_time,
	max(created) as year_last_time
from dw_rfm.b_trade_day_changed_all
where created is not NULL
    and (created >= date_add(add_months('${stat_date}',-12),1) and created <= '${stat_date}')
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
-- 今日新增订单中最近两年的购买记录指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last_tyear_day_temp`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`tyear_payment` double,
	`tyear_buy_times` int,
	`tyear_buy_num` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_last_tyear_day_temp`
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as tyear_payment,
	count(created) as tyear_buy_times,
	sum(product_num) as tyear_buy_num
from dw_rfm.b_trade_day_changed_all
where created is not NULL
	and (created >= date_add(add_months('${stat_date}',-24),1) and created < date_add(add_months('${stat_date}',-12),1))
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
-- 今日新增的订单中两年前的购买指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_before_tyear_day_temp`(
	`tenant` string,
	`plat_code` string,
	`uni_shop_id` string,
	`uni_id` string,
	`btyear_payment` double,
	`btyear_buy_times` int,
	`btyear_buy_num` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_before_tyear_day_temp`
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as btyear_payment,
	count(created) as btyear_buy_times,
	sum(product_num) as btyear_buy_num
from dw_rfm.b_trade_day_changed_all
where created is not NULL and created < date_add(add_months('${stat_date}',-24),1)
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;

-- 计算今日新增订单数据的RFM，结果需与昨日的RFM数据进行关联，对相同的用户进行重设指标
-- 今日所有订单计算出来用户的RFM指标
drop table if exists dw_rfm.b_day_changed_rfm_temp;
create table dw_rfm.b_day_changed_rfm_temp as
select r.tenant,r.plat_code,r.uni_shop_id,r.uni_id,
	   r.earliest_time,
	   r.first_buy_time,
	   case when r.first_payment is null then 0 else r.first_payment end as first_payment,
	   r.second_buy_time,
	   case when r.year_payment is null then 0 else r.year_payment end as year_payment,
	   case when r.year_buy_times is null then 0 else r.year_buy_times end as year_buy_times,
	   case when r.year_buy_num is null then 0 else r.year_buy_num end as year_buy_num,
	   r.year_first_time,
	   r.year_last_time,
	   case when r.tyear_payment is null then 0 else r.tyear_payment end as tyear_payment,
	   case when r.tyear_buy_times is null then 0 else r.tyear_buy_times end as tyear_buy_times,
	   case when r.tyear_buy_num is null then 0 else r.tyear_buy_num end as tyear_buy_num,
	   case when r.btyear_payment is null then 0 else r.btyear_payment end as btyear_payment,
	   case when r.btyear_buy_times is null then 0 else r.btyear_buy_times end as btyear_buy_times,
	   case when r.btyear_buy_num is null then 0 else r.btyear_buy_num end as btyear_buy_num
from (
	select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
		a.first_buy_time as earliest_time,
		a.first_buy_time,a.first_payment,a.second_buy_time,
		b.year_payment,b.year_buy_times,b.year_buy_num,b.year_first_time,b.year_last_time,
		c.tyear_payment,c.tyear_buy_times,c.tyear_buy_num,
		d.btyear_payment,d.btyear_buy_times,d.btyear_buy_num
	from dw_rfm.`b_first_buy_day_temp` a
	left join dw_rfm.`b_last_year_day_temp` b
	on a.tenant=b.tenant and a.plat_code = b.plat_code and a.uni_shop_id = b.uni_shop_id and a.uni_id = b.uni_id
	left join dw_rfm.`b_last_tyear_day_temp` c
	on a.tenant=c.tenant and a.plat_code = c.plat_code and a.uni_shop_id = c.uni_shop_id and a.uni_id = c.uni_id
	left join dw_rfm.`b_before_tyear_day_temp` d
	on a.tenant=d.tenant and a.plat_code = d.plat_code and a.uni_shop_id = d.uni_shop_id and a.uni_id = d.uni_id
) r;

-- 从历史订单数据中，获取两年前的数据，近两年的数据，近一年的边界日期订单

--从历史数据计算近两年的第一天的数据，需要从昨天的用户两年前指标加上，近两年的指标减去
drop table if exists dw_rfm.b_last_tyear_first_day_trade;
create table dw_rfm.b_last_tyear_first_day_trade as
select c.plat_code,c.uni_id,c.uni_shop_id,c.shop_id,c.receive_payment,c.product_num,c.created,b.tenant 
from
(
	select
		a.plat_code,
		a.uni_id,
		a.uni_shop_id,
		a.shop_id,
		case when a.refund_fee = 'NULL' or a.refund_fee is NULL then a.payment else (a.payment - a.refund_fee) end as receive_payment,
		case when a.product_num is null then 1 else a.product_num end as product_num,
		case when lower(a.trade_type) = 'step' then a.pay_time else a.created end as created
	from dw_base.b_std_trade a
	where
	  a.part = substr(add_months('${stat_date}',-24),1,7)
	  and (a.created is not null and substr(a.created,1,10) = add_months('${stat_date}',-24))
	  and a.order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
) c
left join dw_base.b_std_tenant_shop b
on c.plat_code = b.plat_code and c.shop_id = b.shop_id
where b.tenant is not null
group by c.plat_code,c.uni_id,c.uni_shop_id,c.shop_id,c.receive_payment,c.product_num,c.created,b.tenant;

--从历史数据中计算近一年的第一天订单数据，需要给昨日RFM近两年的指标加上，近一年的指标减去
drop table if exists dw_rfm.b_last_year_first_day_trade;
create table dw_rfm.b_last_year_first_day_trade as
select c.plat_code,c.uni_id,c.uni_shop_id,c.shop_id,c.receive_payment,c.product_num,c.created,b.tenant 
from(
	select
		a.plat_code,
		a.uni_id,
		a.uni_shop_id,
		a.shop_id,
		case when a.refund_fee = 'NULL' or a.refund_fee is NULL then a.payment else (a.payment - a.refund_fee) end as receive_payment,
		case when a.product_num is null then 1 else a.product_num end as product_num,
		case when lower(a.trade_type) = 'step' then a.pay_time else a.created end as created
	from dw_base.b_std_trade a
	where
	  a.part = substr(add_months('${stat_date}',-12),1,7)
	  and (a.created is not null and substr(a.created,1,10) = add_months('${stat_date}',-12))
	  and a.order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
) c
left join dw_base.b_std_tenant_shop b
on c.plat_code = b.plat_code and c.shop_id = b.shop_id
where b.tenant is not null
group by c.plat_code,c.uni_id,c.uni_shop_id,c.shop_id,c.receive_payment,c.product_num,c.created,b.tenant;


-- 计算两年前的第一天历史订单数据的统计指标
drop table if exists dw_rfm.b_last_tyear_first_day_statics_temp;
create table dw_rfm.b_last_tyear_first_day_statics_temp as
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as tyear_payment,
	count(created) as tyear_buy_times,
	sum(product_num) as tyear_buy_num
from dw_rfm.b_last_tyear_first_day_trade
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
--计算近一年第一天的历史订单数据的统计指标
drop table if exists dw_rfm.b_last_year_first_day_statics_temp;
create table dw_rfm.b_last_year_first_day_statics_temp as
select
	tenant,
    plat_code,
	uni_shop_id,
    uni_id,
    sum(receive_payment) as year_payment,
	count(created) as year_buy_times,
	sum(product_num) as year_buy_num,
	min(created) as first_buy_time,
	max(created) as last_buy_time
from dw_rfm.b_last_year_first_day_trade
group by
	tenant,
    plat_code,
	uni_shop_id,
    uni_id;
	
-- 在昨天的RFM数据的基础上 计算
-- 两年前的数据+近两年第一天的数据
-- 近两年的数据+近一年第一天的数据-近两年第一天的数据
-- 近一年的数据-近一年第一天的数据
insert overwrite table dw_rfm.b_qqd_shop_rfm partition(part='${stat_date}')
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,
   t.earliest_time,
   t.first_buy_time,
   case when t.first_payment is null then 0 else t.first_payment end as first_payment,
   t.second_buy_time,
   case when t.year_payment is null then 0 else t.year_payment end as year_payment,
   case when t.year_buy_times is null then 0 else t.year_buy_times end as year_buy_times,
   case when t.year_buy_num is null then 0 else t.year_buy_num end as year_buy_num,
   t.year_first_time,
   t.year_last_time,
   case when t.tyear_payment is null then 0 else t.tyear_payment end as tyear_payment,
   case when t.tyear_buy_times is null then 0 else t.tyear_buy_times end as tyear_buy_times,
   case when t.tyear_buy_num is null then 0 else t.tyear_buy_num end as tyear_buy_num,
   case when t.btyear_payment is null then 0 else t.btyear_payment end as btyear_payment,
   case when t.btyear_buy_times is null then 0 else t.btyear_buy_times end as btyear_buy_times,
   case when t.btyear_buy_num is null then 0 else t.btyear_buy_num end as btyear_buy_num,
   '${stat_date}' as stat_date
from 
(
	select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
		a.earliest_time,
		a.first_buy_time,
		a.first_payment,
		a.second_buy_time,
		
		(a.year_payment- if(c.year_payment is null,0,c.year_payment)) as year_payment,
		(a.year_buy_times- if(c.year_buy_times is null,0,c.year_buy_times)) as year_buy_times,
		(a.year_buy_num- if(c.year_buy_num is null,0,c.year_buy_num)) as year_buy_num,
		
		case when c.year_first_time is null then a.year_first_time else c.year_first_time end as year_first_time,
		case when a.year_last_time is null or a.year_last_time < c.last_buy_time then c.last_buy_time else a.year_last_time end as year_last_time,
		
		(a.tyear_payment- if(b.tyear_payment is null,0,b.tyear_payment) + if(c.year_payment is null,0,c.year_payment)) as tyear_payment,
		(a.tyear_buy_times- if(b.tyear_buy_times is null,0,b.tyear_buy_times) + if(c.year_buy_times is null,0,c.year_buy_times)) as tyear_buy_times,
		(a.tyear_buy_num- if(b.tyear_buy_num is null,0,b.tyear_buy_num) + if(c.year_buy_num is null,0,c.year_buy_num)) as tyear_buy_num,
		
		(a.btyear_payment+ if(b.tyear_payment is null,0,b.tyear_payment)) as btyear_payment,
		(a.btyear_buy_times+ if(b.tyear_buy_times is null,0,b.tyear_buy_times)) as btyear_buy_times,
		(a.btyear_buy_num+ if(b.tyear_buy_num is null,0,b.tyear_buy_num)) as btyear_buy_num
	from(
		select * from dw_rfm.b_qqd_shop_rfm where part= date_sub('${stat_date}',1)
	) a
	left join dw_rfm.b_last_tyear_first_day_statics_temp b
	on a.tenant=b.tenant and a.plat_code = b.plat_code and a.uni_shop_id = b.uni_shop_id and a.uni_id = b.uni_id
	left join dw_rfm.b_last_year_first_day_statics_temp c
	on a.tenant=c.tenant and a.plat_code = c.plat_code and a.uni_shop_id = c.uni_shop_id and a.uni_id = c.uni_id
) t;

-- 今日增量数据中与昨日RFM数据对相同用户进行合并，做为今日发生变化的RFM数据
drop table if exists dw_rfm.b_today_rfm_base;
create table dw_rfm.b_today_rfm_base as
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
	case when b.earliest_time is null or b.earliest_time > a.earliest_time then a.earliest_time else b.earliest_time end as earliest_time,
	case when b.first_buy_time is null or b.first_buy_time > a.first_buy_time then a.first_buy_time else b.first_buy_time end as first_buy_time,
	case when b.first_buy_time is null or b.first_buy_time > a.first_buy_time then a.first_payment else b.first_payment end as first_payment,
	case when a.second_buy_time is null then b.second_buy_time when b.second_buy_time is null then a.second_buy_time
		when b.second_buy_time > a.second_buy_time then a.second_buy_time 
		else b.second_buy_time end as second_buy_time,
	
	(a.year_payment + if(b.year_payment is null,0,b.year_payment)) as year_payment,
	(a.year_buy_times+ if(b.year_buy_times is null,0,b.year_buy_times)) as year_buy_times,
	(a.year_buy_num+ if(b.year_buy_num is null,0,b.year_buy_num)) as year_buy_num,
	
	case when a.year_first_time is null then b.year_first_time when b.year_first_time is null then a.year_first_time
		when b.year_first_time > a.year_first_time then a.year_first_time 
		else b.year_first_time end as year_first_time,
	
	case when a.year_last_time is null then b.year_last_time when b.year_last_time is null then a.year_last_time
		when b.year_last_time < a.year_last_time then a.year_last_time 
		else b.year_last_time end as year_last_time,
	
	(a.tyear_payment+ if(b.tyear_payment is null,0,b.tyear_payment)) as tyear_payment,
	(a.tyear_buy_times+ if(b.tyear_buy_times is null,0,b.tyear_buy_times)) as tyear_buy_times,
	(a.tyear_buy_num+ if(b.tyear_buy_num is null,0,b.tyear_buy_num)) as tyear_buy_num,
	
	(a.btyear_payment+ if(b.btyear_payment is null,0,b.btyear_payment)) as btyear_payment,
	(a.btyear_buy_times+ if(b.btyear_buy_times is null,0,b.btyear_buy_times)) as btyear_buy_times,
	(a.btyear_buy_num+ if(b.btyear_buy_num is null,0,b.btyear_buy_num)) as btyear_buy_num
from dw_rfm.b_day_changed_rfm_temp a
join 
(
	select * from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
) b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id;

-- 对今日完全新增的统计结果直接合并
insert into dw_rfm.b_qqd_shop_rfm partition(part='${stat_date}')
select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
   a.earliest_time,
   a.first_buy_time,
   a.first_payment,
   a.second_buy_time,
   a.year_payment,
   a.year_buy_times,
   a.year_buy_num,
   a.year_first_time,
   a.year_last_time,
   a.tyear_payment,
   a.tyear_buy_times,
   a.tyear_buy_num,
   a.btyear_payment,
   a.btyear_buy_times,
   a.btyear_buy_num,
   '${stat_date}' as stat_date
from dw_rfm.b_day_changed_rfm_temp a
left join 
( select * from dw_rfm.b_qqd_shop_rfm where part='${stat_date}' ) b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id
where b.tenant is null;

-- 从RFM数据中去除所有有变化的用户和变化的计算结果合并，与客户表关联作为今日最终的店铺级RFM数据
insert overwrite table dw_rfm.b_qqd_shop_rfm partition(part='${stat_date}')
select c.tenant,c.plat_code,c.uni_shop_id,c.uni_id,
   case when r.earliest_time is null or r.earliest_time > c.modified then c.modified else r.earliest_time end as earliest_time,
   r.first_buy_time,
   case when r.first_payment is null then 0 else r.first_payment end as first_payment,
   r.second_buy_time,
   case when r.year_payment is null then 0 else r.year_payment end as year_payment,
   case when r.year_buy_times is null then 0 else r.year_buy_times end as year_buy_times,
   case when r.year_buy_num is null then 0 else r.year_buy_num end as year_buy_num,
   r.year_first_time,
   r.year_last_time,
   case when r.tyear_payment is null then 0 else r.tyear_payment end as tyear_payment,
   case when r.tyear_buy_times is null then 0 else r.tyear_buy_times end as tyear_buy_times,
   case when r.tyear_buy_num is null then 0 else r.tyear_buy_num end as tyear_buy_num,
   case when r.btyear_payment is null then 0 else r.btyear_payment end as btyear_payment,
   case when r.btyear_buy_times is null then 0 else r.btyear_buy_times end as btyear_buy_times,
   case when r.btyear_buy_num is null then 0 else r.btyear_buy_num end as btyear_buy_num,
   '${stat_date}' as stat_date
from (
	select c1.tenant,c2.plat_code,c2.uni_shop_id,c1.uni_id,c1.modified from dw_base.b_std_customer c1
	left join dw_base.b_std_shop_customer_rel c2
	on c1.uni_id = c2.uni_id
	where c2.plat_code is not null
) c
left outer join 
(
	select tmp.* from dw_rfm.b_today_rfm_base tmp
	union all
	select t.* from (
		select re.* from
		(
			select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,
			   a.earliest_time,
			   a.first_buy_time,
			   a.first_payment,
			   a.second_buy_time,
			   a.year_payment,
			   a.year_buy_times,
			   a.year_buy_num,
			   a.year_first_time,
			   a.year_last_time,
			   a.tyear_payment,
			   a.tyear_buy_times,
			   a.tyear_buy_num,
			   a.btyear_payment,
			   a.btyear_buy_times,
			   a.btyear_buy_num
			from dw_rfm.b_qqd_shop_rfm a where a.part='${stat_date}'
		) re
		left outer join 
		dw_rfm.b_today_rfm_base b
		on re.tenant=b.tenant and re.plat_code=b.plat_code and re.uni_shop_id=b.uni_shop_id and re.uni_id=b.uni_id
		where b.tenant is null
	) t
) r
on c.tenant=r.tenant and c.plat_code=r.plat_code and c.uni_shop_id=r.uni_shop_id and c.uni_id=r.uni_id;


-- 去重获取平台级的最早购买时间
drop table if exists dw_rfm.b_plat_first_buy_day_temp;
create table dw_rfm.b_plat_first_buy_day_temp as
select r.tenant,r.plat_code,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  when concat_ws('',collect_set(r.second_buy_time)) < concat_ws('',collect_set(r.second_buy_time_mid)) then concat_ws('',collect_set(r.second_buy_time))
	  else NULL end as second_buy_time
from(
	select t.tenant,t.plat_code,t.uni_id,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,plat_code,uni_id order by first_buy_time asc) as rank 
		from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
	) t where t.rank <= 2
) r
group by r.tenant,r.plat_code,r.uni_id;

 
--全渠道平台级用户RFM指标
CREATE TABLE IF NOT EXISTS dw_rfm.`b_qqd_plat_rfm`(
	`tenant` string,
	`plat_code` string,
	`uni_id` string,
    `earliest_time` string,
	`first_buy_time` string,
	`first_payment` double,
    `second_buy_time` string, 
    `year_payment` double,
	`year_buy_times` int,
    `year_buy_num` int, 
    `year_first_time` string,
	`year_last_time` string,
	`tyear_payment` double,
    `tyear_buy_times` int, 
    `tyear_buy_num` int,
	`btyear_payment` double,
	`btyear_buy_times` int,
    `btyear_buy_num` int,
	`stat_date` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_qqd_plat_rfm` partition(part='${stat_date}')
select r.tenant,r.plat_code,r.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	'${stat_date}' as stat_date
from (
	select a.tenant,a.plat_code,a.uni_id,
		min(a.earliest_time) earliest_time,
		sum(a.year_payment) year_payment,
		sum(a.year_buy_times) year_buy_times,
		sum(a.year_buy_num) year_buy_num,
		min(a.year_first_time) year_first_time,
		max(a.year_last_time) year_last_time,
		sum(a.tyear_payment) tyear_payment,
		sum(a.tyear_buy_times) tyear_buy_times,
		sum(a.tyear_buy_num) tyear_buy_num,
		sum(a.btyear_payment) btyear_payment,
		sum(a.btyear_buy_times) btyear_buy_times,
		sum(a.btyear_buy_num) btyear_buy_num
	from dw_rfm.`b_qqd_shop_rfm` a 
	where a.part='${stat_date}'
	group by a.tenant,a.plat_code,a.uni_id
) r
left outer join 
	dw_rfm.b_plat_first_buy_day_temp t
on r.tenant = t.tenant and r.plat_code=t.plat_code and r.uni_id=t.uni_id;

-- 去重排序，获取租户级的首次购买时间
drop table if exists dw_rfm.b_tenant_first_buy_day_temp;
create table dw_rfm.b_tenant_first_buy_day_temp as
select r.tenant,r.uni_id,
      concat_ws('',collect_set(r.first_buy_time)) first_buy_time,
	  concat_ws('',collect_set(r.first_payment)) first_payment,
	  case when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) =0 then NULL 
	  when length(concat_ws('',collect_set(r.second_buy_time))) =0 and length(concat_ws('',collect_set(r.second_buy_time_mid))) >0 then concat_ws('',collect_set(r.second_buy_time_mid))
	  when concat_ws('',collect_set(r.second_buy_time)) < concat_ws('',collect_set(r.second_buy_time_mid)) then concat_ws('',collect_set(r.second_buy_time))
	  else NULL end as second_buy_time
from(
	select t.tenant,t.uni_id,
	   case t.rank when 1 then t.first_buy_time else '' end as first_buy_time,
	   case t.rank when 1 then t.first_payment else '' end as first_payment,
	   case t.rank when 1 then t.second_buy_time else '' end as second_buy_time_mid,
	   case t.rank when 2 then t.first_buy_time else '' end as second_buy_time
	from(
		select *,row_number() over (partition by tenant,uni_id order by first_buy_time asc) as rank 
		from dw_rfm.b_plat_first_buy_day_temp
	) t where t.rank <= 2
) r
group by r.tenant,r.uni_id;

-- 全渠道租户级客户RMF指标
CREATE EXTERNAL TABLE IF NOT EXISTS dw_rfm.`b_qqd_tenant_rfm`(
	`tenant` string,
	`uni_id` string,
    `earliest_time` string,
	`first_buy_time` string,
	`first_payment` double,
    `second_buy_time` string, 
    `year_payment` double,
	`year_buy_times` int,
    `year_buy_num` int, 
    `year_first_time` string,
	`year_last_time` string,
	`tyear_payment` double,
    `tyear_buy_times` int, 
    `tyear_buy_num` int,
	`btyear_payment` double,
	`btyear_buy_times` int,
    `btyear_buy_num` int,
	`stat_date` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_qqd_tenant_rfm` partition(part='${stat_date}')
select r.tenant,r.uni_id,
	r.earliest_time,
	t.first_buy_time,t.first_payment,t.second_buy_time,
	r.year_payment,r.year_buy_times,r.year_buy_num,r.year_first_time,r.year_last_time,
	r.tyear_payment,r.tyear_buy_times,r.tyear_buy_num,
	r.btyear_payment,r.btyear_buy_times,r.btyear_buy_num,
	'${stat_date}' as stat_date
from (
	select a.tenant,a.uni_id,
		min(a.earliest_time) earliest_time,
		sum(a.year_payment) year_payment,
		sum(a.year_buy_times) year_buy_times,
		sum(a.year_buy_num) year_buy_num,
		min(a.year_first_time) year_first_time,
		max(a.year_last_time) year_last_time,
		sum(a.tyear_payment) tyear_payment,
		sum(a.tyear_buy_times) tyear_buy_times,
		sum(a.tyear_buy_num) tyear_buy_num,
		sum(a.btyear_payment) btyear_payment,
		sum(a.btyear_buy_times) btyear_buy_times,
		sum(a.btyear_buy_num) btyear_buy_num
	from dw_rfm.`b_qqd_plat_rfm` a 
	where a.part='${stat_date}'
	group by a.tenant,a.uni_id
) r
left outer join 
	dw_rfm.b_tenant_first_buy_day_temp t
	on r.tenant = t.tenant and r.uni_id=t.uni_id;
   
   




