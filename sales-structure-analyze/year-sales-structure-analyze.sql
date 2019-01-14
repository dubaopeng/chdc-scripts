SET mapred.job.name='year-sales-structure-analyze-年销售结构分析';
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


-- 订单表：dw_base.b_std_trade
-- 租户和店铺的关系：dw_base.b_std_tenant_shop
-- 全渠道店铺客户RFM: dw_rfm.b_qqd_shop_rfm,dw_rfm.b_qqd_plat_rfm,dw_rfm.b_qqd_tenant_rfm

-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 所有客户在不同平台店铺中按年份的购买记录汇总
drop table if exists dw_rfm.b_all_year_trade_statics_result;
create table if not exists dw_rfm.b_all_year_trade_statics_result(
	tenant string,
	plat_code string,
	uni_shop_id string,
	shop_id string,
	uni_id string,
	byear string,  -- 所在年份
	payments double, --购买金额
	buy_times bigint --购买次数
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_all_year_trade_statics_result
select b.tenant,a.plat_code,a.uni_shop_id,a.shop_id,a.uni_id,
	   a.byear,a.payments,a.buy_times
from
(
	select t.plat_code,t.uni_shop_id,t.shop_id,t.uni_id,
		   substr(t.created,1,4) byear,
		   sum(t.payment) payments,
		   count(t.created) buy_times
	from(
		select plat_code,uni_shop_id,shop_id,uni_id,
			case when refund_fee = 'NULL' or refund_fee is NULL then payment else (payment - refund_fee) end as payment,
			case when lower(trade_type) = 'step' and pay_time is not null then pay_time else created end as created
		from dw_base.b_std_trade
		where
		  part < '${stat_date}'
		  and (created is not NULL and substr(created,1,10) <= '${stat_date}'
		  and uni_id is not NULL 
		  and payment is not NULL
		  and order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
	) t
	group by t.plat_code,t.uni_shop_id,t.shop_id,t.uni_id,substr(t.created,1,4)
) a
left join dw_base.b_std_tenant_shop b
on a.plat_code=b.plat_code and a.shop_id=b.shop_id
where b.tenant is not null;

-- 按年份计算店铺总的KPI
drop table if exists dw_rfm.b_sale_every_year_all_temp;
create table dw_rfm.b_sale_every_year_all_temp as
select 
	t.tenant,
	t.plat_code,
	t.uni_shop_id,
	t.shop_id,
	t.byear, --月份
	count(t.uni_id) cus_num, -- 客户数量
	sum(t.payments) payments, -- 成交总金额
	sum(t.payments)/sum(t.buy_times) as avg_price,--平均客单价
	sum(t.buy_times)/count(t.uni_id) as avg_times -- 平均购买次数
from dw_rfm.b_all_year_trade_statics_result t
group by t.plat_code,t.uni_shop_id,t.shop_id,t.byear;


--使用每月客户数据，和RFM宽表进行关联，识别新客和回头客

--新客数据计算，新客人数、新客占比、成交金额占比等
drop table if exists dw_rfm.b_sale_new_customer_year_temp;
create table dw_rfm.b_sale_new_customer_year_temp as 
select r1.tenant,r1.plat_code,r1.uni_shop_id,r1.byear,
	   r1.new_cus_num,            --新客人数
	   (r1.new_cus_num/r2.cus_num) as new_rate, -- 新客占比
	   r1.new_payments,  --新客成交金额
	   (r1.new_payments/r2.payments) new_payments_rate, -- 新客成交金额占比
	   r1.new_avg_price,  --新客客单价
	   r1.new_avg_times,  --新客平均购买次数
	   r1.new_repeat_num, --新客复购人数
	   r1.new_repeat_rate, --新客复购率
	   r1.new_one_price,  --新客单次购买客单价
	   r1.new_muti_price, --新客多次购买客单价
	   r1.new_muti_avg_times --新客多次购买平均购买次数
from 
(
	select t.tenant,t.plat_code,t.uni_shop_id,t.byear,
		count(t.uni_id) as new_cus_num,
		sum(t.payments) as new_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as new_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as new_avg_times,--平均次数
		sum(t.new_phurase) as new_repeat_num, --新客复购人数
		(sum(t.new_phurase)/count(t.uni_id)) as new_repeat_rate, --新客复购率
		(sum(t.new_one_payments)/sum(t.new_one_buy_cusnum)) as new_one_price,--新客单次购买客单价
		(sum(t.new_muti_payments)/sum(t.new_phurase)) as new_muti_price, --新客多次购买客单价
		(sum(t.new_muti_times)/sum(t.new_phurase)) as new_muti_avg_times --新客多次购买平均购买次数
	from
	(
		select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,a.byear,a.payments,a.buy_times,
			   case when a.buy_times >= 2 then 1 else 0 end as new_phurase,  --新客复购人数
			   case when a.buy_times = 1 then 1 else 0 end as new_one_buy_cusnum, --新客单次购买人数
			   case when a.buy_times = 1 then a.payments else 0 end as new_one_payments, --新客单次购买金额
			   case when a.buy_times >= 2 then a.payments else 0 end as new_muti_payments,--新客多次购买金额
			   case when a.buy_times >= 2 then a.buy_times else 0 end as new_muti_times -- 新客多次购买次数
		from dw_rfm.b_all_year_trade_statics_result a
		left join (
			select tenant,plat_code,uni_shop_id,uni_id,first_buy_time
			from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear = substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.plat_code,t.uni_shop_id,t.byear
) r1
left join 
   dw_rfm.b_sale_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.plat_code = r2.plat_code and r1.uni_shop_id=r2.uni_shop_id and r1.byear=r2.byear;


--回头客数据计算，回头客人数、回头客占比、成交金额占比等
drop table if exists dw_rfm.b_sale_old_customer_year_temp;
create table dw_rfm.b_sale_old_customer_year_temp as 
select r1.tenant,r1.plat_code,r1.uni_shop_id,r1.byear,
	   r1.old_cus_num,            --回头客人数
	   (r1.old_cus_num/r2.cus_num) as old_rate, -- 回头客占比
	   r1.old_payments,  --回头客成交金额
	   (r1.old_payments/r2.payments) old_payments_rate, -- 回头客成交金额占比
	   r1.old_avg_price,  --回头客客单价
	   r1.old_avg_times,  --回头客平均购买次数
	   r1.old_repeat_num, --回头客复购人数
	   r1.old_repeat_rate, --回头客复购率
	   r1.old_one_price,  --回头客单次购买客单价
	   r1.old_muti_price, --回头客多次购买客单价
	   r1.old_muti_avg_times --回头客多次购买平均购买次数
from 
(
	select t.tenant,t.plat_code,t.uni_shop_id,t.byear,
		count(t.uni_id) as old_cus_num,
		sum(t.payments) as old_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as old_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as old_avg_times, --平均次数
		sum(t.old_phurase) as old_repeat_num, --回头客复购人数
		(sum(t.old_phurase)/count(t.uni_id)) as old_repeat_rate,--回头客复购率
		(sum(t.old_one_payments)/sum(t.old_one_buy_cusnum)) as old_one_price,--回头客单次购买客单价
		(sum(t.old_muti_payments)/sum(t.old_phurase)) as old_muti_price, --回头客多次购买客单价
		(sum(t.old_muti_times)/sum(t.old_phurase)) as old_muti_avg_times --回头客多次购买平均购买次数
	from
	(
		select a.tenant,a.plat_code,a.uni_shop_id,a.uni_id,a.byear,a.payments,a.buy_times,
			   case when a.buy_times >= 2 then 1 else 0 end as old_phurase,
			   case when a.buy_times = 1 then 1 else 0 end as old_one_buy_cusnum, --回头客单次购买人数
			   case when a.buy_times = 1 then a.payments else 0 end as old_one_payments, --回头客单次购买金额
			   case when a.buy_times >= 2 then a.payments else 0 end as old_muti_payments,--回头客多次购买金额
			   case when a.buy_times >= 2 then a.buy_times else 0 end as old_muti_times -- 回头客多次购买次数
		from dw_rfm.b_all_year_trade_statics_result a
		left join (
			select tenant,plat_code,uni_shop_id,uni_id,first_buy_time from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear > substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.plat_code,t.uni_shop_id,t.byear
) r1
left join 
   dw_rfm.b_sale_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.plat_code = r2.plat_code and r1.uni_shop_id=r2.uni_shop_id and r1.byear=r2.byear;


-- 年份对应的统计结果数据
create table if not exists dw_rfm.cix_online_sales_structs_year(
	tenant string,
	plat_code string,
	uni_shop_id string,
	date_col string, --时间字段(年、月、日)
	
	cus_num bigint, --客户总数
	payments double, --付款金额
	avg_price double, --平均客单价
	avg_times double, --平均购买次数
	
	new_cus_num bigint, --新客人数
	new_rate double, -- 新客占比
	new_payments double,  --新客成交金额
	new_payments_rate double, -- 新客成交金额占比
	new_avg_price double,  --新客客单价
	new_avg_times double,  --新客平均购买次数
	new_repeat_num bigint, --新客复购人数
	new_repeat_rate double, --新客复购率
	
	new_one_price double,  --新客单次购买客单价
	new_muti_price double, --新客多次购买客单价
	new_muti_avg_times double,  --新客购买多次平均购买次数
	
	old_cus_num bigint, --回头客人数
	old_rate double, -- 回头客占比
	old_payments double,  --回头客成交金额
	old_payments_rate double, -- 回头客成交金额占比
	old_avg_price double,  --回头客客单价
	old_avg_times double,  --回头客平均购买次数
	old_repeat_num bigint, --回头客复购人数
	old_repeat_rate double, --回头客复购率
	
	old_one_price double,  --回头客单次购买客单价
	old_muti_price double, --回头客多次购买客单价
	old_muti_avg_times double, --回头客购买多次平均购买次数
	
	type int, -- 1:租户级 2:平台级 3:店铺级
	stat_date string,
	modified string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_sales_structs_year
select t.tenant,t.plat_code,t.uni_shop_id,t.byear as date_col,
	   t.cus_num,t.payments,t.avg_price,t.avg_times,
	   
	   if(a.new_cus_num is null,0,a.new_cus_num) as new_cus_num,
	   if(a.new_rate is null,0,a.new_rate) as new_rate,
	   if(a.new_payments is null,0,a.new_payments) as new_payments,
	   if(a.new_payments_rate is null,0,a.new_payments_rate) as new_payments_rate,
	   if(a.new_avg_price is null,0,a.new_avg_price) as new_avg_price,
	   if(a.new_avg_times is null,0,a.new_avg_times) as new_avg_times,
	   if(a.new_repeat_num is null,0,a.new_repeat_num) as new_repeat_num,
	   if(a.new_repeat_rate is null,0,a.new_repeat_rate) as new_repeat_rate,
	   
	   if(a.new_one_price if null,0,a.new_one_price) as new_one_price,  --新客单次购买客单价
	   if(a.new_muti_price if null,0,a.new_muti_price) as new_muti_price, --新客多次购买客单价
	   if(a.new_muti_avg_times if null,0,a.new_muti_avg_times) as new_muti_avg_times,--新客多次购买平均购买次数
	   
	   if(a.old_cus_num is null,0,a.old_cus_num) as old_cus_num,
	   if(a.old_rate is null,0,a.old_rate) as old_rate,
	   if(a.old_payments is null,0,a.old_payments) as old_payments,
	   if(a.old_payments_rate is null,0,a.old_payments_rate) as old_payments_rate,
	   if(a.old_avg_price is null,0,a.old_avg_price) as old_avg_price,
	   if(a.old_avg_times is null,0,a.old_avg_times) as old_avg_times,
	   if(a.old_repeat_num is null,0,a.old_repeat_num) as old_repeat_num,
	   if(a.old_repeat_rate is null,0,a.old_repeat_rate) as old_repeat_rate,
	   
	   if(a.old_one_price is null,0,a.old_one_price) as old_one_price,
	   if(a.old_muti_price is null,0,a.old_muti_price) as old_muti_price,
	   if(a.old_muti_avg_times is null,0,a.old_muti_avg_times) as old_muti_avg_times,
	   
	   3 as type,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from 
dw_rfm.b_sale_every_year_all_temp t
left join dw_rfm.b_sale_new_customer_year_temp a
on t.tenant=a.tenant and t.plat_code=a.plat_code and t.uni_shop_id=a.uni_shop_id and t.byear=a.byear
left join dw_rfm.b_sale_old_customer_year_temp b
on t.tenant=b.tenant and t.plat_code=b.plat_code and t.uni_shop_id=b.uni_shop_id and t.byear=b.byear;

-- 删除临时表
drop table if exists dw_rfm.b_sale_every_year_all_temp;
drop table if exists dw_rfm.b_sale_new_customer_year_temp;
drop table if exists dw_rfm.b_sale_old_customer_year_temp;


-- 平台级客户各年份购买数据
drop table if exists dw_rfm.b_sales_plat_year_trade_temp;
create table if not exists dw_rfm.b_sales_plat_year_trade_temp(
	tenant string,
	plat_code string,
	uni_id string,
	byear string,
	payments double,
	buy_times bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_sales_plat_year_trade_temp
select a.tenant,a.plat_code,a.uni_id,a.byear,
	   sum(a.payments) as payments,
	   sum(a.buy_times) as buy_times
from dw_rfm.b_all_year_trade_statics_result a
group by a.tenant,a.plat_code,a.uni_id,a.byear;

--计算平台级年份KPI数据统计
drop table if exists dw_rfm.b_sale_plat_every_year_all_temp;
create table dw_rfm.b_sale_plat_every_year_all_temp as
select t.tenant,t.plat_code,t.byear
	count(t.uni_id) as cus_num, -- 客户总数
	sum(t.payments) as payments, -- 平台总金额
	sum(t.payments)/sum(t.buy_times) as avg_price,--平均客单价
	sum(t.buy_times)/count(t.uni_id) as avg_times --平均次数
from dw_rfm.b_sales_plat_year_trade_temp t
group by t.tenant,t.plat_code,t.byear;

-- 平台新客数据
drop table if exists dw_rfm.b_sale_plat_new_customer_year_temp;
create table dw_rfm.b_sale_plat_new_customer_year_temp as 
select r1.tenant,r1.plat_code,r1.byear,
	   r1.new_cus_num,            --新客人数
	   (r1.new_cus_num/r2.cus_num) as new_rate, -- 新客占比
	   r1.new_payments,  --新客成交金额
	   (r1.new_payments/r2.payments) new_payments_rate, -- 新客成交金额占比
	   r1.new_avg_price,  --新客客单价
	   r1.new_avg_times,  --新客平均购买次数
	   r1.new_repeat_num, --新客复购人数
	   r1.new_repeat_rate, --新客复购率
	   r1.new_one_price,
	   r1.new_muti_price,
	   r1.new_muti_avg_times
from 
(
	select t.tenant,t.plat_code,t.byear,
		count(t.uni_id) as new_cus_num,
		sum(t.payments) as new_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as new_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as new_avg_times, --平均次数
		sum(t.new_phurase) as new_repeat_num, --新客复购人数
		(sum(t.new_phurase)/count(t.uni_id)) as new_repeat_rate, --新客复购率
		(sum(t.new_one_payments)/sum(t.new_one_buy_cusnum)) as new_one_price,--新客单次购买客单价
		(sum(t.new_muti_payments)/sum(t.new_phurase)) as new_muti_price, --新客多次购买客单价
		(sum(t.new_muti_times)/sum(t.new_phurase)) as new_muti_avg_times --新客多次购买平均购买次数
	from
	(
		select a.tenant,a.plat_code,a.uni_id,a.byear,a.payments,a.buy_times,
			   case when a.buy_times >= 2 then 1 else 0 end as new_phurase,
			   case when a.buy_times = 1 then 1 else 0 end as new_one_buy_cusnum, --新客单次购买人数
			   case when a.buy_times = 1 then a.payments else 0 end as new_one_payments, --新客单次购买金额
			   case when a.buy_times >= 2 then a.payments else 0 end as new_muti_payments,--新客多次购买金额
			   case when a.buy_times >= 2 then a.buy_times else 0 end as new_muti_times -- 新客多次购买次数
		from dw_rfm.b_sales_plat_year_trade_temp a
		left join (
			select tenant,plat_code,uni_id,first_buy_time from dw_rfm.b_qqd_plat_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear = substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.plat_code,t.byear
) r1
left join 
   dw_rfm.b_sale_plat_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.plat_code = r2.plat_code and r1.byear=r2.byear;

--平台级回头客数据计算
drop table if exists dw_rfm.b_sale_plat_old_customer_year_temp;
create table dw_rfm.b_sale_plat_old_customer_year_temp as 
select r1.tenant,r1.plat_code,r1.byear,
	   r1.old_cus_num,            --回头客人数
	   (r1.old_cus_num/r2.cus_num) as old_rate, -- 回头客占比
	   r1.old_payments,  --回头客成交金额
	   (r1.old_payments/r2.payments) old_payments_rate, -- 回头客成交金额占比
	   r1.old_avg_price,  --回头客客单价
	   r1.old_avg_times,  --回头客平均购买次数
	   r1.old_repeat_num, --回头客复购人数
	   r1.old_repeat_rate, --回头客复购率
	   r1.old_one_price,
	   r1.old_muti_price,
	   r1.old_muti_avg_times
from 
(
	select t.tenant,t.plat_code,t.byear,
		count(t.uni_id) as old_cus_num,
		sum(t.payments) as old_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as old_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as old_avg_times, --平均次数
		sum(t.old_phurase) as old_repeat_num, --复购客复购人数
		(sum(t.old_phurase)/count(t.uni_id)) as old_repeat_rate, --复购客复购率
		(sum(t.old_one_payments)/sum(t.old_one_buy_cusnum)) as old_one_price,--回头客单次购买客单价
		(sum(t.old_muti_payments)/sum(t.old_phurase)) as old_muti_price, --回头客多次购买客单价
		(sum(t.old_muti_times)/sum(t.old_phurase)) as old_muti_avg_times --回头客多次购买平均购买次数
	from
	(
		select a.tenant,a.plat_code,a.uni_id,a.byear,a.payments,a.buy_times,
			  case when a.buy_times >= 2 then 1 else 0 end as old_phurase,
			  case when a.buy_times = 1 then 1 else 0 end as old_one_buy_cusnum, --回头客单次购买人数
			  case when a.buy_times = 1 then a.payments else 0 end as old_one_payments, --回头客单次购买金额
			  case when a.buy_times >= 2 then a.payments else 0 end as old_muti_payments,--回头客多次购买金额
			  case when a.buy_times >= 2 then a.buy_times else 0 end as old_muti_times -- 回头客多次购买次数
		from dw_rfm.b_sales_plat_year_trade_temp a
		left join (
			select tenant,plat_code,uni_id,first_buy_time
			from dw_rfm.b_qqd_plat_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear > substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.plat_code,t.byear
) r1
left join 
   dw_rfm.b_sale_plat_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.plat_code = r2.plat_code and r1.byear=r2.byear;

-- 统计完成后需要将中的数据同步给业务库
insert into table dw_rfm.cix_online_sales_structs_year
select t.tenant,t.plat_code,null as uni_shop_id,t.byear as date_col,
	   t.cus_num,t.payments,t.avg_price,t.avg_times,
	   
	   if(a.new_cus_num is null,0,a.new_cus_num) as new_cus_num,
	   if(a.new_rate is null,0,a.new_rate) as new_rate,
	   if(a.new_payments is null,0,a.new_payments) as new_payments,
	   if(a.new_payments_rate is null,0,a.new_payments_rate) as new_payments_rate,
	   if(a.new_avg_price is null,0,a.new_avg_price) as new_avg_price,
	   if(a.new_avg_times is null,0,a.new_avg_times) as new_avg_times,
	   if(a.new_repeat_num is null,0,a.new_repeat_num) as new_repeat_num,
	   if(a.new_repeat_rate is null,0,a.new_repeat_rate) as new_repeat_rate,
	   
	   if(a.new_one_price if null,0,a.new_one_price) as new_one_price,  --新客单次购买客单价
	   if(a.new_muti_price if null,0,a.new_muti_price) as new_muti_price, --新客多次购买客单价
	   if(a.new_muti_avg_times if null,0,a.new_muti_avg_times) as new_muti_avg_times,--新客多次购买平均购买次数
	   
	   if(a.old_cus_num is null,0,a.old_cus_num) as old_cus_num,
	   if(a.old_rate is null,0,a.old_rate) as old_rate,
	   if(a.old_payments is null,0,a.old_payments) as old_payments,
	   if(a.old_payments_rate is null,0,a.old_payments_rate) as old_payments_rate,
	   if(a.old_avg_price is null,0,a.old_avg_price) as old_avg_price,
	   if(a.old_avg_times is null,0,a.old_avg_times) as old_avg_times,
	   if(a.old_repeat_num is null,0,a.old_repeat_num) as old_repeat_num,
	   if(a.old_repeat_rate is null,0,a.old_repeat_rate) as old_repeat_rate,
	 
	   if(a.old_one_price is null,0,a.old_one_price) as old_one_price,
	   if(a.old_muti_price is null,0,a.old_muti_price) as old_muti_price,
	   if(a.old_muti_avg_times is null,0,a.old_muti_avg_times) as old_muti_avg_times,
	   
	   2 as type,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from 
dw_rfm.b_sale_plat_every_year_all_temp t
left join dw_rfm.b_sale_plat_new_customer_year_temp a
on t.tenant=a.tenant and t.plat_code=a.plat_code and t.byear=a.byear
left join dw_rfm.b_sale_plat_old_customer_year_temp b
on t.tenant=b.tenant and t.plat_code=b.plat_code and t.byear=b.byear;

-- 删除临时表
drop table if exists dw_rfm.b_sale_plat_every_year_all_temp;
drop table if exists dw_rfm.b_sale_plat_new_customer_year_temp;
drop table if exists dw_rfm.b_sale_plat_old_customer_year_temp;


-- 租户级客户购买数据
drop table if exists dw_rfm.b_sales_tenant_year_trade_temp;
create table if not exists dw_rfm.b_sales_tenant_year_trade_temp(
	tenant string,
	uni_id string,
	byear string,
	payments double,
	buy_times bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_sales_tenant_year_trade_temp
select a.tenant,a.uni_id,a.byear,
	   sum(a.payments) as payments,
	   sum(a.buy_times) as buy_times
from dw_rfm.b_all_year_trade_statics_result a
group by a.tenant,a.uni_id,a.byear;

--计算租户级的数据
drop table if exists dw_rfm.b_sale_tenant_every_year_all_temp;
create table dw_rfm.b_sale_tenant_every_year_all_temp as
select t.tenant,t.byear,
	count(t.uni_id) as cus_num, -- 客户总数
	sum(t.payments) as payments, -- 平台总金额
	sum(t.payments)/sum(t.buy_times) as avg_price,--平均客单价
	sum(t.buy_times)/count(t.uni_id) as avg_times --平均次数
from dw_rfm.b_sales_tenant_year_trade_temp t
group by t.tenant,t.byear;

-- 租户级新客数据
drop table if exists dw_rfm.b_sale_tenant_new_customer_year_temp;
create table dw_rfm.b_sale_tenant_new_customer_year_temp as 
select r1.tenant,r1.byear,
	   r1.new_cus_num,            --新客人数
	   (r1.new_cus_num/r2.cus_num) as new_rate, -- 新客占比
	   r1.new_payments,  --新客成交金额
	   (r1.new_payments/r2.payments) new_payments_rate, -- 新客成交金额占比
	   r1.new_avg_price,  --新客客单价
	   r1.new_avg_times,  --新客平均购买次数
	   r1.new_repeat_num, --新客复购人数
	   r1.new_repeat_rate, --新客复购率
	   r1.new_one_price,
	   r1.new_muti_price,
	   r1.new_muti_avg_times
from 
(
	select t.tenant,t.byear,
		count(t.uni_id) as new_cus_num,
		sum(t.payments) as new_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as new_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as new_avg_times, --平均次数
		sum(t.new_phurase) as new_repeat_num, --新客复购人数
		(sum(t.new_phurase)/count(t.uni_id)) as new_repeat_rate, --新客复购率
		(sum(t.new_one_payments)/sum(t.new_one_buy_cusnum)) as new_one_price,--新客单次购买客单价
		(sum(t.new_muti_payments)/sum(t.new_phurase)) as new_muti_price, --新客多次购买客单价
		(sum(t.new_muti_times)/sum(t.new_phurase)) as new_muti_avg_times --新客多次购买平均购买次数
	from
	(
		select a.tenant,a.uni_id,a.byear,a.payments,a.buy_times,
			  case when a.buy_times >= 2 then 1 else 0 end as new_phurase,
			  case when a.buy_times = 1 then 1 else 0 end as new_one_buy_cusnum, --新客单次购买人数
			  case when a.buy_times = 1 then a.payments else 0 end as new_one_payments, --新客单次购买金额
			  case when a.buy_times >= 2 then a.payments else 0 end as new_muti_payments,--新客多次购买金额
			  case when a.buy_times >= 2 then a.buy_times else 0 end as new_muti_times -- 新客多次购买次数
		from dw_rfm.b_sales_tenant_year_trade_temp a
		left join (
			select tenant,uni_id,first_buy_time from dw_rfm.b_qqd_tenant_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear = substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.byear
) r1
left join 
   dw_rfm.b_sale_tenant_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.byear=r2.byear;

--租户级回头客数据计算
drop table if exists dw_rfm.b_sale_tenant_old_customer_year_temp;
create table dw_rfm.b_sale_tenant_old_customer_year_temp as 
select r1.tenant,r1.byear,
	   r1.old_cus_num,            --回头客人数
	   (r1.old_cus_num/r2.cus_num) as old_rate, -- 回头客占比
	   r1.old_payments,  --回头客成交金额
	   (r1.old_payments/r2.payments) old_payments_rate, -- 回头客成交金额占比
	   r1.old_avg_price,  --回头客客单价
	   r1.old_avg_times,  --回头客平均购买次数
	   r1.old_repeat_num, --回头客复购人数
	   r1.old_repeat_rate, --回头客复购率
	   r1.old_one_price,
	   r1.old_muti_price,
	   r1.old_muti_avg_times
from 
(
	select t.tenant,t.byear,
		count(t.uni_id) as old_cus_num,
		sum(t.payments) as old_payments, -- 成交总金额
		sum(t.payments)/sum(t.buy_times) as old_avg_price,--平均客单价
		sum(t.buy_times)/count(t.uni_id) as old_avg_times, --平均次数
		sum(t.old_phurase) as old_repeat_num, --复购客复购人数
		(sum(t.old_phurase)/count(t.uni_id)) as old_repeat_rate, --复购客复购率
		(sum(t.old_one_payments)/sum(t.old_one_buy_cusnum)) as old_one_price,--回头客单次购买客单价
		(sum(t.old_muti_payments)/sum(t.old_phurase)) as old_muti_price, --回头客多次购买客单价
		(sum(t.old_muti_times)/sum(t.old_phurase)) as old_muti_avg_times --回头客多次购买平均购买次数
	from
	(
		select a.tenant,a.uni_id,a.byear,a.payments,a.buy_times,
			   case when a.buy_times >= 2 then 1 else 0 end as old_phurase,
			   case when a.buy_times = 1 then 1 else 0 end as old_one_buy_cusnum, --回头客单次购买人数
			   case when a.buy_times = 1 then a.payments else 0 end as old_one_payments, --回头客单次购买金额
			   case when a.buy_times >= 2 then a.payments else 0 end as old_muti_payments,--回头客多次购买金额
			   case when a.buy_times >= 2 then a.buy_times else 0 end as old_muti_times -- 回头客多次购买次数
		from dw_rfm.b_sales_tenant_year_trade_temp a
		left join (
			select tenant,uni_id,first_buy_time from dw_rfm.b_qqd_tenant_rfm where part='${stat_date}'
		) b
		on a.tenant=b.tenant and a.uni_id=b.uni_id
		where b.first_buy_time is not null and a.byear > substr(b.first_buy_time,1,4)
	) t
	group by t.tenant,t.byear
) r1
left join 
   dw_rfm.b_sale_tenant_every_year_all_temp r2
on r1.tenant=r2.tenant and r1.byear=r2.byear;


insert into table dw_rfm.cix_online_sales_structs_year
select t.tenant,null as plat_code,null as uni_shop_id,t.byear as date_col,
	   t.cus_num,t.payments,t.avg_price,t.avg_times,
	   
	   if(a.new_cus_num is null,0,a.new_cus_num) as new_cus_num,
	   if(a.new_rate is null,0,a.new_rate) as new_rate,
	   if(a.new_payments is null,0,a.new_payments) as new_payments,
	   if(a.new_payments_rate is null,0,a.new_payments_rate) as new_payments_rate,
	   if(a.new_avg_price is null,0,a.new_avg_price) as new_avg_price,
	   if(a.new_avg_times is null,0,a.new_avg_times) as new_avg_times,
	   if(a.new_repeat_num is null,0,a.new_repeat_num) as new_repeat_num,
	   if(a.new_repeat_rate is null,0,a.new_repeat_rate) as new_repeat_rate,
	   
	   if(a.new_one_price if null,0,a.new_one_price) as new_one_price,  --新客单次购买客单价
	   if(a.new_muti_price if null,0,a.new_muti_price) as new_muti_price, --新客多次购买客单价
	   if(a.new_muti_avg_times if null,0,a.new_muti_avg_times) as new_muti_avg_times,--新客多次购买平均购买次数
	   
	   if(a.old_cus_num is null,0,a.old_cus_num) as old_cus_num,
	   if(a.old_rate is null,0,a.old_rate) as old_rate,
	   if(a.old_payments is null,0,a.old_payments) as old_payments,
	   if(a.old_payments_rate is null,0,a.old_payments_rate) as old_payments_rate,
	   if(a.old_avg_price is null,0,a.old_avg_price) as old_avg_price,
	   if(a.old_avg_times is null,0,a.old_avg_times) as old_avg_times,
	   if(a.old_repeat_num is null,0,a.old_repeat_num) as old_repeat_num,
	   if(a.old_repeat_rate is null,0,a.old_repeat_rate) as old_repeat_rate,

	   if(a.old_one_price is null,0,a.old_one_price) as old_one_price,
	   if(a.old_muti_price is null,0,a.old_muti_price) as old_muti_price,
	   if(a.old_muti_avg_times is null,0,a.old_muti_avg_times) as old_muti_avg_times,
	   
	   1 as type,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from 
dw_rfm.b_sale_tenant_every_year_all_temp t
left join dw_rfm.b_sale_tenant_new_customer_year_temp a
on t.tenant=a.tenant and t.byear=a.byear
left join dw_rfm.b_sale_tenant_old_customer_year_temp b
on t.tenant=b.tenant and t.byear=b.byear;

--删除临时表
drop table if exists dw_rfm.b_sale_tenant_every_year_all_temp;
drop table if exists dw_rfm.b_sale_tenant_new_customer_year_temp;
drop table if exists dw_rfm.b_sale_tenant_old_customer_year_temp;
drop table if exists dw_rfm.b_all_year_trade_statics_result;

-- 需要对年销售结构数据导入业务库:  cix_online_sales_structs_year












