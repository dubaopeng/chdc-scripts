SET mapred.job.name='label_rfm_history_all_analyze';
--set hive.execution.engine=mr;

set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
--set tez.am.resource.memory.mb=16384;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=4915;
set tez.runtime.unordered.output.buffer.size-mb=1640;
set tez.runtime.io.sort.mb=6553;
set hive.cbo.enable=true;

SET hive.exec.compress.output=true;
set hive.optimize.index.filter=true;
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

set MODIFIED = substr(current_timestamp,1,19);
-- 近一年第一天
set lastyear = substr(date_add(add_months('${stat_date}',-12),0),1,10);
-- 近两年第一天
set twoyear = substr(date_add(add_months('${stat_date}',-24),0),1,10);

-- 获取历史有效订单
drop table if exists label.label_rfm_history_base_trade;
create table if not exists label.label_rfm_history_base_trade(
	plat_code string,
	uni_id string,
	uni_shop_id string,
	shop_id string,
	receive_payment string,
    created string,
	tenant string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_history_base_trade
select
	a.plat_code,
    a.uni_id,
    a.uni_shop_id,
    a.shop_id,
    a.receive_payment,
	a.created,
	b.tenant
from
	dw_base.b_std_tenant_shop b
left join
	(
	select
		plat_code,
		uni_id,
		uni_shop_id,
		shop_id,
		case when lower(refund_fee) = 'null' or refund_fee is NULL then payment else (payment - refund_fee) end as receive_payment,
		case when lower(trade_type) = 'step' then pay_time else created end as created
	from dw_base.b_std_trade
	where
		part <= substr('${stat_date}',1,7)
	  AND  ( created is not NULL and substr(created,1,10) <= '${stat_date}')
	  and uni_id is not NULL 
	  and payment is not NULL
	  and order_status in ('WAIT_SELLER_SEND_GOODS','SELLER_CONSIGNED_PART','TRADE_BUYER_SIGNED','WAIT_BUYER_CONFIRM_GOODS','TRADE_FINISHED','PAID_FORBID_CONSIGN','ORDER_RECEIVED','TRADE_PAID')
	)a
on a.plat_code=b.plat_code and a.shop_id = b.shop_id
where a.plat_code is not null and a.uni_id is not null and a.uni_shop_id is not null;

-- 1、计算店铺总购买金额、总购买次数、平均客单价
-- 2、计算店铺首次购买时间，首次购买金额、最近一次购买时间、最近一次购买金额
-- 3、最近一年购买金额、最近一年购买次数、最近一年平均客单价
-- 4、客户类型识别

-- 1、计算店铺总金额、次数、客单价系列
drop table if exists label.label_rfm_shop_history_all;
create table if not exists label.label_rfm_shop_history_all(
	plat_code string,
	tenant string,
	shop_id string,
	uni_id string,
	uni_shop_id string,
	sum_payment double,
    count_buy bigint,
	avg_price double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_shop_history_all
select
	plat_code,
    tenant,
	shop_id,
	uni_id,
	uni_shop_id,
    sum(receive_payment) as sum_payment, -- 总金额
    count(created) as count_buy, --总购买次数
	case when count(created)=0 then 0 else (sum(receive_payment)/count(created)) end as avg_price --平均客单价
from label.label_rfm_history_base_trade
where tenant is not NULl and shop_id is not NULL and uni_id is not NULL
group by plat_code,tenant,shop_id,uni_id,uni_shop_id;


--交易金额人店天合并
drop table if exists label.label_rfm_history_base_trade_group;
create table if not exists label.label_rfm_history_base_trade_group(
	plat_code string,
	tenant string,
	uni_id string,
	shop_id string,
	uni_shop_id string,
	created string,
    sum_payment string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_history_base_trade_group
select
    plat_code,
	tenant,
    uni_id,
	shop_id,
	uni_shop_id,
    substr(created,1,10) as created,
    sum(receive_payment) as sum_payment
from label.label_rfm_history_base_trade
where plat_code is not NULL and uni_id is not NULL and shop_id is not NULL and tenant is not NULL
group by
    plat_code,
	tenant,
    uni_id,
	shop_id,
	uni_shop_id,
    substr(created,1,10);

-- 2、计算店铺首次购买时间，首次购买金额、最近一次购买时间、最近一次购买金额
drop table if exists label.label_rfm_shop_history_first_last;
create table if not exists label.label_rfm_shop_history_first_last(
	plat_code string,
	tenant string,
	uni_id string,
	shop_id string,
	uni_shop_id string,
	first_created string,
	last_created string,
	first_payment double,
	last_payment double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_shop_history_first_last
select
	a.plat_code,
	a.tenant,
	a.uni_id,
	a.shop_id,
	a.uni_shop_id,
	min(a.created) as first_created, --首次购买时间
	max(a.created) as last_created, --最近一次购买时间
	split(min(concat(b.created,'_',b.sum_payment)),'_')[1] as first_payment,
	split(max(concat(b.created,'_',b.sum_payment)),'_')[1] as last_payment
from label.label_rfm_history_base_trade a
left join label.label_rfm_history_base_trade_group b
	on a.tenant = b.tenant and a.plat_code=b.plat_code and a.uni_shop_id = b.uni_shop_id and a.uni_id = b.uni_id
	where b.tenant is not NULL
group by a.plat_code,a.tenant,a.uni_id,a.shop_id,a.uni_shop_id;


-- 3、最近一年购买金额、最近一年购买次数、最近一年平均客单价
drop table if exists label.label_rfm_shop_history_year;
create table if not exists label.label_rfm_shop_history_year(
	plat_code string,
	tenant string,
	uni_id string,
	shop_id string,
	uni_shop_id string,
	count_buy_365 bigint,
	sum_payment_365 double,
	avg_price_365 double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_shop_history_year
select
    plat_code,
    tenant,
    uni_id,
	shop_id,
	uni_shop_id,
    count(created) as count_buy_365,  --近一年购买次数
    sum(receive_payment) as sum_payment_365, -- 近一年购买金额
	case when count(created)=0 then 0 else (sum(receive_payment)/count(created)) end as avg_price_365 --近一年平均客单价
from label.label_rfm_history_base_trade
where created > add_months('${stat_date}',-12) 
	  and created <= '${stat_date}'
group by plat_code,tenant,uni_id,shop_id,uni_shop_id;


---------------------------------------------
-- 店铺RFM计算
---------------------------------------------
drop table if exists label.label_rfm_shop_tmp;
create table if not exists label.label_rfm_shop_tmp(
	tenant string,
	plat_code string,
	uni_shop_id string,
	shop_id string,
	uni_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
    first_payment double,
	last_created string,
    last_payment double,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 汇总：客户数据进行关联前三步结果数据
insert overwrite table label.label_rfm_shop_tmp partition(part = '${stat_date}')
select t.tenant,t.plat_code,t.uni_shop_id,t.shop_id,t.uni_id,
	  if(r.sum_payment is null,0,r.sum_payment) as all_payment,
	  if(r.count_buy is null,0,r.count_buy) as all_count_buy,
	  if(r.avg_price is null,0,r.avg_price) as all_avg_payment,
	  r.first_created,
	  if(r.first_payment is null,0,r.first_payment) as first_payment,
	  r.last_created,
	  if(r.last_payment is null,0,r.last_payment) as last_payment,
	  if(r.sum_payment_365 is null,0,r.sum_payment_365) as year_payment,
	  if(r.count_buy_365 is null,0,r.count_buy_365) as year_count_buy,
	  if(r.avg_price_365 is null,0,r.avg_price_365) as year_avg_payment,
	  case when r.first_created is null then '1'
        when substr(r.first_created,1,10) > ${hiveconf:lastyear} and r.count_buy_365=1 then '2'
        when substr(r.first_created,1,10) <= ${hiveconf:lastyear} and r.count_buy_365=1 then '3'
        when substr(r.first_created,1,10) > ${hiveconf:lastyear} and r.count_buy_365>=2 then '4'
        when substr(r.first_created,1,10) <= ${hiveconf:lastyear} and r.count_buy_365>=2 then '5'
        when substr(r.last_created,1,10) <= ${hiveconf:lastyear} and substr(r.last_created,1,10) > ${hiveconf:twoyear} then '6'
        when substr(r.last_created,1,10) <= ${hiveconf:twoyear} then '7' end as customer_type
from dw_base.b_tenant_plat_shop_customer t
left join
(
	select
		a.tenant,
		a.plat_code,
		a.uni_shop_id,
		a.shop_id,
		a.uni_id,
		a.sum_payment,
		a.count_buy,
		a.avg_price,
		b.first_created,
		b.last_created,
		if(b.first_payment is null,0,b.first_payment) first_payment,
		if(b.last_payment is null,0,b.last_payment) last_payment,
		if(c.count_buy_365 is null,0,c.count_buy_365) count_buy_365,
		if(c.sum_payment_365 is null,0,c.sum_payment_365) sum_payment_365,
		if(c.avg_price_365 is null,0,c.avg_price_365) avg_price_365
	 from label.label_rfm_shop_history_all a
	left join 
		label.label_rfm_shop_history_first_last b
		on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.uni_id=b.uni_id
	left join
		label.label_rfm_shop_history_year c
		on a.tenant=c.tenant and a.plat_code=c.plat_code and a.uni_shop_id=c.uni_shop_id and a.uni_id=c.uni_id
) r
on t.tenant=r.tenant and t.plat_code=r.plat_code and t.uni_shop_id=r.uni_shop_id and t.uni_id=r.uni_id;


-- 店铺级客户分析结果输出
drop table if exists label.label_rfm_shop;
create table if not exists label.label_rfm_shop(
	tenant string,
	uni_shop_id string,
	uni_id string,
	plat_code string,
	shop_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
    first_payment double,
	last_created string,
    last_payment double,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string,
	stat_date string,
    modified string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' NULL DEFINED AS '' 
STORED AS TEXTFILE;

insert overwrite table label.label_rfm_shop
select
    tenant ,
	uni_shop_id,
    uni_id,
	plat_code,
	shop_id,
    all_payment,
    all_count_buy,
    all_avg_payment,
    first_created,
    first_payment,
    last_created,
    last_payment,
    year_payment,
    year_count_buy,
    year_avg_payment,
	customer_type,
	'${stat_date}' as stat_date,
	${hiveconf:MODIFIED} as modified
from label.label_rfm_shop_tmp
where part = '${stat_date}' 
	and tenant is not NULL 
	and uni_id is not NULL 
	and shop_id is not NULL;

-- 二、基于店铺级的分析平台数据

drop table if exists label.label_rfm_plat_tmp;
create table if not exists label.label_rfm_plat_tmp(
	plat_code string,
	tenant string,
	uni_id string,
	all_uni_shop_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
    first_payment double,
	first_uni_shop_id string,
	last_created string,
    last_payment double,
	last_uni_shop_id string,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_plat_tmp partition(part='${stat_date}')
select t.plat_code,t.tenant,t.uni_id,
	   t.all_uni_shop_id,
	   if(t.all_payment is null,0,t.all_payment) as all_payment,
	   if(t.all_count_buy is null,0,t.all_count_buy) as all_count_buy,
	   if(t.all_avg_payment is null,0,t.all_avg_payment) as all_avg_payment,
	   t.first_created,
	   if(t.first_payment is null,0,t.first_payment) as first_payment,
	   t.first_uni_shop_id,
	   t.last_created,
	   if(t.last_payment is null,0,t.last_payment) as last_payment,
	   t.last_uni_shop_id,  
	   if(t.year_payment is null,0,t.year_payment) as year_payment,
	   if(t.year_count_buy is null,0,t.year_count_buy) as year_count_buy,
	   if(t.year_avg_payment is null,0,t.year_avg_payment) as year_avg_payment,
	   case when t.first_created is null then '1'
        when substr(t.first_created,1,10) > ${hiveconf:lastyear} and t.year_count_buy=1 then '2'
        when substr(t.first_created,1,10) <= ${hiveconf:lastyear} and t.year_count_buy=1 then '3'
        when substr(t.first_created,1,10) > ${hiveconf:lastyear} and t.year_count_buy>=2 then '4'
        when substr(t.first_created,1,10) <= ${hiveconf:lastyear} and t.year_count_buy>=2 then '5'
        when substr(t.last_created,1,10) <= ${hiveconf:lastyear} and substr(t.last_created,1,10) > ${hiveconf:twoyear} then '6'
        when substr(t.last_created,1,10) <= ${hiveconf:twoyear} then '7' end as customer_type
from (
	select r.tenant,r.plat_code,r.uni_id,
			concat_ws(',',collect_set(case when r.first_created is not null then r.uni_shop_id else null end)) as all_uni_shop_id,
			sum(r.all_payment) as all_payment,
			sum(r.all_count_buy) as all_count_buy,
			case when sum(r.all_count_buy)=0 then null else (sum(r.all_payment)/sum(r.all_count_buy)) end as all_avg_payment,
			min(r.first_created) as first_created,
			split(min(concat(r.first_created,'_',r.first_payment)),'_')[1] as first_payment,
			split(min(concat(r.first_created,'_',r.uni_shop_id)),'_')[1] as first_uni_shop_id,
			max(r.last_created) as last_created,
			split(max(concat(r.last_created,'_',r.last_payment)),'_')[1] as last_payment,
			split(max(concat(r.last_created,'_',r.uni_shop_id)),'_')[1] as last_uni_shop_id,
			sum(r.year_payment) as year_payment,
			sum(r.year_count_buy) as year_count_buy,
			case when sum(r.year_count_buy)=0 then null else (sum(r.year_payment)/sum(r.year_count_buy)) end as year_avg_payment
	from label.label_rfm_shop_tmp r
	where r.part='${stat_date}'
	group by r.tenant,r.plat_code,r.uni_id
) t;

-- 数据插入结果表，同步程序同步
drop table if exists label.label_rfm_plat;
create table if not exists label.label_rfm_plat(
	plat_code string,
	tenant string,
	uni_id string,
	all_uni_shop_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
    first_payment double,
	first_uni_shop_id string,
	last_created string,
    last_payment double,
	last_uni_shop_id string,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string,
	stat_date string,
    modified string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' NULL DEFINED AS ''
STORED AS TEXTFILE;

insert overwrite table label.label_rfm_plat
select
    plat_code ,
	tenant,
    uni_id,
	all_uni_shop_id,
    round(all_payment,6) as all_payment,
    all_count_buy,
    round(all_avg_payment,6) as all_avg_payment,
    first_created,
    round(first_payment,6) as first_payment,
	first_uni_shop_id,
    last_created,
    round(last_payment,6) as last_payment,
    last_uni_shop_id,
    round(year_payment,6) as year_payment,
    year_count_buy,
    round(year_avg_payment,6) as year_avg_payment,
	customer_type,
	'${stat_date}' as stat_date,
	${hiveconf:MODIFIED} as modified
from label.label_rfm_plat_tmp
where part = '${stat_date}';


-- 三、基于平台表计算租户级数据
drop table if exists label.label_rfm_tenant_tmp;
create table if not exists label.label_rfm_tenant_tmp(
	tenant string,
	uni_id string,
	all_plat_code string,
	all_uni_shop_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
	first_plat_code string,
	first_uni_shop_id string,
    first_payment double,
	last_created string,
	last_plat_code string,
	last_uni_shop_id string,
    last_payment double,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string
)PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table label.label_rfm_tenant_tmp partition(part = '${stat_date}')
select t.tenant,t.uni_id,
	   t.all_plat_code,
	   t.all_uni_shop_id,
	   if(t.all_payment is null,0,t.all_payment) as all_payment,
	   if(t.all_count_buy is null,0,t.all_count_buy) as all_count_buy,
	   if(t.all_avg_payment is null,0,t.all_avg_payment) as all_avg_payment,
	   t.first_created,
	   t.first_plat_code,
	   t.first_uni_shop_id,
	   if(t.first_payment is null,0,t.first_payment) as first_payment,
	   t.last_created,
	   t.last_plat_code,
	   t.last_uni_shop_id,
	   if(t.last_payment is null,0,t.last_payment) as last_payment,
	   if(t.year_payment is null,0,t.year_payment) as year_payment,
	   if(t.year_count_buy is null,0,t.year_count_buy) as year_count_buy,
	   if(t.year_avg_payment is null,0,t.year_avg_payment) as year_avg_payment,
	   case when t.first_created is null then '1'
        when substr(t.first_created,1,10) > ${hiveconf:lastyear} and t.year_count_buy=1 then '2'
        when substr(t.first_created,1,10) <= ${hiveconf:lastyear} and t.year_count_buy=1 then '3'
        when substr(t.first_created,1,10) > ${hiveconf:lastyear} and t.year_count_buy>=2 then '4'
        when substr(t.first_created,1,10) <= ${hiveconf:lastyear} and t.year_count_buy>=2 then '5'
        when substr(t.last_created,1,10) <= ${hiveconf:lastyear} and substr(t.last_created,1,10) > ${hiveconf:twoyear} then '6'
        when substr(t.last_created,1,10) <= ${hiveconf:twoyear} then '7' end as customer_type
from (
	select r.tenant,r.uni_id,
			concat_ws(',',collect_set(case when r.first_created is not null then r.plat_code else null end)) as all_plat_code,
			concat_ws(',',collect_set(case when r.first_created is not null then r.all_uni_shop_id else null end)) as all_uni_shop_id,
			sum(r.all_payment) as all_payment,
			sum(r.all_count_buy) as all_count_buy,
			case when sum(r.all_count_buy)=0 then null else (sum(r.all_payment)/sum(r.all_count_buy)) end as all_avg_payment,
			min(r.first_created) as first_created,
			split(min(concat(r.first_created,'_',r.plat_code)),'_')[1] as first_plat_code,
			split(min(concat(r.first_created,'_',r.first_uni_shop_id)),'_')[1] as first_uni_shop_id,
			split(min(concat(r.first_created,'_',r.first_payment)),'_')[1] as first_payment,
			max(r.last_created) as last_created,
			split(max(concat(r.last_created,'_',r.plat_code)),'_')[1] as last_plat_code,
			split(max(concat(r.last_created,'_',r.last_uni_shop_id)),'_')[1] as last_uni_shop_id,
			split(max(concat(r.last_created,'_',r.last_payment)),'_')[1] as last_payment,
			sum(r.year_payment) as year_payment,
			sum(r.year_count_buy) as year_count_buy,
			case when sum(r.year_count_buy)=0 then null else (sum(r.year_payment)/sum(r.year_count_buy)) end as year_avg_payment
	from label.label_rfm_plat_tmp r
	where r.part='${stat_date}'
	group by r.tenant,r.uni_id
) t;

-- 租户级数据同步到业务库
drop table if exists label.label_rfm_tenant;
create table if not exists label.label_rfm_tenant(
	tenant string,
	uni_id string,
	all_plat_code string,
	all_uni_shop_id string,
	all_payment double,
    all_count_buy bigint,
	all_avg_payment double,
	first_created string,
	first_plat_code string,
	first_uni_shop_id string,
    first_payment double,
	last_created string,
	last_plat_code string,
	last_uni_shop_id string,
    last_payment double,
	year_payment double,
    year_count_buy bigint,
	year_avg_payment double,
	customer_type string,
	stat_date string,
    modified string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' NULL DEFINED AS ''
STORED AS TEXTFILE;

insert overwrite table label.label_rfm_tenant
select
    tenant,
    uni_id,
	all_plat_code,
	all_uni_shop_id,
    round(all_payment,6) as all_payment,
    all_count_buy,
    round(all_avg_payment,6) as all_avg_payment,
    first_created,
    first_plat_code,
	first_uni_shop_id,
	round(first_payment,6) as first_payment,
    last_created,
    last_plat_code,
    last_uni_shop_id,
    round(last_payment,6) as last_payment,
    round(year_payment,6) as year_payment,
    year_count_buy,
    round(year_avg_payment,6) as year_avg_payment,
    customer_type,
	'${stat_date}' as stat_date,
	${hiveconf:MODIFIED} as modified
from label.label_rfm_tenant_tmp
where part = '${stat_date}';