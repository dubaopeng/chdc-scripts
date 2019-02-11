SET mapred.job.name='category-trade-analyze-everymonth 每月进行行业数据分析';
--set hive.execution.engine=mr;
set hive.tez.container.size=16144;
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

-- 历史订单类目统计

-- 子订单：dw_base.b_std_order
-- 订单表：dw_base.b_std_trade
-- 商品信息: dw_base.b_std_product (需接入)
-- 店铺类目表：dw_base.b_plat_shop_category (需接入)


--首先计算近两年的历史订单记录
CREATE TABLE IF NOT EXISTS dw_rfm.`b_shop_twoyear_trade`(
	`uni_order_id` string,
	`plat_code` string,
	`uni_shop_id` string,
	`shop_id` string,
	`payment` double,
	`created` string,
	`month` string,
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_shop_twoyear_trade`
select
    uni_order_id,
	plat_code,
	uni_shop_id,
	shop_id,
	case when refund_fee = 'NULL' or refund_fee is NULL then payment else (payment - refund_fee) end as payment,
	created,
	substr(created,1,7) as month
from dw_base.b_std_trade
--由于分区使用了月份，不能使用等于日期，两边数据都多取一部分，使用created再过滤一次
where (part > add_months('${stat_date}',-25) and part < add_date('${stat_date}',1)) 
and plat_code = 'TAOBAO'
and (created is not NULL and created >= add_months('${stat_date}',-24) and substr(created,1,10) <= '${stat_date}')
and uni_id is not null 
and payment is not null;

-- 以近两年的历史订单按月份进行划分,计算出有连续订单记录的店铺(每月都有订单),并交易金额大于10000的店铺
-- 有连续2年销售数据（近24个月，每月都有销售数据），过去1年（近12个月）销售金额不少于1W
CREATE TABLE IF NOT EXISTS dw_rfm.`b_need_analyze_category_shops`(
	`plat_code` string,
	`uni_shop_id` string,
	`shop_id` string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_need_analyze_category_shops partition(part = '${stat_date}')
select re.plat_code,re.uni_shop_id,re.shop_id from (
	select r.plat_code,r.uni_shop_id,r.shop_id,sum(payment) as total_payment 
	from 
	(
		select a.plat_code,a.uni_shop_id,b.shop_id,b.payment,b.created
		from (
			select t.plat_code,t.uni_shop_id from(
				select plat_code,uni_shop_id,size(collect_set(month)) as monthnum
				from dw_rfm.b_shop_twoyear_trade 
				group by plat_code,uni_shop_id
			) t
			where t.monthnum=24
		) a
		join dw_rfm.b_shop_twoyear_trade b
		on a.plat_code=a.plat_code and a.uni_shop_id=b.uni_shop_id
	) r
	where r.created >= add_months('${stat_date}',-12) and substr(r.created,1,10) <= '${stat_date}'
	group by r.plat_code,r.uni_shop_id,r.shop_id
) re
where re.total_payment >= 10000;


-- 过去1年（近12月）Top 1 的类目（parent cid）销售金额占全店销售大于等于50%
drop table if exists dw_rfm.b_shop_category_payment_temp;
create table dw_rfm.b_shop_category_payment_temp as
select re.plat_code,re.uni_shop_id,re.shop_id,re.parent_category_id,sum(re.payment) as total_payment
from (
	select r1.plat_code,r1.uni_shop_id,r1.shop_id,r1.uni_order_id,r1.product_id,r1.payment,r1.category_id,r2.parent_category_id
	from (
		select t1.plat_code,t1.uni_shop_id,t1.shop_id,t1.uni_order_id,t1.product_id,t1.payment,c.category_id from (
			select r.plat_code,r.uni_shop_id,r.shop_id,r.uni_order_id,
				   t.product_id,if(t.payment is null,0,t.payment) as payment
			from (
				select a.plat_code,a.uni_shop_id,a.shop_id,b.uni_order_id from dw_rfm.b_need_analyze_category_shops a
				left join 
				(
					select uni_order_id,plat_code,uni_shop_id,shop_id from dw_rfm.b_shop_twoyear_trade
					where created >= add_months('${stat_date}',-12) and substr(created,1,10) <= '${stat_date}'
				)b
				on a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id
				where b.uni_order_id is not null
			) r
			left join 
			dw_base.b_std_order t
			on r.uni_order_id = t.uni_order_id
			where t.product_id is not null
		) t1
		left join dw_base.b_std_product c
		on t1.plat_code=c.plat_code and t1.shop_id=c.shop_id and t1.product_id=c.product_id
		where c.plat_code is not null and c.product_id is not null and c.category_id is not null
	) r1
	left join
	   -- 平台店铺类目信息表
	   dw_base.b_plat_shop_category r2
	on r1.plat_code=r2.plat_code and r1.shop_id=r2.shop_id and r1.category_id=r2.category_id
	where r2.parent_category_id is not null
) re
group by re.plat_code,re.uni_shop_id,re.shop_id,re.parent_category_id;


-- 计算出top1类目销售金额占店铺交易金额50%以上的电铺
insert overwrite table dw_rfm.b_need_analyze_category_shops partition(part = '${stat_date}')
select t.plat_code,t.uni_shop_id,t.shop_id from 
(
	select a.plat_code,a.uni_shop_id,a.shop_id,sum(a.total_payment) total_payment
	from dw_rfm.b_shop_category_payment_temp a
	group by a.plat_code,a.uni_shop_id,a.shop_id
) t
join
(
	select r.plat_code,r.uni_shop_id,r.parent_category_id,r.total_payment as top1category_payment from
	(	select *,row_number() over (partition by plat_code,uni_shop_id,parent_category_id order by total_payment desc) as rank 
		from dw_rfm.b_shop_category_payment_temp
	) r
	where r.rank=1
) t1
on t.plat_code=t1.plat_code and t.uni_shop_id=t1.uni_shop_id
where (t1.top1category_payment/t.total_payment) >= 0.5;


-- 行业top5的店铺记录(产品要求保留下)
CREATE TABLE IF NOT EXISTS dw_rfm.`b_category_top5_shop`(
	`parent_category_id` string,
	`plat_code` string,
	`uni_shop_id` string,
	`shop_id` string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 计算类目销售金额top5的店铺
insert overwrite table dw_rfm.b_category_top5_shop partition(part='${stat_date}')
select re.parent_category_id,re.plat_code,re.uni_shop_id,re.shop_id
from
(
	select t.*,row_number() over (partition by t.parent_category_id order by t.total_payment desc) as rank 
	from 
	(
		select r.plat_code,r.uni_shop_id,r.parent_category_id,sum(r.total_payment) total_payment
		from (
			select a.plat_code,a.uni_shop_id,a.shop_id,a.parent_category_id,a.total_payment 
			from dw_rfm.b_shop_category_payment_temp a
			left semi join
			(
				select plat_code,uni_shop_id,shop_id from dw_rfm.b_need_analyze_category_shops where part='${stat_date}'
			) b
			on a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id and a.shop_id=b.shop_id
		) r
		group by r.plat_code,r.uni_shop_id,r.parent_category_id
	) t
	where t.rank <= 5
) re;

-- 只保留店铺数量等于5的类目 
insert overwrite table dw_rfm.b_category_top5_shop partition(part='${stat_date}')
select a.parent_category_id,a.plat_code,a.uni_shop_id,a.shop_id
from dw_rfm.b_category_top5_shop a
left semi join
(
	select t.parent_category_id,t.shop_num from (
		select parent_category_id,count(uni_shop_id) shop_num
		from 
		dw_rfm.b_category_top5_shop where part='${stat_date}'
	) t
	where t.shop_num = 5
) b
on a.parent_category_id=b.parent_category_id;


-- 计算哪些店铺可以有行业比对数据
-- 只要该类目过去1年（近12月）销售件数占比大于等于全店10%，此店就可以该类目行业数据对比权限

-- 店铺过去1年的各类目卖出数量统计
drop table if exists dw_rfm.b_last_year_shop_category_sel_num;
create table dw_rfm.b_last_year_shop_category_sel_num
select d.plat_code,d.uni_shop_id,d.shop_id,d.parent_category_id,sum(d.product_num) product_nums
from (
	select r1.plat_code,r1.uni_shop_id,r1.shop_id,r1.product_num,r2.parent_category_id
	from (
		select re.plat_code,re.uni_shop_id,re.shop_id,re.product_num,c.category_id
		from (
			select r.plat_code,r.uni_shop_id,r.shop_id,t.product_id,t.product_num from 
			(
				select a.uni_order_id,a.plat_code,a.uni_shop_id,a.shop_id,
				from dw_rfm.b_shop_twoyear_trade a
				where a.created >= add_months('${stat_date}',-12) and substr(a.created,1,10) <= '${stat_date}'
			) r
			left join
				dw_base.b_std_order t
				on r.plat_code=t.plat_code and r.uni_shop_id=t.uni_shop_id and r.uni_order_id = t.uni_order_id
				where t.product_id is not null and t.product_num is not null
			)
		) re
		left join dw_base.b_std_product c
			on re.plat_code=c.plat_code and re.shop_id=c.shop_id and re.product_id=c.product_id
			where c.plat_code is not null and c.product_id is not null and c.category_id is not null
	) r1
	left join dw_base.b_plat_shop_category r2  -- 平台店铺类目信息表
	on r1.plat_code=r2.plat_code and r1.shop_id=r2.shop_id and r1.category_id=r2.category_id
	where r2.parent_category_id is not null
) d
group by d.plat_code,d.uni_shop_id,d.shop_id,d.parent_category_id;


-- 在上面基础上计算店铺总共卖出的商品，类目数量和店铺比对，只取大于10%
-- 以下数据需要同步给业务进行查询判断
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_shop_has_permission_category`(
	`plat_code` string,
	`uni_shop_id` string,
	`shop_id` string,
	`parent_category_id` string,
	`sortno` int,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_shop_has_permission_category
select re.plat_code,re.uni_shop_id,re.shop_id,re.parent_category_id,re.sortno,'${stat_date}' as stat_date
from(
	select r.plat_code,r.uni_shop_id,r.shop_id,r.parent_category_id,
		row_number() over (partition by r.plat_code,r.uni_shop_id,r.shop_id order by r.product_nums desc) as sortno
	from(
		select t.plat_code,t.uni_shop_id,t.shop_id,t.parent_category_id,t.product_nums
		from dw_rfm.b_last_year_shop_category_sel_num t
		left join 
		(
			select a.plat_code,a.uni_shop_id,a.shop_id,sum(a.product_nums) product_nums
			from dw_rfm.b_last_year_shop_category_sel_num a
			group by a.plat_code,a.uni_shop_id,a.shop_id
		) t1
		on t.plat_code = t1.plat_code and t.uni_shop_id=t1.uni_shop_id and t.shop_id=t1.shop_id
		where t1.product_nums > 0 and (t.product_nums/t1.product_nums) >= 0.1;
	) r
) re
where re.sortno <= 10;


-- 接下来需要以行业的几个店铺进行复购率，客户保持率计算，送活跃客户的RFM宽表中计算
-- 行业的计算基于当日的RFM结果表进行统计，需要判断当天的RFM结果表是否已经计算完成，然后开始这个任务

-- 行业复购率计算临时结果表
drop table if exists dw_rfm.b_category_repurchase_temp;
create table dw_rfm.b_category_repurchase_temp as
select t1.parent_category_id,
		avg(t1.rate) as rate,
		avg(t1.new_rate) as new_rate,
		avg(t1.old_rate) as old_rate
from 
(
	select a.parent_category_id,a.plat_code,a.uni_shop_id,b.rate,a.new_rate,b.old_rate
	from dw_rfm.b_category_top5_shop a
	join
	(
		select 
			re.activeNum,re.repurNum,case re.activeNum when 0 then 0 else re.repurNum/re.activeNum end as rate,
			re.activeNewNum,re.newrepurNum,case re.activeNewNum when 0 then 0 else re.newrepurNum/re.activeNewNum end as new_rate,
			re.activeOldNum,re.oldrepurNum,case re.activeOldNum when 0 then 0 else re.oldrepurNum/re.activeOldNum end as old_rate,
		from (
			select r.tenant,r.plat_code,r.uni_shop_id,
			sum(r.active) activeNum,sum(r.repurchase) repurNum,
			sum(r.active_new) activeNewNum,sum(r.new_repurchase) newrepurNum,
			sum(r.active_old) activeOldNum,sum(r.old_repurchase) oldrepurNum
			from (
				select t.tenant,t.plat_code,t.uni_shop_id,
				if(t.year_buy_times>=1,1,0) active, 
				if(t.year_buy_times >= 2,1,0) repurchase, 
				if((t.year_buy_times >=1 and t.first_buy_time >= add_months('${stat_date}',-12),1,0) active_new,
				if((t.year_buy_times >=2 and t.first_buy_time >= add_months('${stat_date}',-12),1,0) new_repurchase,
				if((t.year_buy_times >=1 and t.first_buy_time < add_months('${stat_date}',-12),1,0) active_old,
				if((t.year_buy_times >=2 and t.first_buy_time < add_months('${stat_date}',-12),1,0) old_repurchase
				from dw_rfm.b_qqd_shop_rfm t
				where part='${stat_date}'
			) r
			group by r.tenant,r.plat_code,r.uni_shop_id
		) re
	) b
	on a.plat_code =b.plat_code and a.uni_shop_id=b.uni_shop_id
) t1
group by t1.parent_category_id;

-- 行业客户保持率临时结果表
drop table if exists dw_rfm.b_category_retention_temp;
create create dw_rfm.b_category_retention_temp as
select t1.parent_category_id,avg(retention_rate) as retention_rate
from 
(
	select a.parent_category_id,a.plat_code,a.uni_shop_id,b.retention_rate
	from dw_rfm.b_category_top5_shop a
	join
	(
		select r.tenant,r.plat_code,r.uni_shop_id,
			case sum(r.prev_year_num) when 0 then 0 else sum(r.last_year_num)/sum(r.prev_year_num) end as retention_rate,
		from (
			select t.tenant,t.plat_code,t.uni_shop_id,
			if(t.tyear_buy_times >=1,1,0) prev_year_num,
			if((t.tyear_buy_times >=1 and t.year_buy_times >= 1),1,0) last_year_num
			from dw_rfm.b_qqd_shop_rfm t
			where t.part='${stat_date}'
		) r
		group by r.tenant,r.plat_code,r.uni_shop_id
	) b
	on a.plat_code =b.plat_code and a.uni_shop_id=b.uni_shop_id
) t1
group by t1.parent_category_id;


-- 创建行业客户资产表,需要复购率和保持率联合起来，将该数据同步给业务库
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_category_customer_assets`(
	`parent_category_id` string,
	`active_customer_rate` double,
	`new_customer_rate` double,
	`old_customer_rate` double,
	`retention_rate` double,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`cix_online_category_customer_assets`
select a.parent_category_id,a.rate as active_customer_rate,
		a.new_rate as new_customer_rate,a.old_rate as old_customer_rate,
		b.retention_rate,'${stat_date}' as stat_date
from dw_rfm.b_category_repurchase_temp a
join dw_rfm.b_category_retention_temp b
on a.parent_category_id=b.parent_category_id;

-- 行业的复购间隔分析结果，需要同步到业务库中
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_category_purchase_interval`(
	`parent_category_id` string,
	`customer_type` int,
	`interval_days` int,
	`customer_num` bigint,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_category_purchase_interval
select r.parent_category_id,r.interval_days,round(avg(r.customer_num)) as customer_num,1 as customer_type,'${stat_date}' as stat_date
from(
	select a.parent_category_id,b.interval_days,b.customer_num
	from dw_rfm.b_category_top5_shop a
	join 
	(	
		select t.tenant,t.plat_code,t.uni_shop_id,
		ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1)) interval_days,
		count(t.uni_id) customer_num
		from dw_rfm.b_qqd_shop_rfm t
		where t.part='${stat_date}' and t.year_buy_times >= 2 
		group by t.tenant,t.plat_code,t.uni_shop_id,
		ceil(datediff(t.year_last_time,t.year_first_time)/(t.year_buy_times-1))
	) b
	on a.plat_code = b.plat_code and a.uni_shop_id=b.uni_shop_id
) r
group by r.parent_category_id,r.interval_days
union all
select r1.parent_category_id,r1.interval_days,round(avg(r1.customer_num)) as customer_num,2 as customer_type,'${stat_date}' as stat_date
from(
	select a1.parent_category_id,b1.interval_days,b1.customer_num
	from dw_rfm.b_category_top5_shop a1
	join 
	(	
		select t1.tenant,t1.plat_code,t1.uni_shop_id,
			datediff(t1.second_buy_time,t1.first_buy_time) interval_days,
			count(t1.uni_id) customer_num
		from dw_rfm.b_qqd_shop_rfm t1
		where t1.part ='${stat_date}'
			and t1.first_buy_time >= add_months('${stat_date}',-12)
			and t1.second_buy_time is not null
			and t1.year_buy_times > 1
		group by t1.tenant,t1.plat_code,t1.uni_shop_id,datediff(t1.second_buy_time,t1.first_buy_time)
	) b1
	on a1.plat_code = b1.plat_code and a1.uni_shop_id=b1.uni_shop_id
) r1
group by r1.parent_category_id,r1.interval_days;

--解决union all的目录无法导出问题
insert overwrite table dw_rfm.cix_online_category_purchase_interval
select parent_category_id,customer_type,interval_days,customer_num,stat_date
from dw_rfm.cix_online_category_purchase_interval;


-- 需要对结果数据导出到mysql中
-- cix_online_shop_has_permission_category
-- cix_online_category_customer_assets
-- cix_online_category_purchase_interval


