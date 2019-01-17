SET mapred.job.name='b_std_shop_customer-店铺与客户关系数据同步作业';
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

-- add jar hdfs://nameservice1/user/newbi/newbi/jar/plat-hive-udf-1.0.0.jar;
add jar hdfs://master01.bigdata.shuyun.com:8020/user/hive/jar/plat-hive-udf-1.0.0.jar;
create temporary function shopid_hash as 'com.shuyun.plat.hive.udf.ShopIdHashUDF';

-- 全渠道平台店铺与客户关系数据增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_shop_customer`( 
    `uni_id` string,
	`uni_shop_id` string,
	`shop_id` string,
    `plat_code` string, 
    `plat_account` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/shop_customer';

-- 批量重设分区
msck repair table dw_source.`s_std_shop_customer`;


-- 平台店铺客户正式表
CREATE TABLE IF NOT EXISTS dw_base.`b_std_shop_customer`( 
    `uni_id` string,
	`uni_shop_id` string,
	`shop_id` string,
    `plat_code` string, 
    `plat_account` string
)
partitioned by(`dp` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
lines terminated by '\n';

-- 创建去重的临时表数据
DROP TABLE IF EXISTS dw_source.`s_std_shop_customer_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_std_shop_customer_temp`( 
    `uni_id` string,
	`uni_shop_id` string,
	`shop_id` string,
    `plat_code` string, 
    `plat_account` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
lines terminated by '\n';

-- 去重今日数据，插入临时表
insert into dw_source.s_std_shop_customer_temp 
select t.uni_id,t.uni_shop_id,t.shop_id,t.plat_code,t.plat_account from (
select *,row_number() over (partition by uni_id,uni_shop_id,shop_id,plat_code ) as num 
from dw_source.`s_std_shop_customer` where dt='${today}') t where t.num=1;

-- 数据比对，全量更新和新增店铺和客户关系记录
insert overwrite table dw_base.b_std_shop_customer partition(dp)
SELECT 
    re.uni_id,
    re.uni_shop_id,
    re.shop_id,
    re.plat_code,
    re.plat_account,
    shopid_hash(re.shop_id) as dp
FROM
    (SELECT 
        t.uni_id,
            t.uni_shop_id,
            t.shop_id,
            t.plat_code,
            t.plat_account
    FROM
        (SELECT a.*
    FROM
        dw_base.`b_std_shop_customer` a
    LEFT OUTER JOIN dw_source.s_std_shop_customer_temp b ON (a.uni_id = b.uni_id
        AND a.uni_shop_id = b.uni_shop_id
        AND a.shop_id = b.shop_id
        AND a.plat_code = b.plat_code)
    WHERE
        b.uni_id IS NULL
            AND b.uni_shop_id IS NULL
            AND b.shop_id IS NULL
            AND b.plat_code IS NULL) t 
	UNION ALL SELECT 
        uni_id, uni_shop_id, shop_id, plat_code, plat_account
    FROM
        dw_source.s_std_shop_customer_temp) re;
		
DROP TABLE IF EXISTS dw_source.`s_std_shop_customer_temp`;