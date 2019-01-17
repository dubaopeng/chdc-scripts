SET mapred.job.name='b_std_customer-全渠道客户数据同步作业';
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

-- 创建全渠道客户数据增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_customer`( 
    `uni_id` string,
	`tenant` string,
    `full_name` string, 
    `first_name` string,
    `last_name` string,
    `gender` string,
    `birth_year` int,
	`birth_month` int,
	`birthday_type` int,
	`birthday` bigint,
	`constellation` int,
    `identity_card` string, 
    `email_suffix` string,
    `is_valid_email` int,
    `email` string,
    `email_markting` int,
	`mobile_zone` string,
	`mobile` string,
    `is_valid_mobile` string, 
    `used_mobile` string,
    `register_mobile` string,
    `custom_mobile` string,
    `bindcard_mobile` string,
	`mobile_markting` int,
	`zip` string,
    `country` string, 
    `state` string,
    `city` string,
    `district` string,
    `town` string,
	`address` string,
	`insert_time` string,
    `most_consumption_shop_id` string,
    `last_consumption_shop_id` string,
    `modified` string,
	`hone_type` int,
    `customer_source` string,
	`customer_from_plat` string,
	`customer_from_shop` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/customer';

-- 批量重设分区
msck repair table dw_source.`s_std_customer`;

-- 全渠道客户数据表，以租户id做分区
CREATE TABLE IF NOT EXISTS dw_base.`b_std_customer`( 
    `uni_id` string,
	`tenant` string,
    `full_name` string, 
    `first_name` string,
    `last_name` string,
    `gender` string,
    `birth_year` int,
	`birth_month` int,
	`birthday_type` int,
	`birthday` bigint,
	`constellation` int,
    `identity_card` string, 
    `email_suffix` string,
    `is_valid_email` int,
    `email` string,
    `email_markting` int,
	`mobile_zone` string,
	`mobile` string,
    `is_valid_mobile` string, 
    `used_mobile` string,
    `register_mobile` string,
    `custom_mobile` string,
    `bindcard_mobile` string,
	`mobile_markting` int,
	`zip` string,
    `country` string, 
    `state` string,
    `city` string,
    `district` string,
    `town` string,
	`address` string,
	`insert_time` string,
    `most_consumption_shop_id` string,
    `last_consumption_shop_id` string,
    `modified` string,
	`hone_type` int,
    `customer_source` string,
	`customer_from_plat` string,
	`customer_from_shop` string
)
partitioned by(tenantid string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

-- 第一步:创建增量数据中去重后的数据表
DROP TABLE IF EXISTS dw_source.`s_std_customer_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_std_customer_temp`( 
    `uni_id` string,
	`tenant` string,
    `full_name` string, 
    `first_name` string,
    `last_name` string,
    `gender` string,
    `birth_year` int,
	`birth_month` int,
	`birthday_type` int,
	`birthday` bigint,
	`constellation` int,
    `identity_card` string, 
    `email_suffix` string,
    `is_valid_email` int,
    `email` string,
    `email_markting` int,
	`mobile_zone` string,
	`mobile` string,
    `is_valid_mobile` string, 
    `used_mobile` string,
    `register_mobile` string,
    `custom_mobile` string,
    `bindcard_mobile` string,
	`mobile_markting` int,
	`zip` string,
    `country` string, 
    `state` string,
    `city` string,
    `district` string,
    `town` string,
	`address` string,
	`insert_time` string,
    `most_consumption_shop_id` string,
    `last_consumption_shop_id` string,
    `modified` string,
	`hone_type` int,
    `customer_source` string,
	`customer_from_plat` string,
	`customer_from_shop` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

-- 对增量数据进行去重操作后写入临时表
insert into table dw_source.`s_std_customer_temp` 
select uni_id,tenant,full_name,first_name,last_name,gender,birth_year,birth_month,birthday_type,birthday,constellation,
identity_card,email_suffix, is_valid_email,email,email_markting,mobile_zone,mobile,is_valid_mobile,used_mobile,
register_mobile,custom_mobile,bindcard_mobile,mobile_markting,zip,country, state,city,district,town,address,insert_time,
most_consumption_shop_id,last_consumption_shop_id,modified,hone_type,customer_source,customer_from_plat,customer_from_shop 
from (select *,row_number() over (distribute by uni_id,tenant SORT BY modified desc) as num 
from dw_source.`s_std_customer` where dt='${today}') t where t.num=1;

-- 第三步：查询出全量表与本次增量数据的差集，然后和增量数据合并，就完成了会员数据的增加和更新
insert overwrite table dw_base.b_std_customer partition(tenantid)
SELECT 
    uni_id,
    tenant,
    full_name,
    first_name,
    last_name,
    gender,
    birth_year,
	birth_month,
    birthday_type,
    birthday,
    constellation,
    identity_card,
    email_suffix,
    is_valid_email,
    email,
    email_markting,
    mobile_zone,
    mobile,
    is_valid_mobile,
    used_mobile,
    register_mobile,
    custom_mobile,
    bindcard_mobile,
    mobile_markting,
    zip,
    country,
    state,
    city,
    district,
    town,
    address,
    insert_time,
    most_consumption_shop_id,
    last_consumption_shop_id,
    modified,
    hone_type,
    customer_source,
    customer_from_plat,
    customer_from_shop,
    tenant AS tenantid
FROM
    (SELECT 
        t.uni_id,
            t.tenant,
            t.full_name,
            t.first_name,
            t.last_name,
            t.gender,
            t.birth_year,
			t.birth_month,
            t.birthday_type,
            t.birthday,
            t.constellation,
            t.identity_card,
            t.email_suffix,
            t.is_valid_email,
            t.email,
            t.email_markting,
            t.mobile_zone,
            t.mobile,
            t.is_valid_mobile,
            t.used_mobile,
            t.register_mobile,
            t.custom_mobile,
            t.bindcard_mobile,
            t.mobile_markting,
            t.zip,
            t.country,
            t.state,
            t.city,
            t.district,
            t.town,
            t.address,
            t.insert_time,
            t.most_consumption_shop_id,
            t.last_consumption_shop_id,
            t.modified,
            t.hone_type,
            t.customer_source,
            t.customer_from_plat,
            t.customer_from_shop
    FROM
        (SELECT 
        a.*
    FROM
        dw_base.`b_std_customer` a
    LEFT OUTER JOIN dw_source.`s_std_customer_temp` b 
	ON (a.uni_id = b.uni_id AND a.tenant = b.tenant)
    WHERE
        b.uni_id IS NULL AND b.tenant IS NULL) t UNION ALL SELECT 
        uni_id,
            tenant,
            full_name,
            first_name,
            last_name,
            gender,
            birth_year,
			birth_month,
            birthday_type,
            birthday,
            constellation,
            identity_card,
            email_suffix,
            is_valid_email,
            email,
            email_markting,
            mobile_zone,
            mobile,
            is_valid_mobile,
            used_mobile,
            register_mobile,
            custom_mobile,
            bindcard_mobile,
            mobile_markting,
            zip,
            country,
            state,
            city,
            district,
            town,
            address,
            insert_time,
            most_consumption_shop_id,
            last_consumption_shop_id,
            modified,
            hone_type,
            customer_source,
            customer_from_plat,
            customer_from_shop
    FROM
        dw_source.`s_std_customer_temp`) re;

-- 删除中间临时表
DROP TABLE IF EXISTS dw_source.`s_std_customer_temp`;
