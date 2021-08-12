//source change_spark_version spark-2.3.3.2
//spark-shell --master yarn --executor-memory 16g --num-executors 100 --executor-cores 4 --driver-memory 16g 
//  --conf spark.ui.port=$[$RANDOM%1000 + 8000] --conf spark.driver.extraJavaOptions="-Dscala.color" 
//  --conf spark.dynamicAllocation.enabled=false --conf spark.sql.crossJoin.enabled=true 
//  --conf spark.sql.broadcastTimeout=360000 
//  --jars Heqiao_Ruan/anti-money-launder-address-standardize-1.0.0.jar  

//反洗钱规则打捞分析

import org.apache.spark.sql.types.{StringType, DoubleType, IntegerType, LongType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.suning.usf.amlas.ap.AddressParser  
import com.suning.usf.amlas.Parser.standardize
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "150")

//主表落库+是否失业信息+年龄
//fbicsi.T_BICDT_TPQR_PIM_PB01A_D_model 人行征信，表源后续存在停更可能，需申请fdm_dpa权限
spark.sql("drop table if exists usfinance.peng_20210805_aml_kyds_mainTB")

spark.sql("drop table if exists usfinance.aml_kyds_mainTB")
spark.sql("""
create table usfinance.aml_kyds_mainTB as
select acct_no,id_card,user_age,rgst_time,member_id,mbl,adrs,user_type,if_unemploy
from (select * from finance.mls_member_info_all where length(id_card) = 18)
left join 
(
select id_num as id_card,1.0 as if_unemploy
from fbicsi.T_BICDT_TPQR_PIM_PB01A_D_model
where length(id_num) = 18 and pb01ad04 = '70'
)
using (id_card)
where user_age is not null and user_age > 0 and rgst_time > DATE_SUB(CURRENT_DATE(),180)
--只看半年以内的
""")

//关联流水表
spark.sql("select * from usfinance.aml_kyds_mainTB").dropDuplicates("acct_no").join(
spark.sql("""
select substr(ctac,1,19) as acct_no,tr_tm,tr_amt,rcv_pay,tr_bal_amt,tr_cny_amt,cross_flag,stat_date, fund_use
from fdm_dpa.s022_snzf_t2a_trans_d
cross join (select max(stat_date) as latest_date from fdm_dpa.s022_snzf_t2a_trans_d)
where stat_date >= DATE_SUB(CONCAT(SUBSTR(latest_date,1,4),'-',SUBSTR(latest_date,5,2),'-',SUBSTR(latest_date,7,2)),90)
and length(ctac) > 19
"""),Seq("acct_no"),"left"
).write.mode("overwrite").saveAsTable("usfinance.aml_kyds_mainTB_withliushui")

//KYDS-2:客户高低龄:客户年龄是否大于60岁或小于21岁

//KYDS-3: 客户是否无业: 客户职业是否为无业、待业、离退休

//KYDS-4: 交易笔数激增
//3个月前开户且前推两个月交易笔数，是否除以本月交易笔数均小于20%；

//KYDS-5: 交易金额激增
//3个月前开户且前推两个月交易金额折RMB合计，是否除以本月交易金额折RMB合计均小于20%；

//KYDS-6
//休眠账户活动:3个月前开户且前推两个月交易笔数，是否除以本月交易笔数小于0.05；

//KYDS-7
//交易金额过大:本月交易金额折RMB合计是否大于200000元；
//当月交易金额过大--- 折合人民币超过20万: 入账 + 出账 合计20万以上.


//KYDS2-7
spark.sql("""
select 
*,
case 
when tr_tm>DATE_SUB(latest_date,90) and tr_tm<=DATE_SUB(latest_date,60) then '1'
when tr_tm>DATE_SUB(latest_date,60) and tr_tm<=DATE_SUB(latest_date,30) then '2'
when tr_tm>DATE_SUB(latest_date,30) and tr_tm<=DATE_SUB(latest_date,0) then '3'
end as month,
if(rgst_time < DATE_SUB(CURRENT_DATE(),90),1,0) as is_over_90,
case
when user_age <= 15 then 10.0
when user_age >15 and user_age <18 then 7.0
when user_age >=18 and user_age <= 20 then 3.0
else 0.0 end as kyds2,
if(if_unemploy is null,0,10.0) as kyds3
from usfinance.aml_kyds_mainTB_withliushui
""").
groupBy("acct_no","month").agg(
 count(lit(1)).alias("tr_cnt"),
 sum($"tr_amt").alias("tr_amt"),
 max("is_over_90").alias("is_over_90"),
 max("kyds2").alias("kyds2"),
 max("kyds3").alias("kyds3")
).groupBy("acct_no").agg(
  max("kyds2").alias("kyds2"), 
  max("kyds3").alias("kyds3"),
  max("is_over_90").alias("is_over_90"),
  (sum(when($"month"==="1",$"tr_cnt").otherwise(0.0)) / (sum(when($"month"==="3",$"tr_cnt").otherwise(0.0)) + 0.000001)).
  alias("first_over_now_cnt"),
  (sum(when($"month"==="2",$"tr_cnt").otherwise(0.0)) / (sum(when($"month"==="3",$"tr_cnt").otherwise(0.0)) + 0.000001)).
  alias("second_over_now_cnt"),
  (sum(when($"month"==="1",$"tr_amt").otherwise(0.0)) / (sum(when($"month"==="3",$"tr_amt").otherwise(0.0)) + 0.000001)).
  alias("first_over_now_amt"),
  (sum(when($"month"==="2",$"tr_amt").otherwise(0.0)) / (sum(when($"month"==="3",$"tr_amt").otherwise(0.0)) + 0.000001)).
  alias("second_over_now_amt"),
  (sum(when($"month"==="3",$"tr_amt").otherwise(0.0))).alias("current_month_amt")
).
withColumn("kyds4",when($"first_over_now_cnt"<0.2 && $"second_over_now_cnt"<0.2 && $"is_over_90">0,5.0).otherwise(0.0)).
withColumn("kyds5",when($"first_over_now_amt"<0.2 && $"second_over_now_amt"<0.2 && $"is_over_90">0,5.0).otherwise(0.0)).
withColumn("kyds6",when($"first_over_now_cnt">20 && $"second_over_now_cnt">20 && $"is_over_90">0,5.0).otherwise(0.0)).
withColumn("kyds7",when($"current_month_amt">200000 && $"current_month_amt"<500000,7.0).otherwise(
  when($"current_month_amt">=500000,10.0).otherwise(0.0))).
  select("acct_no","kyds2","kyds3","kyds4","kyds5","kyds6","kyds7").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_2_to_7")


//KYDS-9:
//过渡资金:本月借方交易金额合计除以本月贷方交易金额合计是否在0.95-1.05之间；
//借方: $"rcv_pay"==="02", out
//贷方: $"rcv_pay"==="01", in

//KYDS-10:
//分散转入集中转出:本月贷方交易笔数除以本月借方交易笔数是否大于3

var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")

var B = spark.sql("""select * from usfinance.aml_kyds_mainTB_withliushui
where tr_tm > DATE_SUB(CURRENT_DATE(),30) and tr_tm <= DATE_SUB(CURRENT_DATE(),0)
""").groupBy("acct_no").agg(
  (count(when($"rcv_pay"==="01",1.0)) / count(when($"rcv_pay"==="02",1.0))).alias("in_over_out_count"),
  (sum(when($"rcv_pay"==="02",$"tr_amt").otherwise(0.0)) /
  sum(when($"rcv_pay"==="01",$"tr_amt").otherwise(0.0))+0.000001).alias("out_over_in_amt")
).withColumn("kyds9",when($"out_over_in_amt">0.95 && $"out_over_in_amt"<1.05,5.0).otherwise(0.0)).
withColumn("kyds10",when($"in_over_out_count">3,3.0).otherwise(0.0))

A = A.join(B,Seq("acct_no"),"left").select("acct_no","kyds9","kyds10").
withColumn("kyds9",when($"kyds9".isNull,0.0).otherwise($"kyds9")).
withColumn("kyds10",when($"kyds10".isNull,0.0).otherwise($"kyds10"))

A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_9_10")


//kyds8和19
//关联商户信息
spark.sql("drop table if exists usfinance.aml_kyds_mainTBwithvendor")
spark.sql("""
create table usfinance.aml_kyds_mainTBwithvendor as
select *
from usfinance.aml_kyds_mainTB
left join (select distinct acct_no,cmpy_no as vendor_cd from fbicsi.yc_taoxian04)
using (acct_no)
""")
//近期全量交易信息OMS
spark.sql("""
select
memb_id,vendor_cd,pay_time,pay_tp_aray,statis_date,pay_amt,bill_tp_cd
from BROCK_DWD.T_ORD_RETAIL_GRP_ORD_DTL_D
where statis_date > CONCAT(SUBSTR(DATE_SUB(CURRENT_DATE(),100),1,4),
                          SUBSTR(DATE_SUB(CURRENT_DATE(),100),6,2),
                          SUBSTR(DATE_SUB(CURRENT_DATE(),100),9,2))
    and pay_time is not null
""").withColumn("pay_name", regexp_extract($"pay_tp_aray", "pay_name\":(.*?),\"", 1)).drop("pay_tp_aray").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_withOMS")
spark.sql("""
select *,
case 
  when pay_name rlike '.*微信.*' then '微信'
  when pay_name rlike '.*现金.*' then '现金'
  when pay_name rlike '.*支付宝.*' then '支付宝'
  when pay_name rlike '.*JLF.*' then '家乐福'
  when pay_name rlike '.*家乐福.*' then '家乐福'
  when pay_name rlike '.*银行.*' then '银行'
  when pay_name rlike '.*苏宁.*' then '苏宁'
  when pay_name rlike '.*易付宝.*' then '易付宝'
  when pay_name rlike '.*银联.*' then '银联'
  when pay_name rlike '.*任性付.*' then '任性付'
  when pay_name rlike '.*促销.*' then '促销'  
  when pay_name rlike '.*折扣.*' then '折扣'  
  else 'other' end as pay_platform
from
usfinance.aml_kyds_withOMS
""").drop("pay_name").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_withOMS1")
//把商户户头号和购买者户头号找出来
//vendor_cd是卖家的编号
//memb_id是买家的编号
spark.sql("select * from usfinance.aml_kyds_withOMS1").join(
  spark.sql("select distinct acct_no,vendor_cd from usfinance.aml_kyds_mainTBwithvendor where vendor_cd is not null"),
  Seq("vendor_cd")
).join(
  spark.sql("""
  select distinct acct_no as opp_acct,member_id as memb_id from usfinance.aml_kyds_mainTBwithvendor where member_id is not null
  """),
  Seq("memb_id")
).write.mode("overwrite").saveAsTable("usfinance.aml_kyds_withOMS2")


//KYDS-8: 本月交易对手大于30个
//本月交易对手数量
//30-50个，系数0.3得分3分；
//50个以上，系数0.7得分7分；
spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui").join(
spark.sql("""
select acct_no,count(distinct opp_acct) as unique_opp_cnt
from usfinance.peng_20210805_aml_kyds_withOMS2
where pay_time >= DATE_SUB(CURRENT_DATE(),35) --etl更新可能有延迟，留出5天缓冲
group by acct_no
""").
withColumn("kyds8",when($"unique_opp_cnt">30 && $"unique_opp_cnt" < 50,3.0).otherwise(
when($"unique_opp_cnt" >= 50, 7.0).otherwise(0.0))).select("acct_no","kyds8"),
Seq("acct_no"),"left"
).withColumn("kyds8",when($"kyds8".isNull,0.0).otherwise($"kyds8")).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_8")

//KYDS-19: 贷方交易交易对手为除微信、支付宝外第三方、四方平台笔数占比超过50%，金额占比超过70%
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("select * from usfinance.aml_kyds_withOMS2").filter($"bill_tp_cd"==="1").groupBy("acct_no").agg(
  (avg(when($"pay_platform"==="other",1).otherwise(0.0))).
  alias("non_famous_plat_ratio_cnt"),
  (sum(when($"pay_platform"==="other",$"pay_amt").otherwise(0.0)) / (sum($"pay_amt")+0.00001)).
  alias("non_famous_plat_ratio_amt")
).withColumn("kyds19",when($"non_famous_plat_ratio_cnt">0.5 && $"non_famous_plat_ratio_amt">0.7,10).otherwise(0.0))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds19",when($"kyds19".isNull,0.0).otherwise($"kyds19"))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_19")


//kyds11
//相同联系电话客户多: 相同联系电话客户大于3个同时小于10个
//共用联系电话客户:
spark.sql("select acct_no,mbl from usfinance.aml_kyds_mainTB").dropDuplicates("acct_no").join(
  spark.sql("select mbl,id_card from finance.mls_member_info_all").distinct(),Seq("mbl"),"left"
).groupBy("acct_no").agg(countDistinct("id_card").alias("shared_mbl_cnt")).
withColumn("kyds11",when($"shared_mbl_cnt">3 && $"shared_mbl_cnt"<10,3.0).otherwise(0.0)).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_11")

//kyds12:
//这里地址已经经过标准化
val resDf:DataFrame = standardize(spark, 
spark.table("usfinance.aml_kyds_mainTB").dropDuplicates("acct_no"), 
"adrs", "standardizedAddress").
drop("adrs").
withColumnRenamed("standardizedAddress", "address")

val shareAddr = resDf.
filter($"address".isNotNull).
groupBy("address").agg(
countDistinct($"acct_no").as("sharedAddrAcctCount")
)

shareAddr.join(resDf.select("acct_no", "address").distinct, Seq("address"),"right").
select("acct_no", "address", "sharedAddrAcctCount").distinct.
groupBy("acct_no").agg(
max($"sharedAddrAcctCount").as("maxsharedAddrAcctCount")
).withColumn("kyds12",when($"maxsharedAddrAcctCount">3 && $"maxsharedAddrAcctCount"<10,3.0).otherwise(0.0)).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_12")


//KYDS-16:小额测试:
//当月前若干笔交易中是否含有交易金额为10元以下整数及0.1，0.2元
//当月前若干笔交易金额: --- 同时包含出金和入金
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("""
select *,
row_number() over (partition by acct_no order by tr_tm) as rn
from 
(
select * from usfinance.aml_kyds_mainTB_withliushui where 
CONCAT(SUBSTR(stat_date,1,4),'-',SUBSTR(stat_date,5,2)) >= substr(CURRENT_DATE(),1,7)
)
""").withColumn("is_int",when($"tr_amt" === $"tr_amt".cast("Int"),1).otherwise(0.0)).
withColumn("int_under_10",when($"tr_amt"<=10 && $"is_int">0,1).otherwise(0.0)).
groupBy("acct_no").agg(
  max(when($"rn"<=3 && $"int_under_10">0,7.0).otherwise(0.0)).alias("kyds16_1"),
  max(when($"rn"<=10 && $"int_under_10">0,3.0).otherwise(0.0)).alias("kyds16_2")
).withColumn("kyds16",when($"kyds16_1">0,$"kyds16_1").otherwise($"kyds16_2"))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds16",when($"kyds16".isNull,0.0).otherwise($"kyds16"))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_16")

//kyds18:跨境转账
//借方交易交易对手为境外账户笔数占比超过50%，金额占比超过70%
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("""
select *,
if(cross_flag='1',1.0,0.0) as is_cross
from usfinance.aml_kyds_mainTB_withliushui
""").filter($"tr_tm".isNotNull && $"rcv_pay"==="02").
groupBy($"acct_no").agg(
  avg($"is_cross").alias("cross_cnt_ratio"),
  (sum($"is_cross"*$"tr_amt") / (sum($"tr_amt")+0.00001)).alias("cross_amt_ratio")
)
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds18",
when(($"cross_cnt_ratio">0.5 && $"cross_cnt_ratio".isNotNull) || ($"cross_amt_ratio">0.7 && $"cross_amt_ratio".isNotNull),15.0).
otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_18")


//kyds20:折RMB整数跨境转入
//贷方交易中存在境外转入交易金额折RMB为整数如5000，10000等，且笔数超过5笔或金额大于30000
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("""
select *,
if(cross_flag='1',1.0,0.0) as is_cross,
if(tr_cny_amt is not null,tr_cny_amt,tr_amt) as tr_amt_yuan
from usfinance.aml_kyds_mainTB_withliushui
""").filter($"tr_tm".isNotNull && $"rcv_pay"==="01").
withColumn("is_int",when($"tr_amt_yuan" === $"tr_amt_yuan".cast("Int"),1).otherwise(0.0)).
withColumn("res",when($"tr_amt_yuan">100,$"tr_amt_yuan".cast("Int") % 100).otherwise($"tr_amt_yuan".cast("Int") % 10)).
groupBy($"acct_no").agg(
  (sum(when($"is_int">0 && $"res"===0 && $"is_cross">0,1.0).otherwise(0.0))).alias("cross_int_cnt"),
  (sum(when($"is_int">0 && $"res"===0 && $"is_cross">0,$"tr_amt").otherwise(0.0))).alias("cross_int_amt")
)
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds20",
when(($"cross_int_cnt">5 && $"cross_int_cnt".isNotNull) || ($"cross_int_amt">50000 && $"cross_int_amt".isNotNull),15.0).
otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_20")

//kyds21:交易IP异常
//交易IP地址不在中国大陆境内
//关联IP地址
//我们以机器指纹表为例，取出pay_ip以演示与下面的ip归属地进行关联
var mf = sql(""" 
SELECT account_no as acct_no,pay_ip,statisdate as stat_date
FROM fdm_sor.sor_csi_t_machine_finger_record
where statisdate > CONCAT(SUBSTR(DATE_SUB(CURRENT_DATE(),100),1,4),
                          SUBSTR(DATE_SUB(CURRENT_DATE(),100),6,2),
                          SUBSTR(DATE_SUB(CURRENT_DATE(),100),9,2))
""")
var ip_location = spark.sql("SELECT * FROM fdm_sor.sor_ip_location").
withColumn("start_ip1", substring($"ip_start", 1, 3).cast(IntegerType)).
withColumn("start_ip2", substring($"ip_start", 4, 3).cast(IntegerType)).
withColumn("start_ip3", substring($"ip_start", 7, 3).cast(IntegerType)).
withColumn("end_ip2", substring($"ip_end", 4, 3).cast(IntegerType)).
withColumn("end_ip3", substring($"ip_end", 7, 3).cast(IntegerType)).
drop("ip_owner", "ip_operator", "ip_start", "ip_end", "timezone_city",
"timezone", "country_code", "state_code", "inter_code", "admin_code")
var mf_ip = mf.
withColumn("ip1", split(col("pay_ip"), "\\.").getItem(0)).
withColumn("ip2", split(col("pay_ip"), "\\.").getItem(1)).
withColumn("ip3", split(col("pay_ip"), "\\.").getItem(2)).
withColumn("ip4", split(col("pay_ip"), "\\.").getItem(3))
mf_ip.
join(ip_location, mf_ip.col("ip1") === ip_location.col("start_ip1") &&
mf_ip.col("ip2") === ip_location.col("start_ip2") &&
mf_ip.col("ip3") >= ip_location.col("start_ip3") &&
mf_ip.col("ip3") <= ip_location.col("end_ip3")).
filter($"ip_country" =!= "中国").select("acct_no","stat_date").distinct().
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_foreign_ip")
spark.sql("select *,1.0 as oversea_ip from usfinance.aml_kyds_foreign_ip").join(
  spark.sql("select * from usfinance.aml_kyds_mainTB_withliushui"),Seq("acct_no","stat_date"),"right"
).groupBy("acct_no").agg(max(when($"oversea_ip".isNotNull,15.0).otherwise(0.0)).alias("kyds21")).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_21")


//kyds22 借方交易对手疑似空壳公司
//借方交易对公交易对手成立日期在三个月之内，且金额占比超过50%
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.table("usfinance.aml_kyds_withOMS2").filter($"bill_tp_cd"==="1").join(
  spark.sql("""
  select acct_no,rgst_time from usfinance.aml_kyds_mainTB 
  where user_type != 'PERSON' and rgst_time > DATE_SUB(CURRENT_DATE(),90)
  """).dropDuplicates("acct_no").withColumn("is_recent",lit(1.0)),
  Seq("acct_no"),"left"
).groupBy("opp_acct").agg(
  (sum(when($"is_recent".isNotNull,$"pay_amt").otherwise(0.0)) / (sum($"pay_amt")+0.00001)).alias("kyds22")
).withColumn("kyds22",when($"kyds22">=0.5,20.0).otherwise(0.0)).withColumnRenamed("opp_acct","acct_no")
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds22",when($"kyds22".isNotNull,$"kyds22").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_22")

//kyds23:借方交易对公交易对手过渡资金
//借方交易对公交易对手资金快进快出，当日清零
//这里理解快进快出是（进+出）> 平均余额的10倍，且进出绝对差值小于平均余额的1%
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("select * from usfinance.aml_kyds_mainTB_withliushui").filter($"tr_tm".isNotNull && $"user_type"=!="PERSON").
groupBy("acct_no").agg(
  sum(when($"rcv_pay"==="01",$"tr_amt").otherwise(0.0)).alias("x1"),
  sum(when($"rcv_pay"==="02",$"tr_amt").otherwise(0.0)).alias("x2"),
  avg($"tr_bal_amt").alias("y")
  ).filter(abs($"x1"-$"x2")<$"y"*0.01 && abs($"x1"+$"x2") > $"y"*100).select("acct_no").withColumn("kyds23",lit(20.0))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds23",when($"kyds23".isNotNull,15.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_23")

//kyds24:交易时间异常
//交易时间集中在深夜凌晨23：00-5：00的笔数占比30%
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("""
select *,
if(substr(tr_tm,12,2) in ('23','00','01','02','03','04'),1.0,0.0) as is_night
from usfinance.aml_kyds_mainTB_withliushui
""").filter($"tr_tm".isNotNull).
groupBy($"acct_no").agg(avg($"is_night").alias("night_ratio"))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds24",when($"night_ratio">0.3 && $"night_ratio".isNotNull,15.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_24")


//kyds25:交易金额特殊
//交易金额中10，100的倍数占比30%以上
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("select * from usfinance.aml_kyds_mainTB_withliushui").filter($"tr_amt".isNotNull).
withColumn("is_int",when($"tr_amt" === $"tr_amt".cast("Int"),1).otherwise(0.0)).
withColumn("res",when($"tr_amt">100,$"tr_amt".cast("Int") % 100).otherwise($"tr_amt".cast("Int") % 10)).
withColumn("like_bet_amt",when($"is_int"===1 && $"res"===0,1.0).otherwise(0.0)).groupBy($"acct_no").
agg((avg($"like_bet_amt")).alias("like_bet_ratio"))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds25",when($"like_bet_ratio">0.3,10.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_25")

//Heqiao-0812-
//kyds26: 交易附言中含有“大张”（日元）、“小张”（美元）、“矮子”（日元）、
//数字后面有“张”（万），“条”（10万），“粒”（百万）等字样:
//此处指标计算方式为: 过去30天fund_use字段内含有大张/小张/矮子等的账户列表且数字后面含有
var A = spark.sql("""select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui """)
val B = spark.sql("""
select fund_use, acct_no, tr_tm from usfinance.aml_kyds_mainTB_withliushui
where fund_use rlike '.*[0-9]+条.*' or fund_use rlike '.*[0-9]+粒'
or fund_use rlike '.*[0-9]+张.*' or fund_use rlike '大张' or fund_use rlike '小张'
or fund_use rlike '矮子'
""").
filter($"tr_tm".isNotNull).
groupBy("acct_no").agg(
count($"fund_use").as("past3MAbnormalNotesCount")
)
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds26", when($"past3MAbnormalNotesCount" > 0.0, 15.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_26")

//kyds27: 借方交易对手:
//名称含有“贸易”、“咨询”，“投资”；AND 注册资本小于10万元；AND 法人个人控股；AND 开户地址与企业注册地不同
var A = spark.sql("""select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui""")
//使用城市
//问题: 1. 供应商信息表中的rgst_cptl字段 部分不准确
//2. 法人个人控股这一点 从现有数据源中暂时无法获取 故暂不纳入
//3. 开户地址部分不规范（不是标准化格式地址）故模糊处理。
val B = spark.sql("""
select *
from
(select *
from 
(select *
from 
(select vendor_nm as cmpy_name, rgst_cptl from bi_sor.tsor_vendor_base_info_d) A1
join
(select cmpy_name, id_card, credit_no, cmpy_adrs, city_name from fbicsi.yc_taoxian06) A2
using (cmpy_name)) A3
join
(select id_card, acct_no, adrs from finance.mls_member_info_all) A4
using (id_card)
) A5
join 
(select distinct acct_no, tr_tm from usfinance.aml_kyds_mainTB_withliushui
where tr_tm is not null) A6 
using (acct_no)
""").
withColumn("cityAcct", split(col("adrs"), "\\-").getItem(1).cast(StringType)).//开户城市
filter($"rgst_cptl" < 10).filter($"cmpy_name".contains("贸易") || $"cmpy_name".contains("咨询") || $"cmpy_name".contains("投资")).
filter(! $"cmpy_adrs".contains("cityAcct")).select("acct_no").distinct.withColumn("kyds27Score", lit(1.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds27", when($"kyds27Score" > 0, 5.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_27")

spark.sql("""
select acct_no,kyds2,kyds3,kyds4,kyds5,kyds6,kyds7,kyds8,kyds9,
kyds10,kyds11,kyds12,kyds16,kyds18,kyds19,kyds20,kyds21,kyds22,kyds23,kyds24,kyds25,
kyds26,kyds27
from (select distinct acct_no from usfinance.aml_kyds_mainTB) 
left join usfinance.aml_kyds_2_to_7
using (acct_no)
left join usfinance.aml_kyds_8
using (acct_no)
left join usfinance.aml_kyds_19
using (acct_no)
left join usfinance.aml_kyds_9_10
using (acct_no)
left join usfinance.aml_kyds_11
using (acct_no)
left join usfinance.aml_kyds_12
using (acct_no)
left join usfinance.aml_kyds_16
using (acct_no)
left join usfinance.aml_kyds_18
using (acct_no)
left join usfinance.aml_kyds_20
using (acct_no)
left join usfinance.aml_kyds_21
using (acct_no)
left join usfinance.aml_kyds_22
using (acct_no)
left join usfinance.aml_kyds_23
using (acct_no)
left join usfinance.aml_kyds_24
using (acct_no)
left join usfinance.aml_kyds_25
using (acct_no)
left join usfinance.aml_kyds_26
using (acct_no)
left join usfinance.aml_kyds_27
using (acct_no)
""").write.mode("overwrite").saveAsTable("usfinance.aml_kyds_20210808")

//跨境赌博
spark.sql("""
select * from
(
select *,
kyds2+kyds3+kyds4+kyds5+kyds6+kyds7+kyds8+kyds9+
kyds10+kyds11+kyds12+kyds16+kyds18+kyds19+kyds20+kyds21+kyds24+kyds25 as kjdb_score
from usfinance.aml_kyds_20210808
)
order by kjdb_score desc 
limit 30
""").join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all").dropDuplicates("acct_no"),Seq("acct_no")).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
select("acct_no","id_card","name","credit_no","kjdb_score","kyds2","kyds3","kyds4","kyds5",
"kyds6","kyds7","kyds8","kyds9","kyds10","kyds11","kyds12","kyds16",
"kyds18","kyds19","kyds20","kyds21","kyds24","kyds25").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_kjdb_cases")

//地下钱庄Heqiao-0812
spark.sql("""
select * from 
(
  select *,
  kyds4+kyds5+kyds6+kyds7+kyds8+kyds9+
kyds10+kyds22+kyds23+kyds26+kyds27 as dxqz_score
from usfinance.aml_kyds_20210808
)
order by dxqz_score desc
limit 30
""").join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all").dropDuplicates("acct_no"),Seq("acct_no")).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
select("acct_no","id_card","name","credit_no","dxqz_score","kyds4","kyds5","kyds6","kyds7",
"kyds8","kyds9","kyds10","kyds22","kyds23", "kyds26", "kyds27").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_dxqz_cases")

