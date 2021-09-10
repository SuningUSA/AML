//source change_spark_version spark-2.3.3.2
//spark-shell --master yarn --executor-memory 16g --num-executors 40 --executor-cores 4 --driver-memory 16g --conf spark.ui.port=$[$RANDOM%1000 + 8000] --conf spark.driver.extraJavaOptions="-Dscala.color"  --conf spark.dynamicAllocation.enabled=false --conf spark.sql.crossJoin.enabled=true --conf spark.sql.broadcastTimeout=360000  --jars Heqiao_Ruan/anti-money-launder-address-standardize-1.0.0.jar  
//
//反洗钱规则打捞分析

import org.apache.spark.sql.types.{StringType, DoubleType, IntegerType, LongType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array_contains,col}
import com.suning.usf.amlas.ap.AddressParser  
import com.suning.usf.amlas.Parser.standardize
import com.suning.usf.sm4.{HdfsUtils, SM4Utils, StrTo16}
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "120")
val cur_dat = java.time.LocalDate.now 
val current_date = cur_dat.minusDays(2).toString
val past3d = cur_dat.minusDays(3).toString
val past3dStatDate = past3d.substring(0, 4) + past3d.substring(5, 7) + past3d.substring(8, 10)


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

//更新逻辑: 解密

//主表落库+是否失业信息+年龄
//解密函数:
val key_S019_BICDT_TPQR_PIM_PB01A_ED:String = getKey("JMX2011124IxilE","-1")
val decryptUdf_S019_BICDT_TPQR_PIM_PB01A_ED: UserDefinedFunction = udf((input: String) => {
if (input == null) null
else SM4Utils.decryptData_CBC(input, key_S019_BICDT_TPQR_PIM_PB01A_ED)
})

//fbicsi.T_BICDT_TPQR_PIM_PB01A_D_model 人行征信，表源后续存在停更可能，需申请fdm_dpa权限
spark.sql("drop table if exists usfinance.peng_20210805_aml_kyds_mainTB")
spark.table("fdm_dpa.S019_BICDT_TPQR_PIM_PB01A_ED").
withColumn("id_card", decryptUdf_S019_BICDT_TPQR_PIM_PB01A_ED($"id_num_encry")).
write.mode("overwrite").saveAsTable("usfinance.heqiao_210908_aml_rhDecry_temp")
spark.sql("drop table if exists usfinance.aml_kyds_mainTB")
spark.sql("""
create table usfinance.aml_kyds_mainTB as
select acct_no,id_card,user_age,rgst_time,member_id,mbl,adrs,user_type,if_unemploy
from (select * from finance.mls_member_info_all where length(id_card) = 18)
left join 
(
select id_card as id_card,1.0 as if_unemploy
from usfinance.heqiao_210908_aml_rhDecry_temp
where length(id_card) = 18 and pb01ad04 = '70'
)
using (id_card)
where user_age is not null and user_age > 0 and rgst_time > DATE_SUB(CURRENT_DATE(),180)
--只看半年以内的
""")


//关联流水表
//08-19: 加入客户编号字段
//09-07:加入opp_acct_id, tcta字段
spark.sql("drop table if exists usfinance.aml_kyds_mainTB_withliushui")

spark.sql("select * from usfinance.aml_kyds_mainTB").dropDuplicates("acct_no").join(
spark.sql("""
select substr(ctac,1,19) as acct_no,tr_tm,tr_amt,rcv_pay,tr_bal_amt,tr_cny_amt,cross_flag,
stat_date, fund_use, cust_id, opp_cust_id, latest_date, cttp, 
case when length(opp_acct_id) > 0 then opp_acct_id end as opp_acct_id,
case when length(tcta) > 0 then tcta end as tcta
from 
(select * from fdm_dpa.s022_snzf_t2a_trans_d where tr_tm is not null) A1
cross join 
(select max(stat_date) as latest_date from fdm_dpa.s022_snzf_t2a_trans_d) A2
where A1.stat_date >= DATE_SUB(CONCAT(SUBSTR(A2.latest_date,1,4),'-',SUBSTR(A2.latest_date,5,2),'-',SUBSTR(A2.latest_date,7,2)),90)
and length(A1.ctac) > 19
"""),Seq("acct_no"),"left"
).write.mode("overwrite").saveAsTable("usfinance.aml_kyds_mainTB_withliushui")

//09-07: 替代易购OMS表，在关联后的流水表里加入pay_platform字段 + 交易对手账户字段,
//从流水表的fund_use字段提取支付平台信息(相当于OMS表里的pay_tp_aray字段）
spark.sql("drop table if exists usfinance.yinyin_210901_mainTB_withliushui2_tmp")
spark.sql("""
select *,
(case
  when fund_use rlike '.*微信.*' then '微信'
  when fund_use rlike '.*现金.*' then '现金'
  when fund_use rlike '.*支付宝.*' then '支付宝'
  when fund_use rlike '.*JLF.*' then '家乐福'
  when fund_use rlike '.*家乐福.*' then '家乐福'
  when fund_use rlike '.*银行.*' then '银行'
  when fund_use rlike '.*苏宁.*' then '苏宁'
  when fund_use rlike '.*易付宝.*' then '易付宝'
  when fund_use rlike '.*银联.*' then '银联'
  when fund_use rlike '.*任性付.*' then '任性付'
  when fund_use rlike '.*促销.*' then '促销'
  when fund_use rlike '.*折扣.*' then '折扣'
  else 'other' end) as pay_platform,
(case
  when tcta is not null then substr(tcta,1,19)
  when tcta is null and opp_acct_id is not null then opp_acct_id
  end) as opp_acct
from
usfinance.aml_kyds_mainTB_withliushui
""").write.mode("overwrite").saveAsTable("usfinance.yinyin_210901_mainTB_withliushui2_tmp")

//kyds1:是否当月开户: 命中得10分
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
val B = spark.sql("""
select acct_no, user_type, rgst_time from finance.mls_member_info_all where length(id_card) = 18
""").groupBy("acct_no").agg(
  max($"rgst_time").as("rgst_time")
).filter(datediff(lit(current_date), $"rgst_time") <= 31).
select("acct_no").distinct.withColumn("kyds1", lit(10.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds1", when($"kyds1".isNull, 0.0).otherwise($"kyds1"))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_1")

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

//将所有账户和账户类型进行存储
spark.sql("""
select acct_no, user_type from finance.mls_member_info_all where length(id_card) = 18
""").write.mode("overwrite").saveAsTable("usfinance.heqiao_210819_acctType_tmp")

//08-19 --- 修改kyds-7
spark.sql("""
select 
*,
case 
when tr_tm> DATE_SUB(CURRENT_DATE(), 93) and tr_tm<= DATE_SUB(CURRENT_DATE(), 62) then '1'
when tr_tm> DATE_SUB(CURRENT_DATE(), 62) and tr_tm<= DATE_SUB(CURRENT_DATE(), 31) then '2'
when tr_tm> DATE_SUB(CURRENT_DATE(), 31)  then '3'
end as month,
if(rgst_time < DATE_SUB(CURRENT_DATE(),93),1,0) as is_over_90,
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
  (sum(when($"month"==="3",$"tr_amt").otherwise(0.0))).alias("current_month_amt"),
  sum($"tr_amt").as("acctTransactionAmnt"),
  sum($"tr_cnt").as("acctTransactionCount")
).
withColumn("kyds4",when($"first_over_now_cnt"<0.2 && $"second_over_now_cnt"<0.2 && $"is_over_90">0,5.0).otherwise(0.0)).
withColumn("kyds5",when($"first_over_now_amt"<0.2 && $"second_over_now_amt"<0.2 && $"is_over_90">0,5.0).otherwise(0.0)).
withColumn("kyds6",when($"first_over_now_cnt">20 && $"second_over_now_cnt">20 && $"is_over_90">0,5.0).otherwise(0.0)).
join(spark.table("usfinance.heqiao_210819_acctType_tmp"), Seq("acct_no")).
  select("acct_no","kyds2","kyds3","kyds4","kyds5","kyds6", "current_month_amt", "user_type",
"acctTransactionAmnt", "acctTransactionCount").distinct.
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_2to6_V1")

//kyds-7 修改分析:
val kyds7_P1 = spark.table("usfinance.aml_kyds_2to6_V1").
filter($"user_type" === "CORPORAT").
withColumn("kyds7", when($"current_month_amt" < 100000, -(100.0)).
otherwise(when($"current_month_amt" <= 500000 && $"current_month_amt" > 200000, 3.0).
otherwise(when($"current_month_amt" >= 500000, 10.0).otherwise(0.0))))

val kyds7_P2 = spark.table("usfinance.aml_kyds_2to6_V1").
filter($"user_type" === "PERSON").
withColumn("kyds7", when($"current_month_amt" < 20000, -(100.0)).
otherwise(when($"current_month_amt" <= 100000 && $"current_month_amt" > 50000, 3.0).
otherwise(when($"current_month_amt" >= 100000, 10.0).otherwise(0.0))))

kyds7_P1.union(kyds7_P2).
select("acct_no", "kyds2", "kyds3", "kyds4", "kyds5", "kyds6", "kyds7",
 "acctTransactionAmnt", "acctTransactionCount").distinct.
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_2_to_7")

//KYDS-10:
//分散转入集中转出:本月贷方交易笔数除以本月借方交易笔数是否大于3
//本月，均以过去31天进行计数
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")

var B = spark.sql("""select * from usfinance.aml_kyds_mainTB_withliushui
where tr_tm > DATE_SUB(CURRENT_DATE(),31) and tr_tm <= DATE_SUB(CURRENT_DATE(),0)
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
from usfinance.aml_kyds_withOMS2
where pay_time >= DATE_SUB(CURRENT_DATE(),32) --etl更新可能有延迟，留出2天缓冲
group by acct_no
""").
withColumn("kyds8",when($"unique_opp_cnt">30 && $"unique_opp_cnt" < 50,3.0).otherwise(
when($"unique_opp_cnt" >= 50, 7.0).otherwise(0.0))).select("acct_no","kyds8"),
Seq("acct_no"),"left"
).withColumn("kyds8",when($"kyds8".isNull,0.0).otherwise($"kyds8")).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_8")

//KYDS-8: 本月交易对手大于30个, 不用OMS表，用流水表2来做
//只需把OMS表的名字换成流水表2的名字, pay_time换成tr_tm, 别的无需改动
spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui").join(
spark.sql("""
select acct_no,count(distinct opp_acct) as unique_opp_cnt
from usfinance.yinyin_210901_mainTB_withliushui2_tmp
where tr_tm >= DATE_SUB(CURRENT_DATE(),32) --etl更新可能有延迟，留出2天缓冲
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

//KYDS-19: 不用OMS表，用流水表2来做
//改动包括：把OMS表的名字换成流水表2的名字，把bill_tp_cd字段换为rcv_pay字段, 把pay_amt字段换为tr_amt字段
//贷方交易: rcv_pay = '01'
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("select * from usfinance.yinyin_210901_mainTB_withliushui2_tmp").filter($"rcv_pay"==="01").groupBy("acct_no").agg(
  (avg(when($"pay_platform"==="other",1).otherwise(0.0))).
  alias("non_famous_plat_ratio_cnt"),
  (sum(when($"pay_platform"==="other",$"tr_amt").otherwise(0.0)) / (sum($"tr_amt")+0.00001)).
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

//kyds17: 转账对手银行: 
//KYDS17 向银行转账 贷方交易交易对手为银行账户笔数占比超过50%，金额占比超过70% 10 系数1得10分;
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("""select * from usfinance.aml_kyds_mainTB_withliushui
  where tr_tm > DATE_SUB(CURRENT_DATE(), 93) and tr_tm <= DATE_SUB(CURRENT_DATE(), 0)""").
groupBy("acct_no").agg(
  (count(when($"rcv_pay" === "01" && $"cttp" === "0103", 1.0))/count(when($"rcv_pay" === "01", 1.0))).as("yinhangzhuanzhangCountPcnt"),
  (sum(when($"rcv_pay" === "01" && $"cttp" === "0103", $"tr_amt").otherwise(0.0))/
sum(when($"rcv_pay" === "01", $"tr_amt").otherwise(0.0)) + 0.000001).as("yinhangzhuanzhangAmntPcnt")
).
withColumn("kyds17", when($"yinhangzhuanzhangCountPcnt" > 0.7 && $"yinhangzhuanzhangAmntPcnt" > 0.5, 10.0).otherwise(0.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds17", when($"kyds17".isNull, 0.0).otherwise($"kyds17"))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_17")

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
filter($"ip_country".contains("日本") || $"ip_country".contains("韩国") || $"ip_country".contains("香港") || $"ip_country".contains("澳门") || $"ip_country".contains("印度尼西亚") || $"ip_country".contains("泰国") || $"ip_country".contains("菲律宾") || $"ip_country".contains("老挝") || $"ip_country".contains("缅甸") || $"ip_country".contains("美国")).
select("acct_no","stat_date").distinct().
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_foreign_ip")
spark.sql("select *,1.0 as abnormalPlaceIP from usfinance.aml_kyds_foreign_ip").join(
  spark.sql("select * from usfinance.aml_kyds_mainTB_withliushui"),Seq("acct_no","stat_date"),"right"
).groupBy("acct_no").agg(max(when($"abnormalPlaceIP".isNotNull,15.0).otherwise(0.0)).alias("kyds21")).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_21")

//kyds22, kyds27: 借方交易对手疑似空壳公司
//kyds22: 统计客户在表cpbi.aml_shell_company_score中的出现的借方交易对手个数
//kyds27: 统计客户在表cpbi.aml_shell_company_score中的出现的借方交易对手空壳指数之和
//举例：共计命中两个交易对手A、B，A的空壳指数为L5，B的空壳指数L3，则这个指标为5+3=8；
//kyds29: 统计客户在表cpbi.aml_shell_company_score中的出现的借方交易对手在收到我司客户转入资金三日内出现大量的公转私交易借方交易金额之和
var kKgX = spark.sql("""
select uscc as credit_no, shell_level from cpbi.aml_shell_company_score
""")
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B0 = spark.sql("""select * from usfinance.aml_kyds_withOMS2 where bill_tp_cd = '1' and pay_time > DATE_SUB(CURRENT_DATE(), 93)""").
join(spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type != 'PERSON'").dropDuplicates("acct_no"),Seq("acct_no"), "left").
join(spark.table("fbicsi.yc_taoxian06").select("id_card", "credit_no").distinct, Seq("id_card")).
join(kKgX, Seq("credit_no")).
withColumn("shellScore", substring($"shell_level", 2, 1).cast(DoubleType))

var B1 = B0.select("opp_acct", "credit_no", "shellScore").distinct.
groupBy("opp_acct").agg(
  countDistinct("credit_no").as("kyds22"),
  sum($"shellScore").as("kyds27")
).withColumnRenamed("opp_acct","acct_no").
withColumn("kyds22", when($"kyds22" === 1, 3.0).otherwise(when($"kyds22" === 2, 5.0).otherwise(when($"kyds22" === 3, 7.0).otherwise(when($"kyds22" > 4, 10.0).otherwise(0.0)))))
A = A.join(B1,Seq("acct_no"),"left").withColumn("kyds22",when($"kyds22".isNotNull,$"kyds22").otherwise(0.0)).
withColumn("kyds27",when($"kyds27".isNotNull,$"kyds27").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_22_27")

//kyds22, kyds27: 不用OMS表，用流水表2来做
//改动包括：把OMS表的名字换成流水表2的名字，把bill_tp_cd字段换为rcv_pay字段
var kKgX = spark.sql("""
select uscc as credit_no, shell_level from cpbi.aml_shell_company_score
""")
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B0 = spark.sql("""select * from usfinance.yinyin_210901_mainTB_withliushui2_tmp where rcv_pay = '01' and tr_tm > DATE_SUB(CURRENT_DATE(), 93)""").
join(spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type != 'PERSON'").dropDuplicates("acct_no"),Seq("acct_no"), "left").
join(spark.table("fbicsi.yc_taoxian06").select("id_card", "credit_no").distinct, Seq("id_card")).
join(kKgX, Seq("credit_no")).
withColumn("shellScore", substring($"shell_level", 2, 1).cast(DoubleType))

var B1 = B0.select("opp_acct", "credit_no", "shellScore").distinct.
groupBy("opp_acct").agg(
  countDistinct("credit_no").as("kyds22"),
  sum($"shellScore").as("kyds27")
).withColumnRenamed("opp_acct","acct_no").
withColumn("kyds22", when($"kyds22" === 1, 3.0).otherwise(when($"kyds22" === 2, 5.0).otherwise(when($"kyds22" === 3, 7.0).otherwise(when($"kyds22" > 4, 10.0).otherwise(0.0)))))
A = A.join(B1,Seq("acct_no"),"left").withColumn("kyds22",when($"kyds22".isNotNull,$"kyds22").otherwise(0.0)).
withColumn("kyds27",when($"kyds27".isNotNull,$"kyds27").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_22_27")


//kyds29: 统计客户在表cpbi.aml_shell_company_score中的出现的借方交易对手
//在收到我司客户转入资金三日内出现大量的公转私交易借方交易金额之和
//对于每个借方交易对手，若存在三日内公转私比例过大的情况，则认为有所嫌疑

//大致计算逻辑供理解参考，具体实现时稍有调整, 也可以略过不看，直接读代码注释
"""
mainTB: acct_no, opp_acct_no, tr_tm, tr_amt, rcv_pay(2代表付钱,1代表收钱)
table1: acct_no_step1, opp_acct_no, tr_tm_step1 -> tr_date_step1, tr_amt_step1 (where opp_acct_no is NOT PERSON, in shellTable, rcv_pay = 2)
table1_1 = table1.groupBy("acct_no_step1", "tr_date_step1", "opp_acct_no").agg(
sum($"tr_amt_step1").as("入账金额")
)
table2: acct_no(opp_acct_no), opp_acct_no(acct_no_step2), tr_tm_step2, tr_amt_step2 (where opp_acct_no is PERSON, acct_no is in shellTable rcv_pay = 2)
关联:
table3 = table1.join(table2, table1.col("opp_acct_no") === table2.col("acct_no"))
table3: acct_no_step1, opp_acct_no(空壳公司账户), acct_no_step2(私人账户), tr_tm_step1 -> substring(tr_tm_step1, 1, 10) as tr_date_step1, tr_tm_step2 -> substring(tr_tm_step2, 1, 10) as tr_date_step2
tr_amt_step1, tr_amt_step2 -> filter(tr_tm_step2 > tr_tm_step1 and datediff(tr_tm_step2, tr_tm_step1) <= 3)
table4 = table3.groupBy("acct_no_step1", "tr_date_step1", "opp_acct_no").agg(
sum("tr_amt_step2").as("出账金额")
)
table5 = table1_1.join(table4, Seq("acct_no_step1", "tr_date_step1", "opp_acct_no")).
withColumn("出账比例", 出账金额/入账金额).
groupBy("acct_no_step1").agg(max("出账比例").as("出账比例")) -> 取出最大出账比例
-----> KYDS29 -> 出账比例
acct_no_step1, kyds29(出账比例)
"""

//空壳公司表
var kKgX = spark.sql("""
select uscc as credit_no, shell_level from cpbi.aml_shell_company_score
""")
//主表
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
//流水表2里面93天内的借方交易
var B0 = spark.sql("""select acct_no, opp_acct as opp_acct_no, tr_tm, tr_amt from usfinance.yinyin_210901_mainTB_withliushui2_tmp where rcv_pay = '02' and tr_tm > DATE_SUB(CURRENT_DATE(), 93)""")
var non_personal_acct_opp =
spark.sql("select acct_no as opp_acct_no, id_card, user_type from finance.mls_member_info_all where user_type != 'PERSON'").dropDuplicates

//第一步，我方客户转钱给空壳公司
// B0 和会员表通过交易对手户头号相连后，带上了身份证号和name还有user_type, 注意table1是通过对手户头号相连
var table1 = B0.join(non_personal_acct_opp,Seq("opp_acct_no"),"left")
//table1再和taoxian06通过身份证号相连，带上了信用号
//table1就是流水表2里面93天内的借方交易, 我方是消费者，借方交易对手是商户, 我方转钱给商户空壳公司，带着每一个商户的身份证号和信用号
table1 = table1.join(spark.table("fbicsi.yc_taoxian06").select("id_card", "credit_no").distinct,Seq("id_card"))
//table1再和kKgX通过信用号相连，inner join，所以交易对手只保留在空壳公司表出现里的商户, 带上了shell_level
table1 = table1.join(kKgX, Seq("credit_no"))
//重命名acct_no到acct_no_step1，重命名tr_amt到tr_amt_step1, 交易时间只保留交易日期
//table1最后包含:
//acct_no_step1:我方消费者
//opp_acct_no: 借方交易对手（空壳公司）
//tr_date_step1:我方客户转钱给空壳公司的交易日期
//tr_amt_step1:我方客户转钱给空壳公司的交易金额
table1 = table1.withColumnRenamed("acct_no", "acct_no_step1").
withColumnRenamed("tr_amt", "tr_amt_step1").
withColumn("tr_date_step1", substring($"tr_tm", 1, 10)).
drop("tr_tm", "credit_no", "id_card", "user_type", "shell_level")

//table1_1 计算我方客户在交易当天转给某个空壳公司的总金额
//table1_1: acct_no_step1, tr_date_step1, opp_acct_no, in_amt
var table1_1 = table1.groupBy("acct_no_step1", "tr_date_step1", "opp_acct_no").agg(
sum($"tr_amt_step1").as("in_amt")
)

//第二步，空壳公司转钱给私人
var non_personal_acct =
spark.sql("select acct_no, id_card, user_type from finance.mls_member_info_all where user_type != 'PERSON'").dropDuplicates
// B0 和会员表通过户头号相连后，带上了身份证号和name还有user_type, 注意table2是通过自身户头号相连
var table2 = B0.join(non_personal_acct,Seq("acct_no"),"left")
//table2再和taoxian06通过身份证号相连，带上了信用号
//table2就是流水表2里面93天内的借方交易, 这个表里acct_no是商户空壳公司，借方交易对手是消费者, 空壳公司公转私转钱给私人，带着每一个商户的身份证号和信用号
table2 = table2.join(spark.table("fbicsi.yc_taoxian06").select("id_card", "credit_no").distinct,Seq("id_card"))
//table2再和kKgX通过信用号相连，inner join，所以acct_no只保留在空壳公司表出现里的商户
table2 = table2.join(kKgX, Seq("credit_no"))
//重命名opp_acct_no(接收空壳公司转钱的私人)到opp_acct_no_step2
//重命名tr_amt到tr_amt_step2, 交易时间只保留交易日期
//table2最后包含:
//acct_no:空壳公司
//opp_acct_no_step2: 接收空壳公司转钱的私人
//tr_date_step2:空壳公司转钱给私人的交易日期
//tr_amt_step2:空壳公司转钱给私人的交易金额
table2 = table2.withColumnRenamed("opp_acct_no", "opp_acct_no_step2").
withColumnRenamed("tr_amt", "tr_amt_step2").
withColumn("tr_date_step2", substring($"tr_tm", 1, 10)).
drop("tr_tm", "credit_no", "id_card", "user_type", "shell_level")

//table2_1 计算空壳公司按日期转给私人的总金额
//table2_1: acct_no, tr_date_step2, out_day_amt
var table2_1 = table2.groupBy("acct_no", "tr_date_step2").agg(
sum($"tr_amt_step2").as("out_day_amt")
)

//第三步，通过空壳公司户头号关联table1_1 and table2_1, left join
//在table1_1里，空壳公司是收钱的opp_acct_no，在table2_1里，空壳公司是付钱的acct_no
//table1_1， table2_1 都只包含借方交易（付钱）
//table3: acct_no_step1, tr_date_step1, opp_acct_no, in_amt, tr_date_step2, out_day_amt
var table3 = table1_1.join(table2_1, table1_1.col("opp_acct_no") === table2_1.col("acct_no"), "left").drop("acct_no").
withColumn("out_day_amt",when($"out_day_amt".isNull,0.0).otherwise($"out_day_amt"))
//按tr_date_step2过滤，只保留tr_tm_step1之后的3天内的交易
table3 = table3.filter($"tr_date_step2" > $"tr_date_step1" and datediff($"tr_date_step2", $"tr_date_step1") <= 3)

//第四步，计算空壳公司在收到钱后3天内再转出给私人的总金额, 并计算比例：出账金额/入账金额
//table4: acct_no_step1, tr_date_step1, opp_acct_no, in_amt, out_amt
var table4 = table3.groupBy("acct_no_step1", "tr_date_step1", "opp_acct_no", "in_amt").agg(
sum("out_day_amt").as("out_amt")
).
withColumn("ratio", $"out_amt"/$"in_amt")

//第五步, 取出最大出账比例,计算分数
//金额达到我司客户转入资金
//小于30%，系数0.5，得分5分；
//30—40%，系数1，得10分；
//40%以上，系数1.5，得15分；
var table5 = table4.groupBy("acct_no_step1").agg(max("ratio").as("max_ratio")).
withColumn("kyds29", when($"max_ratio" === 0, 0.0).
when($"max_ratio" > 0 && $"max_ratio" < 0.3, 5.0).
when($"max_ratio" >= 0.3 && $"max_ratio" < 0.4, 10.0).
otherwise(15.0)).
withColumnRenamed("acct_no_step1", "acct_no")
A = A.join(table5,Seq("acct_no"),"left").select("acct_no","kyds29").
withColumn("kyds29",when($"kyds29".isNull,0.0).otherwise($"kyds29"))
spark.sql("""drop table if exists usfinance.aml_kyds_29""")
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_29")



spark.table("usfinance.aml_kyds_29").orderBy($"kyds29".desc).show(false)
//之前最早的逻辑，目前没有使用
spark.sql("""drop table if exists usfinance.aml_kyds_29 """)
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
val B2 = spark.sql("""select * from usfinance.aml_kyds_withOMS2 where bill_tp_cd = '1' and pay_time > DATE_SUB(CURRENT_DATE(), 93)""").
withColumnRenamed("acct_no", "acct_no_in").
withColumnRenamed("opp_acct", "acct_no").
join(spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type = 'PERSON'").dropDuplicates("acct_no"),Seq("acct_no"), "left").
join(spark.table("fbicsi.yc_taoxian06").select("id_card", "credit_no").distinct, Seq("id_card")).
withColumnRenamed("acct_no", "opp_acct").
withColumnRenamed("acct_no_in", "acct_no").
select("pay_time", "opp_acct", "pay_amt").withColumnRenamed("pay_amt", "pay_amt_GZX").
withColumnRenamed("pay_time", "pay_time_GZX")
val B3 = B0.select("opp_acct", "credit_no", "pay_amt", "pay_time").distinct.
withColumnRenamed("pay_amt", "pay_amt_in").
withColumnRenamed("pay_time", "pay_time_in").
join(B2, Seq("opp_acct")).
withColumn("lessThan3DaysIndicator", when(datediff($"pay_time_GZX", $"pay_time_in") <= 3 && $"pay_time_GZX" > $"pay_time_in", 1.0).otherwise(0.0)).
groupBy("opp_acct").
agg(
sum($"pay_amt_in").as("payAmntIn"),
sum(when($"lessThan3DaysIndicator" === 1.0, $"pay_amt_GZX")).as("payAmntGZXIn3Days")
).withColumnRenamed("opp_acct", "acct_no").
withColumn("gzxIndex", when($"payAmntIn" > 0.0, $"payAmntGZXIn3Days"/ $"payAmntIn").otherwise(0.0)).
withColumn("kyds29", when($"gzxIndex" < 0.3, 5.0).otherwise(when($"gzxIndex" >= 0.3 && $"gzxIndex" < 0.4, 10.0).otherwise(when($"gzxIndex" >= 0.4, 15.0).otherwise(0.0))))
A = A.join(B3,Seq("acct_no"),"left").withColumn("kyds29",when($"kyds29".isNotNull,$"kyds29").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_29")
spark.table("usfinance.aml_kyds_29").orderBy($"kyds29".desc).show(false)


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
//新逻辑 -- 交易金额为100的倍数，且金额小于20000占比30%以上
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
var B = spark.sql("select * from usfinance.aml_kyds_mainTB_withliushui").filter($"tr_amt".isNotNull).
//更新逻辑:
//withColumn("is_int",when($"tr_amt" === $"tr_amt".cast("Int"),1).otherwise(0.0)).
//withColumn("res",when($"tr_amt">100,$"tr_amt".cast("Int") % 100).otherwise($"tr_amt".cast("Int") % 10)).
//withColumn("like_bet_amt",when($"is_int"===1 && $"res"===0,1.0).otherwise(0.0)).groupBy($"acct_no").
//agg((avg($"like_bet_amt")).alias("like_bet_ratio"))
withColumn("isAbnormalAmnt", when($"tr_amt" > 0.0 && $"tr_amt" % 100 === 0 && $"tr_amt" < 20000, 1.0).otherwise(0.0)).
groupBy("acct_no").
agg((avg($"isAbnormalAmnt")).as("like_bet_ratio"))
A = A.join(B,Seq("acct_no"),"left").withColumn("kyds25",when($"like_bet_ratio">0.3,10.0).otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_25")


//kyds26: 交易附言中含有“大张”（日元）、“小张”（美元）、“矮子”（日元）、
//数字后面有“张”（万），“条”（10万），“粒”（百万）等字样:
//此处指标计算方式为: 过去30天fund_use字段内含有大张/小张/矮子等的账户列表且数字后面含有
spark.sql("""drop table if exists usfinance.aml_kyds_26""")
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

//kyds28: 借方交易账号为他人信用卡:
//借方交易对手为他人信用卡，且笔数超过10笔(限制在过去3个月)
//由于该表过大(10000分区)且每日更新全量数据,我们取3天之前的stat_date进行分析
val cur_dat = java.time.LocalDate.now 
val current_date = cur_dat.minusDays(2).toString
val past3d = cur_dat.minusDays(3).toString
val past3dStatDate = past3d.substring(0, 4) + past3d.substring(5, 7) + past3d.substring(8, 10)
var A = spark.sql("""select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui""")
//"120016"默认信用卡交易
val A1 = spark.table("fdm_dpa.s022_snzf_t2a_dpst_acct_i_d").filter($"stat_date" === past3dStatDate).
filter($"card_style" === "120016")
val A2 = spark.table("usfinance.aml_kyds_mainTB_withliushui").
select("acct_no", "opp_cust_id", "tr_tm", "tr_amt", "rcv_pay").distinct.
withColumnRenamed("opp_cust_id", "cust_id")
val B = A1.join(A2, Seq("cust_id")).
groupBy("acct_no").agg(
count(when($"tr_tm".isNotNull, 1.0)).as("creditCardOppoTrCount")
).withColumn("kyds28", when($"creditCardOppoTrCount" > 10, 10.0).otherwise(0.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds28", when($"kyds28".isNotNull, $"kyds28").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_28")

////
//kyds30 - 32
//kyds30: 陌生借方交易对手: 与借方交易对手前2个月无交易 
//具体计算方法:如有这样的陌生借方交易对手，则我们认定该账户命中规则，反之则认定该账户未命中.
spark.sql("""drop table if exists usfinance.aml_kyds_30""")
var A = spark.sql("select distinct acct_no from usfinance.yinyin_210901_mainTB_withliushui2_tmp")
//对于某一个借方交易对手，近三个月的第一次交易在距今一个月以内
val W1 = Window.partitionBy($"acct_no", $"opp_acct").orderBy($"tr_tm")
val B = spark.table("usfinance.yinyin_210901_mainTB_withliushui2_tmp").
select("acct_no", "opp_acct", "tr_tm", "tr_amt").distinct.
withColumn("ranks", row_number() over W1).
filter($"ranks" === 1).filter(datediff(lit(current_date), $"tr_tm") <= 31).
select("acct_no").distinct.
withColumn("kyds30", lit(10.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds30", when($"kyds30".isNotNull, $"kyds30").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_30")

//kyds31: 陌生借方交易对手交易金额渐增
//具体计算方法:与陌生交易对手两天内发生2笔及以上交易，金额逐渐变大，最后一笔金额除以第一笔金额=D
//首先取出所有陌生交易对手列表:
spark.sql("""drop table if exists usfinance.aml_kyds_31""")
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
//陌生交易对手:
val B1 = spark.table("usfinance.yinyin_210901_mainTB_withliushui2_tmp").
select("acct_no", "opp_acct", "tr_tm", "tr_amt").distinct.
withColumn("ranks", row_number() over W1).
filter($"ranks" === 1).filter(datediff(lit(current_date), $"tr_tm") <= 31).
withColumnRenamed("tr_tm", "tr_tmFirst").
select("acct_no", "opp_acct", "tr_tmFirst").distinct
//udf实现： 两天内发生2笔及以上交易，金额逐渐变大，最后一笔金额除以第一笔金额=D
//如存在非递增，则将该比例赋值为负数
def checkAmnt(timeList: Seq[Double], amntList: Seq[Double]): Double = {
  val timeAmntList = timeList.zip(amntList).sortBy(_._1)
  var result = -1.0
  val sizes = timeAmntList.size
  var indicator = 1.0
  if(sizes > 2){
    for(i <- 1 to (sizes - 1)){
      if(timeAmntList(i)._2 < timeAmntList(i - 1)._2){
        indicator = -1.0
      }
    }
    result = indicator * (timeAmntList(sizes - 1)._2/ timeAmntList(0)._2)
  }
  result
}
val checkAmntUdf = udf(checkAmnt _)
val B = spark.table("usfinance.yinyin_210901_mainTB_withliushui2_tmp").
select("acct_no", "opp_acct", "tr_tm", "tr_amt").distinct.
join(B1, Seq("acct_no", "opp_acct")).filter(datediff($"tr_tm", $"tr_tmFirst") <= 2). //两天内
withColumn("tr_tm", unix_timestamp($"tr_tm").cast(DoubleType)).
groupBy("acct_no", "opp_acct").agg(
  collect_list($"tr_tm").as("transactionTimeList"),
  collect_list($"tr_amt".cast(DoubleType)).as("transactionAmntList"),
  countDistinct($"tr_tm").as("transactionTimeCount")
).filter($"transactionTimeCount" > 2).
withColumn("ratios", checkAmntUdf.apply($"transactionTimeList", $"transactionAmntList")).
select("acct_no", "ratios").distinct.
withColumn("kyds31", when($"ratios" > 1 && $"ratios" <= 2, 3.0).otherwise(when($"ratios" < 8 && $"ratios" > 2, 5.0).otherwise(when($"ratios" >= 8, 10.0).otherwise(0.0))))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds31", when($"kyds31".isNotNull, $"kyds31").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_31")

//kyds32: 开户证件非身份证
spark.sql("""drop table if exists usfinance.aml_kyds_32 """)
var A = spark.sql("select distinct acct_no from usfinance.aml_kyds_mainTB_withliushui")
val B = A.join(spark.sql("select acct_no, id_card from finance.mls_member_info_all").distinct, Seq("acct_no")).distinct.
groupBy("acct_no").agg(
max("id_card").as("id_card")
).withColumn("kyds32", when($"id_card".isNull, 10.0).otherwise(0.0))
A = A.join(B, Seq("acct_no"), "left").withColumn("kyds32", when($"kyds32".isNotNull, $"kyds32").otherwise(0.0))
A.write.mode("overwrite").saveAsTable("usfinance.aml_kyds_32")



spark.sql("""drop table if exists usfinance.aml_kyds_20210909""")
spark.sql("""
select acct_no,kyds1,kyds2,kyds3,kyds4,kyds5,kyds6,kyds7,kyds8,kyds9,
kyds10,kyds11,kyds12,kyds16,kyds17,kyds18,kyds19,kyds20,kyds21,kyds22,kyds23,kyds24,kyds25,
kyds26,kyds27,kyds28,kyds29,kyds30,kyds31,kyds32,acctTransactionCount, acctTransactionAmnt
from (select distinct acct_no from usfinance.aml_kyds_mainTB) 
left join usfinance.aml_kyds_1 
using (acct_no)
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
left join usfinance.aml_kyds_17
using (acct_no)
left join usfinance.aml_kyds_18
using (acct_no)
left join usfinance.aml_kyds_20
using (acct_no)
left join usfinance.aml_kyds_21
using (acct_no)
left join usfinance.aml_kyds_22_27
using (acct_no)
left join usfinance.aml_kyds_23
using (acct_no)
left join usfinance.aml_kyds_24
using (acct_no)
left join usfinance.aml_kyds_25
using (acct_no)
left join usfinance.aml_kyds_26
using (acct_no)
left join usfinance.aml_kyds_28
using (acct_no)
left join usfinance.aml_kyds_29
using (acct_no)
left join usfinance.aml_kyds_30
using (acct_no)
left join usfinance.aml_kyds_31
using (acct_no)
left join usfinance.aml_kyds_32
using (acct_no)
""").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_20210909")

//跨境赌博
//个人客户 ---- 
spark.sql("""drop table if exists usfinance.aml_kyds_kjdb_cases""")
spark.sql("""
select distinct * from
(
select *,
kyds2+kyds3+kyds4+kyds5+kyds6+kyds7+kyds8+kyds9+
kyds10+kyds11+kyds12+kyds16+kyds18+kyds19+kyds20+kyds21+kyds24+kyds25+kyds28 as kjdb_score
from usfinance.aml_kyds_20210808
)
order by kjdb_score  
""").
join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all").dropDuplicates("acct_no"),Seq("acct_no")).
filter($"user_type" =!= "CORPORAT" && $"user_type".isNotNull).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
select("acct_no","id_card","name","credit_no","kjdb_score","kyds2","kyds3","kyds4","kyds5",
"kyds6","kyds7","kyds8","kyds9","kyds10","kyds11","kyds12","kyds16",
"kyds18","kyds19","kyds20","kyds21","kyds24","kyds25","kyds28", 
"acctTransactionAmnt", "acctTransactionCount","timeRange").
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_kjdb_cases")


//地下钱庄Heqiao-0812
//对公top20:
spark.sql("""
select distinct * from 
(
  select *,
  kyds4+kyds5+kyds6+kyds7+kyds8+kyds9+
kyds10+kyds22+kyds23+kyds26+kyds27+kyds29 as dxqz_score
from usfinance.aml_kyds_20210808
)
order by dxqz_score desc limit 200
""").join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type = 'CORPORAT' ").dropDuplicates("acct_no"),Seq("acct_no")).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
withColumn("timeRange", lit("20210524-20210823")).
select("acct_no","id_card","name","credit_no","dxqz_score","kyds4","kyds5","kyds6","kyds7",
"kyds8","kyds9","kyds10","kyds22","kyds23", "kyds26", "kyds27","kyds29",
"acctTransactionAmnt", "acctTransactionCount","timeRange").orderBy($"dxqz_score".desc).distinct.limit(20).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_dxqz_casesDUIGONG")

//个人top20:
spark.sql("""
select distinct * from 
(
  select *,
  kyds4+kyds5+kyds6+kyds7+kyds8+kyds9+
kyds10+kyds22+kyds23+kyds26+kyds27+kyds29 as dxqz_score
from usfinance.aml_kyds_20210808
)
order by dxqz_score desc
limit 200
""").join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type = 'PERSON' ").dropDuplicates("acct_no"),Seq("acct_no")).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
withColumn("timeRange", lit("20210524-20210823")).
select("acct_no","id_card","name","credit_no","dxqz_score","kyds4","kyds5","kyds6","kyds7",
"kyds8","kyds9","kyds10","kyds22","kyds23", "kyds26", "kyds27","kyds29",
"acctTransactionAmnt", "acctTransactionCount", "timeRange").orderBy($"dxqz_score".desc).distinct.limit(20).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_dxqz_casesGEREN")


//嫌疑诈骗0909 - 个人客户:
spark.sql("""
select distinct * from 
(
  select *,
  kyds1+kyds2+kyds3+kyds4+kyds5+kyds6+kyds9+
  kyds10+kyds11+kyds12+kyds16+kyds17+kyds30+kyds31+
  kyds32 as xyzp_score
from usfinance.aml_kyds_20210808
)
order by xyzp_score desc
limit 20
""").join(
spark.sql("select acct_no, id_card,name,user_type from finance.mls_member_info_all where user_type = 'PERSON' ").dropDuplicates("acct_no"),Seq("acct_no")).
join(
  spark.table("fbicsi.yc_taoxian06").select("cmpy_name", "id_card", "credit_no").distinct,Seq("id_card"),"left"
).withColumn("name",when($"user_type"==="PERSON",$"name").otherwise($"cmpy_name")).
withColumn("acct_no",concat(lit("户头号"),$"acct_no")).
withColumn("id_card",concat(lit("身份证"),$"id_card")).
withColumn("credit_no",concat(lit("信用号"),$"credit_no")).
withColumn("timeRange", lit("20210609-20210908")).
select("acct_no","id_card","name","credit_no","xyzp_score", "kyds1", "kyds2", "kyds3",
"kyds4","kyds5","kyds6","kyds9","kyds10","kyds11","kyds12","kyds16","kyds17",
"kyds30","kyds31","kyds32","acctTransactionAmnt", "acctTransactionCount", 
"timeRange").orderBy($"xyzp_score".desc).distinct.limit(20).
write.mode("overwrite").saveAsTable("usfinance.aml_kyds_xyzp_casesGEREN")
