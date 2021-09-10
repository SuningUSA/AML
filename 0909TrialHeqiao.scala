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
