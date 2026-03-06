# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name: FctsAplLinkExtr
# MAGIC DESCRIPTION:   Pulls data from CMC_APLK_APP_LINK for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the APL_LINK table.
# MAGIC  
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/11/2007              	3028                 	Initial program                                                           		devlIDS30                   	Steph Goddard          	8/23/07  
# MAGIC Bhoomi Dasari    		10/18/2007                 	3028		Added Balancing Snapshot                                         		devlIDS30           	Steph Goddard          	10/18/07
# MAGIC 
# MAGIC Jag Yelavarthi    		2015-01-19                  	TFS#8431   	Changed "IDS_SK" to "Table_Name_SK"                    		IntegrateNewDevl  	Kalyan Neelam     	2015-01-27
# MAGIC 
# MAGIC Ravi Singh         		2018-10- 05      	MTM-5841		Added new shared container instead of                             		IntegrateDev2	Abhiram Dasarathy 	2018-10-30
# MAGIC                                                            					transformer stage 
# MAGIC Anitha Perumal   		2018-12-14               	INC0484960      	Added logic to remove quotes in field                          		IntegrateDev2            	Kalyan Neelam   	2018-12-14
# MAGIC                                                             					APL_LINK_DESC  
# MAGIC Prabhu ES          		2022-02-25                	S2S                	MSSQL connection parameters added                        		IntegrateDev5	Ken Bradmon	2022-05-07

# MAGIC Added Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Link Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, regexp_replace, upper, trim as spark_trim, when, length, rpad, date_format, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

                                                                 
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLinkPkey
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_1 = f"""SELECT 
APLK.APAP_ID,
APLK.APLK_TYPE,
APLK.APLK_ID,
APLK.APLK_MCTR_REAS,
APLK.APLK_DESC,
APLK.APLK_LAST_UPD_DTM,
APLK.APLK_LAST_UPD_USID 
FROM {FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APLK.APLK_LAST_UPD_DTM >= '{BeginDate}' AND 
APLK.APLK_LAST_UPD_DTM <= '{EndDate}'"""

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_Strip = (
    df_FACETS
    .withColumn("APAP_ID", regexp_replace(col("APAP_ID"), "[\r\n\t]", ""))
    .withColumn("APLK_TYPE", regexp_replace(col("APLK_TYPE"), "[\r\n\t]", ""))
    .withColumn("APLK_ID", regexp_replace(col("APLK_ID"), "[\r\n\t]", ""))
    .withColumn("APLK_LAST_UPD_USID", regexp_replace(col("APLK_LAST_UPD_USID"), "[\r\n\t]", ""))
    .withColumn("APLK_MCTR_REAS", regexp_replace(col("APLK_MCTR_REAS"), "[\r\n\t]", ""))
    .withColumn("APAP_LAST_UPD_DTM", date_format(col("APLK_LAST_UPD_DTM"), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("APLK_DESC", upper(regexp_replace(col("APLK_DESC"), "[\r\n\t]", "")))
)

df_BusinessRules = (
    df_Strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat_ws(";", lit("FACETS"), spark_trim(col("APAP_ID")), spark_trim(col("APLK_TYPE")), spark_trim(col("APLK_ID"))))
    .withColumn("APL_LINK_SK", lit(0))
    .withColumn("APL_ID", spark_trim(col("APAP_ID")))
    .withColumn("APL_LINK_TYP_CD_SK",
                when((length(spark_trim(col("APLK_TYPE"))) == 0) | (col("APLK_TYPE").isNull()), lit("NA"))
                .otherwise(spark_trim(col("APLK_TYPE"))))
    .withColumn("APL_LINK_ID", upper(spark_trim(col("APLK_ID"))))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("APL_SK",
                when((length(spark_trim(col("APAP_ID"))) == 0) | (col("APAP_ID").isNull()), lit("NA"))
                .otherwise(spark_trim(col("APAP_ID"))))
    .withColumn("LAST_UPDT_USER_SK",
                when((length(spark_trim(col("APLK_LAST_UPD_USID"))) == 0) | (col("APLK_LAST_UPD_USID").isNull()), lit("NA"))
                .otherwise(spark_trim(col("APLK_LAST_UPD_USID"))))
    .withColumn("APL_LINK_RSN_CD_SK",
                when((length(spark_trim(col("APLK_MCTR_REAS"))) == 0) | (col("APLK_MCTR_REAS").isNull()), lit("NA"))
                .otherwise(spark_trim(col("APLK_MCTR_REAS"))))
    .withColumn("LAST_UPDT_DTM", col("APAP_LAST_UPD_DTM"))
    .withColumn("APL_LINK_DESC", spark_trim(regexp_replace(col("APLK_DESC"), '"', '')))
    .withColumn("CASE_MGT_SK", when(col("APLK_TYPE") == lit("S"), col("APLK_ID")).otherwise(lit("NA")))
    .withColumn("CLM_SK", when(col("APLK_TYPE") == lit("C"), col("APLK_ID")).otherwise(lit("NA")))
    .withColumn("CUST_SVC_SK", when(col("APLK_TYPE") == lit("V"), col("APLK_ID")).otherwise(lit("NA")))
    .withColumn("REL_APL_SK", when(col("APLK_TYPE") == lit("A"), col("APLK_ID")).otherwise(lit("NA")))
    .withColumn("UM_SK", when(col("APLK_TYPE") == lit("U"), col("APLK_ID")).otherwise(lit("NA")))
)

params_AplLinkPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_AplLinkPkey = AplLinkPkey(df_BusinessRules, params_AplLinkPkey)

df_final_IdsAplLinkExtr = df_AplLinkPkey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "APL_LINK_SK",
    "APL_ID",
    "APL_LINK_TYP_CD_SK",
    "APL_LINK_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_SK",
    "CASE_MGT_SK",
    "CLM_SK",
    "CUST_SVC_SK",
    "LAST_UPDT_USER_SK",
    "REL_APL_SK",
    "UM_SK",
    "APL_LINK_RSN_CD_SK",
    "LAST_UPDT_DTM",
    "APL_LINK_DESC"
).withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
 .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
 .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
 .withColumn("APL_LINK_DESC", rpad(col("APL_LINK_DESC"), 70, " "))

write_files(
    df_final_IdsAplLinkExtr,
    f"{adls_path}/key/FctsAplLinkExtr.AplLink.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_2 = f"""SELECT 
APLK.APAP_ID,
APLK.APLK_TYPE,
APLK.APLK_ID,
APLK.APLK_DESC,
APLK.APLK_LAST_UPD_DTM
FROM {FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APLK.APLK_LAST_UPD_DTM >= '{BeginDate}' AND 
APLK.APLK_LAST_UPD_DTM <= '{EndDate}'"""

df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Rules = (
    df_SnapShot_FACETS
    .withColumn("SRC_SYS_CD_SK", GetFkeyCodes("IDS", lit(1), lit("SOURCE SYSTEM"), lit("FACETS"), lit("X")))
    .withColumn("APL_ID", spark_trim(regexp_replace(col("APAP_ID"), "[\r\n\t]", "")))
    .withColumn("APL_LINK_TYP_CD_SK", GetFkeyCodes("FACETS", lit(116), lit("APPEAL LINK TYPE"), col("APLK_TYPE"), lit("X")))
    .withColumn("APL_LINK_ID", upper(spark_trim(col("APLK_ID"))))
    .withColumn("APL_LINK_DESC", spark_trim(col("APLK_DESC")))
    .withColumn("LAST_UPDT_DT_SK", date_format(col("APLK_LAST_UPD_DTM"), "yyyy-MM-dd"))
)

df_final_B_APL_LINK = df_Rules.select(
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LINK_TYP_CD_SK",
    "APL_LINK_ID",
    "APL_LINK_DESC",
    "LAST_UPDT_DT_SK"
).withColumn("LAST_UPDT_DT_SK", rpad(col("LAST_UPDT_DT_SK"), 10, " "))

write_files(
    df_final_B_APL_LINK,
    f"{adls_path}/load/B_APL_LINK.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)