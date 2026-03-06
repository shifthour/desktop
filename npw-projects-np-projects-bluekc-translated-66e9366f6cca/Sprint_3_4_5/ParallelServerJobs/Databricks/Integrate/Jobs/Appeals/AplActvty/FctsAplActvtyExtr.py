# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name: FctsAplActvtyExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APAC_ACTIVITY for loading into IDS APL_ACTVTY.
# MAGIC 
# MAGIC PROCESSING:    Creates appeals activity data
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/02/2007             	3028		Initial program                                                              		devlIDS30                 	Steph Goddard           8/23/07
# MAGIC Bhoomi Dasari    		10/18/2007              	3028		Added Balancing Snapshot                                         		devlIDS30                  	Steph Goddard           10/18/2007
# MAGIC Jag Yelavarthi    		2015-01-19                  	TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"              		IntegrateNewDevl       Kalyan Neelam        	2015-01-27
# MAGIC Ravi Singh          		2018-10- 05              	MTM-5841		Added new shared container instead of                            		IntegrateDev2	Abhiram Dasarathy 	2018-10-30
# MAGIC                                                            					transformer stage 
# MAGIC Prabhu ES          		2022-02-25              	S2S		MSSQL connection parameters added                         		IntegrateDev5	Ken Bradmon	2022-05-17

# MAGIC Added Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal activity Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/AplActvtyPkey
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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

# ODBCConnector - FACETS
extract_query_FACETS = (
    f"SELECT \n"
    f"APAC.APAP_ID,\n"
    f"APAC.APAC_SEQ_NO,\n"
    f"APAC.APLV_SEQ_NO,\n"
    f"APAC.APRP_ID,\n"
    f"APAC.APAC_DTM,\n"
    f"APAC.APAC_METHOD,\n"
    f"APAC.APAC_MCTR_TYPE,\n"
    f"APAC.APAC_SUMMARY,\n"
    f"APAC.APAC_CREATE_DTM,\n"
    f"APAC.APAC_CREATE_USID,\n"
    f"APAC.APAC_LAST_UPD_DTM,\n"
    f"APAC.APAC_LAST_UPD_USID\n"
    f"FROM \n"
    f"{FacetsOwner}.CMC_APAC_ACTIVITY APAC,\n"
    f"{FacetsOwner}.CMC_APAP_APPEALS APAP\n"
    f"WHERE\n"
    f"APAC.APAP_ID = APAP.APAP_ID AND\n"
    f"APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND\n"
    f"APAP.APAP_LAST_UPD_DTM <= '{EndDate}'"
)

jdbc_url_FACETS, jdbc_props_FACETS = get_db_config(facets_secret_name)
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FACETS)
    .options(**jdbc_props_FACETS)
    .option("query", extract_query_FACETS)
    .load()
)

# CTransformerStage - Strip
df_Strip = df_FACETS.select(
    F.regexp_replace(F.col("APAP_ID"), "[\n\r\t]", "").alias("APAP_ID"),
    F.col("APAC_SEQ_NO").alias("APAC_SEQ_NO"),
    F.col("APLV_SEQ_NO").alias("APLV_SEQ_NO"),
    F.regexp_replace(F.col("APRP_ID"), "[\n\r\t]", "").alias("APRP_ID"),
    F.date_format(F.col("APAC_DTM"), "yyyy-MM-dd").alias("APAC_DTM"),
    F.regexp_replace(F.col("APAC_METHOD"), "[\n\r\t]", "").alias("APAC_METHOD"),
    F.regexp_replace(F.col("APAC_MCTR_TYPE"), "[\n\r\t]", "").alias("APAC_MCTR_TYPE"),
    F.regexp_replace(F.col("APAC_SUMMARY"), "[\n\r\t]", "").alias("APAC_SUMMARY"),
    F.date_format(F.col("APAC_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APAC_CREATE_DTM"),
    F.regexp_replace(F.col("APAC_CREATE_USID"), "[\n\r\t]", "").alias("APAC_CREATE_USID"),
    F.date_format(F.col("APAC_LAST_UPD_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APAC_LAST_UPD_DTM"),
    F.regexp_replace(F.col("APAC_LAST_UPD_USID"), "[\n\r\t]", "").alias("APAC_LAST_UPD_USID"),
)

# CTransformerStage - BusinessRules
df_BusinessRules_stagevars = (
    df_Strip
    .withColumn("_RowPassThru", F.lit("Y"))
    .withColumn("_svSrcSysCd", F.lit("FACETS"))
    .withColumn("_svAplId", F.trim(F.col("APAP_ID")))
    .withColumn("_svSeqNo", F.trim(F.col("APAC_SEQ_NO")))
)

df_BusinessRules = (
    df_BusinessRules_stagevars
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("_RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("_svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.col("_svSrcSysCd"),
            F.lit(";"),
            F.col("_svAplId"),
            F.lit(";"),
            F.col("_svSeqNo")
        )
    )
    .withColumn("APL_ACTVTY_SK", F.lit(0))
    .withColumn("APL_ID", F.col("_svAplId"))
    .withColumn("SEQ_NO", F.col("_svSeqNo"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "APL_SK",
        F.when(
            (F.length(F.trim(F.col("APAP_ID"))) == 0) | (F.col("APAP_ID").isNull()),
            F.lit("NA")
        ).otherwise(F.trim(F.col("APAP_ID")))
    )
    .withColumn(
        "APL_RVWR_SK",
        F.when(
            (F.length(F.trim(F.col("APRP_ID"))) == 0) | (F.col("APRP_ID").isNull()),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("APRP_ID"))))
    )
    .withColumn(
        "CRT_USER_SK",
        F.when(
            (F.length(F.trim(F.col("APAC_CREATE_USID"))) == 0) | (F.col("APAC_CREATE_USID").isNull()),
            F.lit("UNK")
        ).otherwise(F.trim(F.col("APAC_CREATE_USID")))
    )
    .withColumn(
        "LAST_UPDT_USER_SK",
        F.when(
            (F.length(F.trim(F.col("APAC_LAST_UPD_USID"))) == 0) | (F.col("APAC_LAST_UPD_USID").isNull()),
            F.lit("UNK")
        ).otherwise(F.trim(F.col("APAC_LAST_UPD_USID")))
    )
    .withColumn(
        "APL_ACTVTY_METH_CD_SK",
        F.when(
            (F.length(F.trim(F.col("APAC_METHOD"))) == 0) | (F.col("APAC_METHOD").isNull()),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("APAC_METHOD"))))
    )
    .withColumn(
        "APL_ACTVTY_TYP_CD_SK",
        F.when(
            (F.length(F.trim(F.col("APAC_MCTR_TYPE"))) == 0) | (F.col("APAC_MCTR_TYPE").isNull()),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("APAC_MCTR_TYPE"))))
    )
    .withColumn("ACTVTY_DT_SK", F.col("APAC_DTM"))
    .withColumn("CRT_DTM", F.col("APAC_CREATE_DTM"))
    .withColumn("LAST_UPDT_DTM", F.col("APAC_LAST_UPD_DTM"))
    .withColumn("APL_LVL_SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn("ACTVTY_SUM", F.upper(F.trim(F.col("APAC_SUMMARY"))))
    .withColumn(
        "APL_RVWR_ID",
        F.when(
            (F.length(F.trim(F.col("APRP_ID"))) == 0) | (F.col("APRP_ID").isNull()),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("APRP_ID"))))
    )
)

# CContainerStage - AplActvtyPkey
params_AplActvtyPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_AplActvtyPkey = AplActvtyPkey(df_BusinessRules, params_AplActvtyPkey)

# Final Select + rpad for IdsAplActvtyExtr
df_IdsAplActvtyExtr_final = df_AplActvtyPkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("APL_ACTVTY_SK"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK"),
    F.col("APL_RVWR_SK"),
    F.col("CRT_USER_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.col("APL_ACTVTY_METH_CD_SK"),
    F.col("APL_ACTVTY_TYP_CD_SK"),
    F.rpad(F.col("ACTVTY_DT_SK"), 10, " ").alias("ACTVTY_DT_SK"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_DTM"),
    F.col("APL_LVL_SEQ_NO"),
    F.col("ACTVTY_SUM"),
    F.col("APL_RVWR_ID")
)

write_files(
    df_IdsAplActvtyExtr_final,
    f"{adls_path}/key/FctsAplActvtyExtr.AplActvty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ODBCConnector - SnapShot_FACETS
extract_query_SnapShot = (
    f"SELECT \n"
    f"APAC.APAP_ID,\n"
    f"APAC.APAC_SEQ_NO,\n"
    f"APAC.APRP_ID,\n"
    f"APAC.APAC_DTM\n"
    f"FROM \n"
    f"{FacetsOwner}.CMC_APAC_ACTIVITY APAC,\n"
    f"{FacetsOwner}.CMC_APAP_APPEALS APAP\n"
    f"WHERE\n"
    f"APAC.APAP_ID = APAP.APAP_ID AND\n"
    f"APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND\n"
    f"APAP.APAP_LAST_UPD_DTM <= '{EndDate}'"
)

jdbc_url_FACETS_snap, jdbc_props_FACETS_snap = get_db_config(facets_secret_name)
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FACETS_snap)
    .options(**jdbc_props_FACETS_snap)
    .option("query", extract_query_SnapShot)
    .load()
)

# CTransformerStage - Rules
df_Rules_stagevars = df_SnapShot_FACETS.withColumn(
    "_svSrcSysCdSk",
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X")
)

df_Rules = df_Rules_stagevars.select(
    F.col("_svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.trim(F.regexp_replace(F.col("APAP_ID"), "[\n\r\t]", "")).alias("APL_ID"),
    F.col("APAC_SEQ_NO").alias("SEQ_NO"),
    F.date_format(F.col("APAC_DTM"), "yyyy-MM-dd").alias("ACTVTY_DT_SK"),
    F.when(
        (F.length(F.trim(F.col("APRP_ID"))) == 0) | (F.col("APRP_ID").isNull()),
        F.lit("NA")
    ).otherwise(
        F.trim(F.regexp_replace(F.upper(F.col("APRP_ID")), "[\n\r\t]", ""))
    ).alias("APL_RVWR_ID")
)

# CSeqFileStage - B_APL_ACTVTY
df_B_APL_ACTVTY_final = df_Rules.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.rpad(F.col("ACTVTY_DT_SK"), 10, " ").alias("ACTVTY_DT_SK"),
    F.col("APL_RVWR_ID")
)

write_files(
    df_B_APL_ACTVTY_final,
    f"{adls_path}/load/B_APL_ACTVTY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)