# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2022  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name: FctsAplLvlExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APLV_APP_LEVEL for loading into IDS APL_LVL.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the APL_LVL table.
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/11/2007              	3028                 	Initial program                                                           		devlIDS30             	Steph Goddard      	8/23/07        
# MAGIC Bhoomi Dasari    		10/18/2007          	3028                 	Added Balancing Snapshot                                         		devlIDS30
# MAGIC Jag Yelavarthi    		2015-01-19                  	TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"               		IntegrateNewDevl      	Kalyan Neelam   	2015-01-27
# MAGIC Manasa Andru     		2016-03-16               	5391		Updated the field names from APLV_DECISION_DT,   	IntegrateDev1               Kalyan Neelam           2016-03-17
# MAGIC                                                        					APLV_INIT_DT, APLV_NOTF_DT to APLV_DECISION_DTM,
# MAGIC                                                              					APLV_INIT_DTM and APLV_NOTF_DTM
# MAGIC Ravi Singh          		2018-10- 17              	MTM-5841     	Added new shared container instead of                        		IntegrateDev2	Abhiram Dasarathy  	2018-10-30
# MAGIC                                                             					transformer stage 
# MAGIC Prabhu ES          		2022-02-25              	S2S		MSSQL connection parameters added                         		IntegrateDev5	Ken Bradmon	2022-05-17

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Level Data
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
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
RunID = get_widget_value('RunID','')
EndDate = get_widget_value('EndDate','')
RunDate = get_widget_value('RunDate','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLvlPkey
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_FACETS = f"""
SELECT CMC_APLV_APP_LEVEL.APAP_ID,
CMC_APLV_APP_LEVEL.APLV_SEQ_NO,
CMC_APLV_APP_LEVEL.APLV_LEVEL,
CMC_APLV_APP_LEVEL.APLV_INIT_DTM,
CMC_APLV_APP_LEVEL.APLV_MCTR_METH,
CMC_APLV_APP_LEVEL.APLV_CREATE_DTM,
CMC_APLV_APP_LEVEL.APLV_CREATE_USID,
CMC_APLV_APP_LEVEL.APLV_LAST_UPD_DTM,
CMC_APLV_APP_LEVEL.APLV_LAST_UPD_USID,
CMC_APLV_APP_LEVEL.APLV_DECISION,
CMC_APLV_APP_LEVEL.APLV_MCTR_REAS,
CMC_APLV_APP_LEVEL.APLV_EXP_IND,
CMC_APLV_APP_LEVEL.APLV_DECISION_DTM,
CMC_APLV_APP_LEVEL.APLV_NOTF_METH,
CMC_APLV_APP_LEVEL.APLV_NOTF_DTM,
CMC_APLV_APP_LEVEL.APLV_MCTR_NOTF,
CMC_APLV_APP_LEVEL.APLV_HEARING_IND,
CMC_APLV_APP_LEVEL.APLV_HEARING_DTM,
CMC_APLV_APP_LEVEL.APLV_MCTR_ADR,
CMC_APLV_APP_LEVEL.APLV_SUMMARY,
CMC_APLV_APP_LEVEL.APST_SEQ_NO,
CMC_APLV_APP_LEVEL.APST_STS,
CMC_APLV_APP_LEVEL.APST_STS_DTM,
CMC_APLV_APP_LEVEL.APLV_USID_PRI,
CMC_APLV_APP_LEVEL.APLV_USID_SEC,
CMC_APLV_APP_LEVEL.APLV_USID_TER,
CMC_APLV_APP_LEVEL.APLV_MCTR_LATE
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL CMC_APLV_APP_LEVEL,
     {FacetsOwner}.CMC_APAP_APPEALS APAP
WHERE CMC_APLV_APP_LEVEL.APAP_ID = APAP.APAP_ID
  AND (
        ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'
          AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
     OR
        ( CMC_APLV_APP_LEVEL.APLV_LAST_UPD_DTM >= '{BeginDate}'
          AND CMC_APLV_APP_LEVEL.APLV_LAST_UPD_DTM <= '{EndDate}' )
      )
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_FACETS)
    .load()
)

df_Strip = df_FACETS.select(
    strip_field(F.col("APAP_ID")).alias("APAP_ID"),
    F.col("APLV_SEQ_NO").alias("APLV_SEQ_NO"),
    strip_field(F.col("APLV_LEVEL")).alias("APLV_LEVEL"),
    F.date_format(F.col("APLV_INIT_DTM"), "yyyy-MM-dd").alias("APLV_INIT_DT"),
    strip_field(F.col("APLV_MCTR_METH")).alias("APLV_MCTR_METH"),
    F.date_format(F.col("APLV_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APLV_CREATE_DTM"),
    strip_field(F.col("APLV_CREATE_USID")).alias("APLV_CREATE_USID"),
    F.date_format(F.col("APLV_LAST_UPD_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APLV_LAST_UPD_DTM"),
    strip_field(F.col("APLV_LAST_UPD_USID")).alias("APLV_LAST_UPD_USID"),
    strip_field(F.col("APLV_DECISION")).alias("APLV_DECISION"),
    strip_field(F.col("APLV_MCTR_REAS")).alias("APLV_MCTR_REAS"),
    strip_field(F.col("APLV_EXP_IND")).alias("APLV_EXP_IND"),
    F.date_format(F.col("APLV_DECISION_DTM"), "yyyy-MM-dd").alias("APLV_DECISION_DT"),
    strip_field(F.col("APLV_NOTF_METH")).alias("APLV_NOTF_METH"),
    F.date_format(F.col("APLV_NOTF_DTM"), "yyyy-MM-dd").alias("APLV_NOTF_DT"),
    strip_field(F.col("APLV_MCTR_NOTF")).alias("APLV_MCTR_NOTF"),
    strip_field(F.col("APLV_HEARING_IND")).alias("APLV_HEARING_IND"),
    F.date_format(F.col("APLV_HEARING_DTM"), "yyyy-MM-dd").alias("APLV_HEARING_DTM"),
    strip_field(F.col("APLV_MCTR_ADR")).alias("APLV_MCTR_ADR"),
    strip_field(F.col("APLV_SUMMARY")).alias("APLV_SUMMARY"),
    F.col("APST_SEQ_NO").alias("APST_SEQ_NO"),
    strip_field(F.col("APST_STS")).alias("APST_STS"),
    F.date_format(F.col("APST_STS_DTM"), "yyyy-MM-dd").alias("APST_STS_DTM"),
    strip_field(F.col("APLV_USID_PRI")).alias("APLV_USID_PRI"),
    strip_field(F.col("APLV_USID_SEC")).alias("APLV_USID_SEC"),
    strip_field(F.col("APLV_USID_TER")).alias("APLV_USID_TER"),
    strip_field(F.col("APLV_MCTR_LATE")).alias("APLV_MCTR_LATE")
)

df_BusinessRules = (
    df_Strip
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("svAplId", trim(F.col("APAP_ID")))
    .withColumn("svAplvSqNo", trim(F.col("APLV_SEQ_NO")))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("svAplId"), F.lit(";"), F.col("svAplvSqNo"))
    )
    .withColumn("APL_LVL_SK", F.lit(0))
    .withColumn("APL_ID", F.col("svAplId"))
    .withColumn("SEQ_NO", F.col("svAplvSqNo"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("APL_SK", F.col("svAplId"))
    .withColumn(
        "CRT_USER_SK",
        F.when(
            (F.length(trim(F.col("APLV_CREATE_USID"))) == 0) | F.col("APLV_CREATE_USID").isNull(),
            F.lit("UNK")
        ).otherwise(trim(F.col("APLV_CREATE_USID")))
    )
    .withColumn(
        "LAST_UPDT_USER_SK",
        F.when(
            (F.length(trim(F.col("APLV_LAST_UPD_USID"))) == 0) | F.col("APLV_LAST_UPD_USID").isNull(),
            F.lit("UNK")
        ).otherwise(trim(F.col("APLV_LAST_UPD_USID")))
    )
    .withColumn(
        "PRI_USER_SK",
        F.when(
            (F.length(trim(F.col("APLV_USID_PRI"))) == 0) | F.col("APLV_USID_PRI").isNull(),
            F.lit("UNK")
        ).otherwise(trim(F.col("APLV_USID_PRI")))
    )
    .withColumn(
        "SEC_USER_SK",
        F.when(
            (F.length(trim(F.col("APLV_USID_SEC"))) == 0) | F.col("APLV_USID_SEC").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_USID_SEC")))
    )
    .withColumn(
        "TRTY_USER_SK",
        F.when(
            (F.length(trim(F.col("APLV_USID_TER"))) == 0) | F.col("APLV_USID_TER").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_USID_TER")))
    )
    .withColumn(
        "APL_LVL_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_LEVEL"))) == 0) | F.col("APLV_LEVEL").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_LEVEL")))
    )
    .withColumn(
        "APL_LVL_CUR_STTUS_CD_SK",
        F.when(
            (F.length(trim(F.col("APST_STS"))) == 0) | F.col("APST_STS").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APST_STS")))
    )
    .withColumn(
        "APL_LVL_DCSN_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_DECISION"))) == 0) | F.col("APLV_DECISION").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_DECISION")))
    )
    .withColumn(
        "APL_LVL_DCSN_RSN_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_MCTR_REAS"))) == 0) | F.col("APLV_MCTR_REAS").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_MCTR_REAS")))
    )
    .withColumn(
        "APL_LVL_DSPT_RSLTN_TYP_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_MCTR_ADR"))) == 0) | F.col("APLV_MCTR_ADR").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_MCTR_ADR")))
    )
    .withColumn(
        "APL_LVL_INITN_METH_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_MCTR_METH"))) == 0) | F.col("APLV_MCTR_METH").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_MCTR_METH")))
    )
    .withColumn(
        "APL_LVL_LATE_DCSN_RSN_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_MCTR_LATE"))) == 0) | F.col("APLV_MCTR_LATE").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_MCTR_LATE")))
    )
    .withColumn(
        "APL_LVL_NTFCTN_CAT_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_MCTR_NOTF"))) == 0) | F.col("APLV_MCTR_NOTF").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_MCTR_NOTF")))
    )
    .withColumn(
        "APL_LVL_NTFCTN_METH_CD_SK",
        F.when(
            (F.length(trim(F.col("APLV_NOTF_METH"))) == 0) | F.col("APLV_NOTF_METH").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_NOTF_METH")))
    )
    .withColumn(
        "EXPDTD_IN",
        F.when(
            (F.length(trim(F.col("APLV_EXP_IND"))) == 0) | F.col("APLV_EXP_IND").isNull(),
            F.lit("X")
        ).otherwise(trim(F.upper(F.col("APLV_EXP_IND"))))
    )
    .withColumn(
        "HRNG_IN",
        F.when(
            (F.length(trim(F.col("APLV_HEARING_IND"))) == 0) | F.col("APLV_HEARING_IND").isNull(),
            F.lit("X")
        ).otherwise(trim(F.upper(F.col("APLV_HEARING_IND"))))
    )
    .withColumn(
        "INITN_DT_SK",
        F.when(
            (F.length(trim(F.col("APLV_INIT_DT"))) == 0) | F.col("APLV_INIT_DT").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_INIT_DT")))
    )
    .withColumn("CRT_DTM", F.col("APLV_CREATE_DTM"))
    .withColumn("CUR_STTUS_DTM", F.col("APST_STS_DTM"))
    .withColumn(
        "DCSN_DT_SK",
        F.when(
            (F.length(trim(F.col("APLV_DECISION_DT"))) == 0) | F.col("APLV_DECISION_DT").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_DECISION_DT")))
    )
    .withColumn(
        "HRNG_DT_SK",
        F.when(
            (F.length(trim(F.col("APLV_HEARING_DTM"))) == 0) | F.col("APLV_HEARING_DTM").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_HEARING_DTM")))
    )
    .withColumn("LAST_UPDT_DTM", F.col("APLV_LAST_UPD_DTM"))
    .withColumn(
        "NTFCTN_DT_SK",
        F.when(
            (F.length(trim(F.col("APLV_NOTF_DT"))) == 0) | F.col("APLV_NOTF_DT").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("APLV_NOTF_DT")))
    )
    .withColumn("CUR_STTUS_SEQ_NO", trim(F.col("APST_SEQ_NO")))
    .withColumn("LVL_DESC", trim(F.upper(F.col("APLV_SUMMARY"))))
)

params_aplLvlPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_AplLvlPkey = AplLvlPkey(df_BusinessRules, params_aplLvlPkey)

df_IdsAplLvlExtr = df_AplLvlPkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("PRI_USER_SK").alias("PRI_USER_SK"),
    F.col("SEC_USER_SK").alias("SEC_USER_SK"),
    F.col("TRTY_USER_SK").alias("TRTY_USER_SK"),
    F.col("APL_LVL_CD_SK").alias("APL_LVL_CD_SK"),
    F.col("APL_LVL_CUR_STTUS_CD_SK").alias("APL_LVL_CUR_STTUS_CD_SK"),
    F.col("APL_LVL_DCSN_CD_SK").alias("APL_LVL_DCSN_CD_SK"),
    F.col("APL_LVL_DCSN_RSN_CD_SK").alias("APL_LVL_DCSN_RSN_CD_SK"),
    F.col("APL_LVL_DSPT_RSLTN_TYP_CD_SK").alias("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    F.col("APL_LVL_INITN_METH_CD_SK").alias("APL_LVL_INITN_METH_CD_SK"),
    F.col("APL_LVL_LATE_DCSN_RSN_CD_SK").alias("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    F.col("APL_LVL_NTFCTN_CAT_CD_SK").alias("APL_LVL_NTFCTN_CAT_CD_SK"),
    F.col("APL_LVL_NTFCTN_METH_CD_SK").alias("APL_LVL_NTFCTN_METH_CD_SK"),
    F.rpad(F.col("EXPDTD_IN"), 1, " ").alias("EXPDTD_IN"),
    F.rpad(F.col("HRNG_IN"), 1, " ").alias("HRNG_IN"),
    F.rpad(F.col("INITN_DT_SK"), 10, " ").alias("INITN_DT_SK"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
    F.rpad(F.col("DCSN_DT_SK"), 10, " ").alias("DCSN_DT_SK"),
    F.rpad(F.col("HRNG_DT_SK"), 10, " ").alias("HRNG_DT_SK"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("NTFCTN_DT_SK"), 10, " ").alias("NTFCTN_DT_SK"),
    F.col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
    F.col("LVL_DESC").alias("LVL_DESC")
)

write_files(
    df_IdsAplLvlExtr,
    f"{adls_path}/key/FctsAplLvlExtr.AplLvl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_SnapShot_FACETS = f"""
SELECT CMC_APLV_APP_LEVEL.APAP_ID,
CMC_APLV_APP_LEVEL.APLV_SEQ_NO,
CMC_APLV_APP_LEVEL.APLV_DECISION_DTM,
CMC_APLV_APP_LEVEL.APST_SEQ_NO,
CMC_APLV_APP_LEVEL.APST_STS_DTM
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL CMC_APLV_APP_LEVEL,
     {FacetsOwner}.CMC_APAP_APPEALS APAP
WHERE CMC_APLV_APP_LEVEL.APAP_ID = APAP.APAP_ID
  AND (
        ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'
          AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
     OR
        ( CMC_APLV_APP_LEVEL.APLV_LAST_UPD_DTM >= '{BeginDate}'
          AND CMC_APLV_APP_LEVEL.APLV_LAST_UPD_DTM <= '{EndDate}' )
      )
"""
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SnapShot_FACETS)
    .load()
)

df_Rules = (
    df_SnapShot_FACETS
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X"))
    .withColumn("svDscnDtSk", F.date_format(F.col("APLV_DECISION_DTM"), "yyyy-MM-dd"))
    .withColumn("SRC_SYS_CD_SK", F.col("svSrcSysCdSk"))
    .withColumn(
        "APL_ID",
        trim(strip_field(F.col("APAP_ID")))
    )
    .withColumn("SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn(
        "CUR_STTUS_DTM",
        F.date_format(F.col("APST_STS_DTM"), "yyyy-MM-dd")
    )
    .withColumn("CUR_STTUS_SEQ_NO", F.col("APST_SEQ_NO"))
    .withColumn(
        "DCSN_DT_SK",
        F.when(
            (F.length(trim(F.col("svDscnDtSk"))) == 0) | F.col("svDscnDtSk").isNull(),
            F.lit("NA")
        ).otherwise(trim(F.col("svDscnDtSk")))
    )
)

df_B_APL_LVL = df_Rules.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("CUR_STTUS_DTM"),
    F.col("CUR_STTUS_SEQ_NO"),
    F.rpad(F.col("DCSN_DT_SK"), 10, " ").alias("DCSN_DT_SK")
)

write_files(
    df_B_APL_LVL,
    f"{adls_path}/load/B_APL_LVL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)