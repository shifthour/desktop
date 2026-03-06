# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     
# MAGIC 	   EdwMbrMcareEvtVerDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS and codes hash file to load into the EDW
# MAGIC 
# MAGIC Calling job Name: EdwMbrNoDriverSeq
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	   IDS:   MBR_MCARE_EVT_VER
# MAGIC   
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   Sequential file for database loading
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                          DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------          --------------------
# MAGIC Neelima Tummala      2020-11-17         US#300498                  Initial Development                                                                                 EnterpriseDev2	   Jeyaprasanna             2020-12-02       
# MAGIC Neelima Tummala      2020-12-17         US#300498                  Updated Incremental logic                                                                      EnterpriseDev2         Jeyaprasanna             2020-12-08

# MAGIC Job name:
# MAGIC IdsEdwMbrMcareEvtVerDExtr
# MAGIC Write MBR_MCARE_EVT_VER_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table MBR_MCARE_EVT_VER.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_MBR_MCARE_EVT_VER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "select MCARE_EVT.MBR_MCARE_EVT_VER_SK, MCARE_EVT.MBR_UNIQ_KEY, MCARE_EVT.MBR_MCARE_EVT_CD, MCARE_EVT.MBR_MCARE_EVT_HCFA_EFF_DT, MCARE_EVT.MBR_MCARE_EVT_VER_SEQ_NO, MCARE_EVT.SRC_SYS_CD, MCARE_EVT.MBR_SK, MCARE_EVT.MBR_MCARE_EVT_SK, MCARE_EVT.ATCHMT_SRC_ID_DTM, MCARE_EVT.MBR_MCARE_EVT_VER_DT, MCARE_EVT.MBR_MCARE_EVT_VER_PERD_STRT_DT, MCARE_EVT.MBR_MCARE_EVT_VER_PERD_END_DT, MCARE_EVT.INSTUT_ID, MCARE_EVT.MBR_MCARE_EVT_VER_BY_NM, MCARE_EVT.CRT_RUN_CYC_EXCTN_SK, MCARE_EVT.LAST_UPDT_RUN_CYC_EXCTN_SK, MCARE_EVT.MBR_MCARE_EVT_CD_SK, MCARE_EVT.MBR_MCARE_EVT_VER_METH_CD_SK, MCARE_EVT.MBR_MCARE_EVT_VER_RSN_CD_SK, MCARE_EVT.MBR_MCARE_EVT_VER_RSLT_CD_SK "
        f"from {IDSOwner}.MBR_MCARE_EVT_VER MCARE_EVT "
        f"where MCARE_EVT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
    )
    .load()
)

df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT SRC_CD, CD_MPPNG_SK, UPPER(SRC_DOMAIN_NM) as SRC_DOMAIN_NM, UPPER(SRC_CLCTN_CD) as SRC_CLCTN_CD, UPPER(SRC_SYS_CD) as SRC_SYS_CD, UPPER(TRGT_CLCTN_CD) as TRGT_CLCTN_CD, UPPER(TRGT_DOMAIN_NM) as TRGT_DOMAIN_NM, TRGT_CD, UPPER(TRGT_CD_NM) as TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_lnk_rslt_cd = df_CD_MPPNG.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("SRC_DOMAIN_NM") == "VERIFICATION RESULT")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "VERIFICATION RESULT")
).select(
    F.col("TRGT_CD").alias("RSLT_TRGT_CD"),
    F.col("CD_MPPNG_SK").alias("RSLT_CD_CD_MPPNG_SK"),
    F.col("TRGT_CD_NM").alias("RSLT_TRGT_CD_NM")
)

df_lnk_event_cd = df_CD_MPPNG.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("SRC_DOMAIN_NM") == "MEDICARE EVENT")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "MEDICARE EVENT")
).select(
    F.col("TRGT_CD").alias("EVENT_TRGT_CD"),
    F.col("CD_MPPNG_SK").alias("EVENT_CD_CD_MPPNG_SK"),
    F.col("TRGT_CD_NM").alias("EVENT_TRGT_CD_NM")
)

df_lnk_event_meth_cd = df_CD_MPPNG.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("SRC_DOMAIN_NM") == "MEMBER COB LAST VERIFICATION METHOD")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "MEMBER COB LAST VERIFICATION METHOD")
).select(
    F.col("TRGT_CD").alias("METH_TRGT_CD"),
    F.col("CD_MPPNG_SK").alias("METH_CD_MPPNG_SK"),
    F.col("TRGT_CD_NM").alias("METH_TRGT_CD_NM")
)

df_lnk_rsn_cd = df_CD_MPPNG.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("SRC_DOMAIN_NM") == "VERIFICATION REASON")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "VERIFICATION REASON")
).select(
    F.col("TRGT_CD").alias("RSN_TRGT_CD"),
    F.col("CD_MPPNG_SK").alias("RSN_CD_MPPNG_SK"),
    F.col("TRGT_CD_NM").alias("RSN_TRGT_CD_NM")
)

df_Lkp_Mcare_Evt = (
    df_db2_MBR_MCARE_EVT_VER_in.alias("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1")
    .join(
        df_lnk_rslt_cd.alias("Lnk_Rslt_Cd"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_RSLT_CD_SK")
        == F.col("Lnk_Rslt_Cd.RSLT_CD_CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_lnk_event_cd.alias("Lnk_Event_cd"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_CD_SK")
        == F.col("Lnk_Event_cd.EVENT_CD_CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_lnk_event_meth_cd.alias("Lnk_Event_Meth_cd"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_METH_CD_SK")
        == F.col("Lnk_Event_Meth_cd.METH_CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_lnk_rsn_cd.alias("Lnk_Rsn_cd"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_RSN_CD_SK")
        == F.col("Lnk_Rsn_cd.RSN_CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_SK").alias("MBR_MCARE_EVT_VER_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_HCFA_EFF_DT").alias("MBR_MCARE_EVT_HCFA_EFF_DT"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_SEQ_NO").alias("MBR_MCARE_EVT_VER_SEQ_NO"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_SK").alias("MBR_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_SK").alias("MBR_MCARE_EVT_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_DT").alias("MBR_MCARE_EVT_VER_DT"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_PERD_STRT_DT").alias("MBR_MCARE_EVT_VER_PERD_STRT_DT"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_PERD_END_DT").alias("MBR_MCARE_EVT_VER_PERD_END_DT"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.INSTUT_ID").alias("INSTUT_ID"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_BY_NM").alias("MBR_MCARE_EVT_VER_BY_NM"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_CD_SK").alias("MBR_MCARE_EVT_CD_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_METH_CD_SK").alias("MBR_MCARE_EVT_VER_METH_CD_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_RSN_CD_SK").alias("MBR_MCARE_EVT_VER_RSN_CD_SK"),
        F.col("Ink_IdsEdwMbrMcareEvtDExtr_inABC_1.MBR_MCARE_EVT_VER_RSLT_CD_SK").alias("MBR_MCARE_EVT_VER_RSLT_CD_SK"),
        F.col("Lnk_Rslt_Cd.RSLT_TRGT_CD").alias("RSLT_TRGT_CD"),
        F.col("Lnk_Rslt_Cd.RSLT_CD_CD_MPPNG_SK").alias("RSLT_CD_CD_MPPNG_SK"),
        F.col("Lnk_Rslt_Cd.RSLT_TRGT_CD_NM").alias("RSLT_TRGT_CD_NM"),
        F.col("Lnk_Event_cd.EVENT_TRGT_CD").alias("EVENT_TRGT_CD"),
        F.col("Lnk_Event_cd.EVENT_CD_CD_MPPNG_SK").alias("EVENT_CD_CD_MPPNG_SK"),
        F.col("Lnk_Event_cd.EVENT_TRGT_CD_NM").alias("EVENT_TRGT_CD_NM"),
        F.col("Lnk_Event_Meth_cd.METH_CD_MPPNG_SK").alias("METH_CD_MPPNG_SK"),
        F.col("Lnk_Event_Meth_cd.METH_TRGT_CD_NM").alias("METH_TRGT_CD_NM"),
        F.col("Lnk_Rsn_cd.RSN_TRGT_CD").alias("RSN_TRGT_CD"),
        F.col("Lnk_Rsn_cd.RSN_CD_MPPNG_SK").alias("RSN_CD_MPPNG_SK"),
        F.col("Lnk_Rsn_cd.RSN_TRGT_CD_NM").alias("RSN_TRGT_CD_NM"),
        F.col("Lnk_Event_Meth_cd.METH_TRGT_CD").alias("METH_TRGT_CD"),
    )
)

df_xmf_businessLogic = (
    df_Lkp_Mcare_Evt
    .withColumn(
        "MBR_MCARE_EVT_NM",
        F.when(
            F.col("MBR_MCARE_EVT_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("EVENT_CD_CD_MPPNG_SK").isNull()
            | (trim(F.col("EVENT_CD_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("EVENT_TRGT_CD_NM")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_METH_CD",
        F.when(
            F.col("MBR_MCARE_EVT_VER_METH_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_METH_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("METH_CD_MPPNG_SK").isNull()
            | (trim(F.col("METH_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("METH_TRGT_CD")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_METH_NM",
        F.when(
            F.col("MBR_MCARE_EVT_VER_METH_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_METH_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("METH_CD_MPPNG_SK").isNull()
            | (trim(F.col("METH_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("METH_TRGT_CD_NM")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_RSN_CD",
        F.when(
            F.col("MBR_MCARE_EVT_VER_RSN_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_RSN_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("RSN_CD_MPPNG_SK").isNull()
            | (trim(F.col("RSN_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("RSN_TRGT_CD")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_RSN_NM",
        F.when(
            F.col("MBR_MCARE_EVT_VER_RSN_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_RSN_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("RSN_CD_MPPNG_SK").isNull()
            | (trim(F.col("RSN_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("RSN_TRGT_CD_NM")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_RSLT_CD",
        F.when(
            F.col("MBR_MCARE_EVT_VER_RSLT_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_RSLT_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("RSLT_CD_CD_MPPNG_SK").isNull()
            | (trim(F.col("RSLT_CD_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("RSLT_TRGT_CD")),
    )
    .withColumn(
        "MBR_MCARE_EVT_VER_RSLT_NM",
        F.when(
            F.col("MBR_MCARE_EVT_VER_RSLT_CD_SK").isNull()
            | (trim(F.col("MBR_MCARE_EVT_VER_RSLT_CD_SK")) == ""),
            "NA",
        ).when(
            F.col("RSLT_CD_CD_MPPNG_SK").isNull()
            | (trim(F.col("RSLT_CD_CD_MPPNG_SK")) == ""),
            "UNK",
        ).otherwise(F.col("RSLT_TRGT_CD_NM")),
    )
    .withColumn(
        "ATCHMT_SRC_ID_DTM",
        TimestampToDate(F.col("ATCHMT_SRC_ID_DTM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
)

df_final = df_xmf_businessLogic.select(
    F.col("MBR_MCARE_EVT_VER_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_MCARE_EVT_CD"),
    F.col("MBR_MCARE_EVT_HCFA_EFF_DT"),
    F.col("MBR_MCARE_EVT_VER_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("MBR_MCARE_EVT_SK"),
    F.col("MBR_MCARE_EVT_NM"),
    F.col("MBR_MCARE_EVT_VER_METH_CD"),
    F.col("MBR_MCARE_EVT_VER_METH_NM"),
    F.col("MBR_MCARE_EVT_VER_RSN_CD"),
    F.col("MBR_MCARE_EVT_VER_RSN_NM"),
    F.col("MBR_MCARE_EVT_VER_RSLT_CD"),
    F.col("MBR_MCARE_EVT_VER_RSLT_NM"),
    F.col("ATCHMT_SRC_ID_DTM"),
    F.col("MBR_MCARE_EVT_VER_DT"),
    F.col("MBR_MCARE_EVT_VER_PERD_STRT_DT"),
    F.col("MBR_MCARE_EVT_VER_PERD_END_DT"),
    F.col("INSTUT_ID"),
    F.col("MBR_MCARE_EVT_VER_BY_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_MCARE_EVT_CD_SK"),
    F.col("MBR_MCARE_EVT_VER_METH_CD_SK"),
    F.col("MBR_MCARE_EVT_VER_RSN_CD_SK"),
    F.col("MBR_MCARE_EVT_VER_RSLT_CD_SK"),
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_MCARE_EVT_VER_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)