# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from MBR_LIFE_EVT and creates MBR_LIFE_EVT_D table   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                  Date              Project/Altiris #                                    Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC -----------------------------      -----------------    -----------------------------------                       ---------------------------------------------------------             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Manasa Andru           2010-12-30     D4692-Proactive: 4489 - Alineo         Originally programmed                                    EnterpriseNewDevl      Steph Goddard             01/13/2011
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/11/2013        5114                              Create Load File for EDW Table MBR_LIFE_EVT_D                             EnterpriseWhseDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrLifeEventDExtr
# MAGIC Read from source table MBR_LIFE_EVT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC Write MBR_LIFE_EVT_D Data into a Sequential file for Load Job IdsEdwMbrLifeEventDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_MBR_LIFE_EVT_in = """SELECT
 MBR_LIFE_EVT.MBR_LIFE_EVT_SK,
 MBR_LIFE_EVT.MBR_UNIQ_KEY,
 MBR_LIFE_EVT.MBR_LIFE_EVT_TYP_CD,
 MBR_LIFE_EVT.LIFE_EVT_STRT_DT_SK,
 COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
 MBR_LIFE_EVT.CRT_RUN_CYC_EXCTN_SK,
 MBR_LIFE_EVT.LAST_UPDT_RUN_CYC_EXCTN_SK,
 MBR_LIFE_EVT.CLM_SK,
 MBR_LIFE_EVT.MBR_SK,
 MBR_LIFE_EVT.SUB_SK,
 MBR_LIFE_EVT.LIFE_EVT_SRC_TYP_CD_SK,
 MBR_LIFE_EVT.MBR_LIFE_EVT_TYP_CD_SK,
 MBR_LIFE_EVT.CLM_ID
 FROM
 """ + f"{IDSOwner}" + """.MBR_LIFE_EVT MBR_LIFE_EVT
 LEFT JOIN """ + f"{IDSOwner}" + """.CD_MPPNG CD
   ON MBR_LIFE_EVT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
 WHERE
   MBR_LIFE_EVT.LAST_UPDT_RUN_CYC_EXCTN_SK >=  """ + IDSRunCycle

df_db2_MBR_LIFE_EVT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_LIFE_EVT_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = """SELECT
   CD_MPPNG_SK,
   COALESCE(TRGT_CD,'UNK') TRGT_CD,
   COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
 FROM
 """ + f"{IDSOwner}" + """.CD_MPPNG"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Cpy_Mppng_Cd_out1 = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_Mppng_Cd_out2 = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_MBR_LIFE_EVT_in.alias("lnk_IdsEdwMbrLifeEventDExtr_InAbc")
    .join(
        df_Cpy_Mppng_Cd_out1.alias("Ref_life_evt_src_typ_cd_Lkp"),
        F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.LIFE_EVT_SRC_TYP_CD_SK")
        == F.col("Ref_life_evt_src_typ_cd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Mppng_Cd_out2.alias("Ref_mbr_life_evt_typ_nm_l_Lkp"),
        F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_LIFE_EVT_TYP_CD_SK")
        == F.col("Ref_mbr_life_evt_typ_nm_l_Lkp.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes_out = df_lkp_Codes.select(
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_LIFE_EVT_SK").alias("MBR_LIFE_EVT_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.LIFE_EVT_STRT_DT_SK").alias("LIFE_EVT_STRT_DT_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.SUB_SK").alias("SUB_SK"),
    F.col("Ref_life_evt_src_typ_cd_Lkp.TRGT_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("Ref_life_evt_src_typ_cd_Lkp.TRGT_CD_NM").alias("LIFE_EVT_SRC_TYP_NM"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.CLM_ID").alias("CLM_ID"),
    F.col("Ref_mbr_life_evt_typ_nm_l_Lkp.TRGT_CD_NM").alias("MBR_LIFE_EVT_TYP_NM"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.LIFE_EVT_SRC_TYP_CD_SK").alias("LIFE_EVT_SRC_TYP_CD_SK"),
    F.col("lnk_IdsEdwMbrLifeEventDExtr_InAbc.MBR_LIFE_EVT_TYP_CD_SK").alias("MBR_LIFE_EVT_TYP_CD_SK")
)

df_xfm_BusinessLogic = (
    df_lkp_Codes_out
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn(
        "LIFE_EVT_SRC_TYP_CD",
        F.when(F.col("LIFE_EVT_SRC_TYP_CD").isNull(), F.lit("UNK"))
        .otherwise(F.col("LIFE_EVT_SRC_TYP_CD"))
    )
    .withColumn(
        "LIFE_EVT_SRC_TYP_NM",
        F.when(F.col("LIFE_EVT_SRC_TYP_NM").isNull(), F.lit("UNK"))
        .otherwise(F.col("LIFE_EVT_SRC_TYP_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_final_xfm_BusinessLogic = df_xfm_BusinessLogic.select(
    F.col("MBR_LIFE_EVT_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_LIFE_EVT_TYP_CD"),
    F.rpad(F.col("LIFE_EVT_STRT_DT_SK"), 10, " ").alias("LIFE_EVT_STRT_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK"),
    F.col("MBR_SK"),
    F.col("SUB_SK"),
    F.col("LIFE_EVT_SRC_TYP_CD"),
    F.col("LIFE_EVT_SRC_TYP_NM"),
    F.col("CLM_ID"),
    F.col("MBR_LIFE_EVT_TYP_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LIFE_EVT_SRC_TYP_CD_SK"),
    F.col("MBR_LIFE_EVT_TYP_CD_SK")
)

write_files(
    df_final_xfm_BusinessLogic,
    f"{adls_path}/load/MBR_LIFE_EVT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)