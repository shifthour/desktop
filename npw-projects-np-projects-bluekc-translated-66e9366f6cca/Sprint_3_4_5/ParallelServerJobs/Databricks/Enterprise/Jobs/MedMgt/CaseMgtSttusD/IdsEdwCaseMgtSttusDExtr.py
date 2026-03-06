# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwCaseMgtSttusDExtr
# MAGIC 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for EDW Table CASE_MGT_STTUS_D.         
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala             07/11/2019      115013                        Original Program to Extract CM Status data from IDS                   IntegrateDev2          Abhiram Dasarathy	2019-07-15

# MAGIC Write CASE_MGT_STTUS Data into a Sequential file for Load Job IdsEdwCaseMgtSttusLoad.
# MAGIC Read Most recent Data from IDS CASE_MGT_STTUS Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwCaseMgtSttusDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Obtain parameter values
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsCaseMgtSttusRunCycle = get_widget_value('IdsCaseMgtSttusRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# db2_CASE_MGT_STTUS_in (IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids_case_mgt_sttus = (
    f"SELECT DISTINCT "
    f"CASE_MGT_STTUS_SK, "
    f"CASE_MGT_ID, "
    f"CASE_MGT_STTUS_SEQ_NO, "
    f"SRC_SYS_CD, "
    f"CRT_RUN_CYC_EXCTN_SK, "
    f"LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"CASE_MGT_SK, "
    f"CRT_BY_USER_SK, "
    f"RTE_TO_USER_SK, "
    f"CASE_MGT_STTUS_CD_SK, "
    f"CASE_MGT_STTUS_RSN_CD_SK, "
    f"CASE_MGT_STTUS_DTM "
    f"FROM {IDSOwner}.CASE_MGT_STTUS "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsCaseMgtSttusRunCycle}"
)
df_db2_CASE_MGT_STTUS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_case_mgt_sttus)
    .load()
)
df_lnk_IdsEdwCaseMgtSttusDExtr_InABC = df_db2_CASE_MGT_STTUS_in

# db2_case_mgt_sts_d (EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_edw_case_mgt_sts_d = (
    f"SELECT "
    f"CASE_MGT_STTUS_SK, "
    f"CRT_RUN_CYC_EXCTN_DT_SK "
    f"FROM {EDWOwner}.CASE_MGT_STTUS_D"
)
df_db2_case_mgt_sts_d = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_edw_case_mgt_sts_d)
    .load()
)
df_case_mgt_crt_dt = df_db2_case_mgt_sts_d

# db2_CD_MPPNG_in (IDS)
extract_query_ids_cd_mppng = (
    f"SELECT "
    f"CD_MPPNG_SK, "
    f"COALESCE(TRGT_CD,'UNK') TRGT_CD, "
    f"COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_cd_mppng)
    .load()
)
df_lnk_CdMppng_out = df_db2_CD_MPPNG_in

# COPY stage for db2_CD_MPPNG_in outputs
df_stts_cd = df_lnk_CdMppng_out.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_sttus_nm = df_lnk_CdMppng_out.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_reasn_cd = df_lnk_CdMppng_out.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_reasn_cd_nm = df_lnk_CdMppng_out.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# APP_USER (IDS)
extract_query_ids_app_user = (
    f"SELECT DISTINCT "
    f"USER_SK, "
    f"USER_ID "
    f"FROM {IDSOwner}.APP_USER "
    f"WHERE USER_SK NOT IN (0,1)"
)
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_app_user)
    .load()
)
df_lnk_AppUserData_out = df_APP_USER

# COPY1 stage for APP_USER outputs
df_user_lkup = df_lnk_AppUserData_out.select(
    col("USER_SK").alias("USER_SK"),
    col("USER_ID").alias("USER_ID")
)
df_Route_lkup = df_lnk_AppUserData_out.select(
    col("USER_SK").alias("USER_SK"),
    col("USER_ID").alias("USER_ID")
)

# lkp_Codes1 (PxLookup)
df_lkp_Codes1 = (
    df_lnk_IdsEdwCaseMgtSttusDExtr_InABC.alias("lnk_IdsEdwCaseMgtSttusDExtr_InABC")
    .join(
        df_stts_cd.alias("stts_cd"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_CD_SK") 
            == col("stts_cd.CD_MPPNG_SK")
        ],
        how="left"
    )
    .join(
        df_sttus_nm.alias("sttus_nm"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_CD_SK") 
            == col("sttus_nm.CD_MPPNG_SK")
        ],
        how="left"
    )
    .join(
        df_reasn_cd.alias("reasn_cd"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_RSN_CD_SK") 
            == col("reasn_cd.CD_MPPNG_SK")
        ],
        how="left"
    )
    .join(
        df_reasn_cd_nm.alias("reasn_cd_nm"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_RSN_CD_SK") 
            == col("reasn_cd_nm.CD_MPPNG_SK")
        ],
        how="left"
    )
    .join(
        df_Route_lkup.alias("Route_lkup"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.RTE_TO_USER_SK") == col("Route_lkup.USER_SK"),
            col("Lnk_IdsCMSTFkey_LKp.CMST_USID_ROUTE") == col("Route_lkup.USER_ID")
        ],
        how="left"
    )
    .join(
        df_user_lkup.alias("user_lkup"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CRT_BY_USER_SK") == col("user_lkup.USER_SK")
        ],
        how="left"
    )
    .join(
        df_case_mgt_crt_dt.alias("case_mgt_crt_dt"),
        on=[
            col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_SK") == col("case_mgt_crt_dt.CASE_MGT_STTUS_SK")
        ],
        how="left"
    )
)

df_lnk_FkeyLkpData_out = df_lkp_Codes1.select(
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_ID").alias("CASE_MGT_ID"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_SK").alias("CASE_MGT_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_RSN_CD_SK").alias("CASE_MGT_STTUS_RSN_CD_SK"),
    col("lnk_IdsEdwCaseMgtSttusDExtr_InABC.CASE_MGT_STTUS_DTM").alias("CASE_MGT_STTUS_DTM"),
    col("stts_cd.TRGT_CD").alias("STTUS_CD"),
    col("sttus_nm.TRGT_CD_NM").alias("STTUS_CD_NM"),
    col("reasn_cd.TRGT_CD").alias("REASON_CD"),
    col("reasn_cd_nm.TRGT_CD_NM").alias("REASON_CD_NM"),
    col("Route_lkup.USER_ID").alias("ROUTE_USER_ID"),
    col("user_lkup.USER_ID").alias("USER_USER_ID"),
    col("case_mgt_crt_dt.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK")
)

# xfrm_BusinessLogic
df_xfrm_BusinessLogic = df_lnk_FkeyLkpData_out.select(
    col("CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK"),
    col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    when(col("CRT_RUN_CYC_EXCTN_DT_SK").isNull(), lit(EDWRunCycleDate))
        .otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
        .alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("CASE_MGT_SK").isNull(), lit("NA"))
        .otherwise(col("CASE_MGT_SK"))
        .alias("CASE_MGT_SK"),
    col("CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    col("RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
    col("STTUS_CD").alias("CASE_MGT_STTUS_CD"),
    col("STTUS_CD_NM").alias("CASE_MGT_STTUS_NM"),
    when(col("REASON_CD").isNull(), lit("NA"))
        .otherwise(col("REASON_CD"))
        .alias("CASE_MGT_STTUS_RSN_CD"),
    when(col("REASON_CD_NM").isNull(), lit("NA"))
        .otherwise(col("REASON_CD_NM"))
        .alias("CASE_MGT_STTUS_RSN_NM"),
    col("CASE_MGT_STTUS_DTM").alias("CASE_MGT_STTUS_DTM"),
    when(col("USER_USER_ID").isNull(), lit("NA"))
        .otherwise(col("USER_USER_ID"))
        .alias("CRT_BY_USER_ID"),
    when(col("ROUTE_USER_ID").isNull(), lit("NA"))
        .otherwise(col("ROUTE_USER_ID"))
        .alias("RTE_TO_USER_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
    col("CASE_MGT_STTUS_RSN_CD_SK").alias("CASE_MGT_STTUS_RSN_CD_SK")
)

df_lnk_IdsEdwCmSttusDExtr_OutABC = df_xfrm_BusinessLogic

# seq_CASE_MGT_STTUS_D_csv_load (PxSequentialFile)
df_final = (
    df_lnk_IdsEdwCmSttusDExtr_OutABC
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CRT_BY_USER_ID", rpad(col("CRT_BY_USER_ID"), 10, " "))
    .withColumn("RTE_TO_USER_ID", rpad(col("RTE_TO_USER_ID"), 10, " "))
    .select(
        "CASE_MGT_STTUS_SK",
        "CASE_MGT_ID",
        "CASE_MGT_STTUS_SEQ_NO",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CASE_MGT_SK",
        "CRT_BY_USER_SK",
        "RTE_TO_USER_SK",
        "CASE_MGT_STTUS_CD",
        "CASE_MGT_STTUS_NM",
        "CASE_MGT_STTUS_RSN_CD",
        "CASE_MGT_STTUS_RSN_NM",
        "CASE_MGT_STTUS_DTM",
        "CRT_BY_USER_ID",
        "RTE_TO_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CASE_MGT_STTUS_CD_SK",
        "CASE_MGT_STTUS_RSN_CD_SK"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CASE_MGT_STTUS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)