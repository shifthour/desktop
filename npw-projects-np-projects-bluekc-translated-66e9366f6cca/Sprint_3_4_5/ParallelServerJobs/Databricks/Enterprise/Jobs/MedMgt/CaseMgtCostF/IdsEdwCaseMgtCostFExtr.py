# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwCaseMgtCostFExtr
# MAGIC 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for EDW Table CASE_MGT_COST_F.         
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                          ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sravya Gorla                    09/02/2019      US#140167               Original Program to Extract CaseManagenet Cost data from IDS                   EnterpriseDev2           Jaideep Mankala          09/24/2019

# MAGIC Write CASE_MGT_COST Data into a Sequential file for Load Job IdsEdwCaseMgtCostLoad.
# MAGIC Read Most recent Data from IDS CASE_MGT_COST Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwCaseMgtCostFExtr
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsCaseMgtCostRunCycle = get_widget_value('IdsCaseMgtCostRunCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_CASE_MGT_COST_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT CASE_MGT_CST_LOG_SK, CASE_MGT_ID, CST_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, CASE_MGT_SK, COST_INPUT_USER_SK, CST_TYP_CD_SK, CST_INPT_DT, FROM_DT, TO_DT, ACTL_CST_AMT, PROJ_CST_AMT, CST_INPT_USER_ID FROM {IDSOwner}.CASE_MGT_CST_LOG WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsCaseMgtCostRunCycle}"
    )
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_db2_case_mgt_cst_f = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"SELECT CASE_MGT_CST_LOG_SK, CRT_RUN_CYC_EXCTN_DT_SK FROM {EDWOwner}.CASE_MGT_CST_LOG_F"
    )
    .load()
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') AS TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_stts_cd = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"), col("TRGT_CD"), col("TRGT_CD_NM")
)
df_sttus_nm = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"), col("TRGT_CD"), col("TRGT_CD_NM")
)

df_lkp_Codes1 = (
    df_db2_CASE_MGT_COST_in.alias("lnk_IdsEdwCaseMgtCostFExtr_InABC")
    .join(
        df_stts_cd.alias("stts_cd"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_TYP_CD_SK") == col("stts_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_sttus_nm.alias("sttus_nm"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_TYP_CD_SK") == col("sttus_nm.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_case_mgt_cst_f.alias("case_mgt_crt_dt"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CASE_MGT_CST_LOG_SK") == col("case_mgt_crt_dt.CASE_MGT_CST_LOG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CASE_MGT_ID").alias("CASE_MGT_ID"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_SEQ_NO").alias("CST_SEQ_NO"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CASE_MGT_SK").alias("CASE_MGT_SK"),
        col("stts_cd.TRGT_CD").alias("CST_TYP_CD"),
        col("sttus_nm.TRGT_CD_NM").alias("CST_TYP_NM"),
        col("case_mgt_crt_dt.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.COST_INPUT_USER_SK").alias("COST_INPUT_USER_SK"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_INPT_DT").alias("CST_INPT_DT"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.FROM_DT").alias("FROM_DT"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.TO_DT").alias("TO_DT"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.ACTL_CST_AMT").alias("ACTL_CST_AMT"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.PROJ_CST_AMT").alias("PROJ_CST_AMT"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_INPT_USER_ID").alias("CST_INPT_USER_ID"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.CST_TYP_CD_SK").alias("CST_TYP_CD_SK"),
        col("lnk_IdsEdwCaseMgtCostFExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes1
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        when(col("CRT_RUN_CYC_EXCTN_DT_SK").isNull(), EDWRunCycleDate).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CASE_MGT_SK", when(col("CASE_MGT_SK").isNull(), lit("NA")).otherwise(col("CASE_MGT_SK")))
    .withColumn("CST_TYP_CD", when(col("CST_TYP_CD").isNull(), lit("NA")).otherwise(col("CST_TYP_CD")))
    .withColumn("CST_TYP_NM", when(col("CST_TYP_NM").isNull(), lit("NA")).otherwise(col("CST_TYP_NM")))
    .withColumn("CST_INPT_DT", TimestampToDate(col("CST_INPT_DT")))
    .withColumn("FROM_DT", TimestampToDate(col("FROM_DT")))
    .withColumn("TO_DT", TimestampToDate(col("TO_DT")))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.select(
    col("CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK"),
    col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    col("CST_SEQ_NO").alias("CST_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CASE_MGT_SK").alias("CASE_MGT_SK"),
    col("CST_TYP_CD").alias("CST_TYP_CD"),
    col("CST_TYP_NM").alias("CST_TYP_NM"),
    col("CST_INPT_DT").alias("CST_INPT_DT"),
    col("FROM_DT").alias("FROM_DT"),
    col("TO_DT").alias("TO_DT"),
    col("ACTL_CST_AMT").alias("ACTL_CST_AMT"),
    col("PROJ_CST_AMT").alias("PROJ_CST_AMT"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CST_INPT_USER_ID").alias("CST_INPT_USER_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("COST_INPUT_USER_SK").alias("COST_INPUT_USER_SK"),
    col("CST_TYP_CD_SK").alias("CST_TYP_CD_SK")
)

df_final = (
    df_xfrm_BusinessLogic
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/CASE_MGT_CST_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)