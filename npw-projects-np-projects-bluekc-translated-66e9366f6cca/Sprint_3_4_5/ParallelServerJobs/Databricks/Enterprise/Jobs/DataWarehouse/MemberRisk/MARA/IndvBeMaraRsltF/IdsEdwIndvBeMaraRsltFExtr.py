# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from IDS INDV_BE_MARA_RSLT table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                 03/04/2015      5460                                Originally Programmed                        EnterpriseNewDevl      Kalyan Neelam              2015-03-11
# MAGIC 
# MAGIC Raja Gummadi                 07/05/2016      12582                              Extract from Temp tables                    EnterpriseDev1            Jag Yelavarthi               2016-07-05
# MAGIC  
# MAGIC Raja Gummadi                 08/09/2016       TFS-12582                     Changed extract to original Tables      EnterpriseDev1           Kalyan Neelam             2016-08-11
# MAGIC 
# MAGIC Harikanth Reddy              11/02/2020       US - 300269                              Added VRSN_ID                     EnterpriseDev2           Jeyaprasanna               2020-11-12
# MAGIC Kotha Venkat
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi            11/11/2022    US-554444                  Added New Variable SrcSys to removed   EnterpriseDev2
# MAGIC                                                                                                    Hardcoded Value

# MAGIC Read all the Data from IDS INDV_BE_MARA_RSLT Table;
# MAGIC Job Name: IdsEdwIndvBeMaraRsltFExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBR_CARE_OPP_I Data into a Sequential file for Load Job IdsEdwMbrCareOppILoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysNm = get_widget_value('SrcSysNm','')

# db2_INDV_BE_MARA_RSLT_in
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_INDV_BE_MARA_RSLT_in = f"""
SELECT 
INDV_BE_MARA_RSLT_1.INDV_BE_MARA_RSLT_SK,
INDV_BE_MARA_RSLT_1.INDV_BE_KEY,
INDV_BE_MARA_RSLT_1.MDL_ID,
INDV_BE_MARA_RSLT_1.PRCS_YR_MO_SK,
INDV_BE_MARA_RSLT_1.SRC_SYS_CD_SK,
INDV_BE_MARA_RSLT_1.CRT_RUN_CYC_EXCTN_SK,
INDV_BE_MARA_RSLT_1.LAST_UPDT_RUN_CYC_EXCTN_SK,
INDV_BE_MARA_RSLT_1.GNDR_CD_SK,
INDV_BE_MARA_RSLT_1.CST_MED_ALW_AMT,
INDV_BE_MARA_RSLT_1.CST_PDX_ALW_AMT,
INDV_BE_MARA_RSLT_1.CST_TOT_ALW_AMT,
INDV_BE_MARA_RSLT_1.DIAG_CD_CAT_CT,
INDV_BE_MARA_RSLT_1.DIAG_CD_UNCAT_CT,
INDV_BE_MARA_RSLT_1.EXPSR_MO_CT,
INDV_BE_MARA_RSLT_1.NDC_CAT_CT,
INDV_BE_MARA_RSLT_1.NDC_UNCAT_CT,
INDV_BE_MARA_RSLT_1.INDV_AGE_NO,
INDV_BE_MARA_RSLT_1.ER_SCORE_NO,
INDV_BE_MARA_RSLT_1.IP_SCORE_NO,
INDV_BE_MARA_RSLT_1.MED_SCORE_NO,
INDV_BE_MARA_RSLT_1.OTHR_SVC_SCORE_NO,
INDV_BE_MARA_RSLT_1.OP_SCORE_NO,
INDV_BE_MARA_RSLT_1.PDX_SCORE_NO,
INDV_BE_MARA_RSLT_1.PHYS_SVC_SCORE_NO,
INDV_BE_MARA_RSLT_1.TOT_SCORE_NO,
COALESCE(CD_MPPNG.TRGT_CD,'UNK') as SRC_SYS_CD,
INDV_BE_MARA_RSLT_1.VRSN_ID as VRSN_ID
FROM {IDSOwner}.INDV_BE_MARA_RSLT AS INDV_BE_MARA_RSLT_1
LEFT JOIN {IDSOwner}.CD_MPPNG AS CD_MPPNG
ON INDV_BE_MARA_RSLT_1.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
WHERE INDV_BE_MARA_RSLT_1.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
AND CD_MPPNG.TRGT_CD = '{SrcSysNm}'
"""
df_db2_INDV_BE_MARA_RSLT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_INDV_BE_MARA_RSLT_in)
    .load()
)

# db2_CD_MPPNG
extract_query_db2_CD_MPPNG = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

# GndrLkup (PxLookup)
df_joined_GndrLkup = df_db2_INDV_BE_MARA_RSLT_in.alias("IdsOut").join(
    df_db2_CD_MPPNG.alias("lnk_CdMppng"),
    F.col("IdsOut.GNDR_CD_SK") == F.col("lnk_CdMppng.CD_MPPNG_SK"),
    "left"
)

df_GndrLkup = df_joined_GndrLkup.select(
    F.col("IdsOut.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("IdsOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("IdsOut.MDL_ID").alias("MDL_ID"),
    F.col("IdsOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("IdsOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("IdsOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("IdsOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IdsOut.GNDR_CD_SK").alias("GNDR_CD_SK"),
    F.col("IdsOut.CST_MED_ALW_AMT").alias("CST_MED_ALW_AMT"),
    F.col("IdsOut.CST_PDX_ALW_AMT").alias("CST_PDX_ALW_AMT"),
    F.col("IdsOut.CST_TOT_ALW_AMT").alias("CST_TOT_ALW_AMT"),
    F.col("IdsOut.DIAG_CD_CAT_CT").alias("DIAG_CD_CAT_CT"),
    F.col("IdsOut.DIAG_CD_UNCAT_CT").alias("DIAG_CD_UNCAT_CT"),
    F.col("IdsOut.EXPSR_MO_CT").alias("EXPSR_MO_CT"),
    F.col("IdsOut.NDC_CAT_CT").alias("NDC_CAT_CT"),
    F.col("IdsOut.NDC_UNCAT_CT").alias("NDC_UNCAT_CT"),
    F.col("IdsOut.INDV_AGE_NO").alias("INDV_AGE_NO"),
    F.col("IdsOut.ER_SCORE_NO").alias("ER_SCORE_NO"),
    F.col("IdsOut.IP_SCORE_NO").alias("IP_SCORE_NO"),
    F.col("IdsOut.MED_SCORE_NO").alias("MED_SCORE_NO"),
    F.col("IdsOut.OTHR_SVC_SCORE_NO").alias("OTHR_SVC_SCORE_NO"),
    F.col("IdsOut.OP_SCORE_NO").alias("OP_SCORE_NO"),
    F.col("IdsOut.PDX_SCORE_NO").alias("PDX_SCORE_NO"),
    F.col("IdsOut.PHYS_SVC_SCORE_NO").alias("PHYS_SVC_SCORE_NO"),
    F.col("IdsOut.TOT_SCORE_NO").alias("TOT_SCORE_NO"),
    F.col("IdsOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_CdMppng.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_CdMppng.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("IdsOut.VRSN_ID").alias("VRSN_ID")
)

# xfrm_BusinessLogic (CTransformerStage)
# We need row numbering for the two links that emit only the first row.
windowSpec = Window.orderBy(F.lit(1))
df_numbered = df_GndrLkup.withColumn("row_number", F.row_number().over(windowSpec))

# lnk_xfm_Data: constraint => lkupout.INDV_BE_MARA_RSLT_SK <> 1 AND <> 0
df_lnk_xfm_Data = (
    df_GndrLkup
    .filter(~( (F.col("INDV_BE_MARA_RSLT_SK") == 1) | (F.col("INDV_BE_MARA_RSLT_SK") == 0) ))
    .select(
        F.col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("MDL_ID").alias("MDL_ID"),
        F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.when(F.col("TRGT_CD").isNull() | (F.length(F.col("TRGT_CD")) == 0), F.lit("UNK")).otherwise(F.col("TRGT_CD")).alias("GNDR_CD"),
        F.when(F.col("TRGT_CD_NM").isNull() | (F.length(F.col("TRGT_CD_NM")) == 0), F.lit("UNK"))
         .otherwise(F.substring(F.col("TRGT_CD_NM"), 1, 35)).alias("GNDR_NM"),
        F.col("CST_MED_ALW_AMT").alias("CST_MED_ALW_AMT"),
        F.col("CST_PDX_ALW_AMT").alias("CST_PDX_ALW_AMT"),
        F.col("CST_TOT_ALW_AMT").alias("CST_TOT_ALW_AMT"),
        F.col("DIAG_CD_CAT_CT").alias("DIAG_CD_CAT_CT"),
        F.col("DIAG_CD_UNCAT_CT").alias("DIAG_CD_UNCAT_CT"),
        F.col("EXPSR_MO_CT").alias("EXPSR_MO_CT"),
        F.col("NDC_CAT_CT").alias("NDC_CAT_CT"),
        F.col("NDC_UNCAT_CT").alias("NDC_UNCAT_CT"),
        F.col("INDV_AGE_NO").alias("INDV_AGE_NO"),
        F.col("ER_SCORE_NO").alias("ER_SCORE_NO"),
        F.col("IP_SCORE_NO").alias("IP_SCORE_NO"),
        F.col("MED_SCORE_NO").alias("MED_SCORE_NO"),
        F.col("OTHR_SVC_SCORE_NO").alias("OTHR_SVC_SCORE_NO"),
        F.col("OP_SCORE_NO").alias("OP_SCORE_NO"),
        F.col("PDX_SCORE_NO").alias("PDX_SCORE_NO"),
        F.col("PHYS_SVC_SCORE_NO").alias("PHYS_SVC_SCORE_NO"),
        F.col("TOT_SCORE_NO").alias("TOT_SCORE_NO"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GNDR_CD_SK").alias("GNDR_CD_SK"),
        F.col("VRSN_ID").alias("VRSN_ID")
    )
)

# lnk_NA_out: constraint => first row only
df_lnk_NA_out = (
    df_numbered
    .filter(F.col("row_number") == 1)
    .select(
        F.lit(1).alias("INDV_BE_MARA_RSLT_SK"),
        F.lit(1).alias("INDV_BE_KEY"),
        F.lit("NA").alias("MDL_ID"),
        F.lit("NA").alias("PRCS_YR_MO_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("NA").alias("GNDR_CD"),
        F.lit("NA").alias("GNDR_NM"),
        F.lit(0).alias("CST_MED_ALW_AMT"),
        F.lit(0).alias("CST_PDX_ALW_AMT"),
        F.lit(0).alias("CST_TOT_ALW_AMT"),
        F.lit(0).alias("DIAG_CD_CAT_CT"),
        F.lit(0).alias("DIAG_CD_UNCAT_CT"),
        F.lit(0).alias("EXPSR_MO_CT"),
        F.lit(0).alias("NDC_CAT_CT"),
        F.lit(0).alias("NDC_UNCAT_CT"),
        F.lit(0).alias("INDV_AGE_NO"),
        F.lit(0).alias("ER_SCORE_NO"),
        F.lit(0).alias("IP_SCORE_NO"),
        F.lit(0).alias("MED_SCORE_NO"),
        F.lit(0).alias("OTHR_SVC_SCORE_NO"),
        F.lit(0).alias("OP_SCORE_NO"),
        F.lit(0).alias("PDX_SCORE_NO"),
        F.lit(0).alias("PHYS_SVC_SCORE_NO"),
        F.lit(0).alias("TOT_SCORE_NO"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("GNDR_CD_SK"),
        F.col("VRSN_ID").alias("VRSN_ID")
    )
)

# lnk_UNK_out: constraint => first row only
df_lnk_UNK_out = (
    df_numbered
    .filter(F.col("row_number") == 1)
    .select(
        F.lit(0).alias("INDV_BE_MARA_RSLT_SK"),
        F.lit(0).alias("INDV_BE_KEY"),
        F.lit("UNK").alias("MDL_ID"),
        F.lit("UNK").alias("PRCS_YR_MO_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("UNK").alias("GNDR_CD"),
        F.lit("UNK").alias("GNDR_NM"),
        F.lit(0).alias("CST_MED_ALW_AMT"),
        F.lit(0).alias("CST_PDX_ALW_AMT"),
        F.lit(0).alias("CST_TOT_ALW_AMT"),
        F.lit(0).alias("DIAG_CD_CAT_CT"),
        F.lit(0).alias("DIAG_CD_UNCAT_CT"),
        F.lit(0).alias("EXPSR_MO_CT"),
        F.lit(0).alias("NDC_CAT_CT"),
        F.lit(0).alias("NDC_UNCAT_CT"),
        F.lit(0).alias("INDV_AGE_NO"),
        F.lit(0).alias("ER_SCORE_NO"),
        F.lit(0).alias("IP_SCORE_NO"),
        F.lit(0).alias("MED_SCORE_NO"),
        F.lit(0).alias("OTHR_SVC_SCORE_NO"),
        F.lit(0).alias("OP_SCORE_NO"),
        F.lit(0).alias("PDX_SCORE_NO"),
        F.lit(0).alias("PHYS_SVC_SCORE_NO"),
        F.lit(0).alias("TOT_SCORE_NO"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("GNDR_CD_SK"),
        F.col("VRSN_ID").alias("VRSN_ID")
    )
)

# fnl_Data (PxFunnel) => union
common_cols = [
    "INDV_BE_MARA_RSLT_SK",
    "INDV_BE_KEY",
    "MDL_ID",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GNDR_CD",
    "GNDR_NM",
    "CST_MED_ALW_AMT",
    "CST_PDX_ALW_AMT",
    "CST_TOT_ALW_AMT",
    "DIAG_CD_CAT_CT",
    "DIAG_CD_UNCAT_CT",
    "EXPSR_MO_CT",
    "NDC_CAT_CT",
    "NDC_UNCAT_CT",
    "INDV_AGE_NO",
    "ER_SCORE_NO",
    "IP_SCORE_NO",
    "MED_SCORE_NO",
    "OTHR_SVC_SCORE_NO",
    "OP_SCORE_NO",
    "PDX_SCORE_NO",
    "PHYS_SVC_SCORE_NO",
    "TOT_SCORE_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GNDR_CD_SK",
    "VRSN_ID"
]

df_lnk_NA_out_sel = df_lnk_NA_out.select(common_cols)
df_lnk_UNK_out_sel = df_lnk_UNK_out.select(common_cols)
df_lnk_xfm_Data_sel = df_lnk_xfm_Data.select(common_cols)

df_fnl_Data = df_lnk_NA_out_sel.union(df_lnk_UNK_out_sel).union(df_lnk_xfm_Data_sel)

# Final select with rpad for char columns
df_fnl_Data = df_fnl_Data.select(
    F.col("INDV_BE_MARA_RSLT_SK"),
    F.col("INDV_BE_KEY"),
    F.col("MDL_ID"),
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GNDR_CD"),
    F.col("GNDR_NM"),
    F.col("CST_MED_ALW_AMT"),
    F.col("CST_PDX_ALW_AMT"),
    F.col("CST_TOT_ALW_AMT"),
    F.col("DIAG_CD_CAT_CT"),
    F.col("DIAG_CD_UNCAT_CT"),
    F.col("EXPSR_MO_CT"),
    F.col("NDC_CAT_CT"),
    F.col("NDC_UNCAT_CT"),
    F.col("INDV_AGE_NO"),
    F.col("ER_SCORE_NO"),
    F.col("IP_SCORE_NO"),
    F.col("MED_SCORE_NO"),
    F.col("OTHR_SVC_SCORE_NO"),
    F.col("OP_SCORE_NO"),
    F.col("PDX_SCORE_NO"),
    F.col("PHYS_SVC_SCORE_NO"),
    F.col("TOT_SCORE_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GNDR_CD_SK"),
    F.col("VRSN_ID")
)

# seq_INDV_BE_MARA_RSLT_F_load (PxSequentialFile)
write_files(
    df_fnl_Data,
    f"{adls_path}/load/INDV_BE_MARA_RSLT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)