# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC 
# MAGIC Razia                                  2021-10-19         US-428909                      Copied over from                                              IntegrateDev1     Goutham K              11/9/2021
# MAGIC                                                                                                                        CblTalnClmLnHlthCstGrpCntl
# MAGIC                                                                                                                        to Load the new Rpcl Table
# MAGIC 
# MAGIC Goutham Kalidindi             2023-05-14         US-583538               Replace Sparse lookup with JOIN Stage                 IntegrateDev1     Reddy Sanam          05/17/2023
# MAGIC                                                                                                      for performace improvement

# MAGIC MR_LN_SVC_CAT lookup
# MAGIC CLM_LN lookup
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
)
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

# No other imports allowed per instructions

# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

# Read from IDS database for Stage: K_CLM_LN
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_K_CLM_LN = f"SELECT CLM_ID AS CLM_ID, CLM_LN_SEQ_NO AS CLM_LN_SEQ_NO, SRC_SYS_CD_SK as CLM_SRC_SYS_CD_SK, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN"
df_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_K_CLM_LN)
    .load()
)

# Read file for Stage: seqCLM_LN_HLTH_CST_GRP_Pkey (PxSequentialFile)
schema_seqCLM_LN_HLTH_CST_GRP_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("CLM_LN_HLTH_CST_GRP_SK", IntegerType(), nullable=True),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_SRC_SYS_CD", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CASE_ADM_ID", StringType(), nullable=False),
    StructField("MR_LN", StringType(), nullable=False),
    StructField("MR_LN_DTL", StringType(), nullable=False),
    StructField("MR_LN_CASE", StringType(), nullable=False),
    StructField("MR_CASES_ADM", IntegerType(), nullable=True),
    StructField("MR_UNIT_DAYS", DecimalType(38,10), nullable=True),
    StructField("MR_PROC", IntegerType(), nullable=True),
    StructField("FCLTY_CASE_ID", StringType(), nullable=True)
])
file_path_seqCLM_LN_HLTH_CST_GRP_Pkey = f"{adls_path}/key/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.pkey.{RunID}.dat"
df_seqCLM_LN_HLTH_CST_GRP_Pkey = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("inferSchema", False)
    .schema(schema_seqCLM_LN_HLTH_CST_GRP_Pkey)
    .csv(file_path_seqCLM_LN_HLTH_CST_GRP_Pkey)
)

# Read from IDS database for Stage: db2_K_MR_LN
extract_query_db2_K_MR_LN = (
    f"SELECT K.MR_LN_ID, K.MR_LN_SVC_CAT_SK "
    f"FROM {IDSOwner}.K_MR_LN_SVC_CAT K "
    f"INNER JOIN (SELECT M.MR_LN_ID, MAX(M.EFF_DT_SK) AS MAX_EFF_DT_SK "
    f"FROM {IDSOwner}.K_MR_LN_SVC_CAT M GROUP BY M.MR_LN_ID) MR "
    f"ON K.MR_LN_ID = MR.MR_LN_ID AND K.EFF_DT_SK = MR.MAX_EFF_DT_SK"
)
df_db2_K_MR_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_db2_K_MR_LN)
    .load()
)

# Read dataset (PxDataSet) for Stage: ds_CD_MPPNG_LkpData -> translates to reading parquet
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# Stage: fltr_FilterData (PxFilter)
df_fltr_FilterData_pre = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == lit("IDS")) &
    (col("SRC_CLCTN_CD") == lit("IDS")) &
    (col("TRGT_CLCTN_CD") == lit("IDS")) &
    (col("SRC_DOMAIN_NM") == lit("SOURCE SYSTEM")) &
    (col("TRGT_DOMAIN_NM") == lit("SOURCE SYSTEM"))
)
df_fltr_FilterData = df_fltr_FilterData_pre.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Stage: LkupFkey (PxLookup)
# Primary link: df_seqCLM_LN_HLTH_CST_GRP_Pkey (alias "PkeyIn")
# Lookup 1 (ClmSrcLkup): df_fltr_FilterData on PkeyIn.CLM_SRC_SYS_CD = ClmSrcLkup.SRC_CD (left join)
# Lookup 2 (MrLnSvcLkup): df_db2_K_MR_LN on PkeyIn.MR_LN = MrLnSvcLkup.MR_LN_ID (left join)
df_LkupFkey = (
    df_seqCLM_LN_HLTH_CST_GRP_Pkey.alias("PkeyIn")
    .join(
        df_fltr_FilterData.alias("ClmSrcLkup"),
        col("PkeyIn.CLM_SRC_SYS_CD") == col("ClmSrcLkup.SRC_CD"),
        "left"
    )
    .join(
        df_db2_K_MR_LN.alias("MrLnSvcLkup"),
        col("PkeyIn.MR_LN") == col("MrLnSvcLkup.MR_LN_ID"),
        "left"
    )
    .select(
        col("PkeyIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("PkeyIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("PkeyIn.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
        col("PkeyIn.CLM_ID").alias("CLM_ID"),
        col("PkeyIn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("PkeyIn.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("PkeyIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PkeyIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("PkeyIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PkeyIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("PkeyIn.CASE_ADM_ID").alias("CASE_ADM_ID"),
        col("PkeyIn.MR_LN").alias("MR_LN"),
        col("PkeyIn.MR_LN_DTL").alias("MR_LN_DTL"),
        col("PkeyIn.MR_LN_CASE").alias("MR_LN_CASE"),
        col("PkeyIn.MR_CASES_ADM").alias("MR_CASES_ADM"),
        col("PkeyIn.MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
        col("PkeyIn.MR_PROC").alias("MR_PROC"),
        col("ClmSrcLkup.CD_MPPNG_SK").alias("CLM_SRC_SYS_CD_SK"),
        col("MrLnSvcLkup.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        col("PkeyIn.FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
    )
)

# Stage: Join_71 (PxJoin) - left outer join on (CLM_ID, CLM_LN_SEQ_NO, CLM_SRC_SYS_CD_SK)
df_Join_71 = (
    df_LkupFkey.alias("In")
    .join(
        df_K_CLM_LN.alias("clmlkup"),
        (
            (col("In.CLM_ID") == col("clmlkup.CLM_ID")) &
            (col("In.CLM_LN_SEQ_NO") == col("clmlkup.CLM_LN_SEQ_NO")) &
            (col("In.CLM_SRC_SYS_CD_SK") == col("clmlkup.CLM_SRC_SYS_CD_SK"))
        ),
        "left"
    )
    .select(
        col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("In.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
        col("In.CLM_ID").alias("CLM_ID"),
        col("In.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("In.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("In.CASE_ADM_ID").alias("CASE_ADM_ID"),
        col("In.MR_LN").alias("MR_LN"),
        col("In.MR_LN_DTL").alias("MR_LN_DTL"),
        col("In.MR_LN_CASE").alias("MR_LN_CASE"),
        col("In.MR_CASES_ADM").alias("MR_CASES_ADM"),
        col("In.MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
        col("In.MR_PROC").alias("MR_PROC"),
        col("In.CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
        col("In.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        col("clmlkup.CLM_LN_SK").alias("CLM_LN_SK"),
        col("In.FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
    )
)

# Stage: xfm_CheckLkpResults (CTransformerStage)
# Compute stage variables:
# svFkeyClmSrcSysCdCheck => 'Y' if IsNull(CLM_SRC_SYS_CD_SK) else 'N'
# svFkeyMrLnSvcCatCheck => 'Y' if IsNull(MR_LN_SVC_CAT_SK) else 'N'
# svFkeyClmLnCheck => 'Y' if IsNull(CLM_LN_SK) else 'N'
dfErrChk = (
    df_Join_71
    .withColumn("svFkeyClmSrcSysCdCheck", when(col("CLM_SRC_SYS_CD_SK").isNull(), lit("Y")).otherwise(lit("N")))
    .withColumn("svFkeyMrLnSvcCatCheck", when(col("MR_LN_SVC_CAT_SK").isNull(), lit("Y")).otherwise(lit("N")))
    .withColumn("svFkeyClmLnCheck", when(col("CLM_LN_SK").isNull(), lit("Y")).otherwise(lit("N")))
)

# Lnk_Main (no row filter): 17 columns, with transformations for null
df_xfm_CheckLkpResults_main = dfErrChk.select(
    when(col("CLM_LN_HLTH_CST_GRP_SK").isNull(), col("CLM_LN_HLTH_CST_GRP_SK")).otherwise(col("CLM_LN_HLTH_CST_GRP_SK")).alias("CLM_LN_HLTH_CST_GRP_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(col("CLM_SRC_SYS_CD_SK").isNull(), lit(0)).otherwise(col("CLM_SRC_SYS_CD_SK")).alias("CLM_SRC_SYS_CD_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("CLM_LN_SK").isNull(), lit(0)).otherwise(col("CLM_LN_SK")).alias("CLM_LN_SK"),
    when(col("MR_LN_SVC_CAT_SK").isNull(), lit(0)).otherwise(col("MR_LN_SVC_CAT_SK")).alias("MR_LN_SVC_CAT_SK"),
    col("MR_CASES_ADM").alias("MR_CASE_ADM_CT"),
    col("MR_PROC").alias("MR_PROC_CT"),
    col("MR_UNIT_DAYS").alias("MR_UNIT_DAY_CT"),
    col("CASE_ADM_ID").alias("CASE_ADM_ID"),
    col("MR_LN").alias("MR_LN_ID"),
    col("MR_LN_CASE").alias("MR_LN_CASE_ID"),
    col("MR_LN_DTL").alias("MR_LN_DTL_ID"),
    col("FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
)

# Lnk_UNK (constraint => first row only), set columns per WhereExpression
df_xfm_CheckLkpResults_unk_pre = dfErrChk.limit(1)
df_xfm_CheckLkpResults_unk = df_xfm_CheckLkpResults_unk_pre.select(
    lit(0).alias("CLM_LN_HLTH_CST_GRP_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(0).alias("CLM_SRC_SYS_CD_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    lit(0).alias("MR_LN_SVC_CAT_SK"),
    lit(0).alias("MR_CASE_ADM_CT"),
    lit(0).alias("MR_PROC_CT"),
    lit(0).alias("MR_UNIT_DAY_CT"),
    lit("UNK").alias("CASE_ADM_ID"),
    lit("UNK").alias("MR_LN_ID"),
    lit("UNK").alias("MR_LN_CASE_ID"),
    lit("UNK").alias("MR_LN_DTL_ID"),
    lit("UNK").alias("FCLTY_CASE_ID")
)

# Lnk_NA (constraint => first row only), set columns per WhereExpression
df_xfm_CheckLkpResults_na_pre = dfErrChk.limit(1)
df_xfm_CheckLkpResults_na = df_xfm_CheckLkpResults_na_pre.select(
    lit(1).alias("CLM_LN_HLTH_CST_GRP_SK"),
    lit("NA").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(1).alias("CLM_SRC_SYS_CD_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_LN_SK"),
    lit(1).alias("MR_LN_SVC_CAT_SK"),
    lit(0).alias("MR_CASE_ADM_CT"),
    lit(0).alias("MR_PROC_CT"),
    lit(0).alias("MR_UNIT_DAY_CT"),
    lit("NA").alias("CASE_ADM_ID"),
    lit("NA").alias("MR_LN_ID"),
    lit("NA").alias("MR_LN_CASE_ID"),
    lit("NA").alias("MR_LN_DTL_ID"),
    lit("NA").alias("FCLTY_CASE_ID")
)

# SrcSysFail (constraint => svFkeyClmSrcSysCdCheck='Y')
JOB_NM_value = "CblTalnIdsClmLnHlthCstGrpReplFkey"  # DSJobName
df_xfm_CheckLkpResults_SrcSysFail = (
    dfErrChk
    .filter(col("svFkeyClmSrcSysCdCheck") == lit("Y"))
    .select(
        col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
        col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        lit(JOB_NM_value).alias("JOB_NM"),
        lit("CDLOOKUP - CLM_SRC_SYS_CD").alias("ERROR_TYP"),
        lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        (
            lit("IDS") + lit(";") + lit("IDS") + lit(";") + lit("IDS") + lit(";") +
            lit("SOURCE SYSTEM") + lit(";") + lit("SOURCE SYSTEM") + lit(";") +
            col("CLM_SRC_SYS_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# MrLnFail (constraint => svFkeyMrLnSvcCatCheck='Y')
df_xfm_CheckLkpResults_MrLnFail = (
    dfErrChk
    .filter(col("svFkeyMrLnSvcCatCheck") == lit("Y"))
    .select(
        col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
        col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        lit(JOB_NM_value).alias("JOB_NM"),
        lit("FKEYLOOKUP - MR_LN_SVC_CAT").alias("ERROR_TYP"),
        lit("MR_LN_SVC_CAT").alias("PHYSCL_FILE_NM"),
        col("MR_LN").alias("FRGN_NAT_KEY_STRING"),
        col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# ClmLnFail (constraint => svFkeyClmLnCheck='Y')
df_xfm_CheckLkpResults_ClmLnFail = (
    dfErrChk
    .filter(col("svFkeyClmLnCheck") == lit("Y"))
    .select(
        col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
        col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        lit(JOB_NM_value).alias("JOB_NM"),
        lit("FKEYLOOKUP - CLM_LN_SK").alias("ERROR_TYP"),
        lit("K_CLM_LN").alias("PHYSCL_FILE_NM"),
        col("CLM_ID").alias("FRGN_NAT_KEY_STRING"),
        col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Stage: Funnel_56 (PxFunnel) -> union of SrcSysFail, MrLnFail, ClmLnFail
df_Funnel_56 = (
    df_xfm_CheckLkpResults_SrcSysFail
    .unionByName(df_xfm_CheckLkpResults_MrLnFail, allowMissingColumns=True)
    .unionByName(df_xfm_CheckLkpResults_ClmLnFail, allowMissingColumns=True)
)

# Stage: seq_FkeyFailedFile_csv (PxSequentialFile) -> write funnel results
# Before writing, apply rpad to any string (varchar) columns where appropriate.
# The schema is not explicitly defined, so we assume columns with obvious string usage need rpad:
df_Funnel_56_for_write = (
    df_Funnel_56
    .withColumn("PRI_NAT_KEY_STRING", rpad(col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("JOB_NM", rpad(col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", rpad(col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", rpad(col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", rpad(col("FRGN_NAT_KEY_STRING"), <...>, " "))
)
output_path_seq_FkeyFailedFile_csv = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{JOB_NM_value}.dat"
write_files(
    df_Funnel_56_for_write.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    output_path_seq_FkeyFailedFile_csv,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Stage: fnl_NA_UNK_Streams (PxFunnel) -> union of Lnk_Main, Lnk_UNK, Lnk_NA
df_fnl_NA_UNK_Streams = (
    df_xfm_CheckLkpResults_main.unionByName(df_xfm_CheckLkpResults_unk, allowMissingColumns=True)
    .unionByName(df_xfm_CheckLkpResults_na, allowMissingColumns=True)
)

# Stage: seq_CLM_LN_HLTH_CST_GRP_Fkey (PxSequentialFile) -> write final
# The final columns in the order specified:
#   CLM_LN_HLTH_CST_GRP_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_SRC_SYS_CD_SK, SRC_SYS_CD_SK,
#   CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, CLM_LN_SK, MR_LN_SVC_CAT_SK,
#   MR_CASE_ADM_CT, MR_PROC_CT, MR_UNIT_DAY_CT, CASE_ADM_ID, MR_LN_ID, MR_LN_CASE_ID,
#   MR_LN_DTL_ID, FCLTY_CASE_ID
# Apply rpad for varchar columns:
df_fnl_NA_UNK_Streams_for_write = (
    df_fnl_NA_UNK_Streams
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .withColumn("CASE_ADM_ID", rpad(col("CASE_ADM_ID"), <...>, " "))
    .withColumn("MR_LN_ID", rpad(col("MR_LN_ID"), <...>, " "))
    .withColumn("MR_LN_CASE_ID", rpad(col("MR_LN_CASE_ID"), <...>, " "))
    .withColumn("MR_LN_DTL_ID", rpad(col("MR_LN_DTL_ID"), <...>, " "))
    .withColumn("FCLTY_CASE_ID", rpad(col("FCLTY_CASE_ID"), <...>, " "))
)

output_path_seq_CLM_LN_HLTH_CST_GRP_Fkey = f"{adls_path}/load/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.{RunID}.dat"
write_files(
    df_fnl_NA_UNK_Streams_for_write.select(
        "CLM_LN_HLTH_CST_GRP_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_SRC_SYS_CD_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "MR_LN_SVC_CAT_SK",
        "MR_CASE_ADM_CT",
        "MR_PROC_CT",
        "MR_UNIT_DAY_CT",
        "CASE_ADM_ID",
        "MR_LN_ID",
        "MR_LN_CASE_ID",
        "MR_LN_DTL_ID",
        "FCLTY_CASE_ID"
    ),
    output_path_seq_CLM_LN_HLTH_CST_GRP_Fkey,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)