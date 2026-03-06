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
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                            IntegrateNewDevl       Kalyan Neelam              2015-03-06
# MAGIC 
# MAGIC Venkatesh Babu Munnangi 11-05-2020                                             Added VRSN_ID                                   IntegrateDev2             Jeyaprasanna                2020-11-13

# MAGIC MARA_CLNLC_CLS lookup
# MAGIC This is a load ready file that will go into Load job
# MAGIC 11-05-2020: Added VRSN_ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

# Hard-coded Job Name based on the JSON
job_name = "CblTalnIdsIndvBeMaraClnclClsFkey"

# 1) Read seqINDV_BE_MARA_CLNCL_CLS_Pkey (PxSequentialFile)
schema_seqINDV_BE_MARA_CLNCL_CLS_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", TimestampType(), True),
    StructField("INDV_BE_MARA_CLNCL_CLS_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("INDV_BE_KEY", DecimalType(38,10), True),
    StructField("CLNCL_CLS_ID", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CT_UNIQ_INSTS", IntegerType(), True),
    StructField("LAST_MO_OBSRV", IntegerType(), True),
    StructField("DX_ADJOR_CONC_PCT", DecimalType(38,10), True),
    StructField("DX_ADJOR_PROSP_LAG_0_PCT", DecimalType(38,10), True),
    StructField("DX_ADJOR_PROSP_LAG_3_PCT", DecimalType(38,10), True),
    StructField("DX_ADJOR_PROSP_LAG_6_PCT", DecimalType(38,10), True),
    StructField("CX_ADJOR_CONC_PCT", DecimalType(38,10), True),
    StructField("CX_ADJOR_PROSP_LAG_0_PCT", DecimalType(38,10), True),
    StructField("CX_ADJOR_PROSP_LAG_3_PCT", DecimalType(38,10), True),
    StructField("CX_ADJOR_PROSP_LAG_6_PCT", DecimalType(38,10), True),
    StructField("VRSN_ID", StringType(), True)
])

df_seqINDV_BE_MARA_CLNCL_CLS_Pkey = (
    spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seqINDV_BE_MARA_CLNCL_CLS_Pkey)
    .load(f"{adls_path}/key/INDV_BE_MARA_CLNCL_CLS.{SrcSysCd}.pkey.{RunID}.dat")
)

# 2) Read db2_K_ClnclClsLkp (DB2ConnectorPX)
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_K_ClnclClsLkp = f"SELECT CLNCL_CLS_ID, SRC_SYS_CD, MARA_CLNCL_CLS_SK FROM {IDSOwner}.K_MARA_CLNCL_CLS"

df_db2_K_ClnclClsLkp = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_db2_K_ClnclClsLkp)
    .load()
)

# 3) LkupFkey (PxLookup) - Left Join
df_LkupFkey = (
    df_seqINDV_BE_MARA_CLNCL_CLS_Pkey.alias("In")
    .join(
        df_db2_K_ClnclClsLkp.alias("Ref_K_FnclLob_In"),
        (F.col("In.CLNCL_CLS_ID") == F.col("Ref_K_FnclLob_In.CLNCL_CLS_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("Ref_K_FnclLob_In.SRC_SYS_CD")),
        how="left"
    )
    .select(
        F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("In.INDV_BE_MARA_CLNCL_CLS_SK").alias("INDV_BE_MARA_CLNCL_CLS_SK"),
        F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("In.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("In.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
        F.col("In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("In.CT_UNIQ_INSTS").alias("CT_UNIQ_INSTS"),
        F.col("In.LAST_MO_OBSRV").alias("LAST_MO_OBSRV"),
        F.col("In.DX_ADJOR_CONC_PCT").alias("DX_ADJOR_CONC_PCT"),
        F.col("In.DX_ADJOR_PROSP_LAG_0_PCT").alias("DX_ADJOR_PROSP_LAG_0_PCT"),
        F.col("In.DX_ADJOR_PROSP_LAG_3_PCT").alias("DX_ADJOR_PROSP_LAG_3_PCT"),
        F.col("In.DX_ADJOR_PROSP_LAG_6_PCT").alias("DX_ADJOR_PROSP_LAG_6_PCT"),
        F.col("In.CX_ADJOR_CONC_PCT").alias("CX_ADJOR_CONC_PCT"),
        F.col("In.CX_ADJOR_PROSP_LAG_0_PCT").alias("CX_ADJOR_PROSP_LAG_0_PCT"),
        F.col("In.CX_ADJOR_PROSP_LAG_3_PCT").alias("CX_ADJOR_PROSP_LAG_3_PCT"),
        F.col("In.CX_ADJOR_PROSP_LAG_6_PCT").alias("CX_ADJOR_PROSP_LAG_6_PCT"),
        F.col("Ref_K_FnclLob_In.MARA_CLNCL_CLS_SK").alias("MARA_CLNCL_CLS_SK"),
        F.col("In.VRSN_ID").alias("VRSN_ID")
    )
)

# 4) xfm_CheckLkpResults (CTransformerStage)
df_xfm_CheckLkpResults = df_LkupFkey.withColumn(
    "svFkeyFail",
    F.when(F.col("MARA_CLNCL_CLS_SK") == 0, F.lit("Y")).otherwise(F.lit("N"))
)

# Output links from Transformer

# 4a) Lnk_Main (no row constraint, pass all rows)
df_Lnk_Main = df_xfm_CheckLkpResults.select(
    F.col("INDV_BE_MARA_CLNCL_CLS_SK").alias("INDV_BE_MARA_CLNCL_CLS_SK"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MARA_CLNCL_CLS_SK").alias("MARA_CLNCL_CLS_SK"),
    F.col("CT_UNIQ_INSTS").alias("CLNCL_CLS_CT"),
    F.col("LAST_MO_OBSRV").alias("CLNCL_CLS_LAST_OBSRV_MO_NO"),
    F.col("CX_ADJOR_CONC_PCT").alias("CXADJOR_CONC_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_0_PCT").alias("CXADJOR_PROSP_LAG_0_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_3_PCT").alias("CXADJOR_PROSP_LAG_3_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_6_PCT").alias("CXADJOR_PROSP_LAG_6_PCT"),
    F.col("DX_ADJOR_CONC_PCT").alias("DXADJOR_CONC_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_0_PCT").alias("DXADJOR_PROSP_LAG_0_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_3_PCT").alias("DXADJOR_PROSP_LAG_3_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_6_PCT").alias("DXADJOR_PROSP_LAG_6_PCT"),
    F.col("VRSN_ID").alias("VRSN_ID")
)

# 4b) Lnk_UNK (constraint produces only first row, overrides columns)
df_Lnk_UNK = (
    df_xfm_CheckLkpResults
    .orderBy()
    .limit(1)
    .select(
        F.lit(0).alias("INDV_BE_MARA_CLNCL_CLS_SK"),
        F.lit(0).alias("INDV_BE_KEY"),
        F.lit("UNK").alias("CLNCL_CLS_ID"),
        F.lit("175301").alias("PRCS_YR_MO_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("MARA_CLNCL_CLS_SK"),
        F.lit(0).alias("CLNCL_CLS_CT"),
        F.lit(0).alias("CLNCL_CLS_LAST_OBSRV_MO_NO"),
        F.lit(0).alias("CXADJOR_CONC_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_0_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_3_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_6_PCT"),
        F.lit(0).alias("DXADJOR_CONC_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_0_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_3_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_6_PCT"),
        F.col("VRSN_ID")
    )
)

# 4c) Lnk_NA (constraint produces only first row, overrides columns)
df_Lnk_NA = (
    df_xfm_CheckLkpResults
    .orderBy()
    .limit(1)
    .select(
        F.lit(1).alias("INDV_BE_MARA_CLNCL_CLS_SK"),
        F.lit(0).alias("INDV_BE_KEY"),
        F.lit("NA").alias("CLNCL_CLS_ID"),
        F.lit("175301").alias("PRCS_YR_MO_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("MARA_CLNCL_CLS_SK"),
        F.lit(0).alias("CLNCL_CLS_CT"),
        F.lit(0).alias("CLNCL_CLS_LAST_OBSRV_MO_NO"),
        F.lit(0).alias("CXADJOR_CONC_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_0_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_3_PCT"),
        F.lit(0).alias("CXADJOR_PROSP_LAG_6_PCT"),
        F.lit(0).alias("DXADJOR_CONC_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_0_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_3_PCT"),
        F.lit(0).alias("DXADJOR_PROSP_LAG_6_PCT"),
        F.col("VRSN_ID")
    )
)

# 4d) lnk_AccumTypCdLkup_Fail (filter svFkeyFail = 'Y')
df_lnk_AccumTypCdLkup_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svFkeyFail") == "Y")
    .select(
        F.col("INDV_BE_MARA_CLNCL_CLS_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(job_name).alias("JOB_NM"),
        F.lit("MARA CLNCL CLS lookup").alias("ERROR_TYP"),
        F.lit("MARA_CLNCL_CLS").alias("PHYSCL_FILE_NM"),
        F.col("CLNCL_CLS_ID").alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# 5) seq_FkeyFailedFile_csv (PxSequentialFile) - Write
f_failed_file_path = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{job_name}.dat"
write_files(
    df_lnk_AccumTypCdLkup_Fail.select(
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
    f_failed_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 6) fnl_NA_UNK_Streams (PxFunnel) - Union of Lnk_Main, Lnk_UNK, Lnk_NA
col_order_funnel = [
    "INDV_BE_MARA_CLNCL_CLS_SK",
    "INDV_BE_KEY",
    "CLNCL_CLS_ID",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MARA_CLNCL_CLS_SK",
    "CLNCL_CLS_CT",
    "CLNCL_CLS_LAST_OBSRV_MO_NO",
    "CXADJOR_CONC_PCT",
    "CXADJOR_PROSP_LAG_0_PCT",
    "CXADJOR_PROSP_LAG_3_PCT",
    "CXADJOR_PROSP_LAG_6_PCT",
    "DXADJOR_CONC_PCT",
    "DXADJOR_PROSP_LAG_0_PCT",
    "DXADJOR_PROSP_LAG_3_PCT",
    "DXADJOR_PROSP_LAG_6_PCT",
    "VRSN_ID"
]

df_fnl_NA_UNK_Streams = (
    df_Lnk_Main.select(col_order_funnel)
    .union(df_Lnk_UNK.select(col_order_funnel))
    .union(df_Lnk_NA.select(col_order_funnel))
)

# 7) seq_INDV_BE_MARA_CLNCL_CLS_Fkey (PxSequentialFile) - Write
# Apply rpad for PRCS_YR_MO_SK (char(6))
df_final_fkey = df_fnl_NA_UNK_Streams.withColumn(
    "PRCS_YR_MO_SK",
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ")
)

write_files(
    df_final_fkey.select(col_order_funnel),
    f"{adls_path}/load/INDV_BE_MARA_CLNCL_CLS.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)