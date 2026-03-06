# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC JOB NAME:  IdsSubGvrnmtEstPrmFkey
# MAGIC CALLED BY: IdsMbrLoad4Seq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   FKey job of IDS SUB_GVRNMT_EST_PRM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                 		Date		Project/Altiris #	Change Description				Development Project		Code Reviewer	Date Reviewed       
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Kalyan Neelam         		2014-03-24               	5235 ACA              	Initial Programming                          		IntegrateNewDevl        	Bhoomi Dasari  	3/27/2014 
# MAGIC Sham Shankaranarayana      	2021-08-20               	US427630            	Converted server job to parallel                          	IntegrateSITF		Ken Bradmon	2022-06-05

# MAGIC FKEY failures are written into this flat file.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Read common record format file from primary key job.
# MAGIC Set all foreign surragote keys
# MAGIC IDS SUB_GVRNMT_EST_PRM Foreign Keying.
# MAGIC Load file kept for auditing.
# MAGIC Update SubGvrnmtEstPrm table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# -----------------------------------------------------------------
# Retrieve Job/Stage Parameters
# -----------------------------------------------------------------
SrcSysCd = get_widget_value('SrcSysCd','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
WhereClause = get_widget_value('WhereClause','')
MonthEndFlag = get_widget_value('MonthEndFlag','')

jobName = "IdsSubGvrnmtEstPrmFkey"

# -----------------------------------------------------------------
# Read seq_Fcts_SubGvrnmtEstPrm_Pkey (PxSequentialFile)
# -----------------------------------------------------------------
schema_seq_Fcts_SubGvrnmtEstPrm_Pkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", StringType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SUB_GVRNMT_EST_PRM_SK", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", StringType(), False),
    StructField("CLS_PLN_PROD_CAT_CD", StringType(), False),
    StructField("EFF_DT_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("TERM_DT_SK", IntegerType(), False),
    StructField("EST_PRM_AMT", DoubleType(), False),
    StructField("CSPD_CAT", StringType(), False)
])

pkey_file_path = f"{adls_path}/key/SUB_GVRNMT_EST_PRM.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_Fcts_SubGvrnmtEstPrm_Pkey = (
    spark.read
    .format("csv")
    .option("quote", "^")
    .option("delimiter", ",")
    .option("header", "false")
    .option("nullValue", "\"")
    .schema(schema_seq_Fcts_SubGvrnmtEstPrm_Pkey)
    .load(pkey_file_path)
)

# This DF corresponds to output link "lnk_SubGvrnmtEst_inAbc"
df_lnk_SubGvrnmtEst_inAbc = df_seq_Fcts_SubGvrnmtEstPrm_Pkey

# -----------------------------------------------------------------
# Read ds_CD_MPPNG_LkpData (PxDataSet -> read as parquet)
# -----------------------------------------------------------------
schema_ds_CD_MPPNG_LkpData = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("SRC_CD_NM", StringType(), True),
    StructField("SRC_CLCTN_CD", StringType(), True),
    StructField("SRC_DRVD_LKUP_VAL", StringType(), True),
    StructField("SRC_DOMAIN_NM", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True),
    StructField("TRGT_CLCTN_CD", StringType(), True),
    StructField("TRGT_DOMAIN_NM", StringType(), True)
])

cd_mppng_file_path = f"{adls_path}/ds/CD_MPPNG.parquet"
df_ds_CD_MPPNG_LkpData = (
    spark.read
    .format("parquet")
    .schema(schema_ds_CD_MPPNG_LkpData)
    .load(cd_mppng_file_path)
)

# -----------------------------------------------------------------
# flt_CDMA (PxFilter) - no conditions, so pass-through
# -----------------------------------------------------------------
df_flt_CDMA = df_ds_CD_MPPNG_LkpData

# -----------------------------------------------------------------
# db2_SUB (DB2ConnectorPX)
# -----------------------------------------------------------------
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_SUB = f"Select SUB_SK,SUB_UNIQ_KEY from {IDSOwner}.SUB"
df_db2_SUB = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_db2_SUB)
    .load()
)

# -----------------------------------------------------------------
# db2_CLNDR_DT (DB2ConnectorPX)
# -----------------------------------------------------------------
extract_query_db2_CLNDR_DT = f"SELECT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT"
df_db2_CLNDR_DT = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_db2_CLNDR_DT)
    .load()
)

# -----------------------------------------------------------------
# cp_DtSk (PxCopy) - pass-through from db2_CLNDR_DT
# -----------------------------------------------------------------
df_cp_DtSk = df_db2_CLNDR_DT

# -----------------------------------------------------------------
# lkp_Code_DT_SKs (PxLookup) implementing reference joins
# -----------------------------------------------------------------
#   Main input: df_lnk_SubGvrnmtEst_inAbc
#   References: df_flt_CDMA, df_db2_SUB, df_cp_DtSk (twice for EFF and TERM)
#   Join columns:
#     - lnk_SubGvrnmtEst_inAbc.CSPD_CAT = df_flt_CDMA.SRC_CD
#     - lnk_SubGvrnmtEst_inAbc.SUB_UNIQ_KEY = df_db2_SUB.SUB_UNIQ_KEY
#     - lnk_SubGvrnmtEst_inAbc.EFF_DT_SK = df_cp_DtSk.CLNDR_DT_SK (alias eff)
#     - lnk_SubGvrnmtEst_inAbc.TERM_DT_SK = df_cp_DtSk.CLNDR_DT_SK (alias term)
df_cp_DtSk_eff = df_cp_DtSk.alias("df_eff")
df_cp_DtSk_term = df_cp_DtSk.alias("df_term")

df_lkp_join_1 = df_lnk_SubGvrnmtEst_inAbc.alias("main").join(
    df_flt_CDMA.alias("cdma"),
    F.col("main.CSPD_CAT") == F.col("cdma.SRC_CD"),
    "left"
)
df_lkp_join_2 = df_lkp_join_1.join(
    df_db2_SUB.alias("sub"),
    F.col("main.SUB_UNIQ_KEY") == F.col("sub.SUB_UNIQ_KEY"),
    "left"
)
df_lkp_join_3 = df_lkp_join_2.join(
    df_cp_DtSk_eff,
    F.col("main.EFF_DT_SK") == F.col("df_eff.CLNDR_DT_SK"),
    "left"
)
df_lkp_join_4 = df_lkp_join_3.join(
    df_cp_DtSk_term,
    F.col("main.TERM_DT_SK") == F.col("df_term.CLNDR_DT_SK"),
    "left"
)

df_lkp_Code_DT_SKs = df_lkp_join_4.select(
    F.col("main.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("main.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("main.DISCARD_IN").alias("DISCARD_IN"),
    F.col("main.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("main.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("main.ERR_CT").alias("ERR_CT"),
    F.col("main.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("main.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("main.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("main.SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("main.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("main.CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("df_eff.CLNDR_DT_SK").alias("EFF_DT_SK"),
    F.col("main.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("main.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("main.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("df_term.CLNDR_DT_SK").alias("TERM_DT_SK"),
    F.col("main.EST_PRM_AMT").alias("EST_PRM_AMT"),
    F.col("main.CSPD_CAT").alias("CSPD_CAT"),
    F.col("sub.SUB_SK").alias("SUB_SK"),
    F.col("cdma.CD_MPPNG_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
)

df_lnk_LkpDataOut = df_lkp_Code_DT_SKs

# -----------------------------------------------------------------
# xfrm_CheckLkpResults (CTransformerStage)
# -----------------------------------------------------------------
# We create a DataFrame that includes the stage variables and then split rows by constraints.

df_xfrm = df_lnk_LkpDataOut.withColumn(
    "svClsPlnProdCatCdSkChk",
    F.when(F.col("CLS_PLN_PROD_CAT_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "svEffDtSkChk",
    F.when(F.col("EFF_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "svTrmDtSkChk",
    F.when(F.col("TERM_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "svSubSkChk",
    F.when(F.col("SUB_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "MonthEndFlag",
    F.lit(MonthEndFlag)
)

# We replicate DataStage's @INROWNUM logic for the links that use the constraint:
# ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# We'll implement it by picking row_number == 1 for those outputs.
w = Window.orderBy(F.lit(1))
df_xfrm_num = df_xfrm.withColumn("_row_num", F.row_number().over(w))

# lnk_SubGvrnmtEstPrm_UNK (only row_num=1)
df_lnk_SubGvrnmtEstPrm_UNK = df_xfrm_num.filter(F.col("_row_num") == 1).select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.lit("UNK").alias("CLS_PLN_PROD_CAT_CD"),
    F.lit("1753-01-01").alias("EFF_DT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("2199-12-31").alias("TERM_DT_SK"),
    F.lit(0).alias("EST_PRM_AMT"),
    F.col("MonthEndFlag").alias("MonthEndFlag")
)

# lnk_SubGvrnmtEstPrm_NA (only row_num=1)
df_lnk_SubGvrnmtEstPrm_NA = df_xfrm_num.filter(F.col("_row_num") == 1).select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.lit("NA").alias("CLS_PLN_PROD_CAT_CD"),
    F.lit("1753-01-01").alias("EFF_DT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("2199-12-31").alias("TERM_DT_SK"),
    F.lit(1).alias("EST_PRM_AMT"),
    F.col("MonthEndFlag").alias("MonthEndFlag")
)

# lnk_SubGvrnmtEstPrm_Main (no special constraint => all rows)
df_lnk_SubGvrnmtEstPrm_Main = df_xfrm_num.select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.when(F.col("EFF_DT_SK").isNull(), F.lit("UNK")).otherwise(F.col("EFF_DT_SK")).alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("SUB_SK").isNotNull(), F.col("SUB_SK")).otherwise(F.lit(0)).alias("SUB_SK"),
    F.when(F.col("CLS_PLN_PROD_CAT_CD_SK").isNotNull(), F.col("CLS_PLN_PROD_CAT_CD_SK")).otherwise(F.lit(0)).alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.when(F.col("TERM_DT_SK").isNull(), F.lit("UNK")).otherwise(F.col("TERM_DT_SK")).alias("TERM_DT_SK"),
    F.col("EST_PRM_AMT").alias("EST_PRM_AMT"),
    F.col("MonthEndFlag").alias("MonthEndFlag")
)

# lnk_ClsPlnProdCatCdSk => constraint "svClsPlnProdCatCdSkChk = 'Y'"
df_lnk_ClsPlnProdCatCdSk = df_xfrm_num.filter(F.col("svClsPlnProdCatCdSkChk") == "Y").select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("PRI_SK"),
    F.col("PRI_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CLS_PLN_PROD_CAT_CD_SK")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_SubSk => constraint "svSubSkChk = 'Y'"
df_lnk_SubSk = df_xfrm_num.filter(F.col("svSubSkChk") == "Y").select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("PRI_SK"),
    F.col("PRI_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("SUB").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("SRC_SYS_CD"),
        F.lit(";"),
        F.when(F.col("EFF_DT_SK").isNotNull(), F.col("EFF_DT_SK")).otherwise(F.lit("0"))
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_EffDtSk => constraint "svEffDtSkChk = 'Y'"
df_lnk_EffDtSk = df_xfrm_num.filter(F.col("svEffDtSkChk") == "Y").select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("PRI_SK"),
    F.col("PRI_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("SRC_SYS_CD"),
        F.lit(";"),
        F.when(F.col("EFF_DT_SK").isNotNull(), F.col("EFF_DT_SK")).otherwise(F.lit("1753-01-01"))
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# lnk_TrmDtSk => constraint "svTrmDtSkChk = 'Y'"
df_lnk_TrmDtSk = df_xfrm_num.filter(F.col("svTrmDtSkChk") == "Y").select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("PRI_SK"),
    F.col("PRI_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),  # as per the job detail 
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("SRC_SYS_CD"),
        F.lit(";"),
        F.when(F.col("TERM_DT_SK").isNotNull(), F.col("TERM_DT_SK")).otherwise(F.lit("1753-01-01"))
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# -----------------------------------------------------------------
# fnl_LkpFail (PxFunnel) to combine 4 reject DataFrames
# -----------------------------------------------------------------
df_fnl_LkpFail_input_1 = df_lnk_ClsPlnProdCatCdSk.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
df_fnl_LkpFail_input_2 = df_lnk_SubSk.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
df_fnl_LkpFail_input_3 = df_lnk_EffDtSk.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
df_fnl_LkpFail_input_4 = df_lnk_TrmDtSk.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

# Funnel just union all
df_fnl_LkpFail = df_fnl_LkpFail_input_1.union(df_fnl_LkpFail_input_2).union(df_fnl_LkpFail_input_3).union(df_fnl_LkpFail_input_4)

# This stage => lnk_SubGvrnmtEstPrmFkey_Fail
df_lnk_SubGvrnmtEstPrmFkey_Fail = df_fnl_LkpFail.select(
    F.col("PRI_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("JOB_NM").alias("JOB_NM"),
    F.col("ERROR_TYP").alias("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM").alias("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# -----------------------------------------------------------------
# seq_FKeyFailFile (PxSequentialFile) - write
# -----------------------------------------------------------------
fkey_fail_file_path = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{jobName}.dat"
df_seq_FKeyFailFile_out = df_lnk_SubGvrnmtEstPrmFkey_Fail.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
write_files(
    df_seq_FKeyFailFile_out,
    fkey_fail_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# -----------------------------------------------------------------
# fnl_NA_UNK_Streams (PxFunnel) - funnel the 3 main DataFrames:
# lnk_SubGvrnmtEstPrm_Main, lnk_SubGvrnmtEstPrm_NA, lnk_SubGvrnmtEstPrm_UNK
# -----------------------------------------------------------------
df_fnl_NA_UNK_Streams_1 = df_lnk_SubGvrnmtEstPrm_Main.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    "MonthEndFlag"
)
df_fnl_NA_UNK_Streams_2 = df_lnk_SubGvrnmtEstPrm_NA.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    "MonthEndFlag"
)
df_fnl_NA_UNK_Streams_3 = df_lnk_SubGvrnmtEstPrm_UNK.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    "MonthEndFlag"
)

df_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams_1.union(df_fnl_NA_UNK_Streams_2).union(df_fnl_NA_UNK_Streams_3)

df_lnk_SubGvrnmtEstPrmFnlOut = df_fnl_NA_UNK_Streams.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    "MonthEndFlag"
)

# -----------------------------------------------------------------
# Flt_SubSbsdyFullDrvr (PxFilter) - 2 output links with same condition:
# "MonthEndFlag='N' or MonthEndFlag='Y'"
# -----------------------------------------------------------------
df_lnk_SubGvrnmtEstPrm_Update = df_lnk_SubGvrnmtEstPrmFnlOut.filter(
    (F.col("MonthEndFlag") == "N") | (F.col("MonthEndFlag") == "Y")
).select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("EST_PRM_AMT").alias("EST_PRM_AMT")
)

df_lnk_SubGvrnmtEstPrm_audit = df_lnk_SubGvrnmtEstPrmFnlOut.filter(
    (F.col("MonthEndFlag") == "N") | (F.col("MonthEndFlag") == "Y")
).select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("EST_PRM_AMT").alias("EST_PRM_AMT")
)

# -----------------------------------------------------------------
# seq_SubGvrnmtEstPrm_load (PxSequentialFile) - write
# -----------------------------------------------------------------
# Before writing, apply rpad for any char columns with a defined length
# EFF_DT_SK => char(10), TERM_DT_SK => char(10) if these appear in the DF
df_seq_SubGvrnmtEstPrm_load_out = df_lnk_SubGvrnmtEstPrm_audit \
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK").cast(StringType()), 10, " ")) \
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK").cast(StringType()), 10, " "))

subgvrnmt_out_file_path = f"{adls_path}/load/processed/SUB_GVRNMT_EST_PRM.{SrcSysCd}.{RunID}.dat"
df_seq_SubGvrnmtEstPrm_load_out = df_seq_SubGvrnmtEstPrm_load_out.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT"
)
write_files(
    df_seq_SubGvrnmtEstPrm_load_out,
    subgvrnmt_out_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------
# db2_SubGvrnmtEstPrm_Update (DB2ConnectorPX) => Upsert to #$IDSOwner#.SUB_GVRNMT_EST_PRM
# -----------------------------------------------------------------
# Prepare final DF for merge. Also apply rpad for char(10) columns.
df_lnk_SubGvrnmtEstPrm_Update_for_merge = df_lnk_SubGvrnmtEstPrm_Update \
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK").cast(StringType()), 10, " ")) \
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK").cast(StringType()), 10, " "))

temp_table_name = f"STAGING.{jobName}_db2_SubGvrnmtEstPrm_Update_temp"

# 1) Drop table if exists
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, ids_jdbc_url, ids_jdbc_props)

# 2) Create the temporary table by writing this DF via JDBC
(
    df_lnk_SubGvrnmtEstPrm_Update_for_merge
    .write
    .format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

# 3) Merge statement
merge_sql = f"""
MERGE INTO {IDSOwner}.SUB_GVRNMT_EST_PRM AS T
USING {temp_table_name} AS S
ON (T.SUB_GVRNMT_EST_PRM_SK = S.SUB_GVRNMT_EST_PRM_SK)
WHEN MATCHED THEN
  UPDATE SET
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
    T.CLS_PLN_PROD_CAT_CD = S.CLS_PLN_PROD_CAT_CD,
    T.EFF_DT_SK = S.EFF_DT_SK,
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.SUB_SK = S.SUB_SK,
    T.CLS_PLN_PROD_CAT_CD_SK = S.CLS_PLN_PROD_CAT_CD_SK,
    T.TERM_DT_SK = S.TERM_DT_SK,
    T.EST_PRM_AMT = S.EST_PRM_AMT
WHEN NOT MATCHED THEN
  INSERT (
    SUB_GVRNMT_EST_PRM_SK,
    SUB_UNIQ_KEY,
    CLS_PLN_PROD_CAT_CD,
    EFF_DT_SK,
    SRC_SYS_CD_SK,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    SUB_SK,
    CLS_PLN_PROD_CAT_CD_SK,
    TERM_DT_SK,
    EST_PRM_AMT
  )
  VALUES (
    S.SUB_GVRNMT_EST_PRM_SK,
    S.SUB_UNIQ_KEY,
    S.CLS_PLN_PROD_CAT_CD,
    S.EFF_DT_SK,
    S.SRC_SYS_CD_SK,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.SUB_SK,
    S.CLS_PLN_PROD_CAT_CD_SK,
    S.TERM_DT_SK,
    S.EST_PRM_AMT
  )
;
"""
execute_dml(merge_sql, ids_jdbc_url, ids_jdbc_props)

# -----------------------------------------------------------------
# db2_SubGvrnmtEstPrm_Update => Reject link => seq_SubGvrnmtEstPrm_Update_reject
# In normal DataStage usage, the rejects would contain error code. We replicate
# the columns plus reject code, but here we simply select them as if they'd come from a failed upsert.
# -----------------------------------------------------------------
df_lnk_SubGvrnmtEstPrm_reject = df_lnk_SubGvrnmtEstPrm_Update_for_merge.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    F.lit(None).alias("RejectERRORCODE"),
    F.lit(None).alias("RejectERRORTEXT")
)

subgvrnmt_rej_file_path = f"{adls_path}/load/processed/SUB_GVRNMT_EST_PRM.{SrcSysCd}.{RunID}_Rej.dat"
df_seq_SubGvrnmtEstPrm_Update_reject_out = df_lnk_SubGvrnmtEstPrm_reject.select(
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_CD_SK",
    "TERM_DT_SK",
    "EST_PRM_AMT",
    "RejectERRORCODE",
    "RejectERRORTEXT"
)
write_files(
    df_seq_SubGvrnmtEstPrm_Update_reject_out,
    subgvrnmt_rej_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)