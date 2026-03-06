# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Dinakar S                  2018-06-19            5205                         Original Programming                                                                             IntegrateWrhsDevl   Kalyan Neelam             2018-06-19

# MAGIC FKEY failures are written into this flat file.
# MAGIC Change Data Capture to capture delete records(records that exist in target and not in source)
# MAGIC This is a load ready file that will go into Load job
# MAGIC Job Name: IdsGrpMdlPolProdFkey_EE
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# db2_GRP_Lkp
jdbc_url_db2_GRP_Lkp, jdbc_props_db2_GRP_Lkp = get_db_config(ids_secret_name)
extract_query_db2_GRP_Lkp = f"SELECT DISTINCT GRP_SK, GRP_ID FROM {IDSOwner}.K_GRP"
df_db2_GRP_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_GRP_Lkp)
    .options(**jdbc_props_db2_GRP_Lkp)
    .option("query", extract_query_db2_GRP_Lkp)
    .load()
)

# db2_GRP_MDL_POL_PROD_Lkp
jdbc_url_db2_GRP_MDL_POL_PROD_Lkp, jdbc_props_db2_GRP_MDL_POL_PROD_Lkp = get_db_config(ids_secret_name)
extract_query_db2_GRP_MDL_POL_PROD_Lkp = f"""SELECT 
  GRP_MDL_POL_PROD_SK,
  GRP_ID,
  MDL_DOC_ID,
  POL_NO,
  CNTR_ID,
  CERT_ID,
  AMNDMNT_ID,
  PROD_ID,
  GRP_MDL_POL_PROD_EFF_DT,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK,
  GRP_SK,
  MDL_POL_AMNDMNT_SK,
  MDL_POL_CERT_SK,
  MDL_POL_CNTR_SK,
  PROD_SK,
  GRP_MDL_POL_PROD_TERM_DT
FROM {IDSOwner}.GRP_MDL_POL_PROD
"""
df_db2_GRP_MDL_POL_PROD_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_GRP_MDL_POL_PROD_Lkp)
    .options(**jdbc_props_db2_GRP_MDL_POL_PROD_Lkp)
    .option("query", extract_query_db2_GRP_MDL_POL_PROD_Lkp)
    .load()
)

# db2_PROD_Lkp
jdbc_url_db2_PROD_Lkp, jdbc_props_db2_PROD_Lkp = get_db_config(ids_secret_name)
extract_query_db2_PROD_Lkp = f"SELECT DISTINCT PROD_SK, PROD_ID FROM {IDSOwner}.K_PROD"
df_db2_PROD_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROD_Lkp)
    .options(**jdbc_props_db2_PROD_Lkp)
    .option("query", extract_query_db2_PROD_Lkp)
    .load()
)

# db2_MDL_POL_AMNDMNT_Lkp
jdbc_url_db2_MDL_POL_AMNDMNT_Lkp, jdbc_props_db2_MDL_POL_AMNDMNT_Lkp = get_db_config(ids_secret_name)
extract_query_db2_MDL_POL_AMNDMNT_Lkp = f"""SELECT DISTINCT
MDL_POL_AMNDMNT_SK,
MDL_DOC_ID,
POL_NO,
AMNDMNT_ID,
AMNDMNT_EFF_DT,
SRC_SYS_CD
FROM {IDSOwner}.K_MDL_POL_AMNDMNT
"""
df_db2_MDL_POL_AMNDMNT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MDL_POL_AMNDMNT_Lkp)
    .options(**jdbc_props_db2_MDL_POL_AMNDMNT_Lkp)
    .option("query", extract_query_db2_MDL_POL_AMNDMNT_Lkp)
    .load()
)

# db2_MDL_POL_CNTR_Lkp_1
jdbc_url_db2_MDL_POL_CNTR_Lkp_1, jdbc_props_db2_MDL_POL_CNTR_Lkp_1 = get_db_config(ids_secret_name)
extract_query_db2_MDL_POL_CNTR_Lkp_1 = f"""SELECT DISTINCT
MDL_POL_CNTR_SK,
MDL_DOC_ID,
POL_NO,
CNTR_ID,
CNTR_EFF_DT,
SRC_SYS_CD
FROM {IDSOwner}.K_MDL_POL_CNTR
"""
df_db2_MDL_POL_CNTR_Lkp_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MDL_POL_CNTR_Lkp_1)
    .options(**jdbc_props_db2_MDL_POL_CNTR_Lkp_1)
    .option("query", extract_query_db2_MDL_POL_CNTR_Lkp_1)
    .load()
)

# db2_MDL_POL_CERT_Lkp_1
jdbc_url_db2_MDL_POL_CERT_Lkp_1, jdbc_props_db2_MDL_POL_CERT_Lkp_1 = get_db_config(ids_secret_name)
extract_query_db2_MDL_POL_CERT_Lkp_1 = f"""SELECT DISTINCT
MDL_POL_CERT_SK,
MDL_DOC_ID,
POL_NO,
CERT_ID,
CERT_EFF_DT,
SRC_SYS_CD
FROM {IDSOwner}.K_MDL_POL_CERT
"""
df_db2_MDL_POL_CERT_Lkp_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MDL_POL_CERT_Lkp_1)
    .options(**jdbc_props_db2_MDL_POL_CERT_Lkp_1)
    .option("query", extract_query_db2_MDL_POL_CERT_Lkp_1)
    .load()
)

# seq_GRP_MDL_POL_PROD_PKEY_
schema_seq_GRP_MDL_POL_PROD_PKEY_ = StructType([
    StructField("GRP_ID", StringType(), True),
    StructField("MDL_DOC_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("CNTR_ID", StringType(), True),
    StructField("CERT_ID", StringType(), True),
    StructField("AMNDMNT_ID", StringType(), True),
    StructField("GRP_MDL_POL_PROD_EFF_DT", DateType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("POLICY_FORM_ID", StringType(), True),
    StructField("POLICY_FORM_DT", DateType(), True),
    StructField("GRP_MDL_POL_PROD_TERM_DT", DateType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("GRP_MDL_POL_PROD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])
df_seq_GRP_MDL_POL_PROD_PKEY_ = (
    spark.read.format("csv")
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_seq_GRP_MDL_POL_PROD_PKEY_)
    .load(f"{adls_path}/key/GRP_MDL_POL_PROD.{SrcSysCd}.pkey.{RunID}.dat")
)

# lkp_MDL_DOC_SK (PxLookup)
df_seqAlias = df_seq_GRP_MDL_POL_PROD_PKEY_.alias("Lnk_IdsGrpMdlPolProdPkey_OutAbc")
df_grpAlias = df_db2_GRP_Lkp.alias("Ref_GRP")
df_prodAlias = df_db2_PROD_Lkp.alias("Ref_PROD")
df_amndAlias = df_db2_MDL_POL_AMNDMNT_Lkp.alias("Ref_MDL_POL_AMDMDNT")
df_cntrAlias = df_db2_MDL_POL_CNTR_Lkp_1.alias("Ref_MDL_POL_CNTR_")
df_certAlias = df_db2_MDL_POL_CERT_Lkp_1.alias("Ref_MDL_POL_CERT_")

df_lkp = (
    df_seqAlias
    .join(df_grpAlias, F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.GRP_ID") == F.col("Ref_GRP.GRP_ID"), how="left")
    .join(df_prodAlias, F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.PROD_ID") == F.col("Ref_PROD.PROD_ID"), how="left")
    .join(
        df_amndAlias,
        (
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_POL_AMDMDNT.MDL_DOC_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POL_NO") == F.col("Ref_MDL_POL_AMDMDNT.POL_NO")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_ID") == F.col("Ref_MDL_POL_AMDMDNT.AMNDMNT_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_DT") == F.col("Ref_MDL_POL_AMDMDNT.AMNDMNT_EFF_DT"))
        ),
        how="left"
    )
    .join(
        df_cntrAlias,
        (
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_POL_CNTR_.MDL_DOC_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POL_NO") == F.col("Ref_MDL_POL_CNTR_.POL_NO")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.CNTR_ID") == F.col("Ref_MDL_POL_CNTR_.CNTR_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_DT") == F.col("Ref_MDL_POL_CNTR_.CNTR_EFF_DT"))
        ),
        how="left"
    )
    .join(
        df_certAlias,
        (
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_POL_CERT_.MDL_DOC_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POL_NO") == F.col("Ref_MDL_POL_CERT_.POL_NO")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.CERT_ID") == F.col("Ref_MDL_POL_CERT_.CERT_ID")) &
            (F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_DT") == F.col("Ref_MDL_POL_CERT_.CERT_EFF_DT"))
        ),
        how="left"
    )
)

df_lkpDataOut = df_lkp.select(
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POL_NO").alias("POL_NO"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.CNTR_ID").alias("CNTR_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.CERT_ID").alias("CERT_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.AMNDMNT_ID").alias("AMNDMNT_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_ID").alias("POLICY_FORM_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.POLICY_FORM_DT").alias("POLICY_FORM_DT"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.GRP_MDL_POL_PROD_TERM_DT").alias("GRP_MDL_POL_PROD_TERM_DT"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.PROD_ID").alias("PROD_ID"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsGrpMdlPolProdPkey_OutAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Ref_GRP.GRP_SK").alias("GRP_SK"),
    F.col("Ref_PROD.PROD_SK").alias("PROD_SK"),
    F.col("Ref_MDL_POL_AMDMDNT.MDL_POL_AMNDMNT_SK").alias("MDL_POL_AMNDMNT_SK"),
    F.col("Ref_MDL_POL_CNTR_.MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    F.col("Ref_MDL_POL_CERT_.MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK")
)

# Txn_Fkey (CTransformerStage)
df_txn = df_lkpDataOut.withColumn(
    "SvGrpLkpCheck",
    F.when(F.coalesce(F.col("GRP_SK"), F.lit(0))==0, F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "SvProdLkpCheck",
    F.when(F.coalesce(F.col("PROD_SK"), F.lit(0))==0, F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "SvAmndnmtLkpCheck",
    F.when(F.coalesce(F.col("MDL_POL_AMNDMNT_SK"), F.lit(0))==0, F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "SvcertLkpCheck",
    F.when(F.coalesce(F.col("MDL_POL_CERT_SK"), F.lit(0))==0, F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "SvcntrLkpCheck",
    F.when(F.coalesce(F.col("MDL_POL_CNTR_SK"), F.lit(0))==0, F.lit("Y")).otherwise(F.lit("N"))
)

df_txn_main = df_txn.select(
    F.col("GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("POL_NO").alias("POL_NO"),
    F.col("CNTR_ID").alias("CNTR_ID"),
    F.col("CERT_ID").alias("CERT_ID"),
    F.col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    F.col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(F.lit(0)).alias("GRP_SK"),
    F.when(F.col("MDL_POL_AMNDMNT_SK").isNotNull(), F.col("MDL_POL_AMNDMNT_SK")).otherwise(F.lit(0)).alias("MDL_POL_AMNDMNT_SK"),
    F.when(F.col("MDL_POL_CERT_SK").isNotNull(), F.col("MDL_POL_CERT_SK")).otherwise(F.lit(0)).alias("MDL_POL_CERT_SK"),
    F.when(F.col("MDL_POL_CNTR_SK").isNotNull(), F.col("MDL_POL_CNTR_SK")).otherwise(F.lit(0)).alias("MDL_POL_CNTR_SK"),
    F.when(F.col("PROD_SK").isNotNull(), F.col("PROD_SK")).otherwise(F.lit(0)).alias("PROD_SK"),
    F.col("GRP_MDL_POL_PROD_TERM_DT").alias("GRP_MDL_POL_PROD_TERM_DT")
)

df_txn_fail_grp = df_txn.filter(F.col("SvGrpLkpCheck")=="Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("MDLDOCFKEY").alias("ERROR_TYP"),
    F.lit("MDLDOCFKEY").alias("PHYSCL_FILE_NM"),
    F.lit(f"{SrcSysCd};MDL_DOC_ID;MDL_DOC_EFF_DT").alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)
df_txn_fail_prod = df_txn.filter(F.col("SvProdLkpCheck")=="Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("MDLDOCFKEY").alias("ERROR_TYP"),
    F.lit("MDLDOCFKEY").alias("PHYSCL_FILE_NM"),
    F.lit(f"{SrcSysCd};MDL_DOC_ID;MDL_DOC_EFF_DT").alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)
df_txn_fail_cntr = df_txn.filter(F.col("SvcntrLkpCheck")=="Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("MDLCNTRFAILURE").alias("ERROR_TYP"),
    F.lit("MDLCNTRFKEY").alias("PHYSCL_FILE_NM"),
    F.lit(f"{SrcSysCd};MDL_DOC_ID;MDL_DOC_EFF_DT").alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)
df_txn_fail_cert = df_txn.filter(F.col("SvcertLkpCheck")=="Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("MDLCERT FAILURE").alias("ERROR_TYP"),
    F.lit("MDLCERT").alias("PHYSCL_FILE_NM"),
    F.lit(f"{SrcSysCd};MDL_DOC_ID;MDL_DOC_EFF_DT").alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)
df_txn_fail_amnd = df_txn.filter(F.col("SvAmndnmtLkpCheck")=="Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("MDLAMNDMNT FAILURE").alias("ERROR_TYP"),
    F.lit("MDLAMNDMNT").alias("PHYSCL_FILE_NM"),
    F.lit(f"{SrcSysCd};MDL_DOC_ID;MDL_DOC_EFF_DT").alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# FN_Fail_LKP_ALL (PxFunnel)
df_funnel_fail = df_txn_fail_grp.unionByName(df_txn_fail_prod).unionByName(df_txn_fail_amnd).unionByName(df_txn_fail_cntr).unionByName(df_txn_fail_cert)

# seq_FkeyFailedFile
df_funnel_fail_final = df_funnel_fail.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.col("JOB_NM"),
    F.col("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_funnel_fail_final,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.NfmIdsGrpMdlPolProdFkey.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# CDC_GRP_MDL_POL_PROD (PxChangeCapture) - compare "db2_GRP_MDL_POL_PROD_Lkp" (old) and "df_txn_main" (new)
df_old = df_db2_GRP_MDL_POL_PROD_Lkp.alias("old")
df_new = df_txn_main.alias("new")
join_keys = ["GRP_MDL_POL_PROD_SK"]

df_cdc_join = df_old.join(df_new, on=join_keys, how="full")

all_columns = [
    "GRP_MDL_POL_PROD_SK","GRP_ID","MDL_DOC_ID","POL_NO","CNTR_ID","CERT_ID","AMNDMNT_ID",
    "GRP_MDL_POL_PROD_EFF_DT","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK","MDL_POL_AMNDMNT_SK","MDL_POL_CNTR_SK","MDL_POL_CERT_SK","PROD_SK","GRP_MDL_POL_PROD_TERM_DT","PROD_ID"
]

def cdc_change_code(row):
    old_null = all(row[f"old_{c}"] is None for c in all_columns if c in row.asDict())
    new_null = all(row[f"new_{c}"] is None for c in all_columns if c in row.asDict())
    if old_null and not new_null:
        return 2  # Insert
    elif new_null and not old_null:
        return 1  # Delete
    else:
        row_changed = False
        for c in all_columns:
            old_val = row[f"old_{c}"]
            new_val = row[f"new_{c}"]
            if old_val is not None and new_val is not None:
                if old_val != new_val:
                    row_changed = True
                    break
            elif old_val != new_val:
                row_changed = True
                break
        if row_changed:
            return 3  # Edit
        else:
            return 0  # Copy

df_cdc_stage = df_cdc_join.select(
    *[
        F.col(f"old.{c}").alias(f"old_{c}") for c in all_columns
    ],
    *[
        F.col(f"new.{c}").alias(f"new_{c}") for c in all_columns
    ]
)

rdd_cdc = df_cdc_stage.rdd.map(lambda x: (x, cdc_change_code(x)))
df_cdc_with_code = rdd_cdc.toDF(["fields", "cdc_code"])

for c in all_columns:
    df_cdc_with_code = df_cdc_with_code.withColumn(
        c,
        F.when(F.col("fields.new_"+c).isNotNull(), F.col("fields.new_"+c))
         .otherwise(F.col("fields.old_"+c))
    )

df_cdc_final = df_cdc_with_code.withColumn("change_code", F.col("cdc_code")).drop("fields","cdc_code")

df_cdc_filtered = df_cdc_final.filter(~(F.col("change_code")==0))

df_cdc_output = df_cdc_filtered.select(
    F.col("GRP_MDL_POL_PROD_SK"),
    F.col("GRP_ID"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("CNTR_ID"),
    F.col("CERT_ID"),
    F.col("AMNDMNT_ID"),
    F.col("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_POL_CNTR_SK"),
    F.col("MDL_POL_CERT_SK"),
    F.col("PROD_SK"),
    F.col("GRP_MDL_POL_PROD_TERM_DT"),
    F.col("PROD_ID"),
    F.col("change_code").alias("change_code")
)

# TXN_GRP_MDL_POL_PROD (CTransformerStage)
df_txn_grp_in = df_cdc_output.alias("lnk_out_cdc_grp_mdl_pol_prod")
df_txn_grp = df_txn_grp_in.select(
    F.col("GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("POL_NO").alias("POL_NO"),
    F.col("CNTR_ID").alias("CNTR_ID"),
    F.col("CERT_ID").alias("CERT_ID"),
    F.col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("change_code")==2, IDSRunCycle).otherwise(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(F.lit(0)).alias("GRP_SK"),
    F.when(F.col("MDL_POL_AMNDMNT_SK").isNotNull(), F.col("MDL_POL_AMNDMNT_SK")).otherwise(F.lit(0)).alias("MDL_POL_AMNDMNT_SK"),
    F.when(F.col("MDL_POL_CERT_SK").isNotNull(), F.col("MDL_POL_CERT_SK")).otherwise(F.lit(0)).alias("MDL_POL_CERT_SK"),
    F.when(F.col("MDL_POL_CNTR_SK").isNotNull(), F.col("MDL_POL_CNTR_SK")).otherwise(F.lit(0)).alias("MDL_POL_CNTR_SK"),
    F.when(F.col("PROD_SK").isNotNull(), F.col("PROD_SK")).otherwise(F.lit(0)).alias("PROD_SK"),
    F.lit("2199-12-31").alias("GRP_MDL_POL_PROD_TERM_DT"),
    F.col("change_code").alias("change_code")
)

df_out_grp_mdl_pol_prod = df_txn_grp.drop("change_code")
df_out_grp_mdl_pol_prod_unk = df_txn_grp.limit(1).select(
    F.lit(0).alias("GRP_MDL_POL_PROD_SK"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("MDL_DOC_ID"),
    F.lit("UNK").alias("POL_NO"),
    F.lit("UNK").alias("CNTR_ID"),
    F.lit("UNK").alias("CERT_ID"),
    F.lit("UNK").alias("AMNDMNT_ID"),
    F.lit("UNK").alias("PROD_ID"),
    F.lit("1753-01-01").alias("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MDL_POL_AMNDMNT_SK"),
    F.lit(0).alias("MDL_POL_CERT_SK"),
    F.lit(0).alias("MDL_POL_CNTR_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit("1753-01-01").alias("GRP_MDL_POL_PROD_TERM_DT"),
    F.lit("UNK").alias("change_code")
)
df_out_grp_mdl_pol_prod_na = df_txn_grp.limit(1).select(
    F.lit(1).alias("GRP_MDL_POL_PROD_SK"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("MDL_DOC_ID"),
    F.lit("NA").alias("POL_NO"),
    F.lit("NA").alias("CNTR_ID"),
    F.lit("NA").alias("CERT_ID"),
    F.lit("NA").alias("AMNDMNT_ID"),
    F.lit("NA").alias("PROD_ID"),
    F.lit("1753-01-01").alias("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MDL_POL_AMNDMNT_SK"),
    F.lit(1).alias("MDL_POL_CERT_SK"),
    F.lit(1).alias("MDL_POL_CNTR_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit("1753-01-01").alias("GRP_MDL_POL_PROD_TERM_DT"),
    F.lit("NA").alias("change_code")
)

# FN_GRPMDLPOLPROD_main_unk_na (PxFunnel)
df_funnel_main = df_out_grp_mdl_pol_prod.unionByName(df_out_grp_mdl_pol_prod_unk).unionByName(df_out_grp_mdl_pol_prod_na)

df_funnel_main_final = df_funnel_main.select(
    F.col("GRP_MDL_POL_PROD_SK"),
    F.col("GRP_ID"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("CNTR_ID"),
    F.col("CERT_ID"),
    F.col("AMNDMNT_ID"),
    F.col("PROD_ID"),
    F.col("GRP_MDL_POL_PROD_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_POL_CERT_SK"),
    F.col("MDL_POL_CNTR_SK"),
    F.col("PROD_SK"),
    F.col("GRP_MDL_POL_PROD_TERM_DT")
)

# Seq_GrpMdlPolProdFKey
df_final_write = df_funnel_main_final.select(
    # Apply rpad(<column>, <...>, ' ') for varchar columns (unknown lengths -> <...>), no rpad for date/int
    rpad(F.col("GRP_MDL_POL_PROD_SK").cast(StringType()), <...>, ' ').alias("GRP_MDL_POL_PROD_SK"),
    rpad(F.col("GRP_ID"), <...>, ' ').alias("GRP_ID"),
    rpad(F.col("MDL_DOC_ID"), <...>, ' ').alias("MDL_DOC_ID"),
    rpad(F.col("POL_NO"), <...>, ' ').alias("POL_NO"),
    rpad(F.col("CNTR_ID"), <...>, ' ').alias("CNTR_ID"),
    rpad(F.col("CERT_ID"), <...>, ' ').alias("CERT_ID"),
    rpad(F.col("AMNDMNT_ID"), <...>, ' ').alias("AMNDMNT_ID"),
    rpad(F.col("PROD_ID"), <...>, ' ').alias("PROD_ID"),
    F.col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    rpad(F.col("SRC_SYS_CD"), <...>, ' ').alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MDL_POL_AMNDMNT_SK").alias("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
    F.col("MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("GRP_MDL_POL_PROD_TERM_DT").alias("GRP_MDL_POL_PROD_TERM_DT")
)

write_files(
    df_final_write,
    f"{adls_path}/load/GRP_MDL_POL_PROD.{SrcSysCd}.Fkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)