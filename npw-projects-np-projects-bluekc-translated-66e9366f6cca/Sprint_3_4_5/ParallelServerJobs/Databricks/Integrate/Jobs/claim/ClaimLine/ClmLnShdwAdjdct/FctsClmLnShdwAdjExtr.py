# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:   Extracts data from Facets Shadow table and lookups CLM_LN_SK in CLM_LN table. 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                           Date                 Project/Altiris #      Change Description                                                                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                        --------------------     ------------------------      -----------------------------------------------------------------------                                                                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                    2015-09-04       5382                       Original Programming                                                                                                                                                           Kalyan Neelam          2015-09-22
# MAGIC Raja Gummadi                    2015-11-18       5382                       Added NULL handling to all fields, added driver table                                                                 IntegrateDev2                  Kalyan Neelam          2015-11-19
# MAGIC Rekha Radhakrishna         2020-10-20       6131                       Added PBM_CALC_CSR_SBSDY_AMT field and set it to Null                       
# MAGIC Prabhu ES                          2022-02-28       S2S Remediation   MSSQL connection parameters added                                                                                        IntegrateDev5                     Manasa Andru           2022-06-10

# MAGIC Primary key is from CLM_LN table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, concat, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions
FacetsOwner = get_widget_value('FacetsOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('IDSOwner','')
DriverTable = get_widget_value('DriverTable','')

# Secret name parameters for $FacetsOwner and $IDSOwner
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Obtain DB connection details for IDS
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
# Read from K_CLM_LN table (ignoring the "?" placeholders in WHERE clause)
query_ids = f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN"
df_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_ids)
    .load()
)

# Obtain DB connection details for Facets
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
# Read from CMC_CDSC_SHAD_MED with DriverTable
query_facets = (
    f"SELECT CMC_CDSC_SHAD_MED.CLCL_ID,"
    f"CMC_CDSC_SHAD_MED.CDML_SEQ_NO,"
    f"CMC_CDSC_SHAD_MED.MEME_CK,"
    f"CMC_CDSC_SHAD_MED.CLCS_STS,"
    f"CMC_CDSC_SHAD_MED.SEPY_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.SESE_RULE,"
    f"CMC_CDSC_SHAD_MED.CDSC_ALT_RULE_IND,"
    f"CMC_CDSC_SHAD_MED.DEDE_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.LTLT_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.CDML_CONSIDER_CHG,"
    f"CMC_CDSC_SHAD_MED.CDSC_PRICE_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_PRICE_UNITS,"
    f"CMC_CDSC_SHAD_MED.CDSC_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_UNITS_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_DED_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DED_ACC_NO,"
    f"CMC_CDSC_SHAD_MED.CDSC_COPAY_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_COINS_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_PAID_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DISALL_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DISALL_EXCD,"
    f"CMC_CDSC_SHAD_MED.CDSC_LOCK_TOKEN,"
    f"CMC_CDSC_SHAD_MED.ATXR_SOURCE_ID,"
    f"CMC_CDSC_SHAD_MED.SYS_LAST_UPD_DTM,"
    f"CMC_CDSC_SHAD_MED.SYS_USUS_ID,"
    f"CMC_CDSC_SHAD_MED.SYS_DBUSER_ID "
    f"FROM {FacetsOwner}.CMC_CDSC_SHAD_MED CMC_CDSC_SHAD_MED , tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = CMC_CDSC_SHAD_MED.CLCL_ID"
)
df_CMC_CDSC_MED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets)
    .load()
)

# Add necessary parameter columns to primary DataFrame
df_in = df_CMC_CDSC_MED
df_in = df_in.withColumn("SrcSysCdSk", lit(SrcSysCdSk))
df_in = df_in.withColumn("SrcSysCd", lit(SrcSysCd))
df_in = df_in.withColumn("RunCycle", lit(RunCycle))
df_in = df_in.withColumn("CurrDate", lit(CurrDate))

# Perform left join for Transformer_0 lookup
df_enriched = df_in.join(
    df_K_CLM_LN,
    (
        (df_in["SrcSysCdSk"] == df_K_CLM_LN["SRC_SYS_CD_SK"])
        & (trim(df_in["CLCL_ID"]) == df_K_CLM_LN["CLM_ID"])
        & (trim(df_in["CDML_SEQ_NO"]) == df_K_CLM_LN["CLM_LN_SEQ_NO"])
    ),
    "left"
)

# Filter rows where svClmLnSk (CLM_LN_SK) is not null and not 0
df_key = df_enriched.filter(
    (col("CLM_LN_SK") != 0) & (col("CLM_LN_SK").isNotNull())
)

# Build output columns
df_key = df_key.withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
df_key = df_key.withColumn("INSRT_UPDT_CD", lit("I"))
df_key = df_key.withColumn("DISCARD_IN", lit("N"))
df_key = df_key.withColumn("PASS_THRU_IN", lit("Y"))
df_key = df_key.withColumn("FIRST_RECYC_DT", col("CurrDate"))
df_key = df_key.withColumn("ERR_CT", lit(0))
df_key = df_key.withColumn("RECYCLE_CT", lit(0))
df_key = df_key.withColumn("SRC_SYS_CD", col("SrcSysCd"))
df_key = df_key.withColumn(
    "PRI_KEY_STRING",
    concat(trim(col("CLCL_ID")), col("CDML_SEQ_NO"), col("SrcSysCd"))
)
df_key = df_key.withColumn("CLM_LN_SK", col("CLM_LN_SK"))
df_key = df_key.withColumn("CLM_ID", trim(col("CLCL_ID")))
df_key = df_key.withColumn("CLM_LN_SEQ_NO", col("CDML_SEQ_NO"))
df_key = df_key.withColumn("SRC_SYS_CD_SK", col("SrcSysCdSk"))
df_key = df_key.withColumn("CRT_RUN_CYC_EXCTN_SK", col("RunCycle"))
df_key = df_key.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("RunCycle"))

df_key = df_key.withColumn(
    "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD",
    when(
        (col("CDSC_DISALL_EXCD").isNull()) | (length(trim(col("CDSC_DISALL_EXCD"))) == 0),
        lit("NA")
    ).otherwise(trim(col("CDSC_DISALL_EXCD")))
)

df_key = df_key.withColumn(
    "CLM_LN_SHADOW_ADJDCT_STTUS_CD",
    when(
        (col("CLCS_STS").isNull()) | (length(trim(col("CLCS_STS"))) == 0),
        lit("NA")
    ).otherwise(trim(col("CLCS_STS")))
)

df_key = df_key.withColumn(
    "SHADOW_MED_UTIL_EDIT_IN",
    when(
        (col("CDSC_ALT_RULE_IND").isNull()) | (length(trim(col("CDSC_ALT_RULE_IND"))) == 0),
        lit(" ")
    ).otherwise(trim(col("CDSC_ALT_RULE_IND")))
)

df_key = df_key.withColumn(
    "CNSD_CHRG_AMT",
    when(
        (col("CDML_CONSIDER_CHG").isNull()) | (length(trim(col("CDML_CONSIDER_CHG"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDML_CONSIDER_CHG")).cast("double"))
)

df_key = df_key.withColumn(
    "INIT_ADJDCT_ALW_AMT",
    when(
        (col("CDSC_PRICE_ALLOW").isNull()) | (length(trim(col("CDSC_PRICE_ALLOW"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_PRICE_ALLOW")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_ALW_AMT",
    when(
        (col("CDSC_ALLOW").isNull()) | (length(trim(col("CDSC_ALLOW"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_ALLOW")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_COINS_AMT",
    when(
        (col("CDSC_COINS_AMT").isNull()) | (length(trim(col("CDSC_COINS_AMT"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_COINS_AMT")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_COPAY_AMT",
    when(
        (col("CDSC_COPAY_AMT").isNull()) | (length(trim(col("CDSC_COPAY_AMT"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_COPAY_AMT")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_DEDCT_AMT",
    when(
        (col("CDSC_DED_AMT").isNull()) | (length(trim(col("CDSC_DED_AMT"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_DED_AMT")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_DSALW_AMT",
    when(
        (col("CDSC_DISALL_AMT").isNull()) | (length(trim(col("CDSC_DISALL_AMT"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_DISALL_AMT")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_PAYBL_AMT",
    when(
        (col("CDSC_PAID_AMT").isNull()) | (length(trim(col("CDSC_PAID_AMT"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_PAID_AMT")).cast("double"))
)

df_key = df_key.withColumn(
    "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
    when(
        (col("CDSC_PRICE_UNITS").isNull()) | (length(trim(col("CDSC_PRICE_UNITS"))) == 0),
        lit(0)
    ).otherwise(trim(col("CDSC_PRICE_UNITS")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
    when(
        (col("CDSC_UNITS_ALLOW").isNull()) | (length(trim(col("CDSC_UNITS_ALLOW"))) == 1),
        lit(0)
    ).otherwise(trim(col("CDSC_UNITS_ALLOW")).cast("double"))
)

df_key = df_key.withColumn(
    "SHADOW_DEDCT_AMT_ACCUM_ID",
    when(
        (col("CDSC_DED_ACC_NO").isNull()) | (length(trim(col("CDSC_DED_ACC_NO"))) == 0),
        lit("NA")
    ).otherwise(trim(col("CDSC_DED_ACC_NO")))
)

df_key = df_key.withColumn(
    "SHADOW_LMT_PFX_ID",
    when(
        (col("LTLT_SHDW_PFX_NVL").isNull()) | (length(trim(col("LTLT_SHDW_PFX_NVL"))) == 0),
        lit("NA")
    ).otherwise(trim(col("LTLT_SHDW_PFX_NVL")))
)

df_key = df_key.withColumn(
    "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
    when(
        (col("DEDE_SHDW_PFX_NVL").isNull()) | (length(trim(col("DEDE_SHDW_PFX_NVL"))) == 0),
        lit("NA")
    ).otherwise(trim(col("DEDE_SHDW_PFX_NVL")))
)

df_key = df_key.withColumn(
    "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
    when(
        (col("SEPY_SHDW_PFX_NVL").isNull()) | (length(trim(col("SEPY_SHDW_PFX_NVL"))) == 0),
        lit("NA")
    ).otherwise(trim(col("SEPY_SHDW_PFX_NVL")))
)

df_key = df_key.withColumn(
    "SHADOW_SVC_RULE_TYP_TX",
    when(
        (col("SESE_RULE").isNull()) | (length(trim(col("SESE_RULE"))) == 0),
        lit("NA")
    ).otherwise(trim(col("SESE_RULE")))
)

df_key = df_key.withColumn("PBM_CALC_CSR_SBSDY_AMT", lit(None).cast("string"))

# Final select in correct column order, applying rpad where needed
df_key = df_key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD"),
    col("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
    rpad(col("SHADOW_MED_UTIL_EDIT_IN"), 1, " ").alias("SHADOW_MED_UTIL_EDIT_IN"),
    col("CNSD_CHRG_AMT"),
    col("INIT_ADJDCT_ALW_AMT"),
    col("SHADOW_ADJDCT_ALW_AMT"),
    col("SHADOW_ADJDCT_COINS_AMT"),
    col("SHADOW_ADJDCT_COPAY_AMT"),
    col("SHADOW_ADJDCT_DEDCT_AMT"),
    col("SHADOW_ADJDCT_DSALW_AMT"),
    col("SHADOW_ADJDCT_PAYBL_AMT"),
    col("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    col("SHADOW_DEDCT_AMT_ACCUM_ID"),
    col("SHADOW_LMT_PFX_ID"),
    col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    col("SHADOW_SVC_RULE_TYP_TX"),
    col("PBM_CALC_CSR_SBSDY_AMT")
)

# Write to file (CSeqFileStage)
write_files(
    df_key,
    f"{adls_path}/key/FctsClmLnShdwAdjExtr.FctsClmLnShdwAdj.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)