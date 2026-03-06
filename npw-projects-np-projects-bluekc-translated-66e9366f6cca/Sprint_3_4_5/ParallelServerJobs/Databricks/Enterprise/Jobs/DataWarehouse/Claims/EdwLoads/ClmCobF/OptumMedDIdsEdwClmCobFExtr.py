# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC               
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                                                             DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                                                                            ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                                                  ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          -----------------
# MAGIC Ramu Avula                     2020-11-06              6264 - PBM Phase II - Government Programs                                  Initial Programming                                            EnterpriseDev5l         Jaideep Mankala             12/10/2020

# MAGIC Code SK lookups for Denormalization
# MAGIC Read all the Data from IDS CLM_COB Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwClmCobFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_CLM_COB_F_in
extract_query_db2_CLM_COB_F_in = f"""
SELECT
COB.CLM_COB_SK,
COB.SRC_SYS_CD_SK,
COB.CLM_ID,
COB.CLM_COB_TYP_CD_SK,
COB.CLM_SK,
COB.CLM_COB_LIAB_TYP_CD_SK,
ALW_AMT,
COPAY_AMT,
DEDCT_AMT,
DSALW_AMT,
MED_COINS_AMT,
MNTL_HLTH_COINS_AMT,
PD_AMT,
SANC_AMT,
COB_CAR_RSN_CD_TX,
COB_CAR_RSN_TX
FROM {IDSOwner}.CLM_COB COB,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE COB.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND COB.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
  AND CD_MPPNG.SRC_CD = '{SrcSysCd}'
"""
df_db2_CLM_COB_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_COB_F_in)
    .load()
)

# Stage: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') as TRGT_CD,
TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# Stage: cpy_cd_mppng (PxCopy)
df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

# Stage: lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_CLM_COB_F_in.alias("lnk_IdsEdwClmCobFExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("lnk_refLiabTyp"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_LIAB_TYP_CD_SK") == col("lnk_refLiabTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_refClmCobType"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_TYP_CD_SK") == col("lnk_refClmCobType.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_SK").alias("CLM_COB_SK"),
        col("lnk_IdsEdwClmCobFExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_ID").alias("CLM_ID"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_SK").alias("CLM_SK"),
        col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK"),
        col("lnk_IdsEdwClmCobFExtr_InABC.ALW_AMT").alias("ALW_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.COPAY_AMT").alias("COPAY_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.DEDCT_AMT").alias("DEDCT_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.DSALW_AMT").alias("DSALW_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.MED_COINS_AMT").alias("MED_COINS_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.PD_AMT").alias("PD_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.SANC_AMT").alias("SANC_AMT"),
        col("lnk_IdsEdwClmCobFExtr_InABC.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
        col("lnk_IdsEdwClmCobFExtr_InABC.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
        col("lnk_refLiabTyp.TRGT_CD").alias("CLM_COB_LIAB_TYP_CD"),
        col("lnk_refClmCobType.TRGT_CD").alias("CLM_COB_TYP_CD")
    )
)

# Stage: xfrm_BusinessLogic (CTransformerStage)
# Output link "lnk_Detail" constraint => CLM_COB_SK != 0 AND != 1
df_detail = (
    df_lkp_Codes
    .filter((col("CLM_COB_SK") != 0) & (col("CLM_COB_SK") != 1))
    .select(
        col("CLM_COB_SK").alias("CLM_COB_SK"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID"),
        when(col("CLM_COB_TYP_CD").isNull(), lit(" ")).otherwise(col("CLM_COB_TYP_CD")).alias("CLM_COB_TYP_CD"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("CLM_SK").alias("CLM_SK"),
        when(col("CLM_COB_LIAB_TYP_CD").isNull(), lit(" ")).otherwise(col("CLM_COB_LIAB_TYP_CD")).alias("CLM_COB_LIAB_TYP_CD"),
        col("ALW_AMT").alias("CLM_COB_ALW_AMT"),
        col("COPAY_AMT").alias("CLM_COB_COPAY_AMT"),
        col("DEDCT_AMT").alias("CLM_COB_DEDCT_AMT"),
        col("DSALW_AMT").alias("CLM_COB_DSALW_AMT"),
        col("MED_COINS_AMT").alias("CLM_COB_MED_COINS_AMT"),
        col("MNTL_HLTH_COINS_AMT").alias("CLM_COB_MNTL_HLTH_COINS_AMT"),
        col("PD_AMT").alias("CLM_COB_PD_AMT"),
        col("SANC_AMT").alias("CLM_COB_SANC_AMT"),
        col("COB_CAR_RSN_CD_TX").alias("CLM_COB_CAR_RSN_CD_TX"),
        col("COB_CAR_RSN_TX").alias("CLM_COB_CAR_RSN_TX"),
        lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
        col("CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK")
    )
)

# Output link "lnk_Na" constraint => ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Create single-row dataframe for "Na"
data_na = [
    (
        1,            # CLM_COB_SK
        "NA",         # SRC_SYS_CD
        "NA",         # CLM_ID
        "NA",         # CLM_COB_TYP_CD
        "1753-01-01", # CRT_RUN_CYC_EXCTN_DT_SK
        "1753-01-01", # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
        1,            # CLM_SK
        "NA",         # CLM_COB_LIAB_TYP_CD
        0.0,          # CLM_COB_ALW_AMT
        0.0,          # CLM_COB_COPAY_AMT
        0.0,          # CLM_COB_DEDCT_AMT
        0.0,          # CLM_COB_DSALW_AMT
        0.0,          # CLM_COB_MED_COINS_AMT
        0.0,          # CLM_COB_MNTL_HLTH_COINS_AMT
        0.0,          # CLM_COB_PD_AMT
        0.0,          # CLM_COB_SANC_AMT
        None,         # CLM_COB_CAR_RSN_CD_TX
        None,         # CLM_COB_CAR_RSN_TX
        1,            # CRT_RUN_CYC_EXCTN_SK
        1,            # LAST_UPDT_RUN_CYC_EXCTN_SK
        1,            # CLM_COB_TYP_CD_SK
        1             # CLM_COB_LIAB_TYP_CD_SK
    )
]
schema_na = StructType([
    StructField("CLM_COB_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_COB_TYP_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CLM_COB_LIAB_TYP_CD", StringType(), True),
    StructField("CLM_COB_ALW_AMT", DoubleType(), True),
    StructField("CLM_COB_COPAY_AMT", DoubleType(), True),
    StructField("CLM_COB_DEDCT_AMT", DoubleType(), True),
    StructField("CLM_COB_DSALW_AMT", DoubleType(), True),
    StructField("CLM_COB_MED_COINS_AMT", DoubleType(), True),
    StructField("CLM_COB_MNTL_HLTH_COINS_AMT", DoubleType(), True),
    StructField("CLM_COB_PD_AMT", DoubleType(), True),
    StructField("CLM_COB_SANC_AMT", DoubleType(), True),
    StructField("CLM_COB_CAR_RSN_CD_TX", StringType(), True),
    StructField("CLM_COB_CAR_RSN_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_COB_TYP_CD_SK", IntegerType(), True),
    StructField("CLM_COB_LIAB_TYP_CD_SK", IntegerType(), True)
])
df_na = spark.createDataFrame(data_na, schema_na)

# Output link "lnk_Unk" constraint => same condition for 1 row
data_unk = [
    (
        0,            # CLM_COB_SK
        "UNK",        # SRC_SYS_CD
        "UNK",        # CLM_ID
        "UNK",        # CLM_COB_TYP_CD
        "1753-01-01", # CRT_RUN_CYC_EXCTN_DT_SK
        "1753-01-01", # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
        0,            # CLM_SK
        "UNK",        # CLM_COB_LIAB_TYP_CD
        0.0,          # CLM_COB_ALW_AMT
        0.0,          # CLM_COB_COPAY_AMT
        0.0,          # CLM_COB_DEDCT_AMT
        0.0,          # CLM_COB_DSALW_AMT
        0.0,          # CLM_COB_MED_COINS_AMT
        0.0,          # CLM_COB_MNTL_HLTH_COINS_AMT
        0.0,          # CLM_COB_PD_AMT
        0.0,          # CLM_COB_SANC_AMT
        None,         # CLM_COB_CAR_RSN_CD_TX
        None,         # CLM_COB_CAR_RSN_TX
        0,            # CRT_RUN_CYC_EXCTN_SK
        0,            # LAST_UPDT_RUN_CYC_EXCTN_SK
        0,            # CLM_COB_TYP_CD_SK
        0             # CLM_COB_LIAB_TYP_CD_SK
    )
]
schema_unk = StructType([
    StructField("CLM_COB_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_COB_TYP_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CLM_COB_LIAB_TYP_CD", StringType(), True),
    StructField("CLM_COB_ALW_AMT", DoubleType(), True),
    StructField("CLM_COB_COPAY_AMT", DoubleType(), True),
    StructField("CLM_COB_DEDCT_AMT", DoubleType(), True),
    StructField("CLM_COB_DSALW_AMT", DoubleType(), True),
    StructField("CLM_COB_MED_COINS_AMT", DoubleType(), True),
    StructField("CLM_COB_MNTL_HLTH_COINS_AMT", DoubleType(), True),
    StructField("CLM_COB_PD_AMT", DoubleType(), True),
    StructField("CLM_COB_SANC_AMT", DoubleType(), True),
    StructField("CLM_COB_CAR_RSN_CD_TX", StringType(), True),
    StructField("CLM_COB_CAR_RSN_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_COB_TYP_CD_SK", IntegerType(), True),
    StructField("CLM_COB_LIAB_TYP_CD_SK", IntegerType(), True)
])
df_unk = spark.createDataFrame(data_unk, schema_unk)

# Stage: Fnl_Clm_Cob_F (PxFunnel)
df_fnl_clm_cob_f = df_detail.unionByName(df_na).unionByName(df_unk)

# Apply rpad to columns that have SqlType=char(10)
df_fnl_clm_cob_f = df_fnl_clm_cob_f.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

# Final column order from the funnel's output pins
df_fnl_clm_cob_f = df_fnl_clm_cob_f.select(
    "CLM_COB_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "CLM_COB_LIAB_TYP_CD",
    "CLM_COB_ALW_AMT",
    "CLM_COB_COPAY_AMT",
    "CLM_COB_DEDCT_AMT",
    "CLM_COB_DSALW_AMT",
    "CLM_COB_MED_COINS_AMT",
    "CLM_COB_MNTL_HLTH_COINS_AMT",
    "CLM_COB_PD_AMT",
    "CLM_COB_SANC_AMT",
    "CLM_COB_CAR_RSN_CD_TX",
    "CLM_COB_CAR_RSN_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_COB_TYP_CD_SK",
    "CLM_COB_LIAB_TYP_CD_SK"
)

# Stage: seq_CLM_COB_F_load_csv (PxSequentialFile)
write_files(
    df_fnl_clm_cob_f,
    f"{adls_path}/load/CLM_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)