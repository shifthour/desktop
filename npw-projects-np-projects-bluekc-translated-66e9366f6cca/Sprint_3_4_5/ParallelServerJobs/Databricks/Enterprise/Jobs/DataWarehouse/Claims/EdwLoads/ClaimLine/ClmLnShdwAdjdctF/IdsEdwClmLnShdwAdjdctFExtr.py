# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING: Extracts Data from IDS CLM_LN_SHADOW table. Extracts both ESI and FACETS sources.
# MAGIC 
# MAGIC 
# MAGIC Called By:IdsClaimLoad1Seq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Developer            Date            Description                                                              Project                       Environment                                  Code Reviewer                     Date  Reviewed
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Raja Gummadi              2015-10-09        Original Programming                                               5382                            EnterpriseDev2                              Bhoomi Dasari                       10/27/2015
# MAGIC Rekha Radhakrishna   2020-10-12       Added field PBM_CALC_CSR_SBSDY_AMT           6131                         EnterpriseDev2                                 Reddy Sanam                        12/16/2020                 
# MAGIC                                                              and OPTUMRX block of code in Source SQL

# MAGIC Add Defaults and Null Handling
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write CLM_LN_SHADOW_ADJDCT_F Data into a Sequential file for Load Job.
# MAGIC Read from source table 
# MAGIC CLM_LN_SHADOW_ADJDCT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ESIRunCycle = get_widget_value('ESIRunCycle','')
OPTUMRXRunCycle = get_widget_value('OPTUMRXRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_CLM_LN_SHADOW_ADJDCT_in
query_db2_CLM_LN_SHADOW_ADJDCT_in = f"""
SELECT 
CLM.CLM_LN_SK, 
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
CLM.CLM_ID, 
CLM.CLM_LN_SEQ_NO, 
CLM.SRC_SYS_CD_SK, 
CLM.CRT_RUN_CYC_EXCTN_SK, 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK, 
CLM.SHADOW_MED_UTIL_EDIT_IN, 
CLM.CNSD_CHRG_AMT, 
CLM.INIT_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_COINS_AMT, 
CLM.SHADOW_ADJDCT_COPAY_AMT, 
CLM.SHADOW_ADJDCT_DEDCT_AMT, 
CLM.SHADOW_ADJDCT_DSALW_AMT, 
CLM.SHADOW_ADJDCT_PAYBL_AMT, 
CLM.INIT_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_DEDCT_AMT_ACCUM_ID, 
CLM.SHADOW_LMT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_DEDCT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_SVC_PAYMT_ID, 
CLM.SHADOW_SVC_RULE_TYP_TX,
CLM.PBM_CALC_CSR_SBSDY_AMT 
FROM 
{IDSOwner}.CLM_LN_SHADOW_ADJDCT CLM,
{IDSOwner}.CD_MPPNG CD 
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK 
  AND CD.TRGT_CD = 'FACETS'

UNION

SELECT 
CLM.CLM_LN_SK, 
COALESCE(cd.TRGT_CD,'UNK') SRC_SYS_CD,
CLM.CLM_ID, 
CLM.CLM_LN_SEQ_NO, 
CLM.SRC_SYS_CD_SK, 
CLM.CRT_RUN_CYC_EXCTN_SK, 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK, 
CLM.SHADOW_MED_UTIL_EDIT_IN, 
CLM.CNSD_CHRG_AMT, 
CLM.INIT_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_COINS_AMT, 
CLM.SHADOW_ADJDCT_COPAY_AMT, 
CLM.SHADOW_ADJDCT_DEDCT_AMT, 
CLM.SHADOW_ADJDCT_DSALW_AMT, 
CLM.SHADOW_ADJDCT_PAYBL_AMT, 
CLM.INIT_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_DEDCT_AMT_ACCUM_ID, 
CLM.SHADOW_LMT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_DEDCT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_SVC_PAYMT_ID, 
CLM.SHADOW_SVC_RULE_TYP_TX,
CLM.PBM_CALC_CSR_SBSDY_AMT 
FROM {IDSOwner}.CLM_LN_SHADOW_ADJDCT CLM,
     {IDSOwner}.CD_MPPNG cd
WHERE CLM.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ESI'
  AND CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ESIRunCycle}

UNION

SELECT 
CLM.CLM_LN_SK, 
COALESCE(cd.TRGT_CD,'UNK') SRC_SYS_CD,
CLM.CLM_ID, 
CLM.CLM_LN_SEQ_NO, 
CLM.SRC_SYS_CD_SK, 
CLM.CRT_RUN_CYC_EXCTN_SK, 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK, 
CLM.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK, 
CLM.SHADOW_MED_UTIL_EDIT_IN, 
CLM.CNSD_CHRG_AMT, 
CLM.INIT_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_ALW_AMT, 
CLM.SHADOW_ADJDCT_COINS_AMT, 
CLM.SHADOW_ADJDCT_COPAY_AMT, 
CLM.SHADOW_ADJDCT_DEDCT_AMT, 
CLM.SHADOW_ADJDCT_DSALW_AMT, 
CLM.SHADOW_ADJDCT_PAYBL_AMT, 
CLM.INIT_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT, 
CLM.SHADOW_DEDCT_AMT_ACCUM_ID, 
CLM.SHADOW_LMT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_DEDCT_PFX_ID, 
CLM.SHADOW_PROD_CMPNT_SVC_PAYMT_ID, 
CLM.SHADOW_SVC_RULE_TYP_TX,
CLM.PBM_CALC_CSR_SBSDY_AMT 
FROM {IDSOwner}.CLM_LN_SHADOW_ADJDCT CLM,
     {IDSOwner}.CD_MPPNG cd
WHERE CLM.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'OPTUMRX'
  AND CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {OPTUMRXRunCycle}
"""

df_db2_CLM_LN_SHADOW_ADJDCT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CLM_LN_SHADOW_ADJDCT_in)
    .load()
)

# db2_CD_MPPNG_in
query_db2_CD_MPPNG_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

# db2_EXCD_in
query_db2_EXCD_in = f"""
SELECT 
EXCD_SK,
EXCD_ID
FROM {IDSOwner}.EXCD
"""

df_db2_EXCD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_EXCD_in)
    .load()
)

# Lkp_Cds (PxLookup)
df_lkp_cds = (
    df_db2_CLM_LN_SHADOW_ADJDCT_in.alias("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC")
    .join(
        df_db2_CD_MPPNG_in.alias("lnk_CdMppng_out"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK")
        == F.col("lnk_CdMppng_out.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_db2_EXCD_in.alias("lnk_excd_out"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK")
        == F.col("lnk_excd_out.EXCD_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK").alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_MED_UTIL_EDIT_IN").alias("SHADOW_MED_UTIL_EDIT_IN"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.INIT_ADJDCT_ALW_AMT").alias("INIT_ADJDCT_ALW_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_ALW_AMT").alias("SHADOW_ADJDCT_ALW_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_DSALW_AMT").alias("SHADOW_ADJDCT_DSALW_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.INIT_ADJDCT_ALW_PRICE_UNIT_CT").alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT").alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_DEDCT_AMT_ACCUM_ID").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_LMT_PFX_ID").alias("SHADOW_LMT_PFX_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_PROD_CMPNT_DEDCT_PFX_ID").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_PROD_CMPNT_SVC_PAYMT_ID").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.SHADOW_SVC_RULE_TYP_TX").alias("SHADOW_SVC_RULE_TYP_TX"),
        F.col("lnk_CdMppng_out.TRGT_CD").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
        F.col("lnk_CdMppng_out.TRGT_CD_NM").alias("CLM_LN_SHADOW_ADJDCT_STTUS_NM"),
        F.col("lnk_excd_out.EXCD_ID").alias("EXCD_ID"),
        F.col("lnk_IdsEdwClmLnShadwAdjdctFExtr_InABC.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT"),
    )
)

# xfrm_BusinessLogic (CTransformerStage)

df_xfrm_bizlogic_main = (
    df_lkp_cds
    .filter(
        (F.col("CLM_LN_SK") != 0) & (F.col("CLM_LN_SK") != 1)
    )
    .select(
        F.col("CLM_LN_SK"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("SRC_SYS_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
        F.when(
            F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD").isNull() | (trim(F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD")).alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
        F.when(
            F.col("CLM_LN_SHADOW_ADJDCT_STTUS_NM").isNull() | (trim(F.col("CLM_LN_SHADOW_ADJDCT_STTUS_NM")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_LN_SHADOW_ADJDCT_STTUS_NM")).alias("CLM_LN_SHADOW_ADJDCT_STTUS_NM"),
        F.col("SHADOW_MED_UTIL_EDIT_IN"),
        F.col("CNSD_CHRG_AMT"),
        F.col("INIT_ADJDCT_ALW_AMT"),
        F.col("SHADOW_ADJDCT_ALW_AMT"),
        F.col("SHADOW_ADJDCT_COINS_AMT"),
        F.col("SHADOW_ADJDCT_COPAY_AMT"),
        F.col("SHADOW_ADJDCT_DEDCT_AMT"),
        F.col("SHADOW_ADJDCT_DSALW_AMT"),
        F.col("SHADOW_ADJDCT_PAYBL_AMT"),
        F.col("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
        F.col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
        F.col("SHADOW_DEDCT_AMT_ACCUM_ID"),
        F.col("SHADOW_LMT_PFX_ID"),
        F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
        F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
        F.when(
            F.col("EXCD_ID").isNull() | (trim(F.col("EXCD_ID")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("EXCD_ID")).alias("EXCD_ID"),
        F.when(
            F.col("SHADOW_SVC_RULE_TYP_TX").isNull() | (trim(F.col("SHADOW_SVC_RULE_TYP_TX")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("SHADOW_SVC_RULE_TYP_TX")).alias("SHADOW_SVC_RULE_TYP_TX"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
        F.col("PBM_CALC_CSR_SBSDY_AMT"),
    )
)

# In_Na: single row
schema_in_na = StructType([
    StructField("CLM_LN_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK", IntegerType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_CD", StringType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_NM", StringType(), True),
    StructField("SHADOW_MED_UTIL_EDIT_IN", StringType(), True),
    StructField("CNSD_CHRG_AMT", DoubleType(), True),
    StructField("INIT_ADJDCT_ALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_ALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_COINS_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_COPAY_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_DEDCT_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_DSALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_PAYBL_AMT", DoubleType(), True),
    StructField("INIT_ADJDCT_ALW_PRICE_UNIT_CT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT", DoubleType(), True),
    StructField("SHADOW_DEDCT_AMT_ACCUM_ID", StringType(), True),
    StructField("SHADOW_LMT_PFX_ID", StringType(), True),
    StructField("SHADOW_PROD_CMPNT_DEDCT_PFX_ID", StringType(), True),
    StructField("SHADOW_PROD_CMPNT_SVC_PAYMT_ID", StringType(), True),
    StructField("EXCD_ID", StringType(), True),
    StructField("SHADOW_SVC_RULE_TYP_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK", IntegerType(), True),
    StructField("PBM_CALC_CSR_SBSDY_AMT", DoubleType(), True),
])
data_in_na = [
    (
        1, "NA", 0, "NA", "1753-01-01", "1753-01-01", 1, "NA", "NA", "N", 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "NA", "NA", "NA", "NA", "NA", "NA",
        100, 100, 100, 1, 0.0
    )
]
df_xfrm_bizlogic_in_na = spark.createDataFrame(data_in_na, schema_in_na)

# In_Unk: single row
schema_in_unk = StructType([
    StructField("CLM_LN_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK", IntegerType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_CD", StringType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_NM", StringType(), True),
    StructField("SHADOW_MED_UTIL_EDIT_IN", StringType(), True),
    StructField("CNSD_CHRG_AMT", DoubleType(), True),
    StructField("INIT_ADJDCT_ALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_ALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_COINS_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_COPAY_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_DEDCT_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_DSALW_AMT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_PAYBL_AMT", DoubleType(), True),
    StructField("INIT_ADJDCT_ALW_PRICE_UNIT_CT", DoubleType(), True),
    StructField("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT", DoubleType(), True),
    StructField("SHADOW_DEDCT_AMT_ACCUM_ID", StringType(), True),
    StructField("SHADOW_LMT_PFX_ID", StringType(), True),
    StructField("SHADOW_PROD_CMPNT_DEDCT_PFX_ID", StringType(), True),
    StructField("SHADOW_PROD_CMPNT_SVC_PAYMT_ID", StringType(), True),
    StructField("EXCD_ID", StringType(), True),
    StructField("SHADOW_SVC_RULE_TYP_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK", IntegerType(), True),
    StructField("PBM_CALC_CSR_SBSDY_AMT", DoubleType(), True),
])
data_in_unk = [
    (
        0, "UNK", 0, "UNK", "1753-01-01", "1753-01-01", 0, "UNK", "UNK", "N", 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "UNK", "UNK", "UNK", "UNK", "UNK", "UNK",
        100, 100, 100, 0, 0.0
    )
]
df_xfrm_bizlogic_in_unk = spark.createDataFrame(data_in_unk, schema_in_unk)

# Funnel_8 (PxFunnel)
df_funnel_8 = (
    df_xfrm_bizlogic_main.unionByName(df_xfrm_bizlogic_in_na)
    .unionByName(df_xfrm_bizlogic_in_unk)
)

df_funnel_8_final = df_funnel_8.select(
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
    F.col("CLM_LN_SHADOW_ADJDCT_STTUS_NM"),
    F.rpad(F.col("SHADOW_MED_UTIL_EDIT_IN"), 1, " ").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("CNSD_CHRG_AMT"),
    F.col("INIT_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("SHADOW_LMT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("EXCD_ID"),
    F.col("SHADOW_SVC_RULE_TYP_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.col("PBM_CALC_CSR_SBSDY_AMT"),
)

write_files(
    df_funnel_8_final,
    f"{adls_path}/load/CLM_LN_SHADOW_ADJDCT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)