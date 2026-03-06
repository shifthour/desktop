# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_LVL_LTR
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                    Steph Goddard           09/15/2007
# MAGIC 
# MAGIC Srikanth Mettpalli            06/3/2013          5114                    Original Programming                                  EnterpriseWrhsDevl         Jag Yelavarthi             2014-01-17
# MAGIC                                                                                       (Server to Parallel Conversion)

# MAGIC Code SK lookups for Denormalization
# MAGIC Write APL_LVL_LTR_D Data into a Sequential file for Load Job IdsEdwAplLvlLtrDLoad.
# MAGIC IDS APL LVL LTR extract from IDS
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwAplLvlLtrDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

# DB2ConnectorPX: db2_APL_LVL_LTR_in
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_APL_LVL_LTR_in = f"""
SELECT 
APL_LVL_LTR.APL_LVL_LTR_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_LVL_LTR.APL_ID,
APL_LVL_LTR.SEQ_NO,
APL_LVL_LTR.APL_LVL_LTR_STYLE_CD_SK,
APL_LVL_LTR.LTR_SEQ_NO,
APL_LVL_LTR.LTR_DEST_ID,
APL_LVL_LTR.APL_LVL_SK,
APL_LVL_LTR.APL_LVL_LTR_TYP_CD_SK,
APL_LVL_LTR.RQST_DT_SK,
APL_LVL_LTR.ADDREE_NM,
APL_LVL_LTR.ADDREE_ADDR_LN_1,
APL_LVL_LTR.ADDREE_ADDR_LN_2,
APL_LVL_LTR.ADDREE_CITY_NM,
APL_LVL_LTR.APL_LVL_LTR_ADDREE_ST_CD_SK,
APL_LVL_LTR.ADDREE_POSTAL_CD,
APL_LVL_LTR.ADDREE_CNTY_NM,
APL_LVL_LTR.ADDREE_PHN_NO,
APL_LVL_LTR.ADDREE_FAX_NO,
APL_LVL_LTR.EXPL_TX,
APL_LVL_LTR.LTR_TX_1,
APL_LVL_LTR.LTR_TX_2,
APL_LVL_LTR.LTR_TX_3,
APL_LVL_LTR.LTR_TX_4,
APL_LVL_LTR.SUBJ_TX
FROM {IDSOwner}.APL_LVL_LTR APL_LVL_LTR
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_LVL_LTR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE APL_LVL_LTR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_APL_LVL_LTR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_APL_LVL_LTR_in)
    .load()
)

# DB2ConnectorPX: db2_APL_LVL_LTR_PRT_in
extract_query_db2_APL_LVL_LTR_PRT_in = f"""
SELECT 
PRT.APL_ID,
PRT.SEQ_NO,
max(PRT.PRT_DT_SK) PRT_DT_SK
FROM {IDSOwner}.APL_LVL_LTR_PRT PRT
GROUP BY
PRT.APL_ID,
PRT.SEQ_NO
"""
df_db2_APL_LVL_LTR_PRT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_APL_LVL_LTR_PRT_in)
    .load()
)

# DB2ConnectorPX: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
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
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# PxCopy: cpy_cd_mppng
df_Ref_AplLvlLtrStyCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_AplLvlLtrTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_AplLvlLtrAddrStCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# PxLookup: lkp_Codes
df_lkp_Codes = (
    df_db2_APL_LVL_LTR_in.alias("lnk_IdsEdwAplLvlLtrDExtr_InABC")
    .join(
        df_Ref_AplLvlLtrStyCd_Lkup.alias("Ref_AplLvlLtrStyCd_Lkup"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_STYLE_CD_SK") == F.col("Ref_AplLvlLtrStyCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_AplLvlLtrTypCd_Lkup.alias("Ref_AplLvlLtrTypCd_Lkup"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_TYP_CD_SK") == F.col("Ref_AplLvlLtrTypCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_AplLvlLtrAddrStCd_Lkup.alias("Ref_AplLvlLtrAddrStCd_Lkup"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_ADDREE_ST_CD_SK") == F.col("Ref_AplLvlLtrAddrStCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_db2_APL_LVL_LTR_PRT_in.alias("Ref_Max_Prt_Dt_Lkp"),
        [
            F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_ID") == F.col("Ref_Max_Prt_Dt_Lkp.APL_ID"),
            F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.SEQ_NO") == F.col("Ref_Max_Prt_Dt_Lkp.SEQ_NO")
        ],
        how="left"
    )
    .select(
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_ID").alias("APL_ID"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.SEQ_NO").alias("APL_LVL_SEQ_NO"),
        F.col("Ref_AplLvlLtrStyCd_Lkup.TRGT_CD").alias("APL_LVL_LTR_STYLE_CD"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_NM").alias("APL_LVL_LTR_ADDREE_NM"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_ADDR_LN_1").alias("APL_LVL_LTR_ADDREE_ADDR_LN_1"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_ADDR_LN_2").alias("APL_LVL_LTR_ADDREE_ADDR_LN_2"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_CITY_NM").alias("APL_LVL_LTR_ADDREE_CITY_NM"),
        F.col("Ref_AplLvlLtrAddrStCd_Lkup.TRGT_CD").alias("APL_LVL_LTR_ADDREE_ST_CD"),
        F.col("Ref_AplLvlLtrAddrStCd_Lkup.TRGT_CD_NM").alias("APL_LVL_LTR_ADDREE_ST_NM"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_POSTAL_CD").alias("APL_LVL_LTR_ADDREE_POSTAL_CD"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_CNTY_NM").alias("APL_LVL_LTR_ADDREE_CNTY_NM"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_PHN_NO").alias("APL_LVL_LTR_ADDREE_PHN_NO"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.ADDREE_FAX_NO").alias("APL_LVL_LTR_ADDREE_FAX_NO"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.EXPL_TX").alias("APL_LVL_LTR_EXPL_TX"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.RQST_DT_SK").alias("APL_LVL_LTR_RQST_DT_SK"),
        F.col("Ref_AplLvlLtrStyCd_Lkup.TRGT_CD_NM").alias("APL_LVL_LTR_STYLE_NM"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.SUBJ_TX").alias("APL_LVL_LTR_SUBJ_TX"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_TX_1").alias("APL_LVL_LTR_TX_1"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_TX_2").alias("APL_LVL_LTR_TX_2"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_TX_3").alias("APL_LVL_LTR_TX_3"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.LTR_TX_4").alias("APL_LVL_LTR_TX_4"),
        F.col("Ref_AplLvlLtrTypCd_Lkup.TRGT_CD").alias("APL_LVL_LTR_TYP_CD"),
        F.col("Ref_AplLvlLtrTypCd_Lkup.TRGT_CD_NM").alias("APL_LVL_LTR_TYP_NM"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_ADDREE_ST_CD_SK").alias("APL_LVL_LTR_ADDREE_ST_CD_SK"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("lnk_IdsEdwAplLvlLtrDExtr_InABC.APL_LVL_LTR_TYP_CD_SK").alias("APL_LVL_LTR_TYP_CD_SK"),
        F.col("Ref_Max_Prt_Dt_Lkp.PRT_DT_SK").alias("PRT_DT_SK")
    )
)

# CTransformerStage: xfrm_BusinessLogic
# lnk_xfm_Data (constraint: APL_LVL_LTR_SK <> 1 AND <> 0)
# Transform columns
df_xfrm_main = df_lkp_Codes.filter(
    (F.col("APL_LVL_LTR_SK") != 1) & (F.col("APL_LVL_LTR_SK") != 0)
).select(
    F.col("APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("NA")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.when(
        F.col("APL_LVL_LTR_STYLE_CD").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_STYLE_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_STYLE_CD")).alias("APL_LVL_LTR_STYLE_CD"),
    F.col("APL_LVL_LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
    F.col("APL_LVL_LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("APL_LVL_LTR_ADDREE_NM").alias("APL_LVL_LTR_ADDREE_NM"),
    F.col("APL_LVL_LTR_ADDREE_ADDR_LN_1").alias("APL_LVL_LTR_ADDREE_ADDR_LN_1"),
    F.col("APL_LVL_LTR_ADDREE_ADDR_LN_2").alias("APL_LVL_LTR_ADDREE_ADDR_LN_2"),
    F.col("APL_LVL_LTR_ADDREE_CITY_NM").alias("APL_LVL_LTR_ADDREE_CITY_NM"),
    F.when(
        F.col("APL_LVL_LTR_ADDREE_ST_CD").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_ADDREE_ST_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_ADDREE_ST_CD")).alias("APL_LVL_LTR_ADDREE_ST_CD"),
    F.when(
        F.col("APL_LVL_LTR_ADDREE_ST_NM").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_ADDREE_ST_NM"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_ADDREE_ST_NM")).alias("APL_LVL_LTR_ADDREE_ST_NM"),
    F.col("APL_LVL_LTR_ADDREE_POSTAL_CD").alias("APL_LVL_LTR_ADDREE_POSTAL_CD"),
    F.col("APL_LVL_LTR_ADDREE_CNTY_NM").alias("APL_LVL_LTR_ADDREE_CNTY_NM"),
    F.col("APL_LVL_LTR_ADDREE_PHN_NO").alias("APL_LVL_LTR_ADDREE_PHN_NO"),
    F.col("APL_LVL_LTR_ADDREE_FAX_NO").alias("APL_LVL_LTR_ADDREE_FAX_NO"),
    F.col("APL_LVL_LTR_EXPL_TX").alias("APL_LVL_LTR_EXPL_TX"),
    F.when(
        F.col("PRT_DT_SK").isNull() | (F.length(F.trim(F.col("PRT_DT_SK"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("PRT_DT_SK")).alias("APL_LVL_LTR_LAST_PRT_DT_SK"),
    F.col("APL_LVL_LTR_RQST_DT_SK").alias("APL_LVL_LTR_RQST_DT_SK"),
    F.when(
        F.col("APL_LVL_LTR_STYLE_NM").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_STYLE_NM"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_STYLE_NM")).alias("APL_LVL_LTR_STYLE_NM"),
    F.col("APL_LVL_LTR_SUBJ_TX").alias("APL_LVL_LTR_SUBJ_TX"),
    F.col("APL_LVL_LTR_TX_1").alias("APL_LVL_LTR_TX_1"),
    F.col("APL_LVL_LTR_TX_2").alias("APL_LVL_LTR_TX_2"),
    F.col("APL_LVL_LTR_TX_3").alias("APL_LVL_LTR_TX_3"),
    F.col("APL_LVL_LTR_TX_4").alias("APL_LVL_LTR_TX_4"),
    F.when(
        F.col("APL_LVL_LTR_TYP_CD").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_TYP_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_TYP_CD")).alias("APL_LVL_LTR_TYP_CD"),
    F.when(
        F.col("APL_LVL_LTR_TYP_NM").isNull() |
        (F.length(F.trim(F.col("APL_LVL_LTR_TYP_NM"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("APL_LVL_LTR_TYP_NM")).alias("APL_LVL_LTR_TYP_NM"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_LTR_ADDREE_ST_CD_SK").alias("APL_LVL_LTR_ADDREE_ST_CD_SK"),
    F.col("APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK"),
    F.col("APL_LVL_LTR_TYP_CD_SK").alias("APL_LVL_LTR_TYP_CD_SK")
)

# lnk_NA_out (constraint: only 1 row)
schema_NA = T.StructType([
    T.StructField("APL_LVL_LTR_SK", T.StringType(), True),
    T.StructField("SRC_SYS_CD", T.StringType(), True),
    T.StructField("APL_ID", T.StringType(), True),
    T.StructField("APL_LVL_SEQ_NO", T.StringType(), True),
    T.StructField("APL_LVL_LTR_STYLE_CD", T.StringType(), True),
    T.StructField("APL_LVL_LTR_SEQ_NO", T.StringType(), True),
    T.StructField("APL_LVL_LTR_DEST_ID", T.StringType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("APL_LVL_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_NM", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_ADDR_LN_1", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_ADDR_LN_2", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_CITY_NM", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_ST_CD", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_ST_NM", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_POSTAL_CD", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_CNTY_NM", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_PHN_NO", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_FAX_NO", T.StringType(), True),
    T.StructField("APL_LVL_LTR_EXPL_TX", T.StringType(), True),
    T.StructField("APL_LVL_LTR_LAST_PRT_DT_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_RQST_DT_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_STYLE_NM", T.StringType(), True),
    T.StructField("APL_LVL_LTR_SUBJ_TX", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TX_1", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TX_2", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TX_3", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TX_4", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TYP_CD", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TYP_NM", T.StringType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.StringType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_ADDREE_ST_CD_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_STYLE_CD_SK", T.StringType(), True),
    T.StructField("APL_LVL_LTR_TYP_CD_SK", T.StringType(), True)
])
row_NA = [
    "1","NA","NA","0","NA","0","NA","1753-01-01","1753-01-01","1","NA",None,None,"NA","NA","NA","NA","NA",None,None,None,"1753-01-01","1753-01-01","NA",None,None,None,None,None,"NA","NA","100","100","1","1","1"
]
df_NA_out = spark.createDataFrame([tuple(row_NA)], schema=schema_NA)

# lnk_UNK_out (constraint: only 1 row)
schema_UNK = schema_NA
row_UNK = [
    "0","UNK","UNK","0","UNK","0","UNK","1753-01-01","1753-01-01","0","UNK",None,None,"UNK","UNK","UNK","UNK","UNK",None,None,None,"1753-01-01","1753-01-01","UNK",None,None,None,None,None,"UNK","UNK","100","100","0","0","0"
]
df_UNK_out = spark.createDataFrame([tuple(row_UNK)], schema=schema_UNK)

# PxFunnel: fnl_Data => union of NA_out, UNK_out, xfrm_main
df_funnel = df_NA_out.unionByName(df_UNK_out).unionByName(df_xfrm_main)

# For columns with SqlType=char(10), apply rpad to length 10
df_funnel = df_funnel.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "APL_LVL_LTR_LAST_PRT_DT_SK", F.rpad(F.col("APL_LVL_LTR_LAST_PRT_DT_SK"), 10, " ")
).withColumn(
    "APL_LVL_LTR_RQST_DT_SK", F.rpad(F.col("APL_LVL_LTR_RQST_DT_SK"), 10, " ")
)

# Final select in the funnel output order
final_columns = [
    "APL_LVL_LTR_SK",
    "SRC_SYS_CD",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "APL_LVL_LTR_STYLE_CD",
    "APL_LVL_LTR_SEQ_NO",
    "APL_LVL_LTR_DEST_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "APL_LVL_SK",
    "APL_LVL_LTR_ADDREE_NM",
    "APL_LVL_LTR_ADDREE_ADDR_LN_1",
    "APL_LVL_LTR_ADDREE_ADDR_LN_2",
    "APL_LVL_LTR_ADDREE_CITY_NM",
    "APL_LVL_LTR_ADDREE_ST_CD",
    "APL_LVL_LTR_ADDREE_ST_NM",
    "APL_LVL_LTR_ADDREE_POSTAL_CD",
    "APL_LVL_LTR_ADDREE_CNTY_NM",
    "APL_LVL_LTR_ADDREE_PHN_NO",
    "APL_LVL_LTR_ADDREE_FAX_NO",
    "APL_LVL_LTR_EXPL_TX",
    "APL_LVL_LTR_LAST_PRT_DT_SK",
    "APL_LVL_LTR_RQST_DT_SK",
    "APL_LVL_LTR_STYLE_NM",
    "APL_LVL_LTR_SUBJ_TX",
    "APL_LVL_LTR_TX_1",
    "APL_LVL_LTR_TX_2",
    "APL_LVL_LTR_TX_3",
    "APL_LVL_LTR_TX_4",
    "APL_LVL_LTR_TYP_CD",
    "APL_LVL_LTR_TYP_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_LVL_LTR_ADDREE_ST_CD_SK",
    "APL_LVL_LTR_STYLE_CD_SK",
    "APL_LVL_LTR_TYP_CD_SK"
]
df_final = df_funnel.select(*final_columns)

# PxSequentialFile: seq_APL_LVL_LTR_D_csv_load (writing to APL_LVL_LTR_D.dat)
# The path does not contain "landing" or "external", so use adls_path
write_files(
    df_final,
    f"{adls_path}/load/APL_LVL_LTR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue="null"
)