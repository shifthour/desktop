# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                                 DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                                    ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------             ---------------------------------------------------------------            ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rajasekhar Mangalampally      09/23/2013                          5114                                Original Programming                                             EnterpriseWrhsDevl                                   Jag Yelavarthi               2013-12-22
# MAGIC                                                                                                                                      (Server to Parallel Conversion)
# MAGIC      
# MAGIC Jag Yelavarthi                         2014-11-03           TFS#9564                                         Added Null validation after lookup operation        EnterpriseNewDevl                                   Kalyan Neelam              2014-11-04
# MAGIC                                                                                                                                       for below 2 target fields:
# MAGIC                                                                                                                                       DRG_METH_CD and DRG_METH_NM

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwDrgDExtr
# MAGIC 
# MAGIC Table:
# MAGIC DRG_D
# MAGIC Read from source table DRG
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write DRG_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) DRG_METH_CD_SK
# MAGIC 2) DRG_DIAG_CAT_CD_SK
# MAGIC 3) DRG_TYP_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_DRG_D_in = f"""
SELECT
  DRG_SK,
  DRG_CD,
  DRG_METH_CD_SK,
  ARTHMTC_AVG_LOS,
  DRG_DIAG_CAT_CD_SK,
  DRG_DESC,
  GEOMTRC_AVG_LOS,
  DRG_TYP_CD_SK,
  WT_FCTR
FROM {IDSOwner}.DRG
"""
df_db2_DRG_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_DRG_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
  CD_MPPNG_SK,
  TRGT_CD,
  TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy_cd_mppnig_Ref_DrgMethdCdLkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppnig_Ref_DrgTypCDLkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppnig_Ref_DrgDiagCatCdLkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_DRG_D_in.alias("lnk_IdsEdwDrgDExtr_InABC")
    .join(
        df_cpy_cd_mppnig_Ref_DrgTypCDLkp.alias("Ref_DrgTypCDLkp"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_TYP_CD_SK") == col("Ref_DrgTypCDLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppnig_Ref_DrgMethdCdLkp.alias("Ref_DrgMethdCdLkp"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_METH_CD_SK") == col("Ref_DrgMethdCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppnig_Ref_DrgDiagCatCdLkp.alias("Ref_DrgDiagCatCdLkp"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_DIAG_CAT_CD_SK") == col("Ref_DrgDiagCatCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_SK").alias("DRG_SK"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_CD").alias("DRG_CD"),
        col("Ref_DrgMethdCdLkp.TRGT_CD").alias("DRG_METH_CD"),
        col("lnk_IdsEdwDrgDExtr_InABC.ARTHMTC_AVG_LOS").alias("DRG_ARTHMTC_AVG_LOS"),
        col("Ref_DrgDiagCatCdLkp.TRGT_CD").alias("DRG_DIAG_CAT_CD"),
        col("Ref_DrgDiagCatCdLkp.TRGT_CD_NM").alias("DRG_DIAG_CAT_NM"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_DESC").alias("DRG_DESC"),
        col("lnk_IdsEdwDrgDExtr_InABC.GEOMTRC_AVG_LOS").alias("DRG_GEOMTRC_AVG_LOS"),
        col("Ref_DrgMethdCdLkp.TRGT_CD_NM").alias("DRG_METH_NM"),
        col("Ref_DrgTypCDLkp.TRGT_CD").alias("DRG_TYP_CD"),
        col("Ref_DrgTypCDLkp.TRGT_CD_NM").alias("DRG_TYP_NM"),
        col("lnk_IdsEdwDrgDExtr_InABC.WT_FCTR").alias("DRG_WT_FCTR"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_DIAG_CAT_CD_SK").alias("DRG_DIAG_CAT_CD_SK"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_METH_CD_SK").alias("DRG_METH_CD_SK"),
        col("lnk_IdsEdwDrgDExtr_InABC.DRG_TYP_CD_SK").alias("DRG_TYP_CD_SK")
    )
)

df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "DRG_METH_CD",
        when(
            (col("DRG_METH_CD").isNull()) | (trim(col("DRG_METH_CD")) == ""),
            "UNK"
        ).otherwise(col("DRG_METH_CD"))
    )
    .withColumn(
        "DRG_METH_NM",
        when(
            (col("DRG_METH_NM").isNull()) | (trim(col("DRG_METH_NM")) == ""),
            "UNK"
        ).otherwise(col("DRG_METH_NM"))
    )
)

df_xfm_BusinessLogic_DrgDMainExtr = (
    df_xfm_BusinessLogic
    .filter((col("DRG_SK") != 0) & (col("DRG_SK") != 1))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("DRG_WT_FCTR_TX", col("DRG_WT_FCTR"))
    .select(
        col("DRG_SK"),
        col("DRG_CD"),
        col("DRG_METH_CD"),
        col("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("DRG_ARTHMTC_AVG_LOS"),
        col("DRG_DIAG_CAT_CD"),
        col("DRG_DIAG_CAT_NM"),
        col("DRG_DESC"),
        col("DRG_GEOMTRC_AVG_LOS"),
        col("DRG_METH_NM"),
        col("DRG_TYP_CD"),
        col("DRG_TYP_NM"),
        col("DRG_WT_FCTR"),
        col("DRG_WT_FCTR_TX"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("DRG_DIAG_CAT_CD_SK"),
        col("DRG_METH_CD_SK"),
        col("DRG_TYP_CD_SK")
    )
)

schema_20_cols = StructType([
    StructField("DRG_SK", IntegerType()),
    StructField("DRG_CD", StringType()),
    StructField("DRG_METH_CD", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("DRG_ARTHMTC_AVG_LOS", DoubleType()),
    StructField("DRG_DIAG_CAT_CD", StringType()),
    StructField("DRG_DIAG_CAT_NM", StringType()),
    StructField("DRG_DESC", StringType()),
    StructField("DRG_GEOMTRC_AVG_LOS", DoubleType()),
    StructField("DRG_METH_NM", StringType()),
    StructField("DRG_TYP_CD", StringType()),
    StructField("DRG_TYP_NM", StringType()),
    StructField("DRG_WT_FCTR", DoubleType()),
    StructField("DRG_WT_FCTR_TX", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("DRG_DIAG_CAT_CD_SK", IntegerType()),
    StructField("DRG_METH_CD_SK", IntegerType()),
    StructField("DRG_TYP_CD_SK", IntegerType())
])

df_xfm_BusinessLogic_UNK = spark.createDataFrame([
    (
        0, "UNK", "UNK", "1753-01-01", "1753-01-01",
        0.0, "UNK", "UNK", "", 0.0, "UNK", "UNK", "UNK",
        0.0, "", 100, 100, 0, 0, 0
    )
], schema_20_cols)

df_xfm_BusinessLogic_NA = spark.createDataFrame([
    (
        1, "NA", "NA", "1753-01-01", "1753-01-01",
        0.0, "NA", "NA", "", 0.0, "NA", "NA", "NA",
        0.0, "", 100, 100, 1, 1, 1
    )
], schema_20_cols)

df_fnl_NA_UNK = (
    df_xfm_BusinessLogic_DrgDMainExtr.unionByName(df_xfm_BusinessLogic_UNK)
    .unionByName(df_xfm_BusinessLogic_NA)
)

df_final = df_fnl_NA_UNK.select(
    col("DRG_SK"),
    col("DRG_CD"),
    col("DRG_METH_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("DRG_ARTHMTC_AVG_LOS"),
    col("DRG_DIAG_CAT_CD"),
    col("DRG_DIAG_CAT_NM"),
    col("DRG_DESC"),
    col("DRG_GEOMTRC_AVG_LOS"),
    col("DRG_METH_NM"),
    col("DRG_TYP_CD"),
    col("DRG_TYP_NM"),
    col("DRG_WT_FCTR"),
    col("DRG_WT_FCTR_TX"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRG_DIAG_CAT_CD_SK"),
    col("DRG_METH_CD_SK"),
    col("DRG_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/DRG_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)