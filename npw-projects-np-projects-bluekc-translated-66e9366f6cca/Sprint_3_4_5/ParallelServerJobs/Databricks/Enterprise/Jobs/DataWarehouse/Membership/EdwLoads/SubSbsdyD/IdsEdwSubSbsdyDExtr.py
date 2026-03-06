# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsSubSbsdyDExtr
# MAGIC 
# MAGIC Called By:  EdwMbrNoDriverSeq
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS SUB_SBSDY to flatfile SUB_SBSDY_D.dat.  Only records with the most recent LAST_UPDT_RUN_CYC_EXCTN_SK are pulled.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC SUB_SBSDY
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: SUB_SBSDY_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                                                                                                                                                   Cope Reviewer                 Review Date
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------                                                                                                                         -----------------------------------       --------------------
# MAGIC 2014-01-24     Santosh Bokka         Original Programming.                                                                                                                                                                                                Bhoomi Dasari                   2/17/2014

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwSubSbsdyDExtr
# MAGIC Subscriber Rate Extract - Pulls from IDS SUB_SBSDY to load to file SUB_SBSDY_D.dat
# MAGIC Write SUB_SBSDY_D Data into a Sequential file for Load Ready Job.
# MAGIC Extract updated SUB_SBSDY IDS Data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_db2_SUB_SBSDY_in, jdbc_props_db2_SUB_SBSDY_in = get_db_config(ids_secret_name)
extract_query_db2_SUB_SBSDY_in = f"""
SELECT 
SBSDY.SUB_SBSDY_SK,
SBSDY.SUB_UNIQ_KEY,
SBSDY.SUB_SBSDY_TYP_CD,
SBSDY.CLS_PLN_PROD_CAT_CD,
SBSDY.EFF_DT_SK,
SBSDY.SRC_SYS_CD_SK,
SBSDY.CRT_RUN_CYC_EXCTN_SK,
SBSDY.LAST_UPDT_RUN_CYC_EXCTN_SK,
SBSDY.SUB_SK,
SBSDY.CLS_PLN_PROD_CAT_CD_SK,
SBSDY.SUB_SBSDY_TYP_CD_SK,
SBSDY.TERM_DT_SK,
SBSDY.SBSDY_AMT,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD
FROM {IDSOwner}.SUB_SBSDY SBSDY
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON SBSDY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE SBSDY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
df_db2_SUB_SBSDY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_SUB_SBSDY_in)
    .options(**jdbc_props_db2_SUB_SBSDY_in)
    .option("query", extract_query_db2_SUB_SBSDY_in)
    .load()
)

jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') AS TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy_CdMppng = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_SUB_SBSDY_in.alias("lnk_IdsEdwSubSbcdyDExtr_In")
    .join(
        df_cpy_CdMppng.alias("Ref_Sub_Sbsdy_Typ_CDLkp"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_SBSDY_TYP_CD_SK") == F.col("Ref_Sub_Sbsdy_Typ_CDLkp.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_cpy_CdMppng.alias("Ref_Cls_Pln_Prod_TgtLkp"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.CLS_PLN_PROD_CAT_CD_SK") == F.col("Ref_Cls_Pln_Prod_TgtLkp.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_SBSDY_SK").alias("SUB_SBSDY_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_SBSDY_TYP_CD").alias("SUB_SBSDY_TYP_CD"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SUB_SBSDY_TYP_CD_SK").alias("SUB_SBSDY_TYP_CD_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SBSDY_AMT").alias("SBSDY_AMT"),
        F.col("lnk_IdsEdwSubSbcdyDExtr_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ref_Sub_Sbsdy_Typ_CDLkp.TRGT_CD_NM").alias("SUB_SBSDY_TYP_CD_NM"),
        F.col("Ref_Cls_Pln_Prod_TgtLkp.TRGT_CD_NM").alias("CLS_PLN_PROD_CAT_NM")
    )
)

df_main = (
    df_lkp_Codes
    .filter((F.col("SUB_SBSDY_SK") != 0) & (F.col("SUB_SBSDY_SK") != 1))
    .select(
        F.col("SUB_SBSDY_SK").alias("SUB_SBSDY_SK"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("SUB_SBSDY_TYP_CD").alias("SUB_SBSDY_TYP_CD"),
        F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
        F.col("EFF_DT_SK").alias("SUB_SBSDY_EFF_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.when(F.col("CLS_PLN_PROD_CAT_NM").isNull(), "UNK").otherwise(F.col("CLS_PLN_PROD_CAT_NM")).alias("CLS_PLN_PROD_CAT_NM"),
        F.when(F.col("SUB_SBSDY_TYP_CD_NM").isNull(), "UNK").otherwise(F.col("SUB_SBSDY_TYP_CD_NM")).alias("SUB_SBSDY_TYP_NM"),
        F.col("TERM_DT_SK").alias("SUB_SBSDY_TERM_DT_SK"),
        F.col("SBSDY_AMT").alias("SBSDY_AMT"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SUB_SBSDY_TYP_CD_SK").alias("SUB_SBSDY_TYP_CD_SK"),
        F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK")
    )
)

schema_unk_na = [
    "SUB_SBSDY_SK",
    "SUB_UNIQ_KEY",
    "SUB_SBSDY_TYP_CD",
    "CLS_PLN_PROD_CAT_CD",
    "SUB_SBSDY_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_NM",
    "SUB_SBSDY_TYP_NM",
    "SUB_SBSDY_TERM_DT_SK",
    "SBSDY_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SBSDY_TYP_CD_SK",
    "CLS_PLN_PROD_CAT_CD_SK"
]

df_unkRow = spark.createDataFrame(
    [
        (
            "0","0","UNK","UNK","1753-01-01","UNK","1753-01-01",EDWRunCycleDate,"0","UNK","UNK","2199-12-31","0","100",EDWRunCycle,"0","0"
        )
    ],
    schema_unk_na
)

df_naRow = spark.createDataFrame(
    [
        (
            "1","1","NA","NA","1753-01-01","NA","1753-01-01",EDWRunCycleDate,"1","NA","NA","2199-12-31","0","100",EDWRunCycle,"1","1"
        )
    ],
    schema_unk_na
)

df_fnl_dataLinks = df_main.unionByName(df_unkRow).unionByName(df_naRow)

df_fnl_dataLinks = (
    df_fnl_dataLinks
    .withColumn("SUB_SBSDY_EFF_DT_SK", F.rpad("SUB_SBSDY_EFF_DT_SK", 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("SUB_SBSDY_TERM_DT_SK", F.rpad("SUB_SBSDY_TERM_DT_SK", 10, " "))
)

df_final = df_fnl_dataLinks.select(
    "SUB_SBSDY_SK",
    "SUB_UNIQ_KEY",
    "SUB_SBSDY_TYP_CD",
    "CLS_PLN_PROD_CAT_CD",
    "SUB_SBSDY_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_NM",
    "SUB_SBSDY_TYP_NM",
    "SUB_SBSDY_TERM_DT_SK",
    "SBSDY_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_SBSDY_TYP_CD_SK",
    "CLS_PLN_PROD_CAT_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_SBSDY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)