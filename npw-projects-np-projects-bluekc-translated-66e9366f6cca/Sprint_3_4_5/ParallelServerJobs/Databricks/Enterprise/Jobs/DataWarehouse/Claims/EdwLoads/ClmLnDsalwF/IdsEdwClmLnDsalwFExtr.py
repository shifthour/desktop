# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                                                                                                      Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                                                                                          PROJECT                   Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------                                                                     ----------------------               ----------------------     -------------------   
# MAGIC Ralph Tucker 11/17/2004 -                        Originally Programmed
# MAGIC Brent Leland   08/30/2005 -                        Changed column output order to match table.
# MAGIC BJ Luce          11/21/2005  -                       add DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK, default to NA and 1
# MAGIC BJ Luce          1/17/2006                            pull DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK from IDS 
# MAGIC Brent Leland   04/04/2006                          Changed parameters to environment parameters
# MAGIC                                                                     Removed trim() off SK values.
# MAGIC Brent Leland    05/11/2006                         Removed trim() on codes and IDs
# MAGIC  
# MAGIC Rama Kamjula  09/18/2013                         Rewritten from Server to parallel version                                                                                                                                Jag Yelavarthi      2013-12-21
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru  11/25/2014    TFS - 9447   Modified Extract SQL, added Transformer, Sort and Remove Duplicate stages to                                                                Kalyan Neelam     2014-12-04
# MAGIC                                                                      avoid missing claims from IDS - CLM_LN_DSALW table because of the same TRGT_CD.
# MAGIC 
# MAGIC Kaushik Kapoor  03/28/2018  TFS - 21223  Adding CLM_LN_DSALW_TYP_CD_SK and SRC_SYS_CD as key while de-duping                   EnterpriseDev2             Kalyan Neelam      2018-04-04

# MAGIC JobName: IdsEdwClmLnDsalwFExtr
# MAGIC 
# MAGIC Job creates loadfile for CLM_LN_DSALW_F  in EDW
# MAGIC Extracts data from CLM_LN_DSALW_F table from IDS.
# MAGIC Extract Data from Code Mapping table.
# MAGIC Business Logic
# MAGIC Creates load file for CLM_LN_DSALW_F - EDW table
# MAGIC Removing duplicates based on CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DSALW_TYP_CD, SRC_SYS_CD
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2014-12-24')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_db2_CLM_LN_DSALW, jdbc_props_db2_CLM_LN_DSALW = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_DSALW = f"""
SELECT
CLM_LN_DSALW_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
ds.CLM_ID,
ds.CLM_LN_SEQ_NO,
ds.CLM_LN_DSALW_TYP_CD_SK,
ds.CLM_LN_SK,
ds.CLM_LN_DSALW_EXCD_SK,
ds.DSALW_AMT,
ds.CLM_LN_DSALW_TYP_CAT_CD_SK,
CL.DSALW_AMT as CLM_LN_DSALW_AMT
FROM {IDSOwner}.CLM_LN_DSALW ds
JOIN {IDSOwner}.W_EDW_ETL_DRVR dr 
  ON ds.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK 
  AND ds.CLM_ID = dr.CLM_ID
JOIN {IDSOwner}.CLM_LN CL 
  ON ds.CLM_ID = CL.CLM_ID 
  AND ds.CLM_LN_SEQ_NO = CL.CLM_LN_SEQ_NO
LEFT JOIN {IDSOwner}.CD_MPPNG CD 
  ON CD.CD_MPPNG_SK = ds.SRC_SYS_CD_SK
"""
df_db2_CLM_LN_DSALW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_LN_DSALW)
    .options(**jdbc_props_db2_CLM_LN_DSALW)
    .option("query", extract_query_db2_CLM_LN_DSALW)
    .load()
)

jdbc_url_db2_CD_MPPNG, jdbc_props_db2_CD_MPPNG = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG = f"""
SELECT
CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG)
    .options(**jdbc_props_db2_CD_MPPNG)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

df_lnk_ClmLnDsalwTyp = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_ClmLnDsalwTypCat = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_stage1 = df_db2_CLM_LN_DSALW.alias("lnk_PreExtr").join(
    df_lnk_ClmLnDsalwTyp.alias("lnk_ClmLnDsalwTyp"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_TYP_CD_SK") == F.col("lnk_ClmLnDsalwTyp.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes = df_lkp_stage1.join(
    df_lnk_ClmLnDsalwTypCat.alias("lnk_ClmLnDsalwTypCat"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_TYP_CAT_CD_SK") == F.col("lnk_ClmLnDsalwTypCat.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes_output = df_lkp_Codes.select(
    F.col("lnk_PreExtr.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("lnk_PreExtr.CLM_ID").alias("CLM_ID"),
    F.col("lnk_PreExtr.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_PreExtr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.col("lnk_PreExtr.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("lnk_PreExtr.DSALW_AMT").alias("DSALW_AMT"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_TYP_CAT_CD_SK").alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    F.col("lnk_ClmLnDsalwTyp.TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("lnk_ClmLnDsalwTypCat.TRGT_CD").alias("CLM_LN_DSALW_TYP_CAT_CD"),
    F.col("lnk_PreExtr.CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT")
)

df_Transformer_temp = df_lkp_Codes_output.withColumn(
    "AmtCheckInd",
    F.when(F.col("DSALW_AMT") == F.col("CLM_LN_DSALW_AMT"), F.lit(2)).otherwise(F.lit(1))
)

df_Transformer_output = df_Transformer_temp.select(
    "CLM_LN_DSALW_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD",
    "CLM_LN_DSALW_TYP_CD_SK",
    "CLM_LN_SK",
    "CLM_LN_DSALW_EXCD_SK",
    "DSALW_AMT",
    "CLM_LN_DSALW_TYP_CAT_CD_SK",
    "CLM_LN_DSALW_TYP_CD",
    "CLM_LN_DSALW_TYP_CAT_CD",
    "CLM_LN_DSALW_AMT",
    "AmtCheckInd"
)

df_Sort = df_Transformer_output.orderBy(
    ["CLM_ID","CLM_LN_SEQ_NO","AmtCheckInd","CLM_LN_DSALW_TYP_CD","SRC_SYS_CD"],
    ascending=[True, True, True, True, True]
)

df_RemoveDuplicates = dedup_sort(
    df_Sort,
    partition_cols=["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","SRC_SYS_CD"],
    sort_cols=[
        ("CLM_ID","A"),
        ("CLM_LN_SEQ_NO","A"),
        ("AmtCheckInd","A"),
        ("CLM_LN_DSALW_TYP_CD","A"),
        ("SRC_SYS_CD","A")
    ]
)

df_RemoveDuplicates_output = df_RemoveDuplicates.select(
    "CLM_LN_DSALW_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD",
    "CLM_LN_DSALW_TYP_CD_SK",
    "CLM_LN_SK",
    "CLM_LN_DSALW_EXCD_SK",
    "DSALW_AMT",
    "CLM_LN_DSALW_TYP_CAT_CD_SK",
    "CLM_LN_DSALW_TYP_CD",
    "CLM_LN_DSALW_TYP_CAT_CD",
    "CLM_LN_DSALW_AMT",
    "AmtCheckInd"
)

df_inrownum = df_RemoveDuplicates_output.withColumn(
    "global_row_id",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

df_lnk_ClmLnDsalwOut_filter = df_RemoveDuplicates_output.filter(
    (F.col("CLM_LN_DSALW_SK") != 0) & (F.col("CLM_LN_DSALW_SK") != 1)
)

df_lnk_ClmLnDsalwOut = df_lnk_ClmLnDsalwOut_filter.select(
    F.col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(F.col("CLM_LN_DSALW_TYP_CD").isNull(), F.lit("NA"))
     .otherwise(trim(F.col("CLM_LN_DSALW_TYP_CD"))).alias("CLM_LN_DSALW_TYP_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_DSALW_EXCD_SK").alias("EXCD_SK"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.when(F.col("CLM_LN_DSALW_TYP_CAT_CD").isNull(), F.lit("NA"))
     .otherwise(trim(F.col("CLM_LN_DSALW_TYP_CAT_CD"))).alias("CLM_LN_DSALW_TYP_CAT_CD"),
    F.col("CLM_LN_DSALW_TYP_CAT_CD_SK").alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
)

df_lnk_NA = df_inrownum.filter(F.col("global_row_id") == 1).select(
    F.lit(1).alias("CLM_LN_DSALW_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit("NA").alias("CLM_LN_DSALW_TYP_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("EXCD_SK"),
    F.lit(0).alias("DSALW_AMT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.lit("NA").alias("CLM_LN_DSALW_TYP_CAT_CD"),
    F.lit(1).alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
)

df_lnk_UNK = df_inrownum.filter(F.col("global_row_id") == 1).select(
    F.lit(0).alias("CLM_LN_DSALW_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit("UNK").alias("CLM_LN_DSALW_TYP_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("EXCD_SK"),
    F.lit(0).alias("DSALW_AMT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.lit("UNK").alias("CLM_LN_DSALW_TYP_CAT_CD"),
    F.lit(0).alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
)

df_fnl_ClmLnDsalwF = df_lnk_ClmLnDsalwOut.unionByName(df_lnk_NA).unionByName(df_lnk_UNK)
df_fnl_ClmLnDsalwF_2 = df_fnl_ClmLnDsalwF.select(
    "CLM_LN_DSALW_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EXCD_SK",
    "DSALW_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK",
    "CLM_LN_DSALW_TYP_CD_SK",
    "CLM_LN_DSALW_TYP_CAT_CD",
    "CLM_LN_DSALW_TYP_CAT_CD_SK"
)

write_files(
    df_fnl_ClmLnDsalwF_2.withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    ).withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    ),
    f"{adls_path}/load/CLM_LN_DSALW_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)