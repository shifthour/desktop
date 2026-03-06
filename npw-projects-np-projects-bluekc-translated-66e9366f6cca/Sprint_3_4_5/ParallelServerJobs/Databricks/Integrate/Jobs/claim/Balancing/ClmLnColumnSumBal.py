# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/29/07 14:28:38 Batch  14547_52124 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/29/07 14:19:25 Batch  14547_51567 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 12:41:15 Batch  14544_45689 PROMOTE bckcett testIDS30 u10913 Ollie move column sum bal changes
# MAGIC ^1_1 10/26/07 12:39:27 Batch  14544_45574 INIT bckcett devlIDS30 u10913 Ollie
# MAGIC ^1_2 09/30/07 15:29:43 Batch  14518_55788 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 09/29/07 17:49:00 Batch  14517_64144 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/25/07 14:09:41 Batch  14513_50983 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                   07/01/2007          3264                              Originally Programmed                           devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, when, abs as abs_, rpad
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","14094457")
ToleranceCd = get_widget_value("ToleranceCd","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","09122127")
SrcTableName = get_widget_value("SrcTableName","B_CLM")
SrcColumnName = get_widget_value("SrcColumnName","CHRG_AMT")
TrgtTableName = get_widget_value("TrgtTableName","CLM")
TrgtColumnName = get_widget_value("TrgtColumnName","CHRG_AMT")
SrcDSN = get_widget_value("SrcDSN","ids_devl")
SrcOwner = get_widget_value("SrcOwner","db2devl")
SrcAcct = get_widget_value("SrcAcct","u10913")
SrcPW = get_widget_value("SrcPW","L4;@1KVAL9:M0GGI5KN<1OAGF<?")
TrgtDSN = get_widget_value("TrgtDSN","ids_devl")
TrgtOwner = get_widget_value("TrgtOwner","db2devl")
TrgtAcct = get_widget_value("TrgtAcct","u10913")
TrgtPW = get_widget_value("TrgtPW","L4;@1KVAL9:M0GGI5KN<1OAGF<?")
UWSOwner = get_widget_value("$UWSOwner","uws_devl.dbo")
CurrentDate = get_widget_value("CurrentDate","")
Source = get_widget_value("Source","FACETS")

uws_secret_name = get_widget_value("uws_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")

# Read the hashed file SRC_SYS_CD (Scenario C: translate to parquet)
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet").select(
    col("CD_MPPNG_SK").cast(IntegerType()).alias("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("SRC_CD"),
    col("SRC_CD_NM")
)

# Build Source dataframe (CODBCStage)
jdbc_url_source, jdbc_props_source = get_db_config(ids_secret_name)
source_query = f"""
SELECT
  SUM(SRC.{SrcColumnName}) AS VALUE,
  SRC.SRC_SYS_CD_SK
FROM {SrcOwner}.{SrcTableName} SRC,
     {SrcOwner}.B_CLM DRVR
WHERE SRC.CLM_ID = DRVR.CLM_ID
  AND SRC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND DRVR.CLM_STTUS_CD_SK IN (
      SELECT CD_MPPNG_SK
      FROM {SrcOwner}.CD_MPPNG
      WHERE SRC_DOMAIN_NM = 'CLAIM STATUS'
        AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09'))
  )
  AND DRVR.CLM_CAT_CD_SK IN (
      SELECT CD_MPPNG_SK
      FROM {SrcOwner}.CD_MPPNG
      WHERE TRGT_CD='STD'
        AND SRC_CD IN ('01','02','91')
  )
GROUP BY SRC.SRC_SYS_CD_SK
"""
df_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_source)
    .options(**jdbc_props_source)
    .option("query", source_query)
    .load()
)

# Build Target dataframe (CODBCStage)
jdbc_url_target, jdbc_props_target = get_db_config(ids_secret_name)
target_query = f"""
SELECT
  SUM(SRC.{TrgtColumnName}) AS VALUE
FROM {TrgtOwner}.{TrgtTableName} SRC,
     {TrgtOwner}.CLM DRVR
WHERE DRVR.SRC_SYS_CD_SK IN (
    SELECT CD_MPPNG_SK
    FROM {TrgtOwner}.CD_MPPNG
    WHERE SRC_CD='{Source}'
      AND SRC_DOMAIN_NM='SOURCE SYSTEM'
)
  AND DRVR.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK
  AND DRVR.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND SRC.CLM_ID = DRVR.CLM_ID
  AND DRVR.CLM_STTUS_CD_SK IN (
      SELECT CD_MPPNG_SK
      FROM {TrgtOwner}.CD_MPPNG
      WHERE SRC_DOMAIN_NM='CLAIM STATUS'
        AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09'))
  )
  AND DRVR.CLM_CAT_CD_SK IN (
      SELECT CD_MPPNG_SK
      FROM {TrgtOwner}.CD_MPPNG
      WHERE TRGT_CD='STD'
        AND SRC_CD IN ('01','02','91')
)
"""
df_Target = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_target)
    .options(**jdbc_props_target)
    .option("query", target_query)
    .load()
)

# ValueCompare stage with @INROWNUM = 1
df_valuecompare_raw = df_Source.alias("Src").join(
    df_Target.alias("Trgt"),
    col("Src.VALUE") == col("Trgt.VALUE"),
    how="left"
)
wvc = Window.orderBy(lit(1))
df_valuecompare_filtered = df_valuecompare_raw.withColumn(
    "row_num",
    row_number().over(wvc)
).filter(col("row_num") == 1).drop("row_num")

df_col_sum = df_valuecompare_filtered.select(
    lit(SrcTableName).alias("SRC_TABLE_NAME"),
    lit(SrcColumnName).alias("SRC_COLUMN_NAME"),
    lit(TrgtTableName).alias("TRGT_TABLE_NAME"),
    lit(TrgtColumnName).alias("TRGT_COLUMN_NAME"),
    lit(ExtrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Src.VALUE").alias("SRC_VALUE"),
    col("Trgt.VALUE").alias("TRGT_VALUE"),
    when(col("Trgt.VALUE").isNull(), lit(0)).otherwise(col("Src.VALUE") - col("Trgt.VALUE")).alias("DIFF"),
    col("Src.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# TransformLogic stage (join with SRC_SYS_CD, stage variables)
df_transformjoin = df_col_sum.alias("COL_SUM").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    col("COL_SUM.SRC_SYS_CD_SK") == col("SRC_SYS_CD.CD_MPPNG_SK"),
    how="left"
)

df_transformlogic = df_transformjoin.select(
    col("COL_SUM.SRC_TABLE_NAME"),
    col("COL_SUM.SRC_COLUMN_NAME"),
    col("COL_SUM.TRGT_TABLE_NAME"),
    col("COL_SUM.TRGT_COLUMN_NAME"),
    col("COL_SUM.CRT_RUN_CYC_EXCTN_SK"),
    col("COL_SUM.SRC_VALUE"),
    col("COL_SUM.TRGT_VALUE"),
    col("COL_SUM.DIFF"),
    col("COL_SUM.SRC_SYS_CD_SK"),
    col("SRC_SYS_CD.TRGT_CD").alias("LOOKUP_SRC_SYS_CD")
)

df_col_sum_dtl = df_transformlogic.select(
    lit("IDS").alias("TRGT_SYS_CD"),
    col("LOOKUP_SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit("Claims").alias("SUBJ_AREA_NM"),
    col("TRGT_TABLE_NAME").alias("TRGT_TBL_NM"),
    col("TRGT_COLUMN_NAME").alias("TRGT_CLMN_NM"),
    col("TRGT_VALUE").alias("TRGT_AMT"),
    col("SRC_VALUE").alias("SRC_AMT"),
    col("DIFF").alias("DIFF_AMT"),
    when(col("DIFF") == 0, lit("BAL"))
    .when(abs_(col("DIFF")) > lit(ToleranceCd), lit("OUT"))
    .otherwise(lit("IN")).alias("TLRNC_CD"),
    lit(ToleranceCd).alias("TLRNC_AMT"),
    lit(ToleranceCd).alias("TLRNC_MULT_AMT"),
    lit("Daily").alias("FREQ_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("TRGT_RUN_CYC_NO"),
    lit(" ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    lit("$UWSAcct").alias("USER_ID")
)

# Ensure final columns (applying rpad for varchar/char placeholders)
df_final = df_col_sum_dtl.select(
    rpad(col("TRGT_SYS_CD"), <...>, " ").alias("TRGT_SYS_CD"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("SUBJ_AREA_NM"), <...>, " ").alias("SUBJ_AREA_NM"),
    rpad(col("TRGT_TBL_NM"), <...>, " ").alias("TRGT_TBL_NM"),
    rpad(col("TRGT_CLMN_NM"), <...>, " ").alias("TRGT_CLMN_NM"),
    col("TRGT_AMT").alias("TRGT_AMT"),
    col("SRC_AMT").alias("SRC_AMT"),
    col("DIFF_AMT").alias("DIFF_AMT"),
    rpad(col("TLRNC_CD"), <...>, " ").alias("TLRNC_CD"),
    col("TLRNC_AMT").alias("TLRNC_AMT"),
    col("TLRNC_MULT_AMT").alias("TLRNC_MULT_AMT"),
    rpad(col("FREQ_CD"), <...>, " ").alias("FREQ_CD"),
    col("TRGT_RUN_CYC_NO").alias("TRGT_RUN_CYC_NO"),
    rpad(col("CRCTN_NOTE"), <...>, " ").alias("CRCTN_NOTE"),
    col("CRT_DTM").alias("CRT_DTM"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("USER_ID"), <...>, " ").alias("USER_ID")
)

# Write final to database (INSERT-style => replicate as MERGE with always-insert)
execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmLnColumnSumBal_COL_SUM_DTL_temp", jdbc_url_target, jdbc_props_target)

df_final.write \
    .format("jdbc") \
    .option("url", jdbc_url_target) \
    .options(**jdbc_props_target) \
    .option("dbtable", "STAGING.ClmLnColumnSumBal_COL_SUM_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {UWSOwner}.CLMN_SUM_DTL AS T
USING STAGING.ClmLnColumnSumBal_COL_SUM_DTL_temp AS S
ON (1=0)
WHEN NOT MATCHED THEN
  INSERT (
    TRGT_SYS_CD,
    SRC_SYS_CD,
    SUBJ_AREA_NM,
    TRGT_TBL_NM,
    TRGT_CLMN_NM,
    TRGT_AMT,
    SRC_AMT,
    DIFF_AMT,
    TLRNC_CD,
    TLRNC_AMT,
    TLRNC_MULT_AMT,
    FREQ_CD,
    TRGT_RUN_CYC_NO,
    CRCTN_NOTE,
    CRT_DTM,
    LAST_UPDT_DTM,
    USER_ID
  )
  VALUES (
    S.TRGT_SYS_CD,
    S.SRC_SYS_CD,
    S.SUBJ_AREA_NM,
    S.TRGT_TBL_NM,
    S.TRGT_CLMN_NM,
    S.TRGT_AMT,
    S.SRC_AMT,
    S.DIFF_AMT,
    S.TLRNC_CD,
    S.TLRNC_AMT,
    S.TLRNC_MULT_AMT,
    S.FREQ_CD,
    S.TRGT_RUN_CYC_NO,
    S.CRCTN_NOTE,
    S.CRT_DTM,
    S.LAST_UPDT_DTM,
    S.USER_ID
  );
"""
execute_dml(merge_sql, jdbc_url_target, jdbc_props_target)