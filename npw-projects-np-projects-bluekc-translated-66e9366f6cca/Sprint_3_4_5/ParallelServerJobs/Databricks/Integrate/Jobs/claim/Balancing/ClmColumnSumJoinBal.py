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
# MAGIC ^1_1 09/25/07 12:58:00 Batch  14513_46683 INIT bckcett devlIDS30 u03651 steffy
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','14094457')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','09122127')
SrcTableName = get_widget_value('SrcTableName','B_CLM')
SrcColumnName = get_widget_value('SrcColumnName','CHRG_AMT')
TrgtTableName = get_widget_value('TrgtTableName','CLM')
TrgtColumnName = get_widget_value('TrgtColumnName','CHRG_AMT')
SrcDSN = get_widget_value('SrcDSN','ids_devl')
SrcOwner = get_widget_value('SrcOwner','db2devl')
SrcAcct = get_widget_value('SrcAcct','u10913')
SrcPW = get_widget_value('SrcPW','L4;@1KVAL9:M0GGI5KN<1OAGF<?')
TrgtDSN = get_widget_value('TrgtDSN','ids_devl')
TrgtOwner = get_widget_value('TrgtOwner','db2devl')
TrgtAcct = get_widget_value('TrgtAcct','u10913')
TrgtPW = get_widget_value('TrgtPW','L4;@1KVAL9:M0GGI5KN<1OAGF<?')
UWSOwner = get_widget_value('$UWSOwner','uws_devl.dbo')
CurrentDate = get_widget_value('CurrentDate','')
Source = get_widget_value('Source','FACETS')

ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')
UWSAcct = get_widget_value('$UWSAcct','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
jdbc_uws_url, jdbc_uws_props = get_db_config(uws_secret_name)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

extract_query_source = f"""
SELECT
  SUM(SRC.{SrcColumnName}) AS VALUE,
  SRC.SRC_SYS_CD_SK
FROM {SrcOwner}.{SrcTableName} SRC,
     {SrcOwner}.B_CLM DRVR
WHERE SRC.CLM_ID = DRVR.CLM_ID
  AND SRC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND DRVR.CLM_STTUS_CD_SK IN
    (SELECT CD_MPPNG_SK
     FROM {SrcOwner}.CD_MPPNG
     WHERE (SRC_DOMAIN_NM = 'CLAIM STATUS')
       AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09')))
  AND DRVR.CLM_CAT_CD_SK IN
    (SELECT CD_MPPNG_SK
     FROM {SrcOwner}.CD_MPPNG
     WHERE (TRGT_CD='STD'
       AND SRC_CD IN ('01','02','91')))
GROUP BY SRC.SRC_SYS_CD_SK
"""

df_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_source)
    .load()
)

extract_query_target = f"""
SELECT
  SUM(TRGT.{TrgtColumnName}) AS VALUE
FROM {TrgtOwner}.{TrgtTableName} TRGT,
     {TrgtOwner}.CLM DRVR
WHERE TRGT.SRC_SYS_CD_SK IN
    (SELECT CD_MPPNG_SK
     FROM {TrgtOwner}.CD_MPPNG
     WHERE SRC_CD='{Source}'
       AND SRC_DOMAIN_NM='SOURCE SYSTEM')
  AND TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND TRGT.CLM_ID = DRVR.CLM_ID
  AND TRGT.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND DRVR.CLM_STTUS_CD_SK IN
    (SELECT CD_MPPNG_SK
     FROM {TrgtOwner}.CD_MPPNG
     WHERE (SRC_DOMAIN_NM = 'CLAIM STATUS')
       AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09')))
  AND DRVR.CLM_CAT_CD_SK IN
    (SELECT CD_MPPNG_SK
     FROM {TrgtOwner}.CD_MPPNG
     WHERE (TRGT_CD='STD'
       AND SRC_CD IN ('01','02','91')))
"""

df_Target = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_target)
    .load()
)

df_COL_SUM_pre = (
    df_Source.alias("S")
    .join(df_Target.alias("T"), F.col("S.VALUE") == F.col("T.VALUE"), how="left")
    .withColumn("SRC_TABLE_NAME", F.lit(SrcTableName))
    .withColumn("SRC_COLUMN_NAME", F.lit(SrcColumnName))
    .withColumn("TRGT_TABLE_NAME", F.lit(TrgtTableName))
    .withColumn("TRGT_COLUMN_NAME", F.lit(TrgtColumnName))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(ExtrRunCycle))
    .withColumn("SRC_VALUE", F.col("S.VALUE"))
    .withColumn("TRGT_VALUE", F.col("T.VALUE"))
    .withColumn("DIFF", F.col("S.VALUE") - F.col("T.VALUE"))
    .withColumn("SRC_SYS_CD_SK", F.col("S.SRC_SYS_CD_SK"))
)

df_COL_SUM = df_COL_SUM_pre.limit(1)

df_TransformLogic_join = (
    df_COL_SUM.alias("COL_SUM")
    .join(df_SRC_SYS_CD.alias("SRC_SYS_CD"),
          F.col("COL_SUM.SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
          how="left")
)

df_TransformLogic = (
    df_TransformLogic_join
    .withColumn("svTlrnc", F.lit(ToleranceCd))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("COL_SUM.DIFF") == 0, F.lit("BAL"))
         .otherwise(
             F.when(F.abs(F.col("COL_SUM.DIFF")) > F.col("svTlrnc"), F.lit("OUT"))
              .otherwise(F.lit("IN"))
         )
    )
    .withColumn("TRGT_SYS_CD", F.lit("IDS"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD.TRGT_CD"))
    .withColumn("SUBJ_AREA_NM", F.lit("Claims"))
    .withColumn("TRGT_TBL_NM", F.col("COL_SUM.TRGT_TABLE_NAME"))
    .withColumn("TRGT_CLMN_NM", F.col("COL_SUM.TRGT_COLUMN_NAME"))
    .withColumn("TRGT_AMT", F.col("COL_SUM.TRGT_VALUE"))
    .withColumn("SRC_AMT", F.col("COL_SUM.SRC_VALUE"))
    .withColumn("DIFF_AMT", F.col("COL_SUM.DIFF"))
    .withColumn("TLRNC_CD", F.col("svTlrncCd"))
    .withColumn("TLRNC_AMT", F.col("svTlrnc"))
    .withColumn("TLRNC_MULT_AMT", F.col("svTlrnc"))
    .withColumn("FREQ_CD", F.lit("Daily"))
    .withColumn("TRGT_RUN_CYC_NO", F.lit(ExtrRunCycle))
    .withColumn("CRCTN_NOTE", F.lit(" "))
    .withColumn("CRT_DTM", current_timestamp())
    .withColumn("LAST_UPDT_DTM", current_timestamp())
    .withColumn("USER_ID", F.lit(UWSAcct))
)

# Final dataframe for DB insert
df_COL_SUM_DTL_pre = df_TransformLogic.select(
    "TRGT_SYS_CD",
    "SRC_SYS_CD",
    "SUBJ_AREA_NM",
    "TRGT_TBL_NM",
    "TRGT_CLMN_NM",
    "TRGT_AMT",
    "SRC_AMT",
    "DIFF_AMT",
    "TLRNC_CD",
    "TLRNC_AMT",
    "TLRNC_MULT_AMT",
    "FREQ_CD",
    "TRGT_RUN_CYC_NO",
    "CRCTN_NOTE",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
)

df_COL_SUM_DTL = (
    df_COL_SUM_DTL_pre
    .withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), <...>, " "))
    .withColumn("TRGT_TBL_NM", F.rpad(F.col("TRGT_TBL_NM"), <...>, " "))
    .withColumn("TRGT_CLMN_NM", F.rpad(F.col("TRGT_CLMN_NM"), <...>, " "))
    .withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), <...>, " "))
    .withColumn("FREQ_CD", F.rpad(F.col("FREQ_CD"), <...>, " "))
    .withColumn("TRGT_RUN_CYC_NO", F.rpad(F.col("TRGT_RUN_CYC_NO"), <...>, " "))
    .withColumn("CRCTN_NOTE", F.rpad(F.col("CRCTN_NOTE"), <...>, " "))
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), <...>, " "))
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmColumnSumJoinBal_COL_SUM_DTL_temp", jdbc_uws_url, jdbc_uws_props)

(
    df_COL_SUM_DTL
    .write
    .format("jdbc")
    .option("url", jdbc_uws_url)
    .options(**jdbc_uws_props)
    .option("dbtable", "STAGING.ClmColumnSumJoinBal_COL_SUM_DTL_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {UWSOwner}.CLMN_SUM_DTL AS T
USING STAGING.ClmColumnSumJoinBal_COL_SUM_DTL_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
  INSERT
  (
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
  VALUES
  (
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

execute_dml(merge_sql, jdbc_uws_url, jdbc_uws_props)