# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FdbNdcBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/18/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard             09/27/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, when, substring, rpad
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


uws_secret_name = get_widget_value('uws_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage)
# Output Pin 1 => TblBalExtr
# --------------------------------------------------------------------------------
query_TblBalExtr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)
df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_TblBalExtr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage)
# Output Pin 2 => NdcCt
# --------------------------------------------------------------------------------
query_NdcCt = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
    f"         TBL_BAL_SUM.SUBJ_AREA_NM"
)
df_NdcCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_NdcCt)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_ndc_tbl_bal_lkup (CHashedFileStage)
# Scenario A: Intermediate hashed file => deduplicate on key columns
# --------------------------------------------------------------------------------
df_NdcCtLoad = df_NdcCt.dropDuplicates(["TRGT_SYS_CD", "SUBJ_AREA_NM"])

# --------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage)
# --------------------------------------------------------------------------------
# Left join df_TblBalExtr (primary link) with df_NdcCtLoad (lookup link) on TRGT_SYS_CD, SUBJ_AREA_NM
df_joined = (
    df_TblBalExtr.alias("TblBalExtr")
    .join(
        df_NdcCtLoad.alias("NdcCtLoad"),
        (
            (col("TblBalExtr.TRGT_SYS_CD") == col("NdcCtLoad.TRGT_SYS_CD"))
            & (col("TblBalExtr.SUBJ_AREA_NM") == col("NdcCtLoad.SUBJ_AREA_NM"))
        ),
        "left"
    )
)

# Simulate row-by-row variables using a window over BAL_DT (ascending)
w = Window.partitionBy("TblBalExtr.TRGT_SYS_CD", "TblBalExtr.SUBJ_AREA_NM").orderBy("TblBalExtr.BAL_DT")

df_enriched = (
    df_joined
    .withColumn("svCurrTlrncCd", col("TblBalExtr.ROW_CT_TLRNC_CD"))
    .withColumn("row_num", row_number().over(w))
    .withColumn("svPrevCount", col("row_num") - lit(1))
    .withColumn("svCurrCount", col("row_num"))  # svCurrCount = svPrevCount + 1
    # svTempTlrncCd logic (approximated for row-based references)
    .withColumn(
        "svTempTlrncCd",
        when(
            substring(col("TblBalExtr.BAL_DT"), 1, 10) != lit(CurrDate),
            "NEXT ROW"
        ).otherwise(
            when((col("svCurrTlrncCd") == "BAL"), "BAL")
            .when((col("svCurrTlrncCd") == "IN"), "IN")
            .otherwise("OUT")
        )
    )
    # svTempRslvIn logic (approximated for row-based references)
    .withColumn(
        "svTempRslvIn",
        when(
            substring(col("TblBalExtr.BAL_DT"), 1, 10) == lit(CurrDate),
            lit(None)
        )
        .otherwise(
            when((col("svCurrTlrncCd") == "BAL"), "Y")
            .when((col("svCurrTlrncCd") == "IN"), "Y")
            .otherwise("N")
        )
    )
    # svTempRslvIncd logic (approximated for row-based references)
    .withColumn(
        "svTempRslvIncd",
        when(
            substring(col("TblBalExtr.BAL_DT"), 1, 10) == lit(CurrDate),
            "NA"
        )
        .otherwise(
            when((col("svCurrTlrncCd") == "BAL"), "BAL")
            .when((col("svCurrTlrncCd") == "IN"), "IN")
            .otherwise("OUT")
        )
    )
)

# Apply the constraint: svPrevCount = NdcCtLoad.ROW_COUNT
df_filtered = df_enriched.filter(col("svPrevCount") == col("NdcCtLoad.ROW_COUNT"))

# Output columns for SubjAreaBalLoad
df_SubjAreaBalLoad = df_filtered.select(
    lit(Target).alias("TRGT_SYS_CD"),
    lit(Subject).alias("SUBJ_AREA_NM"),
    lit(CurrDate).alias("LAST_BAL_DT"),
    col("svTempTlrncCd").alias("TLRNC_CD"),
    col("svTempRslvIn").alias("PREV_ISSUE_CRCTD_IN"),
    lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    lit(CurrDate).alias("CRT_DTM"),
    lit(CurrDate).alias("LAST_UPDT_DTM"),
    lit(UWSAcct).alias("USER_ID")
)

# Use rpad for char/varchar columns (lengths are unknown, using illustrative lengths)
df_SubjAreaBalLoad_padded = (
    df_SubjAreaBalLoad
    .withColumn("TRGT_SYS_CD", rpad(col("TRGT_SYS_CD"), 50, " "))
    .withColumn("SUBJ_AREA_NM", rpad(col("SUBJ_AREA_NM"), 50, " "))
    .withColumn("PREV_ISSUE_CRCTD_IN", rpad(col("PREV_ISSUE_CRCTD_IN"), 1, " "))
)

# --------------------------------------------------------------------------------
# Stage: SUBJ_AREA_BAL_SUM (CODBCStage) - Merge (Upsert) into #$UWSOwner#.SUBJ_AREA_BAL_SUM
# --------------------------------------------------------------------------------
temp_table = "STAGING.UwsNdcSubjAreaBalSum_SUBJ_AREA_BAL_SUM_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
    df_SubjAreaBalLoad_padded
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T1
USING {temp_table} AS T2
ON T1.TRGT_SYS_CD = T2.TRGT_SYS_CD
   AND T1.SUBJ_AREA_NM = T2.SUBJ_AREA_NM
WHEN MATCHED THEN
  UPDATE SET
    T1.LAST_BAL_DT = T2.LAST_BAL_DT,
    T1.TLRNC_CD = T2.TLRNC_CD,
    T1.PREV_ISSUE_CRCTD_IN = T2.PREV_ISSUE_CRCTD_IN,
    T1.TRGT_RUN_CYC_NO = T2.TRGT_RUN_CYC_NO,
    T1.CRT_DTM = T2.CRT_DTM,
    T1.LAST_UPDT_DTM = T2.LAST_UPDT_DTM,
    T1.USER_ID = T2.USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    TRGT_SYS_CD,
    SUBJ_AREA_NM,
    LAST_BAL_DT,
    TLRNC_CD,
    PREV_ISSUE_CRCTD_IN,
    TRGT_RUN_CYC_NO,
    CRT_DTM,
    LAST_UPDT_DTM,
    USER_ID
  )
  VALUES (
    T2.TRGT_SYS_CD,
    T2.SUBJ_AREA_NM,
    T2.LAST_BAL_DT,
    T2.TLRNC_CD,
    T2.PREV_ISSUE_CRCTD_IN,
    T2.TRGT_RUN_CYC_NO,
    T2.CRT_DTM,
    T2.LAST_UPDT_DTM,
    T2.USER_ID
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)