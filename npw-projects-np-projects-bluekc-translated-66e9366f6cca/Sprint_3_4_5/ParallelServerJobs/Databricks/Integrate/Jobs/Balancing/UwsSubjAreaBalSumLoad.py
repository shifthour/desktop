# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 14:13:49 Batch  14542_51233 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 14:09:22 Batch  14542_50966 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/17/07 10:12:55 Batch  14535_36786 PROMOTE bckcett testIDS30 u03651 steffy for ollie
# MAGIC ^1_1 10/17/07 10:10:27 Batch  14535_36634 INIT bckcett devlIDS30 u03651 steffy to test for Ollie
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   UwsSubjAreaBalSumLoad
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  10/10/2007          3264 - Balancing        Originally Programmed                             devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query_tblbalextr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)
df_tblbalextr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_tblbalextr)
    .load()
)

extract_query_rowcount = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
    f"          TBL_BAL_SUM.SUBJ_AREA_NM"
)
df_rowcount = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_rowcount)
    .load()
)

df_rowcount = df_rowcount.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"])

df_transform = df_tblbalextr.alias("TblBalExtr").join(
    df_rowcount.alias("RowCountLookup"),
    (
        (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("RowCountLookup.TRGT_SYS_CD")) &
        (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("RowCountLookup.SUBJ_AREA_NM"))
    ),
    "left"
)

w = Window.orderBy(F.col("TblBalExtr.BAL_DT").asc())

df_transform = df_transform.withColumn("svCurrTlrncCd", F.col("TblBalExtr.ROW_CT_TLRNC_CD"))
df_transform = df_transform.withColumn("svCurrCount", F.row_number().over(w))
df_transform = df_transform.withColumn("svPrevCount", F.col("svCurrCount") - F.lit(1))

df_transform = df_transform.withColumn(
    "svTempTlrncCd",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) != F.lit(CurrDate),
        F.lit("NEXT ROW")
    ).otherwise(
        F.when(
            (F.col("svCurrTlrncCd") == F.lit("BAL")) &
            (F.lit(True)) &
            (F.lit(True)),
            F.lit("BAL")
        )
        .when(
            (F.col("svCurrTlrncCd") == F.lit("BAL")) &
            (F.lit(False)),
            F.lit("IN")
        )
        .when(
            (F.col("svCurrTlrncCd") == F.lit("IN")) &
            (F.lit(True)),
            F.lit("IN")
        )
        .otherwise("OUT")
    )
)

df_transform = df_transform.withColumn(
    "svTempRslvIn",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == F.lit(CurrDate),
        F.col("svTempRslvIn")
    ).otherwise(
        F.when(
            (F.col("svCurrTlrncCd") == F.lit("BAL")) &
            (F.lit(True)),
            F.lit("Y")
        )
        .when(
            (F.col("svCurrTlrncCd") == F.lit("IN")) &
            (F.lit(True)),
            F.lit("Y")
        )
        .otherwise("N")
    )
)

df_transform = df_transform.withColumn(
    "svTempRslvIncd",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == F.lit(CurrDate),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("svCurrTlrncCd") == F.lit("BAL")) &
            (F.lit(True)) &
            (F.lit(True)),
            F.lit("BAL")
        )
        .when(
            (F.col("svCurrTlrncCd") == F.lit("BAL")) &
            (F.lit(False)),
            F.lit("IN")
        )
        .when(
            (F.col("svCurrTlrncCd") == F.lit("IN")) &
            (F.lit(True)),
            F.lit("IN")
        )
        .otherwise("OUT")
    )
)

df_final = df_transform.filter(F.col("svPrevCount") == F.col("RowCountLookup.ROW_COUNT"))

df_final = df_final.select(
    F.lit(Target).alias("TRGT_SYS_CD"),
    F.lit(Subject).alias("SUBJ_AREA_NM"),
    F.lit(CurrDate).alias("LAST_BAL_DT"),
    F.col("svTempTlrncCd").alias("TLRNC_CD"),
    F.col("svTempRslvIn").alias("PREV_ISSUE_CRCTD_IN"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrDate).alias("CRT_DTM"),
    F.lit(CurrDate).alias("LAST_UPDT_DTM"),
    F.lit("$UWSAcct").alias("USER_ID")
)

df_final = df_final.withColumn(
    "PREV_ISSUE_CRCTD_IN",
    F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " ")
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.UwsSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_final.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "STAGING.UwsSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp").mode("overwrite").save()

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING STAGING.UwsSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S
ON T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_BAL_DT = S.LAST_BAL_DT,
    T.TLRNC_CD = S.TLRNC_CD,
    T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
WHEN NOT MATCHED THEN
  INSERT (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
  VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)