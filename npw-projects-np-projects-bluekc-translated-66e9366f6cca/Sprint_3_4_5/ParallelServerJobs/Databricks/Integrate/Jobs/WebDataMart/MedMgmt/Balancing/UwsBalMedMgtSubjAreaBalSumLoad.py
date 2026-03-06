# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/11/07 15:00:54 Batch  14529_54061 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/11/07 14:39:56 Batch  14529_52800 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/10/07 07:56:07 Batch  14528_28571 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/10/07 07:39:54 Batch  14528_27598 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsMedMgtBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/16/2007          3264                              Originally Programmed                           devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
# The job includes an expression "$UWSAcct" for USER_ID; define it as well:
UWSAcct = get_widget_value('UWSAcct','')

# ----------------------------------------------------------------
# TBL_BAL_SUM (CODBCStage) - This stage produces two outputs:
#   1) "TblBalExtr" (for detailed rows)
#   2) "DSLink18" (for aggregated row counts)
# Both come from the same database but run different SQL queries.
# ----------------------------------------------------------------

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          TBL_BAL_SUM.TRGT_SYS_CD,
          TBL_BAL_SUM.SUBJ_AREA_NM,
          TBL_BAL_SUM.BAL_DT,
          TBL_BAL_SUM.CLMN_SUM_TLRNC_CD,
          TBL_BAL_SUM.ROW_TO_ROW_TLRNC_CD
        FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
        WHERE
          TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
          AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
        ORDER BY TBL_BAL_SUM.BAL_DT ASC
        """
    )
    .load()
)

df_TBL_BAL_SUM_DSLink18 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          TBL_BAL_SUM.TRGT_SYS_CD,
          TBL_BAL_SUM.SUBJ_AREA_NM,
          COUNT(*) AS ROW_COUNT
        FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
        WHERE
          TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
          AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
        GROUP BY
          TBL_BAL_SUM.TRGT_SYS_CD,
          TBL_BAL_SUM.SUBJ_AREA_NM
        """
    )
    .load()
)

# ----------------------------------------------------------------
# Hashed_File_17 (CHashedFileStage) - Scenario A:
#   AnyStage → CHashedFileStage → AnyStage 
#   We remove the hashed file and wire the aggregator output directly
#   into the lookup. The aggregator (df_TBL_BAL_SUM_DSLink18) is
#   already guaranteed unique by grouping, so no extra dedup needed.
# ----------------------------------------------------------------
# (No separate DataFrame creation for the hashed file; it is omitted per Scenario A.)

# ----------------------------------------------------------------
# Transform (CTransformerStage)
#   Primary Link:  "TblBalExtr"        => df_TBL_BAL_SUM_TblBalExtr
#   Lookup Link:   "DSLink20" (left)   => df_TBL_BAL_SUM_DSLink18
#   Constraint:    svPrevCount = DSLink20.ROW_COUNT
#   The stage variables refer to row-by-row logic in DataStage, but here we replicate
#   the final effect in Spark by:
#     - Joining on TRGT_SYS_CD,SUBJ_AREA_NM (left join).
#     - For each group, we pick the last row (where row_number == ROW_COUNT).
#     - Then build the output columns accordingly.
# ----------------------------------------------------------------

df_join = df_TBL_BAL_SUM_TblBalExtr.alias("T").join(
    df_TBL_BAL_SUM_DSLink18.alias("H"),
    [
        F.col("T.TRGT_SYS_CD") == F.col("H.TRGT_SYS_CD"),
        F.col("T.SUBJ_AREA_NM") == F.col("H.SUBJ_AREA_NM")
    ],
    "left"
)

windowSpec = Window.partitionBy("T.TRGT_SYS_CD", "T.SUBJ_AREA_NM").orderBy("T.BAL_DT")
df_with_rownum = df_join.withColumn("row_number", F.row_number().over(windowSpec))

# The DataStage link constraint "svPrevCount = DSLink20.ROW_COUNT" implies we want
# the last row in each group (where row_number == H.ROW_COUNT).
df_filtered = df_with_rownum.filter(F.col("row_number") == F.col("H.ROW_COUNT"))

# Now replicate the Transform output columns. The job's expressions include
# references to stage variables. In DataStage it is row-by-row. Here we model
# the final TLRNC_CD and PREV_ISSUE_CRCTD_IN with single-row logic.
expr_tlrnc_cd = (
    F.when(F.substring("T.BAL_DT", 1, 10) != CurrDate, "NEXT ROW")
    .when(
       (F.col("T.CLMN_SUM_TLRNC_CD") == "BAL") & (F.col("T.ROW_TO_ROW_TLRNC_CD") == "BAL"),
       "BAL"
    )
    .when(
       (F.col("T.CLMN_SUM_TLRNC_CD").isin("BAL", "IN"))
       | (F.col("T.ROW_TO_ROW_TLRNC_CD").isin("BAL", "IN")),
       "IN"
    )
    .otherwise("OUT")
)

expr_prev_issue_crctd_in = (
    F.when(F.substring("T.BAL_DT", 1, 10) == CurrDate, "Y")
    .when(
       (F.col("T.CLMN_SUM_TLRNC_CD").isin("BAL", "IN"))
       | (F.col("T.ROW_TO_ROW_TLRNC_CD").isin("BAL", "IN")),
       "Y"
    )
    .otherwise("N")
)

df_transform_output = df_filtered.select(
    F.lit(Target).alias("TRGT_SYS_CD"),
    F.lit(Subject).alias("SUBJ_AREA_NM"),
    F.lit(CurrDate).alias("LAST_BAL_DT"),
    expr_tlrnc_cd.alias("TLRNC_CD"),
    expr_prev_issue_crctd_in.alias("PREV_ISSUE_CRCTD_IN"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrDate).alias("CRT_DTM"),
    F.lit(CurrDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

# ----------------------------------------------------------------
# SUBJ_AREA_BAL_SUM (CODBCStage) - Database output with upsert logic.
#   Primary Key: TRGT_SYS_CD, SUBJ_AREA_NM
#   Insert/Update columns: LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN,
#                          TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID
# We implement MERGE with a temp table in STAGING schema.
# ----------------------------------------------------------------

temp_table = "STAGING.UwsBalMedMgtSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"

# 1) Drop the temp table if it exists
drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

# 2) Write df_transform_output to the temp table
df_transform_output.write.jdbc(
    url=jdbc_url,
    table=temp_table,
    mode="overwrite",
    properties=jdbc_props
)

# 3) Construct the MERGE statement
merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING {temp_table} AS S
ON
    T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM
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
      S.TRGT_SYS_CD,
      S.SUBJ_AREA_NM,
      S.LAST_BAL_DT,
      S.TLRNC_CD,
      S.PREV_ISSUE_CRCTD_IN,
      S.TRGT_RUN_CYC_NO,
      S.CRT_DTM,
      S.LAST_UPDT_DTM,
      S.USER_ID
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)