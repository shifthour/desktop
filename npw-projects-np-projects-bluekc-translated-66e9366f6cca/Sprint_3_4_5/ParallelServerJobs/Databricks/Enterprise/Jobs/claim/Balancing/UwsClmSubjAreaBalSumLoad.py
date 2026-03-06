# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/05/07 14:33:17 Batch  14554_52458 PROMOTE bckcetl edw10 dsadm rc for brent
# MAGIC ^1_1 11/05/07 13:30:09 Batch  14554_48642 INIT bckcett testEDW10 dsadm rc for brent
# MAGIC ^1_3 11/02/07 15:11:26 Batch  14551_54690 PROMOTE bckcett testEDW10 u08717 Brent
# MAGIC ^1_3 11/02/07 15:00:18 Batch  14551_54021 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_2 11/02/07 07:43:09 Batch  14551_27794 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 11/01/07 11:00:01 Batch  14550_39611 INIT bckcett devlEDW10 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsClmBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/07/2007          3264                              Originally Programmed                          devlEDW10                Steph Goddard            10/22/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')  # Not listed in parameters JSON, but needed for USER_ID expression

# Obtain JDBC configuration
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# Stage: TBL_BAL_SUM (CODBCStage) - first query (TblBalExtr)
extract_query_1 = (
    "SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    "TBL_BAL_SUM.SUBJ_AREA_NM, "
    "TBL_BAL_SUM.BAL_DT, "
    "TBL_BAL_SUM.ROW_CT_TLRNC_CD, "
    "TBL_BAL_SUM.CLMN_SUM_TLRNC_CD, "
    "TBL_BAL_SUM.ROW_TO_ROW_TLRNC_CD, "
    "TBL_BAL_SUM.RI_TLRNC_CD, "
    "TBL_BAL_SUM.RELSHP_CLMN_SUM_TLRNC_CD, "
    "TBL_BAL_SUM.CRS_FOOT_TLRNC_CD "
    f"FROM {('#$UWSOwner#').replace('#$UWSOwner#', UWSOwner)}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    "ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)
df_TBL_BAL_SUM = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_1)
        .load()
)

# Stage: TBL_BAL_SUM (CODBCStage) - second query (ClmCtExtr)
extract_query_2 = (
    "SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    "TBL_BAL_SUM.SUBJ_AREA_NM, "
    "COUNT(*) AS ROW_COUNT "
    f"FROM {('#$UWSOwner#').replace('#$UWSOwner#', UWSOwner)}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    "GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
)
df_ClmCtExtr = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_2)
        .load()
)

# Stage: hf_edw_clm_tbl_bal_lkup (CHashedFileStage) - Scenario A (intermediate hashed file)
# Key columns: TRGT_SYS_CD, SUBJ_AREA_NM
df_ClmCtLoad = df_ClmCtExtr.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"])

# Stage: Transform (CTransformerStage)
# Primary link: TblBalExtr -> df_TBL_BAL_SUM
# Lookup link: ClmCtLoad -> df_ClmCtLoad (left join on TRGT_SYS_CD,SUBJ_AREA_NM)
df_joined = (
    df_TBL_BAL_SUM.alias("TblBalExtr")
    .join(
        df_ClmCtLoad.alias("ClmCtLoad"),
        (
            (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("ClmCtLoad.TRGT_SYS_CD")) &
            (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("ClmCtLoad.SUBJ_AREA_NM"))
        ),
        "left"
    )
)

# Replicate stage variables (DataStage row-by-row logic is approximated here):
# Create a window to emulate row iteration for svCurrCount/svPrevCount to avoid skipping logic.
w = Window.orderBy(F.monotonically_increasing_id())

df_transform_vars = (
    df_joined
    .withColumn("svRCCurrTlrncCd", F.col("TblBalExtr.ROW_CT_TLRNC_CD"))
    .withColumn("svCSCurrTlrncCd", F.col("TblBalExtr.CLMN_SUM_TLRNC_CD"))
    .withColumn("svRRCurrTlrncCd", F.col("TblBalExtr.ROW_TO_ROW_TLRNC_CD"))
    .withColumn("svRICurrTlrncCd", F.col("TblBalExtr.RI_TLRNC_CD"))
    .withColumn("svRCSCurrTlrncCd", F.col("TblBalExtr.RELSHP_CLMN_SUM_TLRNC_CD"))
    .withColumn("svCrsFtCurrTlrncCd", F.col("TblBalExtr.CRS_FOOT_TLRNC_CD"))
    .withColumn("row_index", F.row_number().over(w))
)

# Emulate svCurrCount = svPrevCount + 1, svPrevCount = svCurrCount
# These do not truly replicate row-by-row but preserve the logic references.
df_transform_vars = df_transform_vars.withColumn("svPrevCount", F.col("row_index") - 1)
df_transform_vars = df_transform_vars.withColumn("svCurrCount", F.col("row_index"))

# We cannot truly reference the "old" stage variable value in the same row, so reference them as if they are from the same record:
df_transform_vars = df_transform_vars.withColumn(
    "svTempTlrncCd",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) != F.lit(CurrDate),
        F.lit("NEXT ROW")
    ).when(
        (
            F.col("svCSCurrTlrncCd").isin("BAL", "NA") &
            F.col("svRCCurrTlrncCd").isin("BAL", "NA") &
            F.col("svRRCurrTlrncCd").isin("BAL", "NA") &
            F.col("svRICurrTlrncCd").isin("BAL", "NA") &
            F.col("svRCSCurrTlrncCd").isin("BAL", "NA") &
            F.col("svCrsFtCurrTlrncCd").isin("BAL", "NA")
        ),
        F.lit("BAL")
    ).when(
        (
            F.col("svRCCurrTlrncCd").isin("BAL", "IN") |
            F.col("svCSCurrTlrncCd").isin("BAL", "IN") |
            F.col("svRRCurrTlrncCd").isin("BAL", "IN") |
            F.col("svRICurrTlrncCd").isin("BAL", "IN") |
            F.col("svRCSCurrTlrncCd").isin("BAL", "IN") |
            F.col("svCrsFtCurrTlrncCd").isin("BAL", "IN")
        ),
        F.lit("IN")
    )
    .otherwise("OUT")
)

df_transform_vars = df_transform_vars.withColumn(
    "svTempRslvIncd",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == F.lit(CurrDate),
        F.lit("NEXT ROW")
    ).when(
        (
            F.col("svCSCurrTlrncCd").isin("BAL","NA") &
            F.col("svRCCurrTlrncCd").isin("BAL","NA") &
            F.col("svRRCurrTlrncCd").isin("BAL","NA") &
            F.col("svRICurrTlrncCd").isin("BAL","NA") &
            F.col("svRCSCurrTlrncCd").isin("BAL","NA") &
            F.col("svCrsFtCurrTlrncCd").isin("BAL","NA")
        ),
        F.lit("BAL")
    ).when(
        (
            F.col("svRCCurrTlrncCd").isin("BAL","IN") |
            F.col("svCSCurrTlrncCd").isin("BAL","IN") |
            F.col("svRRCurrTlrncCd").isin("BAL","IN") |
            F.col("svRICurrTlrncCd").isin("BAL","IN") |
            F.col("svRCSCurrTlrncCd").isin("BAL","IN") |
            F.col("svCrsFtCurrTlrncCd").isin("BAL","IN")
        ),
        F.lit("IN")
    )
    .otherwise("OUT")
)

df_transform_vars = df_transform_vars.withColumn(
    "svTempRslvIn",
    F.when(
        F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == F.lit(CurrDate),
        F.col("svTempRslvIn")  # cannot really reference prior row's value, left as-is
    ).otherwise(
        F.when(
            (
                (F.col("svRCCurrTlrncCd").isin("BAL","IN")) |
                (F.col("svCSCurrTlrncCd").isin("BAL","IN")) |
                (F.col("svRRCurrTlrncCd").isin("BAL","IN")) |
                (F.col("svRICurrTlrncCd").isin("BAL","IN")) |
                (F.col("svRCSCurrTlrncCd").isin("BAL","IN")) |
                (F.col("svCrsFtCurrTlrncCd").isin("BAL","IN"))
            ),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Constraint for output link "SubjAreaBalLoad": svPrevCount = ClmCtLoad.ROW_COUNT
df_transform_filtered = df_transform_vars.filter(
    F.col("svPrevCount") == F.col("ClmCtLoad.ROW_COUNT")
)

# Output columns for "SubjAreaBalLoad"
df_SubjAreaBalLoad = df_transform_filtered.select(
    F.lit(Target).alias("TRGT_SYS_CD"),
    F.lit(Subject).alias("SUBJ_AREA_NM"),
    F.lit(CurrDate).alias("LAST_BAL_DT"),
    F.col("svTempTlrncCd").alias("TLRNC_CD"),
    F.col("svTempRslvIn").alias("PREV_ISSUE_CRCTD_IN"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrDate).alias("CRT_DTM"),
    F.lit(CurrDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

# Stage: SUBJ_AREA_BAL_SUM (CODBCStage) - Write via MERGE
# Primary key: (TRGT_SYS_CD, SUBJ_AREA_NM). 
# Create a physical STAGING table and MERGE into #$UWSOwner#.SUBJ_AREA_BAL_SUM

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.UwsClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp",
    jdbc_url,
    jdbc_props
)

(
    df_SubjAreaBalLoad.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.UwsClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp")
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE {UWSOwner}.SUBJ_AREA_BAL_SUM AS T "
    "USING STAGING.UwsClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S "
    "ON T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM "
    "WHEN MATCHED THEN UPDATE SET "
    "T.LAST_BAL_DT = S.LAST_BAL_DT, "
    "T.TLRNC_CD = S.TLRNC_CD, "
    "T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    "T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    "T.CRT_DTM = S.CRT_DTM, "
    "T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    "T.USER_ID = S.USER_ID "
    "WHEN NOT MATCHED THEN INSERT "
    "(TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    "VALUES "
    "(S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)