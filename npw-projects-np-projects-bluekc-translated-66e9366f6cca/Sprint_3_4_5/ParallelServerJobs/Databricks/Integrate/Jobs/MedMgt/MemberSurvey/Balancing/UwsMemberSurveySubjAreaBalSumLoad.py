# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyRspnBalCntl
# MAGIC 
# MAGIC PROCESSING: Extraction From Table Balance Summary table for a given subject area and target system and loading into the Subject Area Balance Summary table
# MAGIC                       
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2011-06-01             4673                            Initial Programming                                             IntegrateWrhsDevl      Brent Leland                 06-16-2011


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

# Set up JDBC connection
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage)
#   Output Pin #1 (TblBalExtr)
# --------------------------------------------------------------------------------
df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
        "       TBL_BAL_SUM.SUBJ_AREA_NM, "
        "       TBL_BAL_SUM.BAL_DT, "
        "       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
        "  FROM " + UWSOwner + ".TBL_BAL_SUM TBL_BAL_SUM "
        " WHERE TBL_BAL_SUM.TRGT_SYS_CD = '" + Target + "' "
        "   AND TBL_BAL_SUM.SUBJ_AREA_NM = '" + Subject + "' "
        " ORDER BY TBL_BAL_SUM.BAL_DT ASC"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage)
#   Output Pin #2 (BalSum)
# --------------------------------------------------------------------------------
df_TBL_BAL_SUM_BalSum = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
        "       TBL_BAL_SUM.SUBJ_AREA_NM, "
        "       COUNT(*) AS ROW_COUNT "
        "  FROM " + UWSOwner + ".TBL_BAL_SUM TBL_BAL_SUM "
        " WHERE TBL_BAL_SUM.TRGT_SYS_CD = '" + Target + "' "
        "   AND TBL_BAL_SUM.SUBJ_AREA_NM = '" + Subject + "' "
        " GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
        "          TBL_BAL_SUM.SUBJ_AREA_NM"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_mbrsrvyrspn_tbl_bal_lkup (CHashedFileStage)
#   Scenario A: Intermediate hashed file (no Transformer writes back to the same file).
#   Replace this hashed file with dedup logic on (TRGT_SYS_CD, SUBJ_AREA_NM).
# --------------------------------------------------------------------------------
df_Bal_Sum = dedup_sort(
    df_TBL_BAL_SUM_BalSum,
    partition_cols=["TRGT_SYS_CD", "SUBJ_AREA_NM"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage)
#   Inputs:
#     - Primary Link: TblBalExtr (df_TBL_BAL_SUM_TblBalExtr)
#     - Lookup Link: Bal_Sum (df_Bal_Sum), left join
#   Stage Variables logic must be replicated row-by-row.
# --------------------------------------------------------------------------------
df_joined = (
    df_TBL_BAL_SUM_TblBalExtr.alias("TblBalExtr")
    .join(
        df_Bal_Sum.alias("Bal_Sum"),
        on=[
            F.col("TblBalExtr.TRGT_SYS_CD") == F.col("Bal_Sum.TRGT_SYS_CD"),
            F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("Bal_Sum.SUBJ_AREA_NM"),
        ],
        how="left",
    )
)

df_joined_ordered = df_joined.orderBy(F.col("TblBalExtr.BAL_DT").asc()).collect()

rows_for_output = []

# Initialize stage variable states
svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = "Y"
svTempRslvIncd = ""

for row in df_joined_ordered:
    # Read needed columns from the joined DataFrame
    val_TRGT_SYS_CD = row["TblBalExtr.TRGT_SYS_CD"]
    val_SUBJ_AREA_NM = row["TblBalExtr.SUBJ_AREA_NM"]
    val_BAL_DT = row["TblBalExtr.BAL_DT"]
    val_ROW_CT_TLRNC_CD = row["TblBalExtr.ROW_CT_TLRNC_CD"]
    val_BAL_Sum_ROW_COUNT = row["Bal_Sum.ROW_COUNT"]

    # Stage variable: svCurrTlrncCd
    svCurrTlrncCd = val_ROW_CT_TLRNC_CD if val_ROW_CT_TLRNC_CD is not None else ""

    # Stage variable: svCurrCount
    svCurrCount = svPrevCount + 1

    # Stage variable: svTempTlrncCd
    # We reference the old "svTempTlrncCd" in the expression
    old_svTempTlrncCd = svTempTlrncCd
    if (val_BAL_DT is not None and val_BAL_DT[:10] != CurrDate):
        computed_svTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and old_svTempTlrncCd not in ["OUT", "IN"]:
            computed_svTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and old_svTempTlrncCd == "IN":
            computed_svTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and old_svTempTlrncCd != "OUT":
            computed_svTempTlrncCd = "IN"
        else:
            computed_svTempTlrncCd = "OUT"
    svTempTlrncCd = computed_svTempTlrncCd

    # Stage variable: svTempRslvIn
    # We reference the old "svTempRslvIncd" in the expression
    old_svTempRslvIncd = svTempRslvIncd
    if val_BAL_DT is not None and val_BAL_DT[:10] == CurrDate:
        computed_svTempRslvIn = svTempRslvIn
    else:
        if svCurrTlrncCd == "BAL" and old_svTempRslvIncd not in ["OUT"]:
            computed_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and old_svTempRslvIncd not in ["OUT"]:
            computed_svTempRslvIn = "Y"
        else:
            computed_svTempRslvIn = "N"
    svTempRslvIn = computed_svTempRslvIn

    # Stage variable: svTempRslvIncd
    # We reference the old "svTempRslvIncd" in the expression
    older_svTempRslvIncd = svTempRslvIncd
    if val_BAL_DT is not None and val_BAL_DT[:10] == CurrDate:
        computed_svTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and older_svTempRslvIncd not in ["OUT", "IN"]:
            computed_svTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and older_svTempRslvIncd == "IN":
            computed_svTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and older_svTempRslvIncd != "OUT":
            computed_svTempRslvIncd = "IN"
        else:
            computed_svTempRslvIncd = "OUT"
    svTempRslvIncd = computed_svTempRslvIncd

    # Stage variable: svPrevCount
    # assigned after svCurrCount is known
    # but used for the constraint: svPrevCount = Bal_Sum.ROW_COUNT
    new_svPrevCount = svCurrCount

    # Constraint check: svPrevCount == Bal_Sum.ROW_COUNT
    # This check uses the newly computed svCurrCount
    if val_BAL_Sum_ROW_COUNT is not None and svCurrCount == val_BAL_Sum_ROW_COUNT:
        # Output row into "SubjAreaBalLoad"
        output_row = {
            "TRGT_SYS_CD": Target,
            "SUBJ_AREA_NM": Subject,
            "LAST_BAL_DT": CurrDate,
            "TLRNC_CD": svTempTlrncCd,
            "PREV_ISSUE_CRCTD_IN": svTempRslvIn,
            "TRGT_RUN_CYC_NO": ExtrRunCycle,
            "CRT_DTM": CurrDate,
            "LAST_UPDT_DTM": CurrDate,
            "USER_ID": "<...>"  # $UWSAcct unknown
        }
        rows_for_output.append(output_row)

    # Finally update svPrevCount to switchover for next iteration
    svPrevCount = new_svPrevCount

# Construct the final DataFrame
schema_SubjAreaBalLoad = StructType([
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("LAST_BAL_DT", StringType(), True),
    StructField("TLRNC_CD", StringType(), True),
    StructField("PREV_ISSUE_CRCTD_IN", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", StringType(), True),
    StructField("CRT_DTM", StringType(), True),
    StructField("LAST_UPDT_DTM", StringType(), True),
    StructField("USER_ID", StringType(), True)
])

df_SubjAreaBalLoad = spark.createDataFrame(rows_for_output, schema=schema_SubjAreaBalLoad)

# Apply rpad for char/varchar columns as required
df_SubjAreaBalLoad = (
    df_SubjAreaBalLoad
    .withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), F.lit(<...>), " "))
    .withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), F.lit(<...>), " "))
    .withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), F.lit(<...>), " "))
    .withColumn("PREV_ISSUE_CRCTD_IN", F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), F.lit(1), " "))
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), F.lit(<...>), " "))
    .select(
        "TRGT_SYS_CD",
        "SUBJ_AREA_NM",
        "LAST_BAL_DT",
        "TLRNC_CD",
        "PREV_ISSUE_CRCTD_IN",
        "TRGT_RUN_CYC_NO",
        "CRT_DTM",
        "LAST_UPDT_DTM",
        "USER_ID"
    )
)

# --------------------------------------------------------------------------------
# Stage: SUBJ_AREA_BAL_SUM (CODBCStage) - Upsert (Merge)
# --------------------------------------------------------------------------------
temp_table_name = "STAGING.UwsMemberSurveySubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"

execute_dml("DROP TABLE IF EXISTS " + temp_table_name, jdbc_url, jdbc_props)

(
    df_SubjAreaBalLoad.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("append")
    .save()
)

merge_sql = (
    "MERGE INTO " + UWSOwner + ".SUBJ_AREA_BAL_SUM AS T "
    "USING " + temp_table_name + " AS S "
    "ON T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM "
    "WHEN MATCHED THEN UPDATE SET "
    "  T.LAST_BAL_DT = S.LAST_BAL_DT, "
    "  T.TLRNC_CD = S.TLRNC_CD, "
    "  T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    "  T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    "  T.CRT_DTM = S.CRT_DTM, "
    "  T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    "  T.USER_ID = S.USER_ID "
    "WHEN NOT MATCHED THEN INSERT "
    "("
    "TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    ") VALUES ("
    "S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    ");"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)