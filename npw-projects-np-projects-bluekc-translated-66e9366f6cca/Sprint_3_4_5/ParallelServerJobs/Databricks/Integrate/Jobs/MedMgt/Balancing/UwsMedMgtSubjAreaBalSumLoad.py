# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsMedMgtBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/13/2007          3264                              Originally Programmed                           devlIDS30                  Steph Goddard           9/14/07         
# MAGIC 
# MAGIC Manasa Andru                  12/27/2013        TFS - 2373                Removed hashed file and added direct     IntegrateNewDevl         Kalyan Neelam            2014-01-15
# MAGIC                                                                                                               reference from TBL_BAL_SUM


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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# --------------------------------------------------------------------------------
# Read from CODBCStage: TBL_BAL_SUM (StageName: TBL_BAL_SUM)
# --------------------------------------------------------------------------------
extract_query_tbl_bal_sum = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)
df_TBL_BAL_SUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_tbl_bal_sum)
    .load()
)

# --------------------------------------------------------------------------------
# Read from CODBCStage: ODBC_23 (StageName: ODBC_23) for lookup
# --------------------------------------------------------------------------------
extract_query_odbc_23 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
)
df_ODBC_23 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_odbc_23)
    .load()
)

# --------------------------------------------------------------------------------
# Transformer Stage: (StageName: Transform)
# Primary link:  TblBalExtr (TBL_BAL_SUM)
# Lookup link:   MedMgtCt   (ODBC_23), left join
# Stage Variables must be applied in row order. We will coalesce(1) and mapPartitions
# to replicate DataStage’s row-by-row stage variable logic.
# --------------------------------------------------------------------------------
df_joined = (
    df_TBL_BAL_SUM.alias("TblBalExtr")
    .join(
        df_ODBC_23.alias("MedMgtCt"),
        [
            F.col("TblBalExtr.TRGT_SYS_CD") == F.col("MedMgtCt.TRGT_SYS_CD"),
            F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("MedMgtCt.SUBJ_AREA_NM")
        ],
        "left"
    )
    .orderBy("TblBalExtr.BAL_DT")
    .coalesce(1)
)

rdd_output = df_joined.rdd.mapPartitions(
    lambda rows: (
        # We keep a mutable state across rows in this partition to mimic DS stage variables
        _emit for _emit in ((
            # Re-initialize (only once per partition)
            (exec(
                "global svPrevCount, svTempTlrncCd, svTempRslvIn, svTempRslvIncd;"
                "svPrevCount=0;"
                "svTempTlrncCd='';"
                "svTempRslvIn='Y';"
                "svTempRslvIncd='';"
            ), None))[1]
        ) if False else None
    ) if False else None  # Dummy trick for initialization
)  # This just sets up placeholders; actual logic must continue below.

# Because we cannot define any function, we need everything inline. The above approach
# for one-time partition initialization conflicts with “no functions” or multi-line lambdas.
# Therefore, we must embed the entire row logic in a single mapPartitions usage below.
# Admittedly, it is large, but required to replicate stage-vars exactly.

rdd_final = df_joined.rdd.mapPartitions(lambda part_iter: (
    # Set up stage variables (persisted across rows in this partition)
    (
        exec(
            "global svPrevCount, svTempTlrncCd, svTempRslvIn, svTempRslvIncd;"
            "svPrevCount=0;"
            "svTempTlrncCd='';"
            "svTempRslvIn='Y';"
            "svTempRslvIncd='';"
        ), None
    )[1]
    for _ in []
) if False else _ for _ in (None,) for row in part_iter for _ in (
    # Actual row-by-row logic in a single expression block:
    exec(
        # Extract columns
        "TRGT_SYS_CD = row['TRGT_SYS_CD']\n"
        "SUBJ_AREA_NM = row['SUBJ_AREA_NM']\n"
        "BAL_DT = row['BAL_DT']\n"
        "ROW_CT_TLRNC_CD = row['ROW_CT_TLRNC_CD'] if row['ROW_CT_TLRNC_CD'] is not None else ''\n"
        "ROW_COUNT = row['ROW_COUNT'] if 'ROW_COUNT' in row and row['ROW_COUNT'] is not None else 0\n"
        "\n"
        "# svCurrTlrncCd:\n"
        "svCurrTlrncCd = ROW_CT_TLRNC_CD\n"
        "\n"
        "# svCurrCount:\n"
        "svCurrCount = svPrevCount + 1\n"
        "\n"
        "# Next lines replicate the logic for svTempTlrncCd:\n"
        "bal_dt_str = str(BAL_DT)[:10]\n"
        "old_svTempTlrncCd = svTempTlrncCd\n"
        "if bal_dt_str != CurrDate:\n"
        "    svTempTlrncCd = 'NEXT ROW'\n"
        "else:\n"
        "    if svCurrTlrncCd == 'BAL' and old_svTempTlrncCd not in ('OUT','IN'):\n"
        "        svTempTlrncCd = 'BAL'\n"
        "    elif svCurrTlrncCd == 'BAL' and old_svTempTlrncCd == 'IN':\n"
        "        svTempTlrncCd = 'IN'\n"
        "    elif svCurrTlrncCd == 'IN' and old_svTempTlrncCd != 'OUT':\n"
        "        svTempTlrncCd = 'IN'\n"
        "    else:\n"
        "        svTempTlrncCd = 'OUT'\n"
        "\n"
        "# svTempRslvIn:\n"
        "old_svTempRslvIn = svTempRslvIn\n"
        "if bal_dt_str == CurrDate:\n"
        "    svTempRslvIn = old_svTempRslvIn\n"
        "else:\n"
        "    if svCurrTlrncCd == 'BAL' and svTempRslvIncd != 'OUT':\n"
        "        svTempRslvIn = 'Y'\n"
        "    elif svCurrTlrncCd == 'IN' and svTempRslvIncd != 'OUT':\n"
        "        svTempRslvIn = 'Y'\n"
        "    else:\n"
        "        svTempRslvIn = 'N'\n"
        "\n"
        "# svTempRslvIncd:\n"
        "old_svTempRslvIncd = svTempRslvIncd\n"
        "if bal_dt_str == CurrDate:\n"
        "    svTempRslvIncd = 'NA'\n"
        "else:\n"
        "    if svCurrTlrncCd == 'BAL' and old_svTempRslvIncd not in ('OUT','IN'):\n"
        "        svTempRslvIncd = 'BAL'\n"
        "    elif svCurrTlrncCd == 'BAL' and old_svTempRslvIncd == 'IN':\n"
        "        svTempRslvIncd = 'IN'\n"
        "    elif svCurrTlrncCd == 'IN' and old_svTempRslvIncd != 'OUT':\n"
        "        svTempRslvIncd = 'IN'\n"
        "    else:\n"
        "        svTempRslvIncd = 'OUT'\n"
        "\n"
        "# svPrevCount = svCurrCount:\n"
        "new_svPrevCount = svCurrCount\n"
        "\n"
        "# Collect final row columns for output.\n"
        "transformed_row = (TRGT_SYS_CD, SUBJ_AREA_NM, BAL_DT, ROW_CT_TLRNC_CD, ROW_COUNT, svCurrCount, svTempTlrncCd, svTempRslvIn, svTempRslvIncd, new_svPrevCount)\n"
        "\n"
        "# Update stage variables.\n"
        "svPrevCount = new_svPrevCount\n"
    ),
))
    # Then yield the transformed_row
    or (TRGT_SYS_CD, SUBJ_AREA_NM, BAL_DT, ROW_CT_TLRNC_CD, ROW_COUNT, svCurrCount, svTempTlrncCd, svTempRslvIn, svTempRslvIncd, new_svPrevCount)
)

# Convert RDD back to DataFrame
schema_out = StructType([
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("BAL_DT", StringType(), True),
    StructField("ROW_CT_TLRNC_CD", StringType(), True),
    StructField("ROW_COUNT", IntegerType(), True),
    StructField("svCurrCount", IntegerType(), True),
    StructField("svTempTlrncCd", StringType(), True),
    StructField("svTempRslvIn", StringType(), True),
    StructField("svTempRslvIncd", StringType(), True),
    StructField("svPrevCount", IntegerType(), True)
])
df_stagevars = spark.createDataFrame(rdd_final, schema_out)

# Constraint: "svPrevCount = MedMgtCt.ROW_COUNT"
# That means keep rows where svPrevCount == ROW_COUNT
df_filtered = df_stagevars.filter(F.col("svPrevCount") == F.col("ROW_COUNT"))

# Output columns for link "SubjAreaBalLoad" with the given expressions:
# TRGT_SYS_CD = literal Target
# SUBJ_AREA_NM = literal Subject
# LAST_BAL_DT = CurrDate
# TLRNC_CD = svTempTlrncCd
# PREV_ISSUE_CRCTD_IN (char(1)) => rpad(svTempRslvIn,1,' ')
# TRGT_RUN_CYC_NO = ExtrRunCycle
# CRT_DTM = CurrDate
# LAST_UPDT_DTM = CurrDate
# USER_ID = $UWSAcct
df_final = (
    df_filtered
    .select(
        F.lit(Target).alias("TRGT_SYS_CD"),
        F.lit(Subject).alias("SUBJ_AREA_NM"),
        F.lit(CurrDate).alias("LAST_BAL_DT"),
        F.col("svTempTlrncCd").alias("TLRNC_CD"),
        F.rpad(F.col("svTempRslvIn"), 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
        F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
        F.lit(CurrDate).alias("CRT_DTM"),
        F.lit(CurrDate).alias("LAST_UPDT_DTM"),
        F.lit(UWSAcct).alias("USER_ID")
    )
)

# --------------------------------------------------------------------------------
# Write to CODBCStage: SUBJ_AREA_BAL_SUM (StageName: SUBJ_AREA_BAL_SUM)
# We must generate a merge (upsert) with PK = (TRGT_SYS_CD, SUBJ_AREA_NM)
# Insert/Update columns are (LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN,
#  TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID).
# --------------------------------------------------------------------------------

# 1) Create a physical temp table: STAGING.UwsMedMgtSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp
temp_table_name = "STAGING.UwsMedMgtSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

# 2) Write df_final into that table
df_final.write.jdbc(url=jdbc_url, table=temp_table_name, mode="overwrite", properties=jdbc_props)

# 3) Merge statement onto {UWSOwner}.SUBJ_AREA_BAL_SUM
merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING {temp_table_name} AS S
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
    INSERT
    (
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
    VALUES
    (
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