# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/02/07 14:51:45 Batch  14551_53511 PROMOTE bckcetl edw10 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:44:15 Batch  14551_53058 INIT bckcett testEDW10 dsadm bls for on
# MAGIC ^1_2 11/01/07 12:56:04 Batch  14550_46572 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_2 11/01/07 12:51:30 Batch  14550_46292 INIT bckcett devlEDW10 u03651 steffy
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
# MAGIC Parikshith Chada               07/13/2007          3264                              Originally Programmed                          devlEDW10               Steph Goddard             10/26/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
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
# TBL_BAL_SUM (CODBCStage) - Two output links
# --------------------------------------------------------------------------------
query_TblBalExtr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.CLMN_SUM_TLRNC_CD, "
    f"       TBL_BAL_SUM.ROW_TO_ROW_TLRNC_CD "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)

df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_TblBalExtr)
    .load()
)

query_DSLink18 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
    f"         TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_TBL_BAL_SUM_DSLink18 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DSLink18)
    .load()
)

# --------------------------------------------------------------------------------
# hf_edw_med_mgt_tbl_bal_lkup (CHashedFileStage) - Scenario A (intermediate hashed file)
# Replace with direct dedup on key columns [TRGT_SYS_CD, SUBJ_AREA_NM]
# --------------------------------------------------------------------------------
df_hf_edw_med_mgt_tbl_bal_lkup = df_TBL_BAL_SUM_DSLink18.dropDuplicates(["TRGT_SYS_CD", "SUBJ_AREA_NM"])

# --------------------------------------------------------------------------------
# Transform (CTransformerStage) - row-by-row logic with stage variables
# Primary link: df_TBL_BAL_SUM_TblBalExtr (alias TblBalExtr)
# Lookup link: df_hf_edw_med_mgt_tbl_bal_lkup (alias DSLink20), left join
# Output constraint: svPrevCount = DSLink20.ROW_COUNT => only output final row
# --------------------------------------------------------------------------------
df_Transform_join = (
    df_TBL_BAL_SUM_TblBalExtr.alias("TblBalExtr")
    .join(
        df_hf_edw_med_mgt_tbl_bal_lkup.alias("DSLink20"),
        (
            F.col("TblBalExtr.TRGT_SYS_CD") == F.col("DSLink20.TRGT_SYS_CD")
        )
        & (
            F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("DSLink20.SUBJ_AREA_NM")
        ),
        "left"
    )
    .select(
        F.col("TblBalExtr.TRGT_SYS_CD").alias("TblBalExtr_TRGT_SYS_CD"),
        F.col("TblBalExtr.SUBJ_AREA_NM").alias("TblBalExtr_SUBJ_AREA_NM"),
        F.col("TblBalExtr.BAL_DT").alias("TblBalExtr_BAL_DT"),
        F.col("TblBalExtr.CLMN_SUM_TLRNC_CD").alias("TblBalExtr_CLMN_SUM_TLRNC_CD"),
        F.col("TblBalExtr.ROW_TO_ROW_TLRNC_CD").alias("TblBalExtr_ROW_TO_ROW_TLRNC_CD"),
        F.col("DSLink20.ROW_COUNT").alias("DSLink20_ROW_COUNT")
    )
    .orderBy("TblBalExtr_BAL_DT")
)

rows = df_Transform_join.collect()

svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = "Y"
svTempRslvIncd = ""
final_row = None

for r in rows:
    bal_dt_str = str(r["TblBalExtr_BAL_DT"])[0:10] if r["TblBalExtr_BAL_DT"] else ""
    svCSCurrTlrncCd = r["TblBalExtr_CLMN_SUM_TLRNC_CD"]
    svRRCurrTlrncCd = r["TblBalExtr_ROW_TO_ROW_TLRNC_CD"]
    row_count = r["DSLink20_ROW_COUNT"] if r["DSLink20_ROW_COUNT"] else 0
    svCurrCount = svPrevCount + 1

    old_svTempTlrncCd = svTempTlrncCd
    old_svTempRslvIncd = svTempRslvIncd
    old_svTempRslvIn = svTempRslvIn

    # svTempTlrncCd
    if bal_dt_str != CurrDate:
        svTempTlrncCd = "NEXT ROW"
    else:
        if (svCSCurrTlrncCd in ["BAL", "NA"]) and (svRRCurrTlrncCd in ["BAL", "NA"]) and (old_svTempTlrncCd not in ["OUT", "IN"]):
            svTempTlrncCd = "BAL"
        else:
            if (
                ((svCSCurrTlrncCd in ["BAL", "IN"]) or (svRRCurrTlrncCd in ["BAL", "IN"]))
                and (old_svTempTlrncCd != "OUT")
            ):
                svTempTlrncCd = "IN"
            else:
                svTempTlrncCd = "OUT"

    # svTempRslvIn (5th in the original list, referencing old_svTempRslvIncd)
    if bal_dt_str == CurrDate:
        svTempRslvIn = old_svTempRslvIn
    else:
        if (
            ((svCSCurrTlrncCd in ["BAL", "IN"]) or (svRRCurrTlrncCd in ["BAL", "IN"]))
            and (old_svTempRslvIncd != "OUT")
        ):
            svTempRslvIn = "Y"
        else:
            svTempRslvIn = "N"

    # svTempRslvIncd (6th)
    if bal_dt_str == CurrDate:
        svTempRslvIncd = "NEXT ROW"
    else:
        if (
            (svCSCurrTlrncCd in ["BAL", "NA"])
            and (svRRCurrTlrncCd in ["BAL", "NA"])
            and (old_svTempRslvIncd not in ["OUT", "IN"])
        ):
            svTempRslvIncd = "BAL"
        else:
            if (
                ((svCSCurrTlrncCd in ["BAL", "IN"]) or (svRRCurrTlrncCd in ["BAL", "IN"]))
                and (old_svTempRslvIncd != "OUT")
            ):
                svTempRslvIncd = "IN"
            else:
                svTempRslvIncd = "OUT"

    svPrevCount = svCurrCount

    # Output constraint: svPrevCount = DSLink20.ROW_COUNT
    if svPrevCount == row_count and row_count != 0:
        # Produce the final row
        final_row = {
            "TRGT_SYS_CD": Target,
            "SUBJ_AREA_NM": Subject,
            "LAST_BAL_DT": CurrDate,
            "TLRNC_CD": svTempTlrncCd,
            "PREV_ISSUE_CRCTD_IN": svTempRslvIn,
            "TRGT_RUN_CYC_NO": ExtrRunCycle,
            "CRT_DTM": CurrDate,
            "LAST_UPDT_DTM": CurrDate,
            "USER_ID": UWSAcct
        }

# --------------------------------------------------------------------------------
# Build final DataFrame (Transform output) -> SUBJ_AREA_BAL_SUM (CODBCStage)
# --------------------------------------------------------------------------------
final_schema = StructType([
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("LAST_BAL_DT", StringType(), True),
    StructField("TLRNC_CD", StringType(), True),
    StructField("PREV_ISSUE_CRCTD_IN", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", StringType(), True),
    StructField("CRT_DTM", StringType(), True),
    StructField("LAST_UPDT_DTM", StringType(), True),
    StructField("USER_ID", StringType(), True),
])

if final_row is not None:
    df_enriched = spark.createDataFrame([final_row], schema=final_schema)
else:
    df_enriched = spark.createDataFrame([], schema=final_schema)

df_enriched = (
    df_enriched
    .withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), <...>, " "))
    .withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), <...>, " "))
    .withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), <...>, " "))
    .withColumn("PREV_ISSUE_CRCTD_IN", F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " "))
)

temp_table_name = f"STAGING.UwsMedMgtSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_enriched.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T "
    f"USING {temp_table_name} AS S "
    f"ON (T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"    T.LAST_BAL_DT = S.LAST_BAL_DT, "
    f"    T.TLRNC_CD = S.TLRNC_CD, "
    f"    T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    f"    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    f"    T.CRT_DTM = S.CRT_DTM, "
    f"    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    f"    T.USER_ID = S.USER_ID "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"VALUES "
    f"(S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)