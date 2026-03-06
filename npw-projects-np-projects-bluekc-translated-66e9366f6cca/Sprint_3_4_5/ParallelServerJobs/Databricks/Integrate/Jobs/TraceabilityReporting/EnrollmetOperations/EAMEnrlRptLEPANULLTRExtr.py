# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job: EAMEnrlRptLEPLTRSeq
# MAGIC 
# MAGIC This job reads the data from LEP LTR 2 Annual files and writes to Intermediate files for Database Insertion
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ================================================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                                                      Development Project                         Reviewer                     Review Date
# MAGIC ================================================================================================================================================================================================
# MAGIC John Abraham               2021-10-11                      US391328                                    Original Programming                                                                                       IntegrateDev2                           Reddy Sanam                   10/12/2021

# MAGIC This job reads LEP (Late Enrollment Penalty)  Annual Letter files
# MAGIC  P.Fxxxxx.LEPD.Dyymm01.Thhmmsst (xxxxx is either 'H1352' or 'H6502' for PlanID) and writes a Intermediate files to process database insert.
# MAGIC SQ Filter 'grep ^CT' used to filter the records starting with CT from source files
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as SF
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FileCrtDtFH6502 = get_widget_value('FileCrtDtFH6502','')
FileCrtDtH1352 = get_widget_value('FileCrtDtH1352','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
FileFH1352 = get_widget_value('FileFH1352','')
FileFH6502 = get_widget_value('FileFH6502','')

col_details_fh6502 = {
    "Record": {"len": 165, "type": T.StringType()}
}
df_SQ_LEPLTR_FH6502 = fixed_file_read_write(
    f"{adls_path_raw}/landing/{FileFH6502}", 
    col_details_fh6502, 
    "read"
)

df_Trx_FH6502 = df_SQ_LEPLTR_FH6502.filter(
    trim(SF.substring(SF.col("Record"), 1, 3)).isin("PD", "AD", "HD")
).select(
    trim(SF.substring(SF.col("Record"), 1, 3)).alias("RCRD_TYP_CD"),
    SF.substring(SF.col("Record"), 4, 5).alias("PLAN_ID"),
    SF.substring(SF.col("Record"), 9, 3).alias("PBP_NBR"),
    SF.substring(SF.col("Record"), 12, 3).alias("PLAN_SGMNT_NBR"),
    trim(SF.substring(SF.col("Record"), 15, 12)).alias("MBI"),
    trim(SF.substring(SF.col("Record"), 27, 7)).alias("LAST_NAME"),
    SF.substring(SF.col("Record"), 34, 1).alias("FRST_INTL_NM"),
    SF.substring(SF.col("Record"), 35, 1).alias("GNDR_CD"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 36, 4),
            SF.substring(SF.col("Record"), 40, 2),
            SF.substring(SF.col("Record"), 42, 2)
        ),
        "yyyy-MM-dd"
    ).alias("BRTH_DT"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 45, 4),
            SF.substring(SF.col("Record"), 49, 2),
            SF.substring(SF.col("Record"), 51, 2)
        ),
        "yyyy-MM-dd"
    ).alias("PRD_START_DT"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 53, 4),
            SF.substring(SF.col("Record"), 57, 2),
            SF.substring(SF.col("Record"), 59, 2)
        ),
        "yyyy-MM-dd"
    ).alias("PRD_END_DT"),
    SF.substring(SF.col("Record"), 61, 2).alias("PRD_MNTH_NBR"),
    SF.substring(SF.col("Record"), 63, 3).alias("UNCVRD_PRD_MNTH_NBR"),
    trim(SF.substring(SF.col("Record"), 66, 8)).alias("LEP_AMT"),
    SF.when(
        trim(SF.substring(SF.col("Record"), 74, 10)) == "",
        SF.lit(None)
    ).otherwise(
        trim(SF.substring(SF.col("Record"), 74, 10))
    ).alias("CLEAN_UP_ID"),
    SF.to_date(
        SF.lit(FileCrtDtFH6502[:4] + "-" + FileCrtDtFH6502[4:6] + "-" + FileCrtDtFH6502[6:8]),
        "yyyy-MM-dd"
    ).alias("START_CLCK_DTM"),
    SF.to_date(SF.lit(CurrDt), "yyyy-MM-dd").alias("PRCS_RUN_DT")
)

col_details_fh1352 = {
    "Record": {"len": 165, "type": T.StringType()}
}
df_SQ_LEPLTR_FH1352 = fixed_file_read_write(
    f"{adls_path_raw}/landing/{FileFH1352}", 
    col_details_fh1352, 
    "read"
)

df_Trx_FH1352 = df_SQ_LEPLTR_FH1352.filter(
    trim(SF.substring(SF.col("Record"), 1, 3)).isin("PD", "AD", "HD")
).select(
    trim(SF.substring(SF.col("Record"), 1, 3)).alias("RCRD_TYP_CD"),
    SF.substring(SF.col("Record"), 4, 5).alias("PLAN_ID"),
    SF.substring(SF.col("Record"), 9, 3).alias("PBP_NBR"),
    SF.substring(SF.col("Record"), 12, 3).alias("PLAN_SGMNT_NBR"),
    trim(SF.substring(SF.col("Record"), 15, 12)).alias("MBI"),
    trim(SF.substring(SF.col("Record"), 27, 7)).alias("LAST_NAME"),
    SF.substring(SF.col("Record"), 34, 1).alias("FRST_INTL_NM"),
    SF.substring(SF.col("Record"), 35, 1).alias("GNDR_CD"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 36, 4),
            SF.substring(SF.col("Record"), 40, 2),
            SF.substring(SF.col("Record"), 42, 2)
        ),
        "yyyy-MM-dd"
    ).alias("BRTH_DT"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 45, 4),
            SF.substring(SF.col("Record"), 49, 2),
            SF.substring(SF.col("Record"), 51, 2)
        ),
        "yyyy-MM-dd"
    ).alias("PRD_START_DT"),
    SF.to_date(
        SF.concat_ws(
            "-",
            SF.substring(SF.col("Record"), 53, 4),
            SF.substring(SF.col("Record"), 57, 2),
            SF.substring(SF.col("Record"), 59, 2)
        ),
        "yyyy-MM-dd"
    ).alias("PRD_END_DT"),
    SF.substring(SF.col("Record"), 61, 2).alias("PRD_MNTH_NBR"),
    SF.substring(SF.col("Record"), 63, 3).alias("UNCVRD_PRD_MNTH_NBR"),
    trim(SF.substring(SF.col("Record"), 66, 8)).alias("LEP_AMT"),
    SF.when(
        trim(SF.substring(SF.col("Record"), 74, 10)) == "",
        SF.lit(None)
    ).otherwise(
        trim(SF.substring(SF.col("Record"), 74, 10))
    ).alias("CLEAN_UP_ID"),
    SF.to_date(
        SF.lit(FileCrtDtH1352[:4] + "-" + FileCrtDtH1352[4:6] + "-" + FileCrtDtH1352[6:8]),
        "yyyy-MM-dd"
    ).alias("START_CLCK_DTM"),
    SF.to_date(SF.lit(CurrDt), "yyyy-MM-dd").alias("PRCS_RUN_DT")
)

df_Fn_LEPLTR = df_Trx_FH6502.union(df_Trx_FH1352)

df_Final = df_Fn_LEPLTR.select(
    SF.rpad(SF.col("RCRD_TYP_CD"), 3, " ").alias("RCRD_TYP_CD"),
    SF.rpad(SF.col("PLAN_ID"), 5, " ").alias("PLAN_ID"),
    SF.rpad(SF.col("PBP_NBR"), 3, " ").alias("PBP_NBR"),
    SF.rpad(SF.col("PLAN_SGMNT_NBR"), 3, " ").alias("PLAN_SGMNT_NBR"),
    SF.rpad(SF.col("MBI"), 12, " ").alias("MBI"),
    SF.rpad(SF.col("LAST_NAME"), 7, " ").alias("LAST_NAME"),
    SF.rpad(SF.col("FRST_INTL_NM"), 1, " ").alias("FRST_INTL_NM"),
    SF.rpad(SF.col("GNDR_CD"), 1, " ").alias("GNDR_CD"),
    SF.col("BRTH_DT"),
    SF.col("PRD_START_DT"),
    SF.col("PRD_END_DT"),
    SF.rpad(SF.col("PRD_MNTH_NBR"), 2, " ").alias("PRD_MNTH_NBR"),
    SF.rpad(SF.col("UNCVRD_PRD_MNTH_NBR"), 3, " ").alias("UNCVRD_PRD_MNTH_NBR"),
    SF.rpad(SF.col("LEP_AMT"), 8, " ").alias("LEP_AMT"),
    SF.rpad(SF.col("CLEAN_UP_ID"), 10, " ").alias("CLEAN_UP_ID"),
    SF.col("START_CLCK_DTM"),
    SF.col("PRCS_RUN_DT")
)

write_files(
    df_Final,
    f"{adls_path}/load/EAM_RPT_LEP_ANUL_LTR.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)