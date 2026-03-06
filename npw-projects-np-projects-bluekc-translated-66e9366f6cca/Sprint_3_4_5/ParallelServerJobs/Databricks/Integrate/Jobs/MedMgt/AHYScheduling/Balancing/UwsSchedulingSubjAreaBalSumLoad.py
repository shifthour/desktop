# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingBalCntl
# MAGIC 
# MAGIC PROCESSING: Extraction From Table Balance Summary table for a given subject area and target system and loading into the Subject Area Balance Summary table
# MAGIC                       
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                  2011-02-04             4529                            Initial Programming                                            IntegrateNewDevl      Steph Goddard             02/08/2011


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query_1 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)

df_TBL_BAL_SUM_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
    f"         TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_TBL_BAL_SUM_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_TBL_BAL_SUM_2_dedup = df_TBL_BAL_SUM_2.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"])

df_Transform_input = df_TBL_BAL_SUM_1.alias("TblBalExtr").join(
    df_TBL_BAL_SUM_2_dedup.alias("DSLink20"),
    on=[
        F.col("TblBalExtr.TRGT_SYS_CD") == F.col("DSLink20.TRGT_SYS_CD"),
        F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("DSLink20.SUBJ_AREA_NM")
    ],
    how="left"
)

df_WithStageVars = (
    df_Transform_input
    .orderBy("TblBalExtr.BAL_DT")
    .withColumn("_svCurrTlrncCd", F.col("TblBalExtr.ROW_CT_TLRNC_CD"))
    .withColumn("_rowIndex", F.row_number().over(F.Window.orderBy("TblBalExtr.BAL_DT")))
    .withColumn("_svPrevCount", F.lag("_rowIndex", 1, 0).over(F.Window.orderBy("TblBalExtr.BAL_DT")))
    .withColumn("_svCurrCount", F.col("_svPrevCount") + F.lit(1))
    .withColumn(
        "_svTempTlrncCd",
        F.when( F.substring(F.col("TblBalExtr.BAL_DT"),1,10) != F.lit(CurrDate), F.lit("NEXT ROW"))
         .otherwise(
           F.when(
             (F.col("_svCurrTlrncCd") == F.lit("BAL"))
             & (F.lag("_svTempTlrncCd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT"))
             & (F.lag("_svTempTlrncCd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("IN")),
             F.lit("BAL")
           )
           .otherwise(
             F.when(
               (F.col("_svCurrTlrncCd") == F.lit("BAL"))
               & (F.lag("_svTempTlrncCd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) == F.lit("IN")),
               F.lit("IN")
             )
             .otherwise(
               F.when(
                 (F.col("_svCurrTlrncCd") == F.lit("IN"))
                 & (F.lag("_svTempTlrncCd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT")),
                 F.lit("IN")
               )
               .otherwise(F.lit("OUT"))
             )
           )
         )
    )
    .withColumn(
        "_svTempRslvIncd",
        F.when( F.substring(F.col("TblBalExtr.BAL_DT"),1,10) == F.lit(CurrDate), F.lit("NA"))
         .otherwise(
           F.when(
             (F.col("_svCurrTlrncCd") == F.lit("BAL"))
             & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT"))
             & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("IN")),
             F.lit("BAL")
           )
           .otherwise(
             F.when(
               (F.col("_svCurrTlrncCd") == F.lit("BAL"))
               & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) == F.lit("IN")),
               F.lit("IN")
             )
             .otherwise(
               F.when(
                 (F.col("_svCurrTlrncCd") == F.lit("IN"))
                 & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT")),
                 F.lit("IN")
               )
               .otherwise(F.lit("OUT"))
             )
           )
         )
    )
    .withColumn(
        "_svTempRslvIn",
        F.when( F.substring(F.col("TblBalExtr.BAL_DT"),1,10) == F.lit(CurrDate), F.col("_svTempRslvIn"))
         .otherwise(
           F.when(
             (F.col("_svCurrTlrncCd") == F.lit("BAL"))
             & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT")),
             F.lit("Y")
           )
           .otherwise(
             F.when(
               (F.col("_svCurrTlrncCd") == F.lit("IN"))
               & (F.lag("_svTempRslvIncd",1).over(F.Window.orderBy("TblBalExtr.BAL_DT")) != F.lit("OUT")),
               F.lit("Y")
             )
             .otherwise(F.lit("N"))
           )
         )
         .alias("_svTempRslvIn")
    )
)

df_WithStageVars_2 = df_WithStageVars.withColumn(
    "_svTempRslvIn",
    F.last("_svTempRslvIn", True).over(F.Window.orderBy("TblBalExtr.BAL_DT").rowsBetween(Window.currentRow, Window.currentRow))
)

df_ConstraintFiltered = df_WithStageVars_2.filter(
    F.col("_svPrevCount") == F.col("DSLink20.ROW_COUNT")
)

df_enriched = df_ConstraintFiltered.select(
    F.lit(Target).alias("TRGT_SYS_CD"),
    F.lit(Subject).alias("SUBJ_AREA_NM"),
    F.lit(CurrDate).alias("LAST_BAL_DT"),
    F.col("_svTempTlrncCd").alias("TLRNC_CD"),
    F.col("_svTempRslvIn").alias("PREV_ISSUE_CRCTD_IN"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrDate).alias("CRT_DTM"),
    F.lit(CurrDate).alias("LAST_UPDT_DTM"),
    F.lit(get_widget_value('UWSAcct','')).alias("USER_ID")
)

df_enriched_for_write = df_enriched.select(
    F.rpad("TRGT_SYS_CD", 10, " ").alias("TRGT_SYS_CD"),
    F.rpad("SUBJ_AREA_NM", 50, " ").alias("SUBJ_AREA_NM"),
    "LAST_BAL_DT",
    F.rpad("TLRNC_CD", 10, " ").alias("TLRNC_CD"),
    F.rpad("PREV_ISSUE_CRCTD_IN", 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    F.rpad("USER_ID", 30, " ").alias("USER_ID")
)

temp_table = "STAGING.UwsSchedulingSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
merge_target = f"{UWSOwner}.SUBJ_AREA_BAL_SUM"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
    df_enriched_for_write.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO {merge_target} AS T "
    f"USING {temp_table} AS S "
    f"ON "
    f"(T.TRGT_SYS_CD = S.TRGT_SYS_CD "
    f"AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.LAST_BAL_DT=S.LAST_BAL_DT, "
    f"T.TLRNC_CD=S.TLRNC_CD, "
    f"T.PREV_ISSUE_CRCTD_IN=S.PREV_ISSUE_CRCTD_IN, "
    f"T.TRGT_RUN_CYC_NO=S.TRGT_RUN_CYC_NO, "
    f"T.CRT_DTM=S.CRT_DTM, "
    f"T.LAST_UPDT_DTM=S.LAST_UPDT_DTM, "
    f"T.USER_ID=S.USER_ID "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"VALUES "
    f"(S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)