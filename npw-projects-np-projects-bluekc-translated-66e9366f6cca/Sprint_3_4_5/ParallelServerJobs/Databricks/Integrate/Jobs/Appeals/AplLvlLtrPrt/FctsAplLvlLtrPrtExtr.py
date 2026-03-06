# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  FctsAplLvlLtrPrtExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APLV_APP_LEVEL, CER_ATLT_LETTER_D, CER_ATLP_PRINT_HST, CER_ATXR_ATTACH_U for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the APL_LVL_LTR_PRT table.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari    07/15/2007                    Initial program                                                                                   devlIDS30                       Steph Goddard           8/23/07 
# MAGIC Bhoomi Dasari    10/18/2007                 Added Balancing Snapshot                                         3028               devlIDS30                       Steph Goddard            10/18/2007
# MAGIC 
# MAGIC Jag Yelavarthi    2015-01-19                  Changed "IDS_SK" to "Table_Name_SK"                TFS#8431       IntegrateNewDevl                Kalyan Neelam            2015-01-27
# MAGIC Prabhu ES         2022-02-25                  MSSQL connection parameters added                         S2S               IntegrateDev5	Ken Bradmon	2022-05-19

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Apl Level Ltr Prt Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File
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
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
RunID = get_widget_value('RunID','')
EndDate = get_widget_value('EndDate','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

facets_query = f"""
SELECT 
APLV.APAP_ID,
APLV.APLV_SEQ_NO,
ATLT.ATSY_ID,
ATLT.ATLT_SEQ_NO,
ATLT.ATXR_DEST_ID,
ATLP.ATLP_SEQ_NO,
ATLP.ATLP_REQUEST_DT,
ATLP.ATLP_REQUEST_USUS,
ATLP.ATLP_SUBMITTED_DT,
ATLP.ATLP_PRINTED_DT,
ATLP.ATLP_LAST_UPDATE_DT,
ATLP.ATLP_LAST_UPDATE_USUS,
ATLP.ATLP_DESC,
ATXR.ATXR_DESC
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL APLV,
     {FacetsOwner}.CMC_APAP_APPEALS APAP,
     {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
     {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
     {FacetsOwner}.CER_ATLP_PRINT_HST ATLP
WHERE
APLV.APAP_ID = APAP.APAP_ID
AND APLV.APLV_SEQ_NO = APAP.APLV_SEQ_NO
AND APLV.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
AND ATXR.ATSY_ID = ATLT.ATSY_ID
AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
AND ATLT.ATSY_ID = ATLP.ATSY_ID
AND ATLT.ATXR_DEST_ID = ATLP.ATXR_DEST_ID
AND ATLT.ATLT_SEQ_NO = ATLP.ATLP_SEQ_NO
AND APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'
AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}'
"""

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", facets_query)
    .load()
)

df_strip = (
    df_FACETS
    .withColumn("APAP_ID", strip_field(F.col("APAP_ID")))
    .withColumn("ATSY_ID", strip_field(F.col("ATSY_ID")))
    .withColumn("ATLT_SEQ_NO", strip_field(F.col("ATLT_SEQ_NO")))
    .withColumn("ATXR_DEST_ID", strip_field(F.col("ATXR_DEST_ID")))
    .withColumn("ATLP_SEQ_NO", strip_field(F.col("ATLP_SEQ_NO")))
    .withColumn("ATLP_REQUEST_DT", F.date_format(F.col("ATLP_REQUEST_DT"), "yyyy-MM-dd"))
    .withColumn("ATLP_REQUEST_USUS", strip_field(F.col("ATLP_REQUEST_USUS")))
    .withColumn("ATLP_SUBMITTED_DT", F.date_format(F.col("ATLP_SUBMITTED_DT"), "yyyy-MM-dd"))
    .withColumn("ATLP_PRINTED_DT", F.date_format(F.col("ATLP_PRINTED_DT"), "yyyy-MM-dd"))
    .withColumn("ATLP_LAST_UPDATE_DT", F.date_format(F.col("ATLP_LAST_UPDATE_DT"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("ATLP_LAST_UPDATE_USUS", strip_field(F.col("ATLP_LAST_UPDATE_USUS")))
    .withColumn("ATLP_DESC", strip_field(F.col("ATLP_DESC")))
    .withColumn("ATXR_DESC", strip_field(F.col("ATXR_DESC")))
)

df_businessrules = df_strip.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        + F.lit(";")
        + F.trim(F.col("APAP_ID"))
        + F.lit(";")
        + F.trim(F.col("APLV_SEQ_NO"))
        + F.lit(";")
        + F.trim(F.col("ATSY_ID"))
        + F.lit(";")
        + F.col("ATLT_SEQ_NO")
        + F.lit(";")
        + F.trim(F.col("ATXR_DEST_ID"))
        + F.lit(";")
        + F.col("ATLP_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("APL_LVL_LTR_PRT_SK"),
    F.trim(F.col("APAP_ID")).alias("APL_ID"),
    F.trim(F.col("APLV_SEQ_NO")).alias("SEQ_NO"),
    F.trim(F.col("ATSY_ID")).alias("APL_LVL_LTR_STYLE_CD_SK"),
    F.col("ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    F.trim(F.col("ATXR_DEST_ID")).alias("LTR_DEST_ID"),
    F.col("ATLP_SEQ_NO").alias("PRT_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_LVL_LTR_SK"),
    F.col("ATLP_REQUEST_DT").alias("RQST_DT_SK"),
    F.trim(F.col("ATLP_REQUEST_USUS")).alias("RQST_USER_SK"),
    F.trim(F.col("ATLP_SUBMITTED_DT")).alias("SUBMT_DT_SK"),
    F.trim(F.col("ATLP_PRINTED_DT")).alias("PRT_DT_SK"),
    F.col("ATLP_LAST_UPDATE_DT").alias("LAST_UPDT_DTM"),
    F.when(
        (F.col("ATLP_LAST_UPDATE_USUS").isNull())
        | (F.length(F.col("ATLP_LAST_UPDATE_USUS")) == 0),
        F.lit("NA")
    ).otherwise(F.col("ATLP_LAST_UPDATE_USUS")).alias("LAST_UPDT_USER_SK"),
    F.trim(F.upper(F.col("ATXR_DESC"))).alias("EXPL_TX"),
    F.trim(F.upper(F.col("ATLP_DESC"))).alias("PRT_DESC")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_dummy = """
SELECT 
SRC_SYS_CD,
APL_ID,
SEQ_NO,
APL_LVL_LTR_STYLE_CD_SK,
LTR_SEQ_NO,
LTR_DEST_ID,
PRT_SEQ_NO,
CRT_RUN_CYC_EXCTN_SK,
APL_LVL_LTR_PRT_SK
FROM IDS.dummy_hf_apl_lvl_ltr_prt
"""

df_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_dummy)
    .load()
)

df_join = df_businessrules.alias("transform").join(
    df_lkup.alias("lkup"),
    (
        (F.col("transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
        & (F.col("transform.APL_ID") == F.col("lkup.APL_ID"))
        & (F.col("transform.SEQ_NO") == F.col("lkup.SEQ_NO"))
        & (
            F.col("transform.APL_LVL_LTR_STYLE_CD_SK")
            == F.col("lkup.APL_LVL_LTR_STYLE_CD_SK")
        )
        & (F.col("transform.LTR_SEQ_NO") == F.col("lkup.LTR_SEQ_NO"))
        & (F.col("transform.LTR_DEST_ID") == F.col("lkup.LTR_DEST_ID"))
        & (F.col("transform.PRT_SEQ_NO") == F.col("lkup.PRT_SEQ_NO"))
    ),
    how="left"
)

df_unmatched = df_join.filter(F.col("lkup.APL_LVL_LTR_PRT_SK").isNull())
df_enriched = df_unmatched.selectExpr("*")
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"APL_LVL_LTR_PRT_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))

df_matched = df_join.filter(~F.col("lkup.APL_LVL_LTR_PRT_SK").isNull())
df_matched = (
    df_matched
    .withColumn("APL_LVL_LTR_PRT_SK", F.col("lkup.APL_LVL_LTR_PRT_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_final = df_enriched.unionByName(df_matched).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle)
)

df_key = df_final.select(
    F.col("transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("transform.ERR_CT").alias("ERR_CT"),
    F.col("transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_LTR_PRT_SK").alias("APL_LVL_LTR_PRT_SK"),
    F.col("transform.APL_ID").alias("APL_ID"),
    F.col("transform.SEQ_NO").alias("SEQ_NO"),
    F.col("transform.APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK"),
    F.col("transform.LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("transform.LTR_DEST_ID").alias("LTR_DEST_ID"),
    F.col("transform.PRT_SEQ_NO").alias("PRT_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("transform.APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
    F.col("transform.RQST_DT_SK").alias("RQST_DT_SK"),
    F.col("transform.RQST_USER_SK").alias("RQST_USER_SK"),
    F.col("transform.SUBMT_DT_SK").alias("SUBMT_DT_SK"),
    F.col("transform.PRT_DT_SK").alias("PRT_DT_SK"),
    F.col("transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("transform.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("transform.EXPL_TX").alias("EXPL_TX"),
    F.col("transform.PRT_DESC").alias("PRT_DESC"),
)

df_key_rpad = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), 12, " "))
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
    .withColumn("SUBMT_DT_SK", F.rpad(F.col("SUBMT_DT_SK"), 10, " "))
    .withColumn("PRT_DT_SK", F.rpad(F.col("PRT_DT_SK"), 10, " "))
)

write_files(
    df_key_rpad,
    f"{adls_path}/key/FctsAplLvlLtrPrtExtr.AplLvlLtrPrt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.select(
    F.col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("transform.APL_ID").alias("APL_ID"),
    F.col("transform.SEQ_NO").alias("SEQ_NO"),
    F.col("transform.APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK"),
    F.col("transform.LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("transform.LTR_DEST_ID").alias("LTR_DEST_ID"),
    F.col("transform.PRT_SEQ_NO").alias("PRT_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_LTR_PRT_SK").alias("APL_LVL_LTR_PRT_SK"),
)

temp_table_updt = "STAGING.FctsAplLvlLtrPrtExtr_hf_apl_lvl_ltr_prt_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_updt}", jdbc_url_ids, jdbc_props_ids)
(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_updt)
    .mode("append")
    .save()
)

merge_sql_updt = """
MERGE INTO IDS.dummy_hf_apl_lvl_ltr_prt AS T
USING STAGING.FctsAplLvlLtrPrtExtr_hf_apl_lvl_ltr_prt_updt_temp AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.APL_ID = S.APL_ID
    AND T.SEQ_NO = S.SEQ_NO
    AND T.APL_LVL_LTR_STYLE_CD_SK = S.APL_LVL_LTR_STYLE_CD_SK
    AND T.LTR_SEQ_NO = S.LTR_SEQ_NO
    AND T.LTR_DEST_ID = S.LTR_DEST_ID
    AND T.PRT_SEQ_NO = S.PRT_SEQ_NO
)
WHEN NOT MATCHED THEN
INSERT (
    SRC_SYS_CD,
    APL_ID,
    SEQ_NO,
    APL_LVL_LTR_STYLE_CD_SK,
    LTR_SEQ_NO,
    LTR_DEST_ID,
    PRT_SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    APL_LVL_LTR_PRT_SK
)
VALUES (
    S.SRC_SYS_CD,
    S.APL_ID,
    S.SEQ_NO,
    S.APL_LVL_LTR_STYLE_CD_SK,
    S.LTR_SEQ_NO,
    S.LTR_DEST_ID,
    S.PRT_SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.APL_LVL_LTR_PRT_SK
);
"""
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

snap_query = f"""
SELECT 
APLV.APAP_ID,
APLV.APLV_SEQ_NO,
ATLT.ATSY_ID,
ATLT.ATLT_SEQ_NO,
ATLT.ATXR_DEST_ID,
ATLP.ATLP_SEQ_NO,
ATLP.ATLP_PRINTED_DT,
ATLP.ATLP_LAST_UPDATE_DT
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL APLV,
     {FacetsOwner}.CMC_APAP_APPEALS APAP,
     {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
     {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
     {FacetsOwner}.CER_ATLP_PRINT_HST ATLP
WHERE
APLV.APAP_ID = APAP.APAP_ID
AND APLV.APLV_SEQ_NO = APAP.APLV_SEQ_NO
AND APLV.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
AND ATXR.ATSY_ID = ATLT.ATSY_ID
AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
AND ATLT.ATSY_ID = ATLP.ATSY_ID
AND ATLT.ATXR_DEST_ID = ATLP.ATXR_DEST_ID
AND ATLT.ATLT_SEQ_NO = ATLP.ATLP_SEQ_NO
AND APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'
AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}'
"""

df_snapshot = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", snap_query)
    .load()
)

df_rules = df_snapshot.select(
    F.expr("GetFkeyCodes('IDS', 1, 'SOURCE SYSTEM', 'FACETS', 'X')").alias("SRC_SYS_CD_SK"),
    F.trim(strip_field(F.col("APAP_ID"))).alias("APL_ID"),
    F.col("APLV_SEQ_NO").alias("SEQ_NO"),
    F.expr("GetFkeyCodes('FACETS', 117, 'ATTACHMENT TYPE', ATSY_ID, 'X')").alias("APL_LVL_LTR_STYLE_CD_SK"),
    F.col("ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    strip_field(F.col("ATXR_DEST_ID")).alias("LTR_DEST_ID"),
    strip_field(F.col("ATLP_SEQ_NO")).alias("PRT_SEQ_NO"),
    F.date_format(F.col("ATLP_PRINTED_DT"), "yyyy-MM-dd").alias("PRT_DT_SK"),
    F.date_format(F.col("ATLP_LAST_UPDATE_DT"), "yyyy-MM-dd").alias("LAST_UPDT_DT_SK"),
)

df_b_apl_lvl_ltr_prt = (
    df_rules
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), 12, " "))
    .withColumn("PRT_DT_SK", F.rpad(F.col("PRT_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
    .select(
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "APL_LVL_LTR_STYLE_CD_SK",
        "LTR_SEQ_NO",
        "LTR_DEST_ID",
        "PRT_SEQ_NO",
        "PRT_DT_SK",
        "LAST_UPDT_DT_SK",
    )
)

write_files(
    df_b_apl_lvl_ltr_prt,
    f"{adls_path}/load/B_APL_LVL_LTR_PRT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)