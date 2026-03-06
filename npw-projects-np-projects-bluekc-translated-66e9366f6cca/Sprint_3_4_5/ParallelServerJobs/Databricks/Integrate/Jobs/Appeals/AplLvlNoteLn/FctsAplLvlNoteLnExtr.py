# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  FctsAplLvlNoteLnExtr
# MAGIC Called by:  FctsAplExtrSeq
# MAGIC 
# MAGIC Processing:  IDS Appeals Level Note Line Facets extract 
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                  Development             Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                          Environment              Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------             -----------------------  -------------------   
# MAGIC Hugh Sisson    07/25/2007   3028              Original program                                                                                                 Steph Goddard   9/6/07
# MAGIC Bhoomi Dasari 10/18/2007   3028              Added Balancing Snapshot                                                                                Steph Goddard   10/18/2007
# MAGIC Brent Leland    07-07-2010   Prod Support  Added cast to CANC.ATND_TEXT column 
# MAGIC                                                                     due to Sybase varbinary data type  
# MAGIC 
# MAGIC Jag Yelavarthi   2015-01-19    TFS#8431    Changed "IDS_SK" to "Table_Name_SK"                 IntegrateNewDevl        Kalyan Neelam       2015-01-27
# MAGIC Prabhu ES        2022-02-25     S2S              MSSQL connection parameters added                       IntegrateDev5\(9)Ken Bradmon\(9)2022-05-19

# MAGIC Balancing Snapshot
# MAGIC Trim and Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Level Status Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing sequential file to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')
RunID = get_widget_value('RunID', '')
RunDate = get_widget_value('RunDate', '')
BeginDate = get_widget_value('BeginDate', '')
EndDate = get_widget_value('EndDate', '')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = f"""
SELECT DISTINCT
    CAAL.APAP_ID,
    CAAL.APLV_SEQ_NO,
    CANC.ATNT_SEQ_NO,
    CANC.ATXR_DEST_ID,
    CANC.ATND_SEQ_NO,
    CAST(CANC.ATND_TEXT AS VARCHAR(255)) ATND_TEXT
FROM
    {FacetsOwner}.CMC_APAP_APPEALS CAA,
    {FacetsOwner}.CMC_APLV_APP_LEVEL CAAL,
    {FacetsOwner}.CER_ATXR_ATTACH_U CAAU,
    {FacetsOwner}.CER_ATNT_NOTE_D CAND,
    {FacetsOwner}.CER_ATND_NOTE_C CANC
WHERE
    CAA.APAP_ID = CAAL.APAP_ID
    AND CAAL.ATXR_SOURCE_ID = CAAU.ATXR_SOURCE_ID
    AND CAAU.ATSY_ID = CAND.ATSY_ID
    AND CAAU.ATXR_DEST_ID = CAND.ATXR_DEST_ID
    AND CAND.ATSY_ID = CANC.ATSY_ID
    AND CAND.ATXR_DEST_ID = CANC.ATXR_DEST_ID
    AND CAND.ATNT_SEQ_NO = CANC.ATNT_SEQ_NO
    AND (
         (CAA.APAP_LAST_UPD_DTM >= '{BeginDate}' AND CAA.APAP_LAST_UPD_DTM <= '{EndDate}')
         OR
         (CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}')
        )
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

df_StripF = (
    df_FACETS
    .withColumn(
        "APAP_ID",
        F.when(
            F.col("APAP_ID").isNull() |
            (F.length(trim(replace(replace(replace(F.col("APAP_ID"), "\n", ""), "\r", ""), "\t", ""))) == 0),
            F.lit("")
        ).otherwise(
            F.upper(trim(replace(replace(replace(F.col("APAP_ID"), "\n", ""), "\r", ""), "\t", "")))
        )
    )
    .withColumn("APLV_SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn("ATNT_SEQ_NO", F.col("ATNT_SEQ_NO"))
    .withColumn("ATXR_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("ATND_SEQ_NO", F.col("ATND_SEQ_NO"))
    .withColumn(
        "ATND_TEXT",
        F.when(
            F.col("ATND_TEXT").isNull() |
            (F.length(trim(replace(replace(replace(F.col("ATND_TEXT"), "\n", ""), "\r", ""), "\t", ""))) == 0),
            F.lit("")
        ).otherwise(
            F.upper(trim(replace(replace(replace(F.col("ATND_TEXT"), "\n", ""), "\r", ""), "\t", "")))
        )
    )
)

RowPassThru = F.lit("Y")
svSrcSysCd = F.lit("FACETS")
df_BusinessRules = (
    df_StripF
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", RowPassThru)
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", svSrcSysCd)
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            svSrcSysCd,
            F.trim(F.col("APAP_ID")),
            F.col("APLV_SEQ_NO"),
            F.col("ATNT_SEQ_NO"),
            F.col("ATXR_DEST_ID"),
            F.col("ATND_SEQ_NO")
        )
    )
    .withColumn("APL_LVL_NOTE_LN_SK", F.lit(0))
    .withColumn("APL_ID", F.col("APAP_ID"))
    .withColumn("LVL_SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn("NOTE_SEQ_NO", F.col("ATNT_SEQ_NO"))
    .withColumn("NOTE_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("LN_SEQ_NO", F.col("ATND_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("APL_LVL_NOTE_SK", F.lit(0))
    .withColumn("LN_TX", F.col("ATND_TEXT"))
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_hf_apl_lvl_note_ln = """
SELECT
 SRC_SYS_CD,
 APL_ID,
 LVL_SEQ_NO,
 NOTE_SEQ_NO,
 NOTE_DEST_ID,
 LN_SEQ_NO,
 CRT_RUN_CYC_EXCTN_SK,
 APL_LVL_NOTE_LN_SK
FROM IDS.dummy_hf_apl_lvl_note_ln
"""
df_hf_apl_lvl_note_ln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_hf_apl_lvl_note_ln)
    .load()
)

df_PrimaryKey_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_apl_lvl_note_ln.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.APL_ID") == F.col("lkup.APL_ID"),
            F.col("Transform.LVL_SEQ_NO") == F.col("lkup.LVL_SEQ_NO"),
            F.col("Transform.NOTE_SEQ_NO") == F.col("lkup.NOTE_SEQ_NO"),
            F.col("Transform.NOTE_DEST_ID") == F.col("lkup.NOTE_DEST_ID"),
            F.col("Transform.LN_SEQ_NO") == F.col("lkup.LN_SEQ_NO")
        ],
        how="left"
    )
)

df_PrimaryKey = (
    df_PrimaryKey_join
    .withColumn(
        "APL_LVL_NOTE_LN_SK",
        F.when(
            F.col("lkup.APL_LVL_NOTE_LN_SK").isNull(),
            F.lit(None).cast(IntegerType())
        ).otherwise(F.col("lkup.APL_LVL_NOTE_LN_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(
            F.col("lkup.APL_LVL_NOTE_LN_SK").isNull(),
            F.lit(CurrRunCycle).cast(IntegerType())
        ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = df_PrimaryKey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"APL_LVL_NOTE_LN_SK",<schema>,<secret_name>)

df_PrimaryKey_final = (
    df_enriched
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCrtRunCycExtcnSk").cast(IntegerType()))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle).cast(IntegerType()))
)

df_updt = df_PrimaryKey_final.filter(F.col("lkup.APL_LVL_NOTE_LN_SK").isNull())

temp_table_updt = "STAGING.FctsAplLvlNoteLnExtr_hf_apl_lvl_note_ln_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_updt}", jdbc_url_ids, jdbc_props_ids)
(
    df_updt.select(
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.APL_ID").alias("APL_ID"),
        F.col("Transform.LVL_SEQ_NO").alias("LVL_SEQ_NO"),
        F.col("Transform.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("Transform.NOTE_DEST_ID").alias("NOTE_DEST_ID"),
        F.col("Transform.LN_SEQ_NO").alias("LN_SEQ_NO"),
        F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_NOTE_LN_SK").alias("APL_LVL_NOTE_LN_SK")
    )
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_updt)
    .mode("overwrite")
    .save()
)
merge_sql_hf = f"""
MERGE IDS.dummy_hf_apl_lvl_note_ln as T
USING {temp_table_updt} as S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.APL_ID = S.APL_ID AND
    T.LVL_SEQ_NO = S.LVL_SEQ_NO AND
    T.NOTE_SEQ_NO = S.NOTE_SEQ_NO AND
    T.NOTE_DEST_ID = S.NOTE_DEST_ID AND
    T.LN_SEQ_NO = S.LN_SEQ_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        APL_ID,
        LVL_SEQ_NO,
        NOTE_SEQ_NO,
        NOTE_DEST_ID,
        LN_SEQ_NO,
        CRT_RUN_CYC_EXCTN_SK,
        APL_LVL_NOTE_LN_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.APL_ID,
        S.LVL_SEQ_NO,
        S.NOTE_SEQ_NO,
        S.NOTE_DEST_ID,
        S.LN_SEQ_NO,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.APL_LVL_NOTE_LN_SK
    );
"""
execute_dml(merge_sql_hf, jdbc_url_ids, jdbc_props_ids)

df_Key = df_PrimaryKey_final.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_NOTE_LN_SK").alias("APL_LVL_NOTE_LN_SK"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.LVL_SEQ_NO").alias("LVL_SEQ_NO"),
    F.col("Transform.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("Transform.NOTE_DEST_ID").alias("NOTE_DEST_ID"),
    F.col("Transform.LN_SEQ_NO").alias("LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK"),
    F.rpad(F.col("Transform.LN_TX"), 100, " ").alias("LN_TX")
)

out_file_path_IdsAplLvlNoteLnExtr = f"{adls_path}/key/FctsAplLvlNoteLnExtr.AplLvlNoteLn.dat.{RunID}"
write_files(
    df_Key,
    out_file_path_IdsAplLvlNoteLnExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_snapshot_facets = f"""
SELECT DISTINCT
    CAAL.APAP_ID,
    CAAL.APLV_SEQ_NO,
    CANC.ATNT_SEQ_NO,
    CANC.ATXR_DEST_ID,
    CANC.ATND_SEQ_NO
FROM
    {FacetsOwner}.CMC_APAP_APPEALS CAA,
    {FacetsOwner}.CMC_APLV_APP_LEVEL CAAL,
    {FacetsOwner}.CER_ATXR_ATTACH_U CAAU,
    {FacetsOwner}.CER_ATNT_NOTE_D CAND,
    {FacetsOwner}.CER_ATND_NOTE_C CANC
WHERE
    CAA.APAP_ID = CAAL.APAP_ID
    AND CAAL.ATXR_SOURCE_ID = CAAU.ATXR_SOURCE_ID
    AND CAAU.ATSY_ID = CAND.ATSY_ID
    AND CAAU.ATXR_DEST_ID = CAND.ATXR_DEST_ID
    AND CAND.ATSY_ID = CANC.ATSY_ID
    AND CAND.ATXR_DEST_ID = CANC.ATXR_DEST_ID
    AND CAND.ATNT_SEQ_NO = CANC.ATNT_SEQ_NO
    AND (
         (CAA.APAP_LAST_UPD_DTM >= '{BeginDate}' AND CAA.APAP_LAST_UPD_DTM <= '{EndDate}')
         OR
         (CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}')
        )
"""
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_snapshot_facets)
    .load()
)

df_Rules = (
    df_SnapShot_FACETS.alias("FacetsIn")
    .withColumn("SRC_SYS_CD_SK", GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X"))
    .withColumn(
        "APL_ID",
        trim(replace(replace(replace(F.col("FacetsIn.APAP_ID"), "\n", ""), "\r", ""), "\t", ""))
    )
    .withColumn("LVL_SEQ_NO", F.col("FacetsIn.APLV_SEQ_NO"))
    .withColumn("NOTE_SEQ_NO", F.col("FacetsIn.ATNT_SEQ_NO"))
    .withColumn("NOTE_DEST_ID", F.col("FacetsIn.ATXR_DEST_ID"))
    .withColumn("LN_SEQ_NO", F.col("FacetsIn.ATND_SEQ_NO"))
    .withColumn(
        "APL_LVL_NOTE_SK",
        GetFkeyAplLvlNote("FACETS", 127, F.col("FacetsIn.APAP_ID"), F.col("FacetsIn.APLV_SEQ_NO"), F.col("FacetsIn.ATNT_SEQ_NO"), F.col("FacetsIn.ATXR_DEST_ID"), "X")
    )
)

df_B_APL_LVL_NOTE_LN = df_Rules.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("APL_ID"),
    F.col("LVL_SEQ_NO"),
    F.col("NOTE_SEQ_NO"),
    F.col("NOTE_DEST_ID"),
    F.col("LN_SEQ_NO"),
    F.col("APL_LVL_NOTE_SK")
)

out_file_path_B_APL_LVL_NOTE_LN = f"{adls_path}/load/B_APL_LVL_NOTE_LN.dat"
write_files(
    df_B_APL_LVL_NOTE_LN,
    out_file_path_B_APL_LVL_NOTE_LN,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)