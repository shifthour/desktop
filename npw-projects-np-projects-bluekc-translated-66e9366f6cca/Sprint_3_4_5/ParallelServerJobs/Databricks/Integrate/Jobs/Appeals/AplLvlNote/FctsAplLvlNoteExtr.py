# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  FctsAplLvlNoteExtr
# MAGIC Called by:    FctsAplExtrSeq
# MAGIC 
# MAGIC Processing:   IDS Appeals Level Note Facets extract
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Reset BeginDate, if necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                Development                             Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Environment                             Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------                        ------------------------    -------------------------   
# MAGIC Hugh Sisson    07/25/2007   3028              Original program                                                                                                           Steph Goddard          9/6/07
# MAGIC Bhoomi Dasari 10/18/2007   3028              Added Balancing Snapshot                         
# MAGIC 
# MAGIC Jag Yelavarthi   2015-01-19  TFS#8431     Changed "IDS_SK" to "Table_Name_SK"                    IntegrateNewDevl                     Kalyan Neelam   2015-01-27
# MAGIC Prabhu ES         2022-02-25  S2S               MSSQL connection parameters added                          IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-05-19

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunDate = get_widget_value('RunDate','')
RunID = get_widget_value('RunID','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_apl_lvl_note = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query","SELECT SRC_SYS_CD, APL_ID, LVL_SEQ_NO, NOTE_SEQ_NO, ATXR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, APL_LVL_NOTE_SK FROM dummy_hf_apl_lvl_note")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = (
    "SELECT DISTINCT CAAL.APAP_ID, CAAL.APLV_SEQ_NO, CAND.ATNT_SEQ_NO, CAND.ATXR_DEST_ID, CAAU.ATXR_DESC "
    f"FROM {FacetsOwner}.CMC_APAP_APPEALS CAA, {FacetsOwner}.CMC_APLV_APP_LEVEL CAAL, "
    f"{FacetsOwner}.CER_ATXR_ATTACH_U CAAU, {FacetsOwner}.CER_ATNT_NOTE_D CAND "
    "WHERE CAA.APAP_ID = CAAL.APAP_ID "
    "AND CAAL.ATXR_SOURCE_ID = CAAU.ATXR_SOURCE_ID "
    "AND CAAU.ATSY_ID = CAND.ATSY_ID "
    "AND CAAU.ATXR_DEST_ID = CAND.ATXR_DEST_ID "
    f"AND ((CAA.APAP_LAST_UPD_DTM >= '{BeginDate}' AND CAA.APAP_LAST_UPD_DTM <= '{EndDate}') "
    f"OR (CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}'))"
)
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
            F.col("APAP_ID").isNull() | (F.length(F.trim(F.regexp_replace(F.col("APAP_ID"), "[\n\r\t]", ""))) == 0),
            F.lit("")
        ).otherwise(F.upper(F.trim(F.regexp_replace(F.col("APAP_ID"), "[\n\r\t]", ""))))
    )
    .withColumn(
        "ATXR_DESC",
        F.when(
            F.col("ATXR_DESC").isNull() | (F.length(F.trim(F.regexp_replace(F.col("ATXR_DESC"), "[\n\r\t]", ""))) == 0),
            F.lit("")
        ).otherwise(F.upper(F.trim(F.regexp_replace(F.col("ATXR_DESC"), "[\n\r\t]", ""))))
    )
    .select(
        F.col("APAP_ID"),
        F.col("APLV_SEQ_NO"),
        F.col("ATNT_SEQ_NO"),
        F.col("ATXR_DEST_ID"),
        F.col("ATXR_DESC")
    )
)

df_BusinessRules = (
    df_StripF
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS"), F.lit(";"),
            F.trim(F.col("APAP_ID")), F.lit(";"),
            F.col("APLV_SEQ_NO"), F.lit(";"),
            F.col("ATNT_SEQ_NO"), F.lit(";"),
            F.col("ATXR_DEST_ID")
        )
    )
    .withColumn("APL_LVL_NOTE_SK", F.lit(0))
    .withColumn("APL_ID", F.trim(F.col("APAP_ID")))
    .withColumn("LVL_SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn("NOTE_SEQ_NO", F.col("ATNT_SEQ_NO"))
    .withColumn("ATXR_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("APL_LVL_SK", F.lit(0))
    .withColumn("ATXR_DESC", F.col("ATXR_DESC"))
)

df_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_apl_lvl_note.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
            & (F.col("Transform.APL_ID") == F.col("lkup.APL_ID"))
            & (F.col("Transform.LVL_SEQ_NO") == F.col("lkup.LVL_SEQ_NO"))
            & (F.col("Transform.NOTE_SEQ_NO") == F.col("lkup.NOTE_SEQ_NO"))
            & (F.col("Transform.ATXR_DEST_ID") == F.col("lkup.ATXR_DEST_ID"))
        ),
        "left"
    )
)

df_primaryKey = (
    df_join
    .withColumn(
        "APL_LVL_NOTE_SK",
        F.when(F.col("lkup.APL_LVL_NOTE_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.APL_LVL_NOTE_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.APL_LVL_NOTE_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "NEW_CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.APL_LVL_NOTE_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = df_primaryKey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"APL_LVL_NOTE_SK",<schema>,<secret_name>)

df_updt = df_enriched.filter(F.col("lkup.APL_LVL_NOTE_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.LVL_SEQ_NO").alias("LVL_SEQ_NO"),
    F.col("Transform.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK")
)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.LVL_SEQ_NO").alias("LVL_SEQ_NO"),
    F.col("Transform.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    F.col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("Transform.ATXR_DESC").alias("NOTE_SUM_TX"),
    F.col("NEW_CRT_RUN_CYC_EXCTN_SK").alias("NewCrtRunCycExtcnSk")
)

df_IdsAplLvlNoteExtr = (
    df_Key
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.rpad(F.col("SRC_SYS_CD"),10," ").alias("SRC_SYS_CD"),
        F.rpad(F.col("PRI_KEY_STRING"),20," ").alias("PRI_KEY_STRING"),
        F.col("APL_LVL_NOTE_SK"),
        F.rpad(F.col("APL_ID"),12," ").alias("APL_ID"),
        F.col("LVL_SEQ_NO"),
        F.col("NOTE_SEQ_NO"),
        F.col("ATXR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_SK"),
        F.rpad(F.col("NOTE_SUM_TX"),70," ").alias("NOTE_SUM_TX")
    )
)

write_files(
    df_IdsAplLvlNoteExtr,
    f"{adls_path}/key/FctsAplLvlNoteExtr.AplLvlNote.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsAplLvlNoteExtr_hf_apl_lvl_note_updt_temp", jdbc_url_ids, jdbc_props_ids)
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsAplLvlNoteExtr_hf_apl_lvl_note_updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_updt = (
    "MERGE dummy_hf_apl_lvl_note AS T "
    "USING STAGING.FctsAplLvlNoteExtr_hf_apl_lvl_note_updt_temp AS S "
    "ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    "AND T.APL_ID = S.APL_ID "
    "AND T.LVL_SEQ_NO = S.LVL_SEQ_NO "
    "AND T.NOTE_SEQ_NO = S.NOTE_SEQ_NO "
    "AND T.ATXR_DEST_ID = S.ATXR_DEST_ID "
    "WHEN MATCHED THEN UPDATE SET "
    "T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    "T.APL_LVL_NOTE_SK = S.APL_LVL_NOTE_SK "
    "WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, APL_ID, LVL_SEQ_NO, NOTE_SEQ_NO, ATXR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, APL_LVL_NOTE_SK) "
    "VALUES (S.SRC_SYS_CD, S.APL_ID, S.LVL_SEQ_NO, S.NOTE_SEQ_NO, S.ATXR_DEST_ID, S.CRT_RUN_CYC_EXCTN_SK, S.APL_LVL_NOTE_SK);"
)
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

extract_query_snapshots = (
    "SELECT DISTINCT CAAL.APAP_ID, CAAL.APLV_SEQ_NO, CAND.ATNT_SEQ_NO, CAND.ATXR_DEST_ID "
    f"FROM {FacetsOwner}.CMC_APAP_APPEALS CAA, {FacetsOwner}.CMC_APLV_APP_LEVEL CAAL, "
    f"{FacetsOwner}.CER_ATXR_ATTACH_U CAAU, {FacetsOwner}.CER_ATNT_NOTE_D CAND "
    "WHERE CAA.APAP_ID = CAAL.APAP_ID "
    "AND CAAL.ATXR_SOURCE_ID = CAAU.ATXR_SOURCE_ID "
    "AND CAAU.ATSY_ID = CAND.ATSY_ID "
    "AND CAAU.ATXR_DEST_ID = CAND.ATXR_DEST_ID "
    f"AND ((CAA.APAP_LAST_UPD_DTM >= '{BeginDate}' AND CAA.APAP_LAST_UPD_DTM <= '{EndDate}') "
    f"OR (CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}'))"
)
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_snapshots)
    .load()
)

df_Rules = (
    df_SnapShot_FACETS.alias("FacetsIn")
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS",1,"SOURCE SYSTEM","FACETS","X"))
    .withColumn(
        "svAplLvlSk",
        GetFkeyAplLvl("FACETS",118,F.col("FacetsIn.APAP_ID"),F.col("FacetsIn.APLV_SEQ_NO"),"X")
    )
    .withColumn(
        "APL_ID",
        F.when(
            F.col("FacetsIn.APAP_ID").isNull()
            | (F.length(F.trim(F.regexp_replace(F.col("FacetsIn.APAP_ID"), "[\n\r\t]", ""))) == 0),
            F.lit("")
        ).otherwise(F.upper(F.trim(F.regexp_replace(F.col("FacetsIn.APAP_ID"), "[\n\r\t]", ""))))
    )
    .select(
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("FacetsIn.APLV_SEQ_NO").alias("LVL_SEQ_NO"),
        F.col("FacetsIn.ATNT_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("FacetsIn.ATXR_DEST_ID").alias("NOTE_DEST_ID"),
        F.col("svAplLvlSk").alias("APL_LVL_SK")
    )
)

df_B_APL_LVL_NOTE = df_Rules.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("APL_ID"),12," ").alias("APL_ID"),
    F.col("LVL_SEQ_NO"),
    F.col("NOTE_SEQ_NO"),
    F.col("NOTE_DEST_ID"),
    F.col("APL_LVL_SK")
)

write_files(
    df_B_APL_LVL_NOTE,
    f"{adls_path}/load/B_APL_LVL_NOTE.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)