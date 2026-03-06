# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APLV_APP_LEVEL and CER_ATLT_LETTER_D for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Create appeals level letter data
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Steph Goddard    07/10/2007               Initial program                                                                                      devlIDS30                           Brent Leland             08-20-2007
# MAGIC Bhoomi Dasari    10/18/2007                 Added Balancing Snapshot                                         3028               devlIDS30
# MAGIC 
# MAGIC Jag Yelavarthi    2015-01-19                  Changed "IDS_SK" to "Table_Name_SK"                TFS#8431       IntegrateNewDevl                 Kalyan Neelam         2015-01-27
# MAGIC Prabhu ES           2022-02-25                MSSQL connection parameters added                         S2S                IntegrateDev5	Ken Bradmon	2022-05-19

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeals Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
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
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')  # Chosen for the dummy hashed table scenario

# Read from dummy table replacing hashed file "hf_apl_lvl_ltr" (Scenario B)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_apl_lvl_ltr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, APL_ID, SEQ_NO, APL_LVL_LTR_STYLE_CD, LTR_SEQ_NO, LTR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, APL_LVL_LTR_SK FROM IDS.dummy_hf_apl_lvl_ltr"
    )
    .load()
)

# Read from FACETS (ODBCConnector) Stage
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
facets_query = f"""
SELECT 
 APLV.APAP_ID,
 APLV.APLV_SEQ_NO,
 ATLT.ATSY_ID,
 ATLT.ATXR_DEST_ID,
 ATLT.ATLT_SEQ_NO,
 ATLT.ATLD_ID,
 ATLT.ATLT_FROM_NAME,
 ATLT.ATLT_FROM_ADDR1,
 ATLT.ATLT_FROM_ADDR2,
 ATLT.ATLT_FROM_CITY,
 ATLT.ATLT_FROM_STATE,
 ATLT.ATLT_FROM_ZIP,
 ATLT.ATLT_FROM_COUNTY,
 ATLT.ATLT_FROM_PHONE,
 ATLT.ATLT_FROM_FAX,
 ATLT.ATLT_SUBJECT,
 ATLT.ATLT_REQUEST_DT,
 ATLT.ATLT_DATA1,
 ATLT.ATLT_DATA2,
 ATLT.ATLT_DATA3,
 ATLT.ATLT_DATA4,
 ATXR.ATXR_DESC
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL APLV,
     {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
     {FacetsOwner}.CER_ATXR_ATTACH_U ATXR
WHERE APLV.APLV_LAST_UPD_DTM >= '{BeginDate}'
  AND APLV.APLV_LAST_UPD_DTM <= '{EndDate}'
  AND APLV.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
  AND ATXR.ATSY_ID = ATLT.ATSY_ID
  AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
  AND ATXR.ATSY_ID = 'LA01'
  AND ATXR.ATTB_ID = 'APLV'
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", facets_query)
    .load()
)

# Strip Stage (CTransformerStage)
df_Strip = (
    df_FACETS
    .withColumn("APAP_ID", strip_field(F.col("APAP_ID")))
    .withColumn("APLV_SEQ_NO", F.col("APLV_SEQ_NO"))
    .withColumn("ATSY_ID", strip_field(F.col("ATSY_ID")))
    .withColumn("ATXR_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("ATLT_SEQ_NO", F.col("ATLT_SEQ_NO"))
    .withColumn("ATLD_ID", strip_field(F.col("ATLD_ID")))
    .withColumn("ATLT_FROM_NAME", F.upper(strip_field(F.col("ATLT_FROM_NAME"))))
    .withColumn("ATLT_FROM_ADDR1", F.upper(strip_field(F.col("ATLT_FROM_ADDR1"))))
    .withColumn("ATLT_FROM_ADDR2", F.upper(strip_field(F.col("ATLT_FROM_ADDR2"))))
    .withColumn("ATLT_FROM_CITY", F.upper(strip_field(F.col("ATLT_FROM_CITY"))))
    .withColumn("ATLT_FROM_STATE", F.upper(strip_field(F.col("ATLT_FROM_STATE"))))
    .withColumn("ATLT_FROM_ZIP", strip_field(F.col("ATLT_FROM_ZIP")))
    .withColumn("ATLT_FROM_COUNTY", F.upper(strip_field(F.col("ATLT_FROM_COUNTY"))))
    .withColumn("ATLT_FROM_PHONE", strip_field(F.col("ATLT_FROM_PHONE")))
    .withColumn("ATLT_FROM_FAX", strip_field(F.col("ATLT_FROM_FAX")))
    .withColumn("ATLT_SUBJECT", F.upper(strip_field(F.col("ATLT_SUBJECT"))))
    .withColumn("ATLT_REQUEST_DT", F.col("ATLT_REQUEST_DT"))
    .withColumn("ATLT_DATA1", F.upper(strip_field(F.col("ATLT_DATA1"))))
    .withColumn("ATLT_DATA2", F.upper(strip_field(F.col("ATLT_DATA2"))))
    .withColumn("ATLT_DATA3", F.upper(strip_field(F.col("ATLT_DATA3"))))
    .withColumn("ATLT_DATA4", F.upper(strip_field(F.col("ATLT_DATA4"))))
    .withColumn("ATXR_DESC", F.upper(strip_field(F.col("ATXR_DESC"))))
)

# BusinessRules Stage (CTransformerStage)
df_BusinessRules = (
    df_Strip
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("svSeqNo", trim(F.col("APLV_SEQ_NO")))
    .withColumn("svAplId", trim(F.col("APAP_ID")))
    .withColumn("svStyleCd", trim(F.col("ATSY_ID")))
    .withColumn("svLtrSeqNo", F.col("ATLT_SEQ_NO"))
    .withColumn("svLtrDestID", trim(F.col("ATXR_DEST_ID")))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.col("svSrcSysCd"),
            F.col("svAplId"),
            F.col("svSeqNo"),
            F.col("svStyleCd"),
            F.col("svLtrSeqNo"),
            F.col("svLtrDestID")
        )
    )
    .withColumn("APL_LVL_LTR_SK", F.lit(0))
    .withColumn("SRC_SYS_CD_SK", F.lit(0))
    .withColumn("APL_ID", F.col("svAplId"))
    .withColumn("SEQ_NO", F.col("svSeqNo"))
    .withColumn("APL_LVL_LTR_STYLE_CD", F.col("svStyleCd"))
    .withColumn("LTR_SEQ_NO", F.col("svLtrSeqNo"))
    .withColumn("LTR_DEST_ID", F.col("svLtrDestID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("APL_LVL_SK", F.lit(0))
    .withColumn("APL_LVL_LTR_TYP_CD", trim(F.col("ATLD_ID")))
    .withColumn("RQST_DT_SK", F.substring(F.col("ATLT_REQUEST_DT"), 1, 10))
    .withColumn("ADDREE_NM", trim(F.col("ATLT_FROM_NAME")))
    .withColumn("ADDREE_ADDR_LN_1", trim(F.col("ATLT_FROM_ADDR1")))
    .withColumn("ADDREE_ADDR_LN_2", trim(F.col("ATLT_FROM_ADDR2")))
    .withColumn("ADDREE_CITY_NM", trim(F.col("ATLT_FROM_CITY")))
    .withColumn("APL_LVL_LTR_ADDREE_ST_CD", trim(F.col("ATLT_FROM_STATE")))
    .withColumn("ADDREE_POSTAL_CD", trim(F.col("ATLT_FROM_ZIP")))
    .withColumn("ADDREE_CNTY_NM", trim(F.col("ATLT_FROM_COUNTY")))
    .withColumn("ADDREE_PHN_NO", trim(F.col("ATLT_FROM_PHONE")))
    .withColumn("ADDREE_FAX_NO", trim(F.col("ATLT_FROM_FAX")))
    .withColumn("EXPL_TX", trim(F.col("ATXR_DESC")))
    .withColumn("LTR_TX_1", trim(F.col("ATLT_DATA1")))
    .withColumn("LTR_TX_2", trim(F.col("ATLT_DATA2")))
    .withColumn("LTR_TX_3", trim(F.col("ATLT_DATA3")))
    .withColumn("LTR_TX_4", trim(F.col("ATLT_DATA4")))
    .withColumn("SUBJ_TX", trim(F.col("ATLT_SUBJECT")))
)

# PrimaryKey Stage (CTransformerStage) - left join with df_hf_apl_lvl_ltr
join_cond = [
    F.col("BusinessRules.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
    F.col("BusinessRules.APL_ID") == F.col("lkup.APL_ID"),
    F.col("BusinessRules.SEQ_NO") == F.col("lkup.SEQ_NO"),
    F.col("BusinessRules.APL_LVL_LTR_STYLE_CD") == F.col("lkup.APL_LVL_LTR_STYLE_CD"),
    F.col("BusinessRules.LTR_SEQ_NO") == F.col("lkup.LTR_SEQ_NO"),
    F.col("BusinessRules.LTR_DEST_ID") == F.col("lkup.LTR_DEST_ID")
]

df_enriched = (
    df_BusinessRules.alias("BusinessRules")
    .join(df_hf_apl_lvl_ltr.alias("lkup"), join_cond, how="left")
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup.APL_LVL_LTR_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "SK",
        F.when(F.col("lkup.APL_LVL_LTR_SK").isNull(), F.lit(None))
         .otherwise(F.col("lkup.APL_LVL_LTR_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

# SurrogateKeyGen call for the SK that replaces KeyMgtGetNextValueConcurrent
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# Prepare separate dataframes for the two output links from PrimaryKey

# 1) "updt" link => constraint IsNull(lkup.APL_LVL_LTR_SK) = True => new inserts
df_updt = (
    df_enriched
    .filter(F.col("lkup.APL_LVL_LTR_SK").isNull())
    .select(
        F.col("BusinessRules.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("BusinessRules.APL_ID").alias("APL_ID"),
        F.col("BusinessRules.SEQ_NO").alias("SEQ_NO"),
        F.col("BusinessRules.APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
        F.col("BusinessRules.LTR_SEQ_NO").alias("LTR_SEQ_NO"),
        F.col("BusinessRules.LTR_DEST_ID").alias("LTR_DEST_ID"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("SK").alias("APL_LVL_LTR_SK")
    )
)

# Write df_updt to a staging table, then MERGE into dummy_hf_apl_lvl_ltr
temp_table_updt = "STAGING.FctsAplLvlLtrExtr_hf_apl_lvl_ltr_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_updt}", jdbc_url_ids, jdbc_props_ids)

df_updt.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_updt,
    mode="overwrite",
    properties=jdbc_props_ids
)

merge_sql_updt = f"""
MERGE IDS.dummy_hf_apl_lvl_ltr AS T
USING {temp_table_updt} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.APL_ID = S.APL_ID
    AND T.SEQ_NO = S.SEQ_NO
    AND T.APL_LVL_LTR_STYLE_CD = S.APL_LVL_LTR_STYLE_CD
    AND T.LTR_SEQ_NO = S.LTR_SEQ_NO
    AND T.LTR_DEST_ID = S.LTR_DEST_ID

WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    APL_ID,
    SEQ_NO,
    APL_LVL_LTR_STYLE_CD,
    LTR_SEQ_NO,
    LTR_DEST_ID,
    CRT_RUN_CYC_EXCTN_SK,
    APL_LVL_LTR_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.APL_ID,
    S.SEQ_NO,
    S.APL_LVL_LTR_STYLE_CD,
    S.LTR_SEQ_NO,
    S.LTR_DEST_ID,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.APL_LVL_LTR_SK
  )
;
"""
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# 2) "Key" link => all rows => to "IdsAplLvlLtr" stage
df_key = (
    df_enriched
    .select(
        F.col("BusinessRules.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("BusinessRules.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("BusinessRules.DISCARD_IN").alias("DISCARD_IN"),
        F.col("BusinessRules.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("BusinessRules.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("BusinessRules.ERR_CT").alias("ERR_CT"),
        F.col("BusinessRules.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("BusinessRules.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("BusinessRules.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("APL_LVL_LTR_SK"),
        F.col("BusinessRules.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("BusinessRules.APL_ID").alias("APL_ID"),
        F.col("BusinessRules.SEQ_NO").alias("SEQ_NO"),
        F.col("BusinessRules.APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
        F.col("BusinessRules.LTR_SEQ_NO").alias("LTR_SEQ_NO"),
        F.col("BusinessRules.LTR_DEST_ID").alias("LTR_DEST_ID"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BusinessRules.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("BusinessRules.APL_LVL_LTR_TYP_CD").alias("APL_LVL_LTR_TYP_CD"),
        F.col("BusinessRules.RQST_DT_SK").alias("RQST_DT_SK"),
        F.col("BusinessRules.ADDREE_NM").alias("ADDREE_NM"),
        F.col("BusinessRules.ADDREE_ADDR_LN_1").alias("ADDREE_ADDR_LN_1"),
        F.col("BusinessRules.ADDREE_ADDR_LN_2").alias("ADDREE_ADDR_LN_2"),
        F.col("BusinessRules.ADDREE_CITY_NM").alias("ADDREE_CITY_NM"),
        F.col("BusinessRules.APL_LVL_LTR_ADDREE_ST_CD").alias("APL_LVL_LTR_ADDREE_ST_CD"),
        F.col("BusinessRules.ADDREE_POSTAL_CD").alias("ADDREE_POSTAL_CD"),
        F.col("BusinessRules.ADDREE_CNTY_NM").alias("ADDREE_CNTY_NM"),
        F.col("BusinessRules.ADDREE_PHN_NO").alias("ADDREE_PHN_NO"),
        F.col("BusinessRules.ADDREE_FAX_NO").alias("ADDREE_FAX_NO"),
        F.col("BusinessRules.EXPL_TX").alias("EXPL_TX"),
        F.col("BusinessRules.LTR_TX_1").alias("LTR_TX_1"),
        F.col("BusinessRules.LTR_TX_2").alias("LTR_TX_2"),
        F.col("BusinessRules.LTR_TX_3").alias("LTR_TX_3"),
        F.col("BusinessRules.LTR_TX_4").alias("LTR_TX_4"),
        F.col("BusinessRules.SUBJ_TX").alias("SUBJ_TX")
    )
)

# "IdsAplLvlLtr" Stage => final file output "FctsAplLvlLtrExtr.AplLvlLtr.dat.#RunID#"
# Before writing, apply rpad() to char/varchar columns that have an explicit length in the JSON.

df_key_final = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("APL_LVL_LTR_STYLE_CD", F.rpad(F.col("APL_LVL_LTR_STYLE_CD"), 10, " "))
    .withColumn("APL_LVL_LTR_TYP_CD", F.rpad(F.col("APL_LVL_LTR_TYP_CD"), 10, " "))
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
    .withColumn("APL_LVL_LTR_ADDREE_ST_CD", F.rpad(F.col("APL_LVL_LTR_ADDREE_ST_CD"), 10, " "))
)
idsAplLvlLtr_file_path = f"{adls_path}/key/FctsAplLvlLtrExtr.AplLvlLtr.dat.{RunID}"
write_files(
    df_key_final,
    idsAplLvlLtr_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# SnapShot_FACETS Stage
snap_facets_query = f"""
SELECT 
 APLV.APAP_ID,
 ATLT.ATSY_ID,
 ATLT.ATXR_DEST_ID,
 ATLT.ATLT_SEQ_NO,
 ATLT.ATLT_REQUEST_DT
FROM {FacetsOwner}.CMC_APLV_APP_LEVEL APLV,
     {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
     {FacetsOwner}.CER_ATXR_ATTACH_U ATXR
WHERE APLV.APLV_LAST_UPD_DTM >= '{BeginDate}'
  AND APLV.APLV_LAST_UPD_DTM <= '{EndDate}'
  AND APLV.ATXR_SOURCE_ID = ATXR.ATXR_SOURCE_ID
  AND ATXR.ATSY_ID = ATLT.ATSY_ID
  AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
  AND ATXR.ATSY_ID = 'LA01'
  AND ATXR.ATTB_ID = 'APLV'
"""
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", snap_facets_query)
    .load()
)

# Rules Stage
df_Rules = (
    df_SnapShot_FACETS
    .withColumn("svSrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.lit("FACETS"), F.lit("X")))
    .withColumn("svAplLvlLtrStyCdSk", GetFkeyCodes(F.lit("FACETS"), F.lit(115), F.lit("ATTACHMENT TYPE"), F.col("ATSY_ID"), F.lit("X")))
    .withColumn("SRC_SYS_CD_SK", F.col("svSrcSysCdSk"))
    .withColumn("APL_ID", trim(strip_field(F.col("APAP_ID"))))
    .withColumn("SEQ_NO", F.col("ATLT_SEQ_NO"))
    .withColumn("APL_LVL_LTR_STYLE_CD_SK", F.col("svAplLvlLtrStyCdSk"))
    .withColumn("LTR_SEQ_NO", F.col("ATLT_SEQ_NO"))
    .withColumn("LTR_DEST_ID", F.col("ATXR_DEST_ID"))
    .withColumn("RQST_DT_SK", F.substring(F.col("ATLT_REQUEST_DT"), 1, 10))
)

# "B_APL_LVL_LTR" => final file output
df_B_APL_LVL_LTR = (
    df_Rules
    .select(
        F.col("SRC_SYS_CD_SK"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID"),
        F.col("RQST_DT_SK")
    )
)

# Apply rpad to columns with type=char in the JSON if known length
df_B_APL_LVL_LTR_final = (
    df_B_APL_LVL_LTR
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
)

file_path_b_apl_lvl_ltr = f"{adls_path}/load/B_APL_LVL_LTR.dat"
write_files(
    df_B_APL_LVL_LTR_final,
    file_path_b_apl_lvl_ltr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)