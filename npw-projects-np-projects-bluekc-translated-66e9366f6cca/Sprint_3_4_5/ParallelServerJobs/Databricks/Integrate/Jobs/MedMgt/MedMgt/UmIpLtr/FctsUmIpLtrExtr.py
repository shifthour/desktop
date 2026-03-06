# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                         Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                   ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  03/17/2009      3808 - BICC                           Initial development                                                         devlIDS                           Steph Goddard                      04/02/2009
# MAGIC Prabhu ES                      2022-03-07        S2S Remediation                   MSSQL ODBC conn params added                        IntegrateDev5		Harsha Ravuri		06-14-2022
# MAGIC Vikas Abbu                      2022-04-25        S2S Remediation                   Fixed Source SQL                                               IntegrateDev5		Harsha Ravuri		06-14-2022

# MAGIC Balancing snapshot of source table
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# 1) Retrieve parameter values
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','IdsUmIpLtrExtr.dat')
CurrRunCycle = get_widget_value('CurrRunCycle','666')
DriverTable = get_widget_value('DriverTable','TMP_IDS_UM')
RunID = get_widget_value('RunID','200903232009')
CurrDate = get_widget_value('CurrDate','2009-03-23')
SrcSysCdSk = get_widget_value('SrcSysCdSk','666')

# 2) Read-modify-write hashed file scenario (Scenario B) for "hf_um_ip_ltr_lkup" / "hf_um_ip_ltr"
#    Treat this as a dummy table "dummy_hf_um_ip_ltr" in the IDS database
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_hf_um_ip_ltr_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, UM_REF_ID, LTR_SEQ_NO, ATSY_ID, ATXR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, UM_IP_LTR_SK FROM dummy_hf_um_ip_ltr")
    .load()
)

# 3) Facets_Source (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT UMIN.UMUM_REF_ID,
                  ATLT.ATLT_SEQ_NO AS UMSV_SEQ_NO,
                  ATLT.ATSY_ID,
                  ATLT.ATXR_DEST_ID
             FROM {FacetsOwner}.CMC_UMIN_INPATIENT UMIN,
                  {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
                  {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
                  tempdb..{DriverTable} as DRVR
            WHERE UMIN.UMUM_REF_ID = DRVR.UM_REF_ID
              AND ATXR.ATXR_SOURCE_ID = UMIN.ATXR_SOURCE_ID
              AND UMIN.ATXR_SOURCE_ID <> '1753-01-01'
              AND ATXR.ATSY_ID = ATLT.ATSY_ID
              AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID"""
    )
    .load()
)

# 4) CD_MPPNG (DB2Connector) => read from IDS
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT SRC_CD,TRGT_CD FROM {IDSOwner}.CD_MPPNG WHERE src_domain_nm = 'ATTACHMENT TYPE'\n\n{IDSOwner}.CD_MPPNG")
    .load()
)

# 5) hf_um_ip_ltr_atsy_id (CHashedFileStage, Scenario A) => deduplicate on key columns => "SRC_CD"
df_hf_um_ip_ltr_atsy_id = dedup_sort(df_CD_MPPNG, ["SRC_CD"], [("SRC_CD","A")])

# 6) Snapshot (CTransformerStage) with left lookup lkupAtsyId2 => df_hf_um_ip_ltr_atsy_id
df_Snapshot_intermediate = df_Facets_Source.alias("Transform").join(
    df_hf_um_ip_ltr_atsy_id.alias("lkupAtsyId2"),
    col("Transform.ATSY_ID") == col("lkupAtsyId2.SRC_CD"),
    "left"
)

df_Snapshot = (
    df_Snapshot_intermediate
    .withColumn(
        "svAtsyId",
        when(
            length(trim(col("lkupAtsyId2.TRGT_CD"))) == 0,
            lit("NA")
        ).otherwise(col("lkupAtsyId2.TRGT_CD"))
    )
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .select(
        col("SRC_SYS_CD_SK"),
        col("Transform.UMUM_REF_ID").alias("UM_REF_ID"),
        col("Transform.UMSV_SEQ_NO").alias("UM_SVC_SEQ_NO"),
        col("svAtsyId").alias("ATSY_ID"),
        col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID")
    )
)

# Apply rpad for any char columns
df_Snapshot = (
    df_Snapshot
    .withColumn("UM_REF_ID", rpad("UM_REF_ID", 9, " "))
    .withColumn("ATSY_ID", rpad("ATSY_ID", 4, " "))
)

# 7) Snapshot_File (CSeqFileStage)
write_files(
    df_Snapshot,
    f"{adls_path}/load/B_UM_IP_LTR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# 8) Facets (ODBCConnector)
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT UMIN.UMUM_REF_ID,
                  ATLT.ATLT_SEQ_NO AS UMSV_SEQ_NO,
                  ATLT.ATSY_ID,
                  ATLT.ATXR_DEST_ID,
                  ATXR_CREATE_USUS,
                  ATXR_LAST_UPD_USUS,
                  UMIN.UMUM_REF_ID AS UM_ID,
                  ATLT.ATLD_ID,
                  UMIN.ATXR_SOURCE_ID AS ATCHMT_SRC_DTM,
                  ATXR.ATXR_CREATE_DT,
                  ATXR.ATXR_LAST_UPD_DT
             FROM {FacetsOwner}.CMC_UMIN_INPATIENT UMIN,
                  {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
                  {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
                  tempdb..{DriverTable} as DRVR
            WHERE UMIN.UMUM_REF_ID = DRVR.UM_REF_ID
              AND ATXR.ATXR_SOURCE_ID = UMIN.ATXR_SOURCE_ID
              AND UMIN.ATXR_SOURCE_ID <> '1753-01-01'
              AND ATXR.ATSY_ID = ATLT.ATSY_ID
              AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID"""
    )
    .load()
)

# 9) StripField (CTransformerStage)
df_Strip = (
    df_Facets.alias("Extract")
    .withColumn("UMUM_REF_ID", strip_field(col("Extract.UMUM_REF_ID")))
    .withColumn("UMSV_SEQ_NO", col("Extract.UMSV_SEQ_NO"))
    .withColumn("ATXR_DEST_ID", col("Extract.ATXR_DEST_ID"))
    .withColumn("ATSY_ID", strip_field(col("Extract.ATSY_ID")))
    .withColumn("ATXR_CREATE_USUS", strip_field(col("Extract.ATXR_CREATE_USUS")))
    .withColumn("ATXR_LAST_UPD_USUS", col("Extract.ATXR_LAST_UPD_USUS"))
    .withColumn("UM_ID", strip_field(col("Extract.UM_ID")))
    .withColumn("ATLD_ID", strip_field(col("Extract.ATLD_ID")))
    .withColumn("ATCHMT_SRC_DTM", col("Extract.ATCHMT_SRC_DTM"))
    .withColumn("ATXR_CREATE_DT", col("Extract.ATXR_CREATE_DT"))
    .withColumn("ATXR_LAST_UPD_DT", col("Extract.ATXR_LAST_UPD_DT"))
)

# 10) BusinessRules (CTransformerStage) with left lookup lkupAtsyId => df_hf_um_ip_ltr_atsy_id
df_BusinessRules_joined = df_Strip.alias("Strip").join(
    df_hf_um_ip_ltr_atsy_id.alias("lkupAtsyId"),
    col("Strip.ATSY_ID") == col("lkupAtsyId.SRC_CD"),
    "left"
)

df_BusinessRules = (
    df_BusinessRules_joined
    .withColumn("RowPassThru", lit("Y"))
    .withColumn(
        "svAtsyId",
        when(
            length(trim(col("lkupAtsyId.TRGT_CD"))) == 0,
            lit("NA")
        ).otherwise(col("lkupAtsyId.TRGT_CD"))
    )
)

df_BusinessRules_out = df_BusinessRules.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    F.concat_ws(";", lit("FACETS"), col("Strip.UMUM_REF_ID"), col("Strip.UMSV_SEQ_NO"), col("svAtsyId"), col("Strip.ATXR_DEST_ID")).alias("PRI_KEY_STRING"),
    col("Strip.UMUM_REF_ID").alias("UM_REF_ID"),
    col("Strip.UMSV_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("UM_SK"),
    col("Strip.UMUM_REF_ID").alias("UMUM_REF_ID"),
    col("Strip.UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    col("Strip.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    col("svAtsyId").alias("ATSY_ID_TRGT"),
    col("Strip.ATSY_ID").alias("ATSY_ID"),
    when(length(trim(col("Strip.ATXR_CREATE_USUS"))) == 0, lit("NA")).otherwise(col("Strip.ATXR_CREATE_USUS")).alias("ATXR_CREATE_USUS"),
    when(length(trim(col("Strip.ATXR_LAST_UPD_USUS"))) == 0, lit("NA")).otherwise(col("Strip.ATXR_LAST_UPD_USUS")).alias("ATXR_LAST_UPD_USUS"),
    col("Strip.UM_ID").alias("UM_ID"),
    when(length(col("Strip.ATLD_ID")) == 0, lit("NA")).otherwise(col("Strip.ATLD_ID")).alias("ATLD_ID"),
    col("Strip.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.date_format(col("Strip.ATXR_CREATE_DT"), "yyyy-MM-dd").alias("ATXR_CREATE_DT"),
    F.date_format(col("Strip.ATXR_LAST_UPD_DT"), "yyyy-MM-dd").alias("ATXR_LAST_UPD_DT")
)

# 11) PrimaryKey (CTransformerStage) => left join with df_hf_um_ip_ltr_lkup
df_PrimaryKey_joined = df_BusinessRules_out.alias("Transform").join(
    df_hf_um_ip_ltr_lkup.alias("lkup"),
    (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) &
    (col("Transform.UM_REF_ID") == col("lkup.UM_REF_ID")) &
    (col("Transform.UM_SVC_SEQ_NO") == col("lkup.LTR_SEQ_NO")) &
    (col("Transform.ATSY_ID_TRGT") == col("lkup.ATSY_ID")) &
    (col("Transform.ATXR_DEST_ID") == col("lkup.ATXR_DEST_ID")),
    "left"
)

df_enriched = (
    df_PrimaryKey_joined
    .withColumn("UM_IP_LTR_SK", col("lkup.UM_IP_LTR_SK"))
    .withColumn(
        "temp_CRT_RUN_CYC_EXCTN_SK",
        when(F.isnull(col("lkup.UM_IP_LTR_SK")), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

# Apply SurrogateKeyGen to fill UM_IP_LTR_SK where null
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'UM_IP_LTR_SK',<schema>,<secret_name>)

df_PrimaryKey = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("UM_IP_LTR_SK"),
    col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    col("Transform.UM_SVC_SEQ_NO").alias("SEQ_NO"),
    col("temp_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.UM_SK").alias("UM_SK"),
    col("Transform.UMUM_REF_ID").alias("UMUM_REF_ID"),
    col("Transform.UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    col("Transform.ATSY_ID_TRGT").alias("ATSY_ID_TRGT"),
    col("Transform.ATSY_ID").alias("ATSY_ID"),
    col("Transform.ATXR_CREATE_USUS").alias("ATXR_CREATE_USUS"),
    col("Transform.ATXR_LAST_UPD_USUS").alias("ATXR_LAST_UPD_USUS"),
    col("Transform.UM_ID").alias("UM_ID"),
    col("Transform.ATLD_ID").alias("ATLD_ID"),
    col("Transform.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    col("Transform.ATXR_CREATE_DT").alias("ATXR_CREATE_DT"),
    col("Transform.ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT")
)

# 12) hf_um_ip_ltr (CHashedFileStage, scenario B) => only new rows (constraint IsNull(lkup.UM_IP_LTR_SK)=true)
df_updt = df_enriched.filter(F.isnull(col("lkup.UM_IP_LTR_SK")))

df_updt_select = df_updt.select(
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    col("Transform.UM_SVC_SEQ_NO").alias("LTR_SEQ_NO"),
    col("Transform.ATSY_ID_TRGT").alias("ATSY_ID"),
    col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    col("temp_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("UM_IP_LTR_SK").alias("UM_IP_LTR_SK")
)

# Merge (insert-only) into dummy_hf_um_ip_ltr
execute_dml("DROP TABLE IF EXISTS STAGING.FctsUmIpLtrExtr_hf_um_ip_ltr_temp", jdbc_url_ids, jdbc_props_ids)

df_updt_select.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsUmIpLtrExtr_hf_um_ip_ltr_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO dummy_hf_um_ip_ltr AS T
USING STAGING.FctsUmIpLtrExtr_hf_um_ip_ltr_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.UM_REF_ID = S.UM_REF_ID
AND T.LTR_SEQ_NO = S.LTR_SEQ_NO
AND T.ATSY_ID = S.ATSY_ID
AND T.ATXR_DEST_ID = S.ATXR_DEST_ID
WHEN MATCHED THEN
  UPDATE SET
    -- do nothing
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, UM_REF_ID, LTR_SEQ_NO, ATSY_ID, ATXR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, UM_IP_LTR_SK)
  VALUES (S.SRC_SYS_CD, S.UM_REF_ID, S.LTR_SEQ_NO, S.ATSY_ID, S.ATXR_DEST_ID, S.CRT_RUN_CYC_EXCTN_SK, S.UM_IP_LTR_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# 13) IdsUmIpLtrExtr (CSeqFileStage) => final DataFrame from df_PrimaryKey
df_IdsUmIpLtrExtr = (
    df_PrimaryKey
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
    .withColumn("UMUM_REF_ID", rpad("UMUM_REF_ID", 9, " "))
    .withColumn("UM_ID", rpad("UM_ID", 9, " "))
    .withColumn("ATSY_ID_TRGT", rpad("ATSY_ID_TRGT", 4, " "))
    .withColumn("ATSY_ID", rpad("ATSY_ID", 4, " "))
    .withColumn("ATXR_CREATE_USUS", rpad("ATXR_CREATE_USUS", 10, " "))
    .withColumn("ATXR_LAST_UPD_USUS", rpad("ATXR_LAST_UPD_USUS", 10, " "))
    .withColumn("ATLD_ID", rpad("ATLD_ID", 8, " "))
    .withColumn("ATXR_CREATE_DT", rpad("ATXR_CREATE_DT", 10, " "))
    .withColumn("ATXR_LAST_UPD_DT", rpad("ATXR_LAST_UPD_DT", 10, " "))
)

write_files(
    df_IdsUmIpLtrExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)