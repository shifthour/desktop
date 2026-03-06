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
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                  Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                            ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  03/18/2009      3808 - BICC                           Initial development                                                            devlIDS                                 Steph Goddard                     04/02/2009
# MAGIC Prabhu ES                       2022-03-07       S2S Remediation                   MSSQL ODBC conn params added                                 IntegrateDev5		Harsha Ravuri		06-14-2022
# MAGIC Vikas Abbu                       2022-04-25       S2S Remediation                   Fixed Source SQL                                                           IntegrateDev5		Harsha Ravuri		06-14-2022

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
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# -- hf_um_svc_ltr_lkup (Scenario B: replaced with dummy table read)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_um_svc_ltr_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, UM_REF_ID, UM_SVC_LTR_SEQ_NO, LTR_SEQ_NO, ATSY_ID, LTR_DEST_ID, CRT_RUN_CYC_EXCTN_SK, UM_SVC_LTR_SK FROM {IDSOwner}.dummy_hf_um_svc_ltr"
    )
    .load()
)

# -- Facets_Source
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
        SELECT UMSV.UMUM_REF_ID,
               UMSV.UMSV_SEQ_NO,
               ATLT.ATLT_SEQ_NO,
               ATLT.ATSY_ID,
               ATLT.ATXR_DEST_ID
          FROM {FacetsOwner}.CMC_UMSV_SERVICES UMSV,
               {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
               {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
               tempdb..{DriverTable} as DRVR
         WHERE UMSV.UMUM_REF_ID = DRVR.UM_REF_ID
           AND ATXR.ATXR_SOURCE_ID = UMSV.ATXR_SOURCE_ID
           AND UMSV.ATXR_SOURCE_ID <> '1753-01-01'
           AND ATXR.ATSY_ID = ATLT.ATSY_ID
           AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
        """
    )
    .load()
)

# -- CD_MPPNG (DB2Connector / reading from IDS)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT SRC_CD, TRGT_CD FROM {IDSOwner}.CD_MPPNG WHERE src_domain_nm = 'ATTACHMENT TYPE'"
    )
    .load()
)

# -- hf_um_ip_ltr_atsy_id (Scenario C: read/write as parquet)
# Write the data from CD_MPPNG into the parquet
write_files(
    df_CD_MPPNG,
    "hf_um_ip_ltr_atsy_id.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
# Read the parquet back for lookups
df_hf_um_ip_ltr_atsy_id = spark.read.parquet("hf_um_ip_ltr_atsy_id.parquet")

# -- Snapshot (transform: primary link Facets_Source, lookup link hf_um_ip_ltr_atsy_id)
df_snapshot = (
    df_Facets_Source.alias("Transform")
    .join(
        df_hf_um_ip_ltr_atsy_id.alias("lkupAtsyId2"),
        F.col("Transform.ATSY_ID") == F.col("lkupAtsyId2.SRC_CD"),
        how="left"
    )
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("Transform.UMUM_REF_ID").alias("UM_REF_ID"),
        F.col("Transform.UMSV_SEQ_NO").alias("UM_SVC_SEQ_NO"),
        F.col("Transform.ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
        F.when(F.col("lkupAtsyId2.TRGT_CD").isNull(), F.lit("NA")).otherwise(F.col("lkupAtsyId2.TRGT_CD")).alias("ATSY_ID"),
        F.col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID")
    )
)

# -- Snapshot_File (write to B_UM_SVC_LTR.dat)
write_files(
    df_snapshot,
    f"{adls_path}/load/B_UM_SVC_LTR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Facets
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
        SELECT UMSV.UMUM_REF_ID,
               UMSV.UMSV_SEQ_NO,
               ATLT.ATLT_SEQ_NO,
               ATLT.ATSY_ID,
               ATLT.ATXR_DEST_ID,
               ATXR_CREATE_USUS,
               ATXR_LAST_UPD_USUS,
               UMSV.UMUM_REF_ID AS UM_ID,
               ATLT.ATLD_ID,
               UMSV.ATXR_SOURCE_ID AS ATCHMT_SRC_DTM,
               ATXR.ATXR_CREATE_DT,
               ATXR.ATXR_LAST_UPD_DT
          FROM {FacetsOwner}.CMC_UMSV_SERVICES UMSV,
               {FacetsOwner}.CER_ATLT_LETTER_D ATLT,
               {FacetsOwner}.CER_ATXR_ATTACH_U ATXR,
               tempdb..{DriverTable} as DRVR
         WHERE UMSV.UMUM_REF_ID = DRVR.UM_REF_ID
           AND ATXR.ATXR_SOURCE_ID = UMSV.ATXR_SOURCE_ID
           AND UMSV.ATXR_SOURCE_ID <> '1753-01-01'
           AND ATXR.ATSY_ID = ATLT.ATSY_ID
           AND ATXR.ATXR_DEST_ID = ATLT.ATXR_DEST_ID
        """
    )
    .load()
)

# -- StripField
df_strip_field = df_Facets.alias("Extract").select(
    strip_field(F.col("Extract.UMUM_REF_ID")).alias("UMUM_REF_ID"),
    F.col("Extract.UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    F.col("Extract.ATLT_SEQ_NO").alias("ATLT_SEQ_NO"),
    F.col("Extract.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    strip_field(F.col("Extract.ATSY_ID")).alias("ATSY_ID"),
    strip_field(F.col("Extract.ATXR_CREATE_USUS")).alias("ATXR_CREATE_USUS"),
    F.col("Extract.ATXR_LAST_UPD_USUS").alias("ATXR_LAST_UPD_USUS"),
    strip_field(F.col("Extract.UM_ID")).alias("UM_ID"),
    strip_field(F.col("Extract.ATLD_ID")).alias("ATLD_ID"),
    F.col("Extract.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("Extract.ATXR_CREATE_DT").alias("ATXR_CREATE_DT"),
    F.col("Extract.ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT")
)

# -- BusinessRules (primary link Strip, lookup link hf_um_ip_ltr_atsy_id)
df_BusinessRules_temp = (
    df_strip_field.alias("Strip")
    .join(
        df_hf_um_ip_ltr_atsy_id.alias("lkupAtsyId"),
        F.col("Strip.ATSY_ID") == F.col("lkupAtsyId.SRC_CD"),
        how="left"
    )
)

col_svAtsyId = F.when(
    (F.col("lkupAtsyId.TRGT_CD").isNull()) | (F.length(F.col("lkupAtsyId.TRGT_CD")) == 0),
    F.lit("NA")
).otherwise(F.col("lkupAtsyId.TRGT_CD"))

df_BusinessRules = df_BusinessRules_temp.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit("FACETS"), F.col("Strip.UMUM_REF_ID"), F.col("Strip.UMSV_SEQ_NO"), F.col("Strip.ATLT_SEQ_NO"), col_svAtsyId, F.col("Strip.ATXR_DEST_ID")).alias("PRI_KEY_STRING"),
    F.col("Strip.UMUM_REF_ID").alias("UM_REF_ID"),
    F.col("Strip.UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    F.col("Strip.ATLT_SEQ_NO").alias("ATLT_SEQ_NO"),
    col_svAtsyId.alias("ATSY_ID_TRGT"),
    F.when(
        (F.col("Strip.ATSY_ID").isNull()) | (F.length(F.col("Strip.ATSY_ID")) == 0),
        F.lit("NA")
    ).otherwise(F.col("Strip.ATSY_ID")).alias("ATSY_ID"),
    F.col("Strip.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("UM_SK"),
    F.when(
        (F.col("Strip.ATXR_CREATE_USUS").isNull()) | (F.length(F.col("Strip.ATXR_CREATE_USUS")) == 0),
        F.lit("NA")
    ).otherwise(F.col("Strip.ATXR_CREATE_USUS")).alias("ATXR_CREATE_USUS"),
    F.when(
        (F.col("Strip.ATXR_LAST_UPD_USUS").isNull()) | (F.length(F.col("Strip.ATXR_LAST_UPD_USUS")) == 0),
        F.lit("NA")
    ).otherwise(F.col("Strip.ATXR_LAST_UPD_USUS")).alias("ATXR_LAST_UPD_USUS"),
    F.col("Strip.UM_ID").alias("UM_ID"),
    F.when(
        (F.col("Strip.ATLD_ID").isNull()) | (F.length(F.col("Strip.ATLD_ID")) == 0),
        F.lit("NA")
    ).otherwise(F.col("Strip.ATLD_ID")).alias("ATLD_ID"),
    F.col("Strip.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.date_format(F.col("Strip.ATXR_CREATE_DT"), "yyyy-MM-dd").alias("ATXR_CREATE_DT"),
    F.date_format(F.col("Strip.ATXR_LAST_UPD_DT"), "yyyy-MM-dd").alias("ATXR_LAST_UPD_DT")
)

# -- PrimaryKey (primary link = df_BusinessRules, lookup link = df_hf_um_svc_ltr_lkup)
df_primaryKey_temp = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_um_svc_ltr_lkup.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.UM_REF_ID") == F.col("lkup.UM_REF_ID"),
            F.col("Transform.UMSV_SEQ_NO") == F.col("lkup.UM_SVC_LTR_SEQ_NO"),
            F.col("Transform.ATLT_SEQ_NO") == F.col("lkup.LTR_SEQ_NO"),
            F.col("Transform.ATSY_ID_TRGT") == F.col("lkup.ATSY_ID"),
            F.col("Transform.ATXR_DEST_ID") == F.col("lkup.LTR_DEST_ID")
        ],
        how="left"
    )
)

df_primaryKey = df_primaryKey_temp.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lkup.UM_SVC_LTR_SK").alias("EXISTING_UM_SVC_LTR_SK"),
    F.col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Transform.UMSV_SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.ATLT_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("EXISTING_CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("ZERO_CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("ZERO_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.UM_SK").alias("UM_SK"),
    F.col("Transform.UM_REF_ID").alias("UMUM_REF_ID"),
    F.col("Transform.UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    F.col("Transform.ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.col("Transform.ATSY_ID_TRGT").alias("ATSY_ID_TRGT"),
    F.col("Transform.ATSY_ID").alias("ATSY_ID"),
    F.col("Transform.ATXR_CREATE_USUS").alias("ATXR_CREATE_USUS"),
    F.col("Transform.ATXR_LAST_UPD_USUS").alias("ATXR_LAST_UPD_USUS"),
    F.col("Transform.UM_ID").alias("UM_ID"),
    F.col("Transform.ATLD_ID").alias("ATLD_ID"),
    F.col("Transform.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("Transform.ATXR_CREATE_DT").alias("ATXR_CREATE_DT"),
    F.col("Transform.ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT")
)

df_primaryKey = df_primaryKey.withColumn(
    "UM_IP_LTR_SK",
    F.when(
        F.col("EXISTING_UM_SVC_LTR_SK").isNotNull(),
        F.col("EXISTING_UM_SVC_LTR_SK")
    ).otherwise(F.lit(None).cast(IntegerType()))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(
        F.col("EXISTING_UM_SVC_LTR_SK").isNull(),
        F.lit(CurrRunCycle).cast(IntegerType())
    ).otherwise(F.col("EXISTING_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle).cast(IntegerType())
)

df_enriched = df_primaryKey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'UM_IP_LTR_SK',<schema>,<secret_name>)

df_key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("UM_IP_LTR_SK").alias("UM_IP_LTR_SK"),
    F.col("UM_REF_ID").alias("UM_REF_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("UM_SK").alias("UM_SK"),
    F.col("UMUM_REF_ID").alias("UMUM_REF_ID"),
    F.col("UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    F.col("ATXR_DEST_ID").alias("ATXR_DEST_ID"),
    F.col("ATSY_ID_TRGT").alias("ATSY_ID_TRGT"),
    F.col("ATSY_ID").alias("ATSY_ID"),
    F.col("ATXR_CREATE_USUS").alias("ATXR_CREATE_USUS"),
    F.col("ATXR_LAST_UPD_USUS").alias("ATXR_LAST_UPD_USUS"),
    F.col("UM_ID").alias("UM_ID"),
    F.col("ATLD_ID").alias("ATLD_ID"),
    F.col("ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("ATXR_CREATE_DT").alias("ATXR_CREATE_DT"),
    F.col("ATXR_LAST_UPD_DT").alias("ATXR_LAST_UPD_DT")
)

df_updt = df_enriched.filter(F.col("EXISTING_UM_SVC_LTR_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("UM_REF_ID").alias("UM_REF_ID"),
    F.col("SEQ_NO").alias("UM_SVC_SEQ_NO"),
    F.col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("ATSY_ID_TRGT").alias("ATSY_ID"),
    F.col("ATXR_DEST_ID").alias("LTR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("UM_IP_LTR_SK").alias("UM_SVC_LTR_SK")
)

# -- hf_um_svc_ltr (Scenario B: write to dummy table)
df_updt.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids).option("dbtable", f"{IDSOwner}.dummy_hf_um_svc_ltr").mode("append").save()

# -- IdsUmSvcLtrExtr (CSeqFileStage) writes #TmpOutFile# in directory "key"
write_files(
    df_key,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)