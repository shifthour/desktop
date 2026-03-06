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
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUMSvcSttusExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMVT_STATUS to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description				Development Project	Code Reviewer		Date Reviewed
# MAGIC -----------------------	-------------------	-----------------------	---------------------------------------------------------		----------------------------------	---------------------------------	-------------------------
# MAGIC    
# MAGIC Tracy Davis	03/6/2009	3808		Created job				devlIDS			Steph Goddard                         03/30/2009
# MAGIC Prabhu ES               2022-03-07              S2S Remediation     MSSQL ODBC conn params added                        IntegrateDev5		Harsha Ravuri		06-14-2022

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# CMC_UMVT_STATUS (ODBCConnector)
extract_query_CMC_UMVT_STATUS = f"""
SELECT 
STTUS.UMUM_REF_ID,
STTUS.UMSV_SEQ_NO,
STTUS.UMVT_SEQ_NO,
STTUS.USUS_ID,
STTUS.UMVT_STS,
STTUS.UMVT_STS_DTM,
STTUS.UMVT_MCTR_REAS
FROM {FacetsOwner}.CMC_UMVT_STATUS AS STTUS,
     tempdb..{DriverTable} as T
WHERE STTUS.UMUM_REF_ID = T.UM_REF_ID
"""
df_CMC_UMVT_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMVT_STATUS)
    .load()
)

# StripField (CTransformerStage)
df_StripField = (
    df_CMC_UMVT_STATUS
    .withColumn("UMUM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UMSV_SEQ_NO", F.col("UMSV_SEQ_NO"))
    .withColumn("UMVT_SEQ_NO", F.col("UMVT_SEQ_NO"))
    .withColumn("USUS_ID", trim(strip_field(F.col("USUS_ID"))))
    .withColumn("UMVT_STS", trim(strip_field(F.col("UMVT_STS"))))
    .withColumn("UMVT_STS_DTM", F.col("UMVT_STS_DTM"))
    .withColumn("UMVT_MCTR_REAS", trim(strip_field(F.col("UMVT_MCTR_REAS"))))
)

# BusinessRules (CTransformerStage)
df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("UM_SK", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.lit("FACETS"), F.col("UMUM_REF_ID"), F.col("UMSV_SEQ_NO"), F.col("UMVT_SEQ_NO")))
    .withColumn("UMUM_REF_ID", F.col("UMUM_REF_ID"))
    .withColumn("UMSV_SEQ_NO", F.col("UMSV_SEQ_NO"))
    .withColumn("UMVT_SEQ_NO", F.col("UMVT_SEQ_NO"))
    .withColumn("USUS_ID", F.when(F.length(F.col("USUS_ID")) == 0, F.lit("NA")).otherwise(F.col("USUS_ID")))
    .withColumn("UMVT_STS", F.when(F.length(F.col("UMVT_STS")) == 0, F.lit("NA")).otherwise(F.col("UMVT_STS")))
    .withColumn("UMVT_STS_DTM", F.col("UMVT_STS_DTM"))  
    .withColumn(
        "UMVT_MCTR_REAS",
        F.when(
            (F.length(F.col("UMVT_MCTR_REAS")) == 0) | (F.upper(F.col("UMVT_MCTR_REAS")) == F.lit("NONE")),
            F.lit("NA")
        ).otherwise(F.col("UMVT_MCTR_REAS"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
)

# hf_um_svc_sttus_lkup (CHashedFileStage) -> scenario B read from dummy table
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_hf_um_svc_sttus = """
SELECT 
SRC_SYS_CD,
UMUM_REF_ID,
UMSV_SEQ_NO,
UMVT_SEQ_NO,
CRT_RUN_CYC_EXCTN_SK,
UM_SVC_STTUS_SK
FROM IDS.dummy_hf_um_svc_sttus
"""
df_hf_um_svc_sttus_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_hf_um_svc_sttus)
    .load()
)

# PrimaryKey (CTransformerStage) - left join, surrogate key logic
df_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_um_svc_sttus_lkup.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
            (F.col("Transform.UMUM_REF_ID") == F.col("lkup.UMUM_REF_ID")) &
            (F.col("Transform.UMSV_SEQ_NO") == F.col("lkup.UMSV_SEQ_NO")) &
            (F.col("Transform.UMVT_SEQ_NO") == F.col("lkup.UMVT_SEQ_NO"))
        ),
        "left"
    )
)

df_enriched = (
    df_join
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", F.col("Transform.INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", F.col("Transform.DISCARD_IN"))
    .withColumn("PASS_THRU_IN", F.col("Transform.PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", F.col("Transform.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", F.col("Transform.ERR_CT"))
    .withColumn("RECYCLE_CT", F.col("Transform.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", F.col("Transform.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", F.col("Transform.PRI_KEY_STRING"))
    .withColumn("UMUM_REF_ID", F.col("Transform.UMUM_REF_ID"))
    .withColumn("UMSV_SEQ_NO", F.col("Transform.UMSV_SEQ_NO"))
    .withColumn("UMVT_SEQ_NO", F.col("Transform.UMVT_SEQ_NO"))
    .withColumn("USUS_ID", F.col("Transform.USUS_ID"))
    .withColumn("UMVT_STS", F.col("Transform.UMVT_STS"))
    .withColumn("UMVT_STS_DTM", F.col("Transform.UMVT_STS_DTM"))
    .withColumn("UMVT_MCTR_REAS", F.col("Transform.UMVT_MCTR_REAS"))
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.UM_SVC_STTUS_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "UM_SVC_STTUS_SK",
        F.when(F.col("lkup.UM_SVC_STTUS_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.UM_SVC_STTUS_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"UM_SVC_STTUS_SK",<schema>,<secret_name>)

df_Key = df_enriched
df_updt = df_enriched.filter(F.col("lkup.UM_SVC_STTUS_SK").isNull()).select(
    F.col("SRC_SYS_CD"),
    F.col("UMUM_REF_ID"),
    F.col("UMSV_SEQ_NO"),
    F.col("UMVT_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("UM_SVC_STTUS_SK")
)

# hf_um_svc_sttus_write (CHashedFileStage) -> scenario B write to dummy table
spark.sql("DROP TABLE IF EXISTS STAGING.FctsUMSvcSttusExtr_hf_um_svc_sttus_write_temp")
(
    df_updt
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.FctsUMSvcSttusExtr_hf_um_svc_sttus_write_temp")
    .mode("overwrite")
    .save()
)
merge_sql_hf_um_svc_sttus = """
MERGE IDS.dummy_hf_um_svc_sttus AS T
USING STAGING.FctsUMSvcSttusExtr_hf_um_svc_sttus_write_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.UMUM_REF_ID = S.UMUM_REF_ID
   AND T.UMSV_SEQ_NO = S.UMSV_SEQ_NO
   AND T.UMVT_SEQ_NO = S.UMVT_SEQ_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    UMUM_REF_ID,
    UMSV_SEQ_NO,
    UMVT_SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    UM_SVC_STTUS_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.UMUM_REF_ID,
    S.UMSV_SEQ_NO,
    S.UMVT_SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.UM_SVC_STTUS_SK
  );
"""
execute_dml(merge_sql_hf_um_svc_sttus, jdbc_url_ids, jdbc_props_ids)

# IdsUmSvcSttusExtr (CSeqFileStage)
final_select_IdsUmSvcSttusExtr = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("UMUM_REF_ID").alias("UMUM_REF_ID"),
    F.col("UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    F.col("UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
    F.rpad(F.col("USUS_ID"),10," ").alias("USUS_ID"),
    F.rpad(F.col("UMVT_STS"),2," ").alias("UMVT_STS"),
    F.col("UMVT_STS_DTM").alias("UMVT_STS_DTM"),
    F.rpad(F.col("UMVT_MCTR_REAS"),4," ").alias("UMVT_MCTR_REAS"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("UM_SVC_STTUS_SK").alias("UM_SVC_STTUS_SK")
)
write_files(
    final_select_IdsUmSvcSttusExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CMC_UMVT_STATUS_Snapshot (ODBCConnector)
extract_query_CMC_UMVT_STATUS_Snapshot = f"""
SELECT 
STTUS.UMUM_REF_ID,
STTUS.UMSV_SEQ_NO,
STTUS.UMVT_SEQ_NO
FROM {FacetsOwner}.CMC_UMVT_STATUS AS STTUS,
     tempdb..{DriverTable} as T
WHERE STTUS.UMUM_REF_ID = T.UM_REF_ID
"""
df_CMC_UMVT_STATUS_Snapshot = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMVT_STATUS_Snapshot)
    .load()
)

# StripField_Snapshot (CTransformerStage)
df_StripField_Snapshot = (
    df_CMC_UMVT_STATUS_Snapshot
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("UM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UM_SVC_SEQ_NO", F.col("UMSV_SEQ_NO"))
    .withColumn("UM_SVC_STTUS_SEQ_NO", F.col("UMVT_SEQ_NO"))
)

# B_UM_SVC_STTUS (CSeqFileStage)
df_B_UM_SVC_STTUS_select = df_StripField_Snapshot.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("UM_REF_ID"),9," ").alias("UM_REF_ID"),
    F.col("UM_SVC_SEQ_NO"),
    F.col("UM_SVC_STTUS_SEQ_NO")
)
write_files(
    df_B_UM_SVC_STTUS_select,
    f"{adls_path}/load/B_UM_SVC_STTUS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)