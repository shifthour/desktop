# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004,2008,2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmSttusAuditExtr
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC   
# MAGIC                          
# MAGIC PROCESSING:  Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                       Development                                Date 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                              Project              Code Reviewer    Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                      ----------------------   -------------------------   ------------------       
# MAGIC Steph Goddard        04/01/2004-                                   Originally Programmed 
# MAGIC SharonAndrew         06/23/2004-                                   Added source to primary key.  Changed pass-thru to "Y" 
# MAGIC Brent Leland            10/06/2004                                    Facets 4.11 changed status date column to timestamp.
# MAGIC Brent Leland            10/09/2004                                    Added Status timestamp field
# MAGIC SharonAndrew         04/13/2005-                                  changed status codes per reversal changes to 91/R
# MAGIC Ralph Tucker           05/17/2005                                   Removed field key designator on link collector for CLST_STS_DTM. 
# MAGIC Steph Goddard        02/16/2006                                    Combined extract, transform, primary key for sequencer
# MAGIC BJ Luce                   03/20/2006                                    add hf_clm_nasco_dup_bypass, identifies claims that are nasco
# MAGIC                                                                                        dups. If the claim is on the file, a row is not generated for it
# MAGIC                                                                                        in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Steph Goddard        03/29/2006                                   Changed to pull last status date time record - also changed for 
# MAGIC                                                                                       reversals instead of defaulting sequence to 1
# MAGIC Laurel Kindley          11/02/2006                                   Removed logic to pull last status date time record.  This was 
# MAGIC                                                                                       causing records to not get picked up.  
# MAGIC Brent Leland            05/02/2007      IAD Prod.Supp.    Moved FORMAT.DATE routine to stage variable in "BusinessRules"  devlIDS30
# MAGIC                                                                                       transform to cut down the number of calls to improve efficentcy.
# MAGIC Oliver Nielsen          08/20/2007      Balancing             Added Balancing Snapshot File                                                             devlIDS30
# MAGIC Bhoomi D                03/21/2008      3255                     Added one new field and renamed two fields                                        IDSCurDevl          Steph Goddard   03/31/2008
# MAGIC Hugh Sisson            07/21/2008     3567                      Changed primary key process                                                               devlIDS                Steph Goddard   08/12/2008
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2
# MAGIC 
# MAGIC Harikanth Reddy    10/12/2020                                     Brought up to standards                                                             IntegrateDev2
# MAGIC Kotha Venkat                                                               
# MAGIC 
# MAGIC Prabhu ES               2022-03-29       S2S                     MSSQL ODBC conn params added                                              IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC Pulling Facets Claim Status and Audit Data
# MAGIC Hash file (hf_clm_sttus_audit_allcol) cleared at end of the job
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Remove duplicates based on Claim ID, ClaimIDAuditSeqNum, and Source System Code,ClaimsttusAuditsk
# MAGIC This container is used in:
# MAGIC FctsClmSttusAuditExtr
# MAGIC NascoClmSttusAuditTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in ClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass,
# MAGIC do not build a regular claim row.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmSttusAuditPK
# COMMAND ----------

DriverTable = get_widget_value("DriverTable","")
CurrentDate = get_widget_value("CurrentDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
LhoFacetsStgPW = get_widget_value("LhoFacetsStgPW","")
LhoFacetsStgOwner = get_widget_value("LhoFacetsStgOwner","")
LhoFacetsStgDSN = get_widget_value("LhoFacetsStgDSN","")
LhoFacetsStgAcct = get_widget_value("LhoFacetsStgAcct","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
IDSOwner = get_widget_value("IDSOwner","")

lhofacetsstg_secret_name = get_widget_value("lhofacetsstg_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsDB = get_widget_value("FacetsDB","")
facetsdb_secret_name = get_widget_value("facetsdb_secret_name","")

# Read from hashed file hf_clm_fcts_reversals (Scenario C)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
    .select(
        "CLCL_ID",
        "CLCL_CUR_STS",
        "CLCL_PAID_DT",
        "CLCL_ID_ADJ_TO",
        "CLCL_ID_ADJ_FROM"
    )
)

# Read from hashed file clm_nasco_dup_bypass (Scenario C)
df_clm_nasco_dup_bypass = (
    spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
    .select("CLM_ID")
)

# ODBCConnector CMC_CLST_STATUS
extract_query = f"""
SELECT
  STAT.CLCL_ID,
  STAT.CLST_SEQ_NO,
  STAT.CLST_STS,
  STAT.USUS_ID,
  STAT.CLST_STS_DTM,
  STAT.CLST_MCTR_REAS,
  STAT.CLST_USID_ROUTE,
  STAT.CLMI_ITS_CUR_STS
FROM {LhoFacetsStgOwner}.CMC_CLST_STATUS STAT,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = STAT.CLCL_ID
"""
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
df_CMC_CLST_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripField Transformer
df_Strip = (
    df_CMC_CLST_STATUS
    .withColumn("CLAIM_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLAIM_STATUS_AUDIT_SEQ_NO", F.col("CLST_SEQ_NO"))
    .withColumn("SOURCE_SYS_CD", F.lit(SrcSysCd))
    .withColumn("CLST_STS_DTM", F.col("CLST_STS_DTM"))
    .withColumn("CREATE_BY_USER_ID", strip_field(trim(F.col("USUS_ID"))))
    .withColumn("ROUTE_TO_USER_ID", strip_field(trim(F.col("CLST_USID_ROUTE"))))
    .withColumn("CLAIM_STATUS_CD", strip_field(trim(F.col("CLST_STS"))))
    .withColumn("CLAIM_STTUS_CHG_REASN", strip_field(trim(F.col("CLST_MCTR_REAS"))))
    .withColumn("CLMI_ITS_CUR_STS", strip_field(trim(F.col("CLMI_ITS_CUR_STS"))))
)

# BusinessRules Transformer with 2 left lookups
df_main = df_Strip.alias("Strip")
df_rev = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_dup = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

df_joined = (
    df_main
    .join(
        df_rev,
        F.col("Strip.CLAIM_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_dup,
        F.col("Strip.CLAIM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
)

SttusDate = F.date_format(F.col("Strip.CLST_STS_DTM"), "yyyy-MM-dd")
SttusDateTime = F.col("Strip.CLST_STS_DTM")

# Output: StripOut
df_StripOut = (
    df_joined
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Strip.CLST_STS_DTM").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), trim(F.col("Strip.CLAIM_ID")), F.lit(";"), F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_STTUS_AUDIT_SK"),
        trim(F.col("Strip.CLAIM_ID")).alias("CLAIM_ID"),
        F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLAIM_STATUS_AUDIT_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.lit(0).alias("CLAIM_SK"),
        F.when(
            F.col("Strip.CREATE_BY_USER_ID").isNull() | (F.length(trim(F.col("Strip.CREATE_BY_USER_ID"))) == 0),
            "NA"
        ).otherwise(trim(F.col("Strip.CREATE_BY_USER_ID"))).alias("CREATE_BY_USER_ID"),
        F.when(
            F.col("Strip.ROUTE_TO_USER_ID").isNull() | (F.length(trim(F.col("Strip.ROUTE_TO_USER_ID"))) == 0),
            "NA"
        ).otherwise(trim(F.col("Strip.ROUTE_TO_USER_ID"))).alias("ROUTE_TO_USER_ID"),
        F.when(
            F.col("Strip.CLAIM_STATUS_CD").isNull() | (F.length(trim(F.col("Strip.CLAIM_STATUS_CD"))) == 0),
            "NA"
        ).otherwise(F.upper(trim(F.col("Strip.CLAIM_STATUS_CD")))).alias("CLAIM_STATUS_CD"),
        F.when(
            F.col("Strip.CLAIM_STTUS_CHG_REASN").isNull() | (F.length(trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))) == 0),
            "NA"
        ).otherwise(F.upper(trim(F.col("Strip.CLAIM_STTUS_CHG_REASN")))).alias("CLAIM_STTUS_CHG_REASN"),
        SttusDate.alias("CLST_STS_DT"),
        SttusDateTime.alias("CLST_STS_DTM"),
        F.when(
            F.col("Strip.CLMI_ITS_CUR_STS").isNull() | (F.length(F.col("Strip.CLMI_ITS_CUR_STS")) == 0),
            "NA"
        ).otherwise(F.col("Strip.CLMI_ITS_CUR_STS")).alias("CLMI_ITS_CUR_STS")
    )
)

# Output: reversals
df_reversals = (
    df_joined
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Strip.CLST_STS_DTM").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), trim(F.col("Strip.CLAIM_ID")), F.lit("R;"), F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_STTUS_AUDIT_SK"),
        F.concat(trim(F.col("Strip.CLAIM_ID")), F.lit("R")).alias("CLAIM_ID"),
        F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLAIM_STATUS_AUDIT_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.lit(0).alias("CLAIM_SK"),
        F.when(
            F.col("Strip.CREATE_BY_USER_ID").isNull() | (F.length(trim(F.col("Strip.CREATE_BY_USER_ID"))) == 0),
            "NA"
        ).otherwise(trim(F.col("Strip.CREATE_BY_USER_ID"))).alias("CREATE_BY_USER_ID"),
        F.when(
            F.col("Strip.ROUTE_TO_USER_ID").isNull() | (F.length(trim(F.col("Strip.ROUTE_TO_USER_ID"))) == 0),
            "NA"
        ).otherwise(trim(F.col("Strip.ROUTE_TO_USER_ID"))).alias("ROUTE_TO_USER_ID"),
        F.lit("R").alias("CLAIM_STATUS_CD"),
        F.when(
            F.col("Strip.CLAIM_STTUS_CHG_REASN").isNull() | (F.length(trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))) == 0),
            "NA"
        ).otherwise(F.upper(trim(F.col("Strip.CLAIM_STTUS_CHG_REASN")))).alias("CLAIM_STTUS_CHG_REASN"),
        SttusDate.alias("CLST_STS_DT"),
        SttusDateTime.alias("CLST_STS_DTM"),
        F.when(
            F.col("Strip.CLMI_ITS_CUR_STS").isNull() | (F.length(F.col("Strip.CLMI_ITS_CUR_STS")) == 0),
            "NA"
        ).otherwise(F.col("Strip.CLMI_ITS_CUR_STS")).alias("CLMI_ITS_CUR_STS")
    )
)

# Collector (union of reversals and StripOut)
df_collector = df_reversals.unionByName(df_StripOut)

# hf_rmd_sttusAuditExtr is an intermediate hashed file (Scenario A), remove it and deduplicate
df_collector_dedup = dedup_sort(
    df_collector,
    ["CLM_STTUS_AUDIT_SK", "CLAIM_ID", "CLAIM_STATUS_AUDIT_SEQ_NO"],
    []
)

# Snapshot Transformer input
df_snapshot_input = df_collector_dedup

# Snapshot outputs

# AllCol
df_AllCol = df_snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_STTUS_AUDIT_SK").alias("CLM_STTUS_AUDIT_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLAIM_SK").alias("CLM_SK"),
    F.col("CREATE_BY_USER_ID").alias("CRT_BY_APP_USER"),
    F.col("ROUTE_TO_USER_ID").alias("RTE_TO_APP_USER"),
    F.col("CLAIM_STATUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLAIM_STTUS_CHG_REASN").alias("CLM_STTUS_CHG_RSN_CD"),
    F.col("CLST_STS_DT").alias("CLM_STTUS_DT_SK"),
    F.col("CLST_STS_DTM").alias("CLM_STTUS_DTM"),
    F.col("CLMI_ITS_CUR_STS").alias("TRNSMSN_SRC_CD")
)

# Snapshot (2 columns)
df_SnapshotOut = df_snapshot_input.select(
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLAIM_STATUS_AUDIT_SEQ_NO")
)

# Transform (3 columns)
df_Transform = df_snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO")
)

# Next Transformer stage (input is df_SnapshotOut)
df_transformer_in = df_SnapshotOut
df_Transformer = df_transformer_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO")
)

# B_CLM_STTUS_AUDIT writes from df_Transformer
# Ensure rpad if char columns. CLM_ID is char(12).
df_to_write_BCLM = df_Transformer.withColumn(
    "CLM_ID",
    F.rpad(F.col("CLM_ID"), 12, " ")
)

write_files(
    df_to_write_BCLM,
    f"{adls_path}/load/B_CLM_STTUS_AUDIT.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container ClmSttusAuditPK
params = {}
df_ClmSttusAuditPK = ClmSttusAuditPK(df_AllCol, df_Transform, params)

# FctsClmStatusAuditExtr writes from df_ClmSttusAuditPK
# Must preserve column order and apply rpad for any char columns.

df_fcts_write = df_ClmSttusAuditPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_STTUS_AUDIT_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_STTUS_AUDIT_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("CRT_BY_APP_USER"), 10, " ").alias("CRT_BY_APP_USER"),
    F.rpad(F.col("RTE_TO_APP_USER"), 10, " ").alias("RTE_TO_APP_USER"),
    F.rpad(F.col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_STTUS_CHG_RSN_CD"), 4, " ").alias("CLM_STTUS_CHG_RSN_CD"),
    F.rpad(F.col("CLM_STTUS_DT_SK"), 10, " ").alias("CLM_STTUS_DT_SK"),
    F.col("CLM_STTUS_DTM"),
    F.rpad(F.col("TRNSMSN_SRC_CD"), 2, " ").alias("TRNSMSN_SRC_CD")
)

write_files(
    df_fcts_write,
    f"{adls_path}/key/LhoFctsClmSttusAuditExtr.LhoFctsClmSttusAudit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)