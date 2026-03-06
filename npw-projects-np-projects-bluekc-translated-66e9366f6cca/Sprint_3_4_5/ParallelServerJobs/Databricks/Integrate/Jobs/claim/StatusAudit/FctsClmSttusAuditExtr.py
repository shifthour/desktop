# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004,2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
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
# MAGIC Prabhu ES               2022-02-28      S2S Remediation   MSSQL connection parameters added                                                 IntegrateDev5        Kalyan Neelam    2022-06-10

# MAGIC Pulling Facets Claim Status and Audit Data
# MAGIC Hash file (hf_clm_sttus_audit_allcol) cleared at end of the job
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
CurrentDate = get_widget_value('CurrentDate','2008-07-29')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20080729')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmSttusAuditPK
# COMMAND ----------

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_CMC_CLST_STATUS = f"""
SELECT  STAT.CLCL_ID,
        STAT.CLST_SEQ_NO,
        STAT.CLST_STS,
        STAT.USUS_ID,
        STAT.CLST_STS_DTM,
        STAT.CLST_MCTR_REAS,
        STAT.CLST_USID_ROUTE,
        STAT.CLMI_ITS_CUR_STS
FROM {FacetsOwner}.CMC_CLST_STATUS STAT,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = STAT.CLCL_ID
"""
df_CMC_CLST_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_CLST_STATUS)
    .load()
)

df_Strip = (
    df_CMC_CLST_STATUS
    .withColumn("CLAIM_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLAIM_STATUS_AUDIT_SEQ_NO", F.col("CLST_SEQ_NO"))
    .withColumn("SOURCE_SYS_CD", F.lit("FACETS"))
    .withColumn("CLST_STS_DTM", F.col("CLST_STS_DTM"))
    .withColumn("CREATE_BY_USER_ID", strip_field(F.trim(F.col("USUS_ID"))))
    .withColumn("ROUTE_TO_USER_ID", strip_field(F.col("CLST_USID_ROUTE")))
    .withColumn("CLAIM_STATUS_CD", strip_field(F.col("CLST_STS")))
    .withColumn("CLAIM_STTUS_CHG_REASN", strip_field(F.trim(F.col("CLST_MCTR_REAS"))))
    .withColumn("CLMI_ITS_CUR_STS", strip_field(F.trim(F.col("CLMI_ITS_CUR_STS"))))
    .select(
        "CLAIM_ID",
        "CLAIM_STATUS_AUDIT_SEQ_NO",
        "SOURCE_SYS_CD",
        "CLST_STS_DTM",
        "CREATE_BY_USER_ID",
        "ROUTE_TO_USER_ID",
        "CLAIM_STATUS_CD",
        "CLAIM_STTUS_CHG_REASN",
        "CLMI_ITS_CUR_STS"
    )
)

df_Bus = (
    df_Strip.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("Strip.CLAIM_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("Strip.CLAIM_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
)

df_stripOut = (
    df_Bus
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.col("Strip.CLST_STS_DTM"))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit(SrcSysCd), F.lit(";"), F.trim(F.col("Strip.CLAIM_ID")), F.lit(";"),
            F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO")
        )
    )
    .withColumn("CLM_STTUS_AUDIT_SK", F.lit(0))
    .withColumn("CLAIM_ID", F.trim(F.col("Strip.CLAIM_ID")))
    .withColumn("CLAIM_STATUS_AUDIT_SEQ_NO", F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("CLAIM_SK", F.lit(0))
    .withColumn(
        "CREATE_BY_USER_ID",
        F.when(
            F.col("Strip.CREATE_BY_USER_ID").isNull() | (F.length(F.trim(F.col("Strip.CREATE_BY_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.CREATE_BY_USER_ID")))
    )
    .withColumn(
        "ROUTE_TO_USER_ID",
        F.when(
            F.col("Strip.ROUTE_TO_USER_ID").isNull() | (F.length(F.trim(F.col("Strip.ROUTE_TO_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.ROUTE_TO_USER_ID")))
    )
    .withColumn(
        "CLAIM_STATUS_CD",
        F.when(
            F.col("Strip.CLAIM_STATUS_CD").isNull() | (F.length(F.trim(F.col("Strip.CLAIM_STATUS_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.CLAIM_STATUS_CD"))))
    )
    .withColumn(
        "CLAIM_STTUS_CHG_REASN",
        F.when(
            F.col("Strip.CLAIM_STTUS_CHG_REASN").isNull() | (F.length(F.trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))))
    )
    .withColumn("CLST_STS_DT", F.date_format(F.col("Strip.CLST_STS_DTM"), "yyyy-MM-dd"))
    .withColumn("CLST_STS_DTM", F.col("Strip.CLST_STS_DTM"))
    .withColumn(
        "CLMI_ITS_CUR_STS",
        F.when(
            F.col("Strip.CLMI_ITS_CUR_STS").isNull() | (F.length(F.col("Strip.CLMI_ITS_CUR_STS")) == 0),
            F.lit("NA")
        ).otherwise(F.col("Strip.CLMI_ITS_CUR_STS"))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_STTUS_AUDIT_SK",
        "CLAIM_ID",
        "CLAIM_STATUS_AUDIT_SEQ_NO",
        "CRT_RUN_CYC_EXTCN_SK",
        "LAST_UPDT_RUN_CYC_EXTCN_SK",
        "CLAIM_SK",
        "CREATE_BY_USER_ID",
        "ROUTE_TO_USER_ID",
        "CLAIM_STATUS_CD",
        "CLAIM_STTUS_CHG_REASN",
        "CLST_STS_DT",
        "CLST_STS_DTM",
        "CLMI_ITS_CUR_STS"
    )
)

df_reversals = (
    df_Bus
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
        (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
    )
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.col("Strip.CLST_STS_DTM"))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS"), F.lit(";"), F.trim(F.col("Strip.CLAIM_ID")), F.lit("R;"),
            F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO")
        )
    )
    .withColumn("CLM_STTUS_AUDIT_SK", F.lit(0))
    .withColumn("CLAIM_ID", F.concat(F.trim(F.col("Strip.CLAIM_ID")), F.lit("R")))
    .withColumn("CLAIM_STATUS_AUDIT_SEQ_NO", F.col("Strip.CLAIM_STATUS_AUDIT_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("CLAIM_SK", F.lit(0))
    .withColumn(
        "CREATE_BY_USER_ID",
        F.when(
            F.col("Strip.CREATE_BY_USER_ID").isNull() | (F.length(F.trim(F.col("Strip.CREATE_BY_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.CREATE_BY_USER_ID")))
    )
    .withColumn(
        "ROUTE_TO_USER_ID",
        F.when(
            F.col("Strip.ROUTE_TO_USER_ID").isNull() | (F.length(F.trim(F.col("Strip.ROUTE_TO_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.ROUTE_TO_USER_ID")))
    )
    .withColumn("CLAIM_STATUS_CD", F.lit("R"))
    .withColumn(
        "CLAIM_STTUS_CHG_REASN",
        F.when(
            F.col("Strip.CLAIM_STTUS_CHG_REASN").isNull() | (F.length(F.trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.CLAIM_STTUS_CHG_REASN"))))
    )
    .withColumn("CLST_STS_DT", F.date_format(F.col("Strip.CLST_STS_DTM"), "yyyy-MM-dd"))
    .withColumn("CLST_STS_DTM", F.col("Strip.CLST_STS_DTM"))
    .withColumn(
        "CLMI_ITS_CUR_STS",
        F.when(
            F.col("Strip.CLMI_ITS_CUR_STS").isNull() | (F.length(F.col("Strip.CLMI_ITS_CUR_STS")) == 0),
            F.lit("NA")
        ).otherwise(F.col("Strip.CLMI_ITS_CUR_STS"))
    )
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_STTUS_AUDIT_SK",
        "CLAIM_ID",
        "CLAIM_STATUS_AUDIT_SEQ_NO",
        "CRT_RUN_CYC_EXTCN_SK",
        "LAST_UPDT_RUN_CYC_EXTCN_SK",
        "CLAIM_SK",
        "CREATE_BY_USER_ID",
        "ROUTE_TO_USER_ID",
        "CLAIM_STATUS_CD",
        "CLAIM_STTUS_CHG_REASN",
        "CLST_STS_DT",
        "CLST_STS_DTM",
        "CLMI_ITS_CUR_STS"
    )
)

df_collector = df_reversals.unionByName(df_stripOut)

df_Transformer_27 = df_collector.select(
    "SRC_SYS_CD",
    "CLAIM_ID",
    "CLAIM_STATUS_AUDIT_SEQ_NO",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "PRI_KEY_STRING",
    "CLM_STTUS_AUDIT_SK",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "CLAIM_SK",
    "CREATE_BY_USER_ID",
    "ROUTE_TO_USER_ID",
    "CLAIM_STATUS_CD",
    "CLAIM_STTUS_CHG_REASN",
    "CLST_STS_DT",
    "CLST_STS_DTM",
    "CLMI_ITS_CUR_STS"
)

df_dedup = dedup_sort(
    df_Transformer_27,
    ["SRC_SYS_CD","CLAIM_ID","CLAIM_STATUS_AUDIT_SEQ_NO"],
    []
)

df_Snapshot = df_dedup

df_Snapshot_AllCol = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    "CLM_STTUS_AUDIT_SK",
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

df_Snapshot_Snapshot = df_Snapshot.select(
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLAIM_STATUS_AUDIT_SEQ_NO")
)

df_Snapshot_Transform = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO")
)

df_Transformer = df_Snapshot_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO")
)

df_B_CLM_STTUS_AUDIT = df_Transformer

b_clm_sttus_audit_path = f"{adls_path}/load/B_CLM_STTUS_AUDIT.FACETS.dat.{RunID}"
df_B_CLM_STTUS_AUDIT_write = df_B_CLM_STTUS_AUDIT.select(
    F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 1, " ").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID").cast(StringType()), 1, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_STTUS_AUDIT_SEQ_NO").cast(StringType()), 1, " ").alias("CLM_STTUS_AUDIT_SEQ_NO")
)
write_files(
    df_B_CLM_STTUS_AUDIT_write,
    b_clm_sttus_audit_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmSttusAuditPK = {
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "FacetsDB": "",
    "FacetsOwner": FacetsOwner,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "$IDSOwner": IDSOwner
}
df_ClmSttusAuditPK_Key = ClmSttusAuditPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmSttusAuditPK)

df_FctsClmStatusAuditExtr = df_ClmSttusAuditPK_Key.select(
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
    F.col("CLM_ID"),
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

fcts_clm_sttus_audit_extr_path = f"{adls_path}/key/FctsClmSttusAuditExtr.FctsClmSttusAudit.dat.{RunID}"
write_files(
    df_FctsClmStatusAuditExtr,
    fcts_clm_sttus_audit_extr_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)