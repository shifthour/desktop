# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT TYPE data from WEB User Info and writes out a key file to load into the IDS EVT_TYP table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-11-02        4529                      Initial Programming                                                                             IntegrateNewDevl       SAndrew                           12/07/2010
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                               IntegrateNewDevl     Steph Goddard          02/11/2011

# MAGIC IDS EVENT TYPE Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
WebUserInformationOwner = get_widget_value('WebUserInformationOwner','')
webuserinformation_secret_name = get_widget_value('webuserinformation_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','100')
SrcSysCd = get_widget_value('SrcSysCd','WEBUSERINFO')
CurrDate = get_widget_value('CurrDate','2010-11-04')
LastRunDateTime = get_widget_value('LastRunDateTime','2010-09-01')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
hashed_file_secret_name = get_widget_value('hashed_file_secret_name','')

# ----------------------------------------------------------------------------
# Stage: EVT_TYP (CODBCStage) - Reading from a database table
# ----------------------------------------------------------------------------
jdbc_url_webuserinformation, jdbc_props_webuserinformation = get_db_config(webuserinformation_secret_name)
extract_query = (
    f"SELECT EVT_TYP.EVT_TYP_CD, EVT_TYP.EVT_TYP_NM, EVT_TYP.STAFF_TYP_CD, "
    f"EVT_TYP.LAST_UPDT_USER_ID, EVT_TYP.LAST_UPDT_DTM "
    f"FROM {WebUserInformationOwner}.EVT_TYP EVT_TYP "
    f"WHERE EVT_TYP.LAST_UPDT_DTM > '{LastRunDateTime}' "
    f"AND (EVT_TYP.EVT_TYP_CD = ?)"
)
df_EVT_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_webuserinformation)
    .options(**jdbc_props_webuserinformation)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# ----------------------------------------------------------------------------
df_BusinessRules = df_EVT_TYP

# Create the "Transform" output link dataframe
df_Transform = (
    df_BusinessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(UpCase(trim(F.col("EVT_TYP_CD"))), F.lit(";"), F.lit(SrcSysCd)))
    .withColumn("EVT_TYP_SK", F.lit(0))
    .withColumn("EVT_TYP_ID", UpCase(trim(F.col("EVT_TYP_CD"))))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("STAFF_TYP_ID", UpCase(trim(F.col("STAFF_TYP_CD"))))
    .withColumn(
        "EVT_TYP_DESC",
        F.when(
            F.col("EVT_TYP_NM").isNull() | (F.length(trim(F.col("EVT_TYP_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(UpCase(trim(F.col("EVT_TYP_NM"))))
    )
    .withColumn(
        "LAST_UPDT_DTM",
        F.when(
            F.col("LAST_UPDT_DTM").isNull() | (F.length(trim(F.col("LAST_UPDT_DTM"))) == 0),
            F.lit(CurrDateTimestamp),
        ).otherwise(FORMAT.DATE(F.col("LAST_UPDT_DTM"), "SYBASE", "TIMESTAMP", "DB2TIMESTAMP"))
    )
    .withColumn(
        "LAST_UPDT_USER_ID",
        F.when(
            F.col("LAST_UPDT_USER_ID").isNull() | (F.length(trim(F.col("LAST_UPDT_USER_ID"))) == 0),
            F.lit("UNK"),
        ).otherwise(UpCase(trim(F.col("LAST_UPDT_USER_ID"))))
    )
)

# Create the "Snapshot" output link dataframe
df_Snapshot = (
    df_BusinessRules
    .select(
        UpCase(trim(F.col("EVT_TYP_CD"))).alias("EVT_TYP_ID"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: B_EVT_TYP (CSeqFileStage) - Writing df_Snapshot to file
# ----------------------------------------------------------------------------
write_files(
    df_Snapshot.select("EVT_TYP_ID", "SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_EVT_TYP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: hf_evt_typ_lkup (CHashedFileStage) - Scenario B (read-modify-write)
# Treat as a dummy table named dummy_hf_evt_typ
# ----------------------------------------------------------------------------
jdbc_url_hf, jdbc_props_hf = get_db_config(hashed_file_secret_name)
df_hf_evt_typ_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf)
    .options(**jdbc_props_hf)
    .option("query", "SELECT EVT_TYP_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_TYP_SK FROM dummy_hf_evt_typ")
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Pkey (CTransformerStage) - Join Transform (Primary) with lkup (left join)
# ----------------------------------------------------------------------------
df_join = df_Transform.alias("Transform").join(
    df_hf_evt_typ_lkup.alias("lkup"),
    (
        (F.col("Transform.EVT_TYP_ID") == F.col("lkup.EVT_TYP_ID")) &
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
    ),
    "left"
)

df_withVars = (
    df_join
    .withColumn(
        "SK_temp",
        F.when(F.col("lkup.EVT_TYP_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.EVT_TYP_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup.EVT_TYP_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

# Translate the KeyMgtGetNextValueConcurrent call into SurrogateKeyGen on "SK_temp"
df_enriched = df_withVars
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK_temp",<schema>,<secret_name>)

# Output link: Key
df_Key = (
    df_enriched
    .withColumnRenamed("SK_temp", "EVT_TYP_SK")
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumnRenamed("NewCrtRunCycExtcnSk", "CRT_RUN_CYC_EXCTN_SK")
)

df_Key = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "EVT_TYP_SK",
    "EVT_TYP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "STAFF_TYP_ID",
    "EVT_TYP_DESC",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

# Apply rpad for char columns
df_Key = df_Key.withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
df_Key = df_Key.withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
df_Key = df_Key.withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))

# ----------------------------------------------------------------------------
# Stage: WebUserInfoEvtTyp (CSeqFileStage) - Write df_Key to file
# ----------------------------------------------------------------------------
write_files(
    df_Key,
    f"{adls_path}/key/WebUserInfoEvtTypExtr.EvtTyp.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: hf_evt_typ_updt (CHashedFileStage) - Same dummy table, continue scenario B
# Constraint => IsNull(lkup.EVT_TYP_SK) = @TRUE
# ----------------------------------------------------------------------------
df_updt = df_enriched.filter(F.col("lkup.EVT_TYP_SK").isNull())

df_updt = df_updt.select(
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK_temp").alias("EVT_TYP_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.WebUserInfoEvtTypExtr_hf_evt_typ_updt_temp", jdbc_url_hf, jdbc_props_hf)

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_hf)
    .options(**jdbc_props_hf)
    .option("dbtable", "STAGING.WebUserInfoEvtTypExtr_hf_evt_typ_updt_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE INTO dummy_hf_evt_typ AS T
USING STAGING.WebUserInfoEvtTypExtr_hf_evt_typ_updt_temp AS S
ON (T.EVT_TYP_ID = S.EVT_TYP_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD)
WHEN MATCHED THEN 
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.EVT_TYP_SK = S.EVT_TYP_SK
WHEN NOT MATCHED THEN 
  INSERT (
    EVT_TYP_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    EVT_TYP_SK
  )
  VALUES (
    S.EVT_TYP_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.EVT_TYP_SK
  );
"""

execute_dml(merge_sql, jdbc_url_hf, jdbc_props_hf)