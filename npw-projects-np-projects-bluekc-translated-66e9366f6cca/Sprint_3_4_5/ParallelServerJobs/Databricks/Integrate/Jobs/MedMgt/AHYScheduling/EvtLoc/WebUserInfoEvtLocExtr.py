# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT LOCATION data from WEB User Info and writes out a key file to load into the IDS EVT_LOC table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-11-02        4529                      Initial Programming                                                                            IntegrateNewDevl       SAndrew                           12/07/2010
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                              IntegrateNewDevl      Steph Goddard         02/11/2011

# MAGIC IDS EVENT LOCATION Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, length, lit, substring, rpad, to_date, to_timestamp
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# No additional imports beyond this point

# Parameter definitions (including database-owner parameter handling)
WebUserInformationOwner = get_widget_value('WebUserInformationOwner','')
webuserinformation_secret_name = get_widget_value('webuserinformation_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrDate = get_widget_value('CurrDate','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
dummy_hf_evt_loc_secret_name = get_widget_value('dummy_hf_evt_loc_secret_name','')

# Connect and read from the LOC source (CODBCStage)
jdbc_url_loc, jdbc_props_loc = get_db_config(webuserinformation_secret_name)
loc_query = f"SELECT LOC.LOC_ID, LOC.LOC_NM, LOC.LOC_CNTCT_NM, LOC.LOC_ADDR_LN_1, LOC.LOC_ROOM_ID, LOC.LOC_CITY_NM, LOC.LOC_ST_CD, LOC.LOC_ZIP_CD, LOC.LOC_PHN_NO, LOC.EFF_DT, LOC.TERM_DT, LOC.LAST_UPDT_USER_ID, LOC.LAST_UPDT_DTM FROM {WebUserInformationOwner}.LOC LOC WHERE LOC.LAST_UPDT_DTM > '{LastRunDateTime}'"
df_LOC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_loc)
    .options(**jdbc_props_loc)
    .option("query", loc_query)
    .load()
)

# "BusinessRules" Transformer logic, output pin "Transform" => "Pkey"
df_BusinessRules_Transform = df_LOC \
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0)) \
    .withColumn("INSRT_UPDT_CD", lit("I")) \
    .withColumn("DISCARD_IN", lit("N")) \
    .withColumn("PASS_THRU_IN", lit("Y")) \
    .withColumn("FIRST_RECYC_DT", lit(CurrDate)) \
    .withColumn("ERR_CT", lit(0)) \
    .withColumn("RECYCLE_CT", lit(0)) \
    .withColumn("SRC_SYS_CD", lit(SrcSysCd)) \
    .withColumn("PRI_KEY_STRING", trim(col("LOC_ID")) + lit(";") + lit(SrcSysCd)) \
    .withColumn("EVT_LOC_SK", lit(0)) \
    .withColumn("EVT_LOC_ID", trim(col("LOC_ID"))) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0)) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0)) \
    .withColumn(
        "EFF_DT_SK",
        when(
            col("EFF_DT").isNull() | (length(trim(col("EFF_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            to_date(trim(col("EFF_DT")), "yyyy-MM-dd").cast("string")
        )
    ) \
    .withColumn(
        "TERM_DT_SK",
        when(
            col("TERM_DT").isNull() | (length(trim(col("TERM_DT"))) == 0),
            lit("2199-12-31")
        ).otherwise(
            to_date(trim(col("TERM_DT")), "yyyy-MM-dd").cast("string")
        )
    ) \
    .withColumn(
        "EVT_LOC_CNTCT_NM",
        when(
            col("LOC_CNTCT_NM").isNull() | (length(trim(col("LOC_CNTCT_NM"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LOC_CNTCT_NM")))
        )
    ) \
    .withColumn(
        "EVT_LOC_NM",
        when(
            col("LOC_NM").isNull() | (length(trim(col("LOC_NM"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LOC_NM")))
        )
    ) \
    .withColumn(
        "EVT_LOC_ADDR_LN_1",
        when(
            col("LOC_ADDR_LN_1").isNull() | (length(trim(col("LOC_ADDR_LN_1"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LOC_ADDR_LN_1")))
        )
    ) \
    .withColumn(
        "EVT_LOC_ROOM_ID",
        when(
            col("LOC_ROOM_ID").isNull() | (length(trim(col("LOC_ROOM_ID"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LOC_ROOM_ID")))
        )
    ) \
    .withColumn(
        "EVT_LOC_CITY_NM",
        when(
            col("LOC_CITY_NM").isNull() | (length(trim(col("LOC_CITY_NM"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LOC_CITY_NM")))
        )
    ) \
    .withColumn(
        "LOC_ST_CD",
        when(
            col("LOC_ST_CD").isNull() | (length(trim(col("LOC_ST_CD"))) == 0),
            lit("UNK")
        ).otherwise(
            trim(col("LOC_ST_CD"))
        )
    ) \
    .withColumn(
        "EVT_LOC_ZIP_CD",
        when(
            col("LOC_ZIP_CD").isNull() | (length(trim(col("LOC_ZIP_CD"))) == 0),
            lit("00000")
        ).otherwise(
            trim(col("LOC_ZIP_CD"))
        )
    ) \
    .withColumn(
        "EVT_LOC_PHN_NO",
        when(
            col("LOC_PHN_NO").isNull() | (length(trim(col("LOC_PHN_NO"))) == 0),
            lit("0000000000")
        ).when(
            length(trim(col("LOC_PHN_NO"))) == 14,
            substring(trim(col("LOC_PHN_NO")), 2, 3)
            + substring(trim(col("LOC_PHN_NO")), 7, 3)
            + substring(trim(col("LOC_PHN_NO")), 11, 4)
        ).otherwise(
            trim(col("LOC_PHN_NO"))
        )
    ) \
    .withColumn(
        "LAST_UPDT_DTM",
        when(
            col("LAST_UPDT_DTM").isNull() | (length(trim(col("LAST_UPDT_DTM"))) == 0),
            lit(CurrDateTimestamp)
        ).otherwise(
            to_timestamp(trim(col("LAST_UPDT_DTM")), "yyyy-MM-dd HH:mm:ss").cast("string")
        )
    ) \
    .withColumn(
        "LAST_UPDT_USER_ID",
        when(
            trim(col("LAST_UPDT_USER_ID")).isNull() | (length(trim(col("LAST_UPDT_USER_ID"))) == 0),
            lit("UNK")
        ).otherwise(
            UpCase(trim(col("LAST_UPDT_USER_ID")))
        )
    )

# "BusinessRules" Transformer logic, output pin "Snapshot" => "B_EVT_LOC"
df_BusinessRules_Snapshot = df_BusinessRules_Transform.select(
    col("EVT_LOC_ID"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# "B_EVT_LOC" (CSeqFileStage) - Write to file
write_files(
    df_BusinessRules_Snapshot.select("EVT_LOC_ID", "SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_EVT_LOC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"'
)

# Read-modify-write hashed file scenario for "hf_evt_loc_lkup" => "dummy_hf_evt_loc"
jdbc_url_dummy_hf_evt_loc, jdbc_props_dummy_hf_evt_loc = get_db_config(dummy_hf_evt_loc_secret_name)
query_hf_evt_loc = "SELECT EVT_LOC_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_LOC_SK FROM dummy_hf_evt_loc"
df_hf_evt_loc_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy_hf_evt_loc)
    .options(**jdbc_props_dummy_hf_evt_loc)
    .option("query", query_hf_evt_loc)
    .load()
)

# "Pkey" Transformer: left join (Transform => primary, lkup => left)
df_pkey_join = df_BusinessRules_Transform.alias("Transform").join(
    df_hf_evt_loc_lkup.alias("lkup"),
    (col("Transform.EVT_LOC_ID") == col("lkup.EVT_LOC_ID")) & (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")),
    how="left"
)

df_pkey_join = df_pkey_join.withColumn(
    "NewCrtRunCycExtcnSk",
    when(col("lkup.EVT_LOC_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_pkey_join = df_pkey_join.withColumn(
    "EVT_LOC_SK",
    when(col("lkup.EVT_LOC_SK").isNull(), lit(None)).otherwise(col("lkup.EVT_LOC_SK"))
)

df_enriched = df_pkey_join
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'EVT_LOC_SK',<schema>,<secret_name>)

# "Key" link => "WebUserInfoEvtLoc"
df_Key = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("EVT_LOC_SK").alias("EVT_LOC_SK"),
    col("Transform.EVT_LOC_ID").alias("EVT_LOC_ID"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Transform.EVT_LOC_CNTCT_NM").alias("EVT_LOC_CNTCT_NM"),
    col("Transform.EVT_LOC_NM").alias("EVT_LOC_NM"),
    col("Transform.EVT_LOC_ADDR_LN_1").alias("EVT_LOC_ADDR_LN_1"),
    col("Transform.EVT_LOC_ROOM_ID").alias("EVT_LOC_ROOM_ID"),
    col("Transform.EVT_LOC_CITY_NM").alias("EVT_LOC_CITY_NM"),
    col("Transform.LOC_ST_CD").alias("LOC_ST_CD"),
    col("Transform.EVT_LOC_ZIP_CD").alias("EVT_LOC_ZIP_CD"),
    col("Transform.EVT_LOC_PHN_NO").alias("EVT_LOC_PHN_NO"),
    col("Transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("Transform.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_WebUserInfoEvtLoc = df_Key \
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " ")) \
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " ")) \
    .withColumn("EVT_LOC_ZIP_CD", rpad(col("EVT_LOC_ZIP_CD"), 5, " "))

write_files(
    df_WebUserInfoEvtLoc.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "EVT_LOC_SK",
        "EVT_LOC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "EVT_LOC_CNTCT_NM",
        "EVT_LOC_NM",
        "EVT_LOC_ADDR_LN_1",
        "EVT_LOC_ROOM_ID",
        "EVT_LOC_CITY_NM",
        "LOC_ST_CD",
        "EVT_LOC_ZIP_CD",
        "EVT_LOC_PHN_NO",
        "LAST_UPDT_DTM",
        "LAST_UPDT_USER_ID"
    ),
    f"{adls_path}/key/WebUserInfoEvtLocExtr.EvtLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"'
)

# "updt" link => "hf_evt_loc_updt" (same hashed file => scenario B => MERGE to dummy_hf_evt_loc)
df_updt = df_enriched.filter(col("lkup.EVT_LOC_SK").isNull()).select(
    col("Transform.EVT_LOC_ID").alias("EVT_LOC_ID"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("EVT_LOC_SK").alias("EVT_LOC_SK")
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.WebUserInfoEvtLocExtr_hf_evt_loc_updt_temp",
    jdbc_url_dummy_hf_evt_loc,
    jdbc_props_dummy_hf_evt_loc
)

df_updt.write.format("jdbc") \
    .option("url", jdbc_url_dummy_hf_evt_loc) \
    .options(**jdbc_props_dummy_hf_evt_loc) \
    .option("dbtable", "STAGING.WebUserInfoEvtLocExtr_hf_evt_loc_updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE dummy_hf_evt_loc AS T
USING STAGING.WebUserInfoEvtLocExtr_hf_evt_loc_updt_temp AS S
ON T.EVT_LOC_ID = S.EVT_LOC_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.EVT_LOC_SK = S.EVT_LOC_SK
WHEN NOT MATCHED THEN
  INSERT (
    EVT_LOC_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    EVT_LOC_SK
  )
  VALUES (
    S.EVT_LOC_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.EVT_LOC_SK
  );
"""

execute_dml(merge_sql, jdbc_url_dummy_hf_evt_loc, jdbc_props_dummy_hf_evt_loc)