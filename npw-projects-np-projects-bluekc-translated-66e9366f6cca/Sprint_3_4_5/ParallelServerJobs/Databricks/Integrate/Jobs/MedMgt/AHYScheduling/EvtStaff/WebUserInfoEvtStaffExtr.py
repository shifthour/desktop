# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT STAFF data from WEB User Info and writes out a key file to load into the IDS EVT_STAFF table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-11-02        4529                      Initial Programming                                                                           IntegrateNewDevl       SAndrew                           12/07/2010
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                             IntegrateNewDevl        Steph Goddard         02/11/2011

# MAGIC IDS EVENT STAFF Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
WebUserInformationOwner = get_widget_value('WebUserInformationOwner','')
webuserinformationowner_secret_name = get_widget_value('webuserinformationowner_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrDate = get_widget_value('CurrDate','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSycCdSk = get_widget_value('SrcSycCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# STAFF (CODBCStage) => Read from DB
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(webuserinformationowner_secret_name)
extract_query = (
    f"SELECT STAFF.STAFF_ID, STAFF.STAFF_NM, STAFF.STAFF_EMAIL_ADDR, "
    f"       STAFF.STAFF_PHN_NO, STAFF.STAFF_ACTV_IN, STAFF.STAFF_TYP_CD, "
    f"       STAFF.EFF_DT, STAFF.TERM_DT, STAFF.LAST_UPDT_USER_ID, STAFF.LAST_UPDT_DTM "
    f"FROM {WebUserInformationOwner}.STAFF STAFF "
    f"WHERE STAFF.LAST_UPDT_DTM > '{LastRunDateTime}'"
)
df_STAFF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
#   Input: Extract (df_STAFF)
#   Outputs:
#     1) Transform => Pkey
#     2) Snapshot => B_EVT_STAFF
# --------------------------------------------------------------------------------

# 1) "Transform" link: 21 columns
df_BusinessRules_Transform = df_STAFF.select(
    # JOB_EXCTN_RCRD_ERR_SK => 0
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),

    # INSRT_UPDT_CD (char(10)) => 'I'
    F.lit("I").alias("INSRT_UPDT_CD"),

    # DISCARD_IN (char(1)) => 'N'
    F.lit("N").alias("DISCARD_IN"),

    # PASS_THRU_IN (char(1)) => 'Y'
    F.lit("Y").alias("PASS_THRU_IN"),

    # FIRST_RECYC_DT => CurrDate (param)
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),

    # ERR_CT => 0
    F.lit(0).alias("ERR_CT"),

    # RECYCLE_CT => 0
    F.lit(0).alias("RECYCLE_CT"),

    # SRC_SYS_CD => SrcSysCd (param)
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),

    # PRI_KEY_STRING => trim(STAFF_ID) + ";" + SrcSysCd
    F.concat(trim(F.col("STAFF_ID")), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),

    # EVT_STAFF_SK => 0
    F.lit(0).alias("EVT_STAFF_SK"),

    # EVT_STAFF_ID => trim(STAFF_ID)
    trim(F.col("STAFF_ID")).alias("EVT_STAFF_ID"),

    # CRT_RUN_CYC_EXCTN_SK => 0
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),

    # LAST_UPDT_RUN_CYC_EXCTN_SK => 0
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),

    # STAFF_TYP_ID => upper(trim(STAFF_TYP_CD))
    F.upper(trim(F.col("STAFF_TYP_CD"))).alias("STAFF_TYP_ID"),

    # EVT_STAFF_ACTV_IN (char(1)) => conditional logic
    F.when(
        F.col("STAFF_ACTV_IN").isNull() | (F.length(trim(F.col("STAFF_ACTV_IN"))) == 0),
        F.lit("Y")
    )
    .when(F.col("STAFF_ACTV_IN") == "1", F.lit("Y"))
    .when(F.col("STAFF_ACTV_IN") == "0", F.lit("N"))
    .otherwise(F.lit("N"))
    .alias("EVT_STAFF_ACTV_IN"),

    # EFF_DT_SK (char(10))
    F.when(
        F.col("EFF_DT").isNull() | (F.length(trim(F.col("EFF_DT"))) == 0),
        F.lit("1753-01-01")
    )
    .otherwise(
        F.date_format(
            F.to_timestamp(trim(F.col("EFF_DT")), "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd"
        )
    )
    .alias("EFF_DT_SK"),

    # TERM_DT_SK (char(10))
    F.when(
        F.col("TERM_DT").isNull() | (F.length(trim(F.col("TERM_DT"))) == 0),
        F.lit("2199-12-31")
    )
    .otherwise(
        F.date_format(
            F.to_timestamp(trim(F.col("TERM_DT")), "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd"
        )
    )
    .alias("TERM_DT_SK"),

    # EVT_STAFF_EMAIL_ADDR => if null => 'UNK'
    F.when(
        F.col("STAFF_EMAIL_ADDR").isNull() | (F.length(trim(F.col("STAFF_EMAIL_ADDR"))) == 0),
        F.lit("UNK")
    )
    .otherwise(trim(F.col("STAFF_EMAIL_ADDR")))
    .alias("EVT_STAFF_EMAIL_ADDR"),

    # EVT_STAFF_NM => if null => 'UNK' else upcase
    F.when(
        F.col("STAFF_NM").isNull() | (F.length(trim(F.col("STAFF_NM"))) == 0),
        F.lit("UNK")
    )
    .otherwise(F.upper(trim(F.col("STAFF_NM"))))
    .alias("EVT_STAFF_NM"),

    # EVT_STAFF_PHN_NO => if null => '0000000000', else transform if length=14
    F.when(
        F.col("STAFF_PHN_NO").isNull() | (F.length(trim(F.col("STAFF_PHN_NO"))) == 0),
        F.lit("0000000000")
    )
    .when(
        F.length(trim(F.col("STAFF_PHN_NO"))) == 14,
        F.concat(
            F.substring(trim(F.col("STAFF_PHN_NO")), 2, 3), 
            F.substring(trim(F.col("STAFF_PHN_NO")), 7, 3),
            F.substring(trim(F.col("STAFF_PHN_NO")), 11, 4)
        )
    )
    .otherwise(trim(F.col("STAFF_PHN_NO")))
    .alias("EVT_STAFF_PHN_NO"),

    # LAST_UPDT_DTM => if null => CurrDateTimestamp, else parse
    F.when(
        F.col("LAST_UPDT_DTM").isNull() | (F.length(trim(F.col("LAST_UPDT_DTM"))) == 0),
        F.lit(CurrDateTimestamp)
    )
    .otherwise(
        F.date_format(F.to_timestamp(F.col("LAST_UPDT_DTM"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")
    )
    .alias("LAST_UPDT_DTM"),

    # LAST_UPDT_USER_ID => if null => 'UNK' else upcase
    F.when(
        (trim(F.col("LAST_UPDT_USER_ID")).isNull()) | (F.length(trim(F.col("LAST_UPDT_USER_ID"))) == 0),
        F.lit("UNK")
    )
    .otherwise(F.upper(trim(F.col("LAST_UPDT_USER_ID"))))
    .alias("LAST_UPDT_USER_ID")
)

# 2) "Snapshot" link: 2 columns => B_EVT_STAFF
df_BusinessRules_Snapshot = df_STAFF.select(
    trim(F.col("STAFF_ID")).alias("EVT_STAFF_ID"),
    F.lit(SrcSycCdSk).alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# B_EVT_STAFF (CSeqFileStage) => Write to file B_EVT_STAFF.dat
# --------------------------------------------------------------------------------
# Column order: [EVT_STAFF_ID, SRC_SYS_CD_SK]
df_BusinessRules_Snapshot_ordered = df_BusinessRules_Snapshot.select(
    "EVT_STAFF_ID",
    "SRC_SYS_CD_SK"
)
write_files(
    df_BusinessRules_Snapshot_ordered,
    f"{adls_path}/load/B_EVT_STAFF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# hf_evt_staff_lkup (CHashedFileStage) => Scenario B (Read-Modify-Write)
#   Replaced by dummy table: dummy_hf_evt_staff
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_evt_staff_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT EVT_STAFF_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_STAFF_SK FROM dummy_hf_evt_staff")
    .load()
)

# --------------------------------------------------------------------------------
# Pkey (CTransformerStage)
#   1) Primary link from df_BusinessRules_Transform
#   2) Lookup link from df_hf_evt_staff_lkup (left join)
#   Stage variables:
#       SK => if isnull(lkup.EVT_STAFF_SK) then KeyMgtGetNextValueConcurrent(...) else lkup.EVT_STAFF_SK
#       NewCrtRunCycExtcnSk => if isnull(lkup.EVT_STAFF_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
# --------------------------------------------------------------------------------

joined_cols = [
    df_BusinessRules_Transform["*"],
    df_hf_evt_staff_lkup["EVT_STAFF_ID"].alias("lkup_EVT_STAFF_ID"),
    df_hf_evt_staff_lkup["SRC_SYS_CD"].alias("lkup_SRC_SYS_CD"),
    df_hf_evt_staff_lkup["CRT_RUN_CYC_EXCTN_SK"].alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
    df_hf_evt_staff_lkup["EVT_STAFF_SK"].alias("lkup_EVT_STAFF_SK")
]

df_joined = df_BusinessRules_Transform.alias("Transform").join(
    df_hf_evt_staff_lkup.alias("lkup"),
    on=[
        F.col("Transform.EVT_STAFF_ID") == F.col("lkup.EVT_STAFF_ID"),
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")
    ],
    how="left"
).select(*joined_cols)

df_with_stage_vars = df_joined.select(
    F.col("*"),
    F.when(
        F.col("lkup_EVT_STAFF_SK").isNull(),
        F.lit(None).cast(IntegerType())  # SurrogateKeyGen will fill
    )
    .otherwise(F.col("lkup_EVT_STAFF_SK"))
    .alias("SK"),
    F.when(
        F.col("lkup_EVT_STAFF_SK").isNull(),
        F.lit(CurrRunCycle).cast(IntegerType())
    )
    .otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    .alias("NewCrtRunCycExtcnSk")
)

df_enriched = df_with_stage_vars.drop("lkup_EVT_STAFF_ID", "lkup_SRC_SYS_CD", "lkup_CRT_RUN_CYC_EXCTN_SK", "lkup_EVT_STAFF_SK")

# SurrogateKeyGen replaces KeyMgtGetNextValueConcurrent for SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# Generate final columns for the "Key" link => WebUserInfoEvtStaff
df_Pkey = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("EVT_STAFF_SK"),
    F.col("EVT_STAFF_ID").alias("EVT_STAFF_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("STAFF_TYP_ID").alias("STAFF_TYP_ID"),
    F.col("EVT_STAFF_ACTV_IN").alias("EVT_STAFF_ACTV_IN"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("EVT_STAFF_EMAIL_ADDR").alias("EVT_STAFF_EMAIL_ADDR"),
    F.col("EVT_STAFF_NM").alias("EVT_STAFF_NM"),
    F.col("EVT_STAFF_PHN_NO").alias("EVT_STAFF_PHN_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

# "updt" link => constraint = isNull(lkup.EVT_STAFF_SK)
# We already joined, so we check if "lkup_EVT_STAFF_SK" was null => we have an indicator in SK (null prior to SurrogateKeyGen).
# The rows that needed new SK are the same rows that had lkup_EVT_STAFF_SK null originally.
# We can filter by: SK newly filled AND lkup was null => we can rely on lkup_EVT_STAFF_SK was null to identify "insert" rows.
# But we dropped those columns, so let's do it from the df_with_stage_vars before SurrogateKeyGen.

df_updt_candidate = df_with_stage_vars.filter(F.col("lkup_EVT_STAFF_SK").isNull())

df_hf_evt_staff_updt = df_updt_candidate.select(
    trim(F.col("EVT_STAFF_ID")).alias("EVT_STAFF_ID"),
    trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("EVT_STAFF_SK")  # SurrogateKeyGen result after we re-join with the same row
)

# We must rejoin with df_enriched (which has the final SK) to get the updated SK values:
df_hf_evt_staff_updt = df_hf_evt_staff_updt.alias("u").join(
    df_enriched.select("EVT_STAFF_ID", "SRC_SYS_CD", "SK").alias("e"),
    on=[
        F.col("u.EVT_STAFF_ID") == F.col("e.EVT_STAFF_ID"),
        F.col("u.SRC_SYS_CD") == F.col("e.SRC_SYS_CD")
    ],
    how="inner"
).select(
    F.col("u.EVT_STAFF_ID").alias("EVT_STAFF_ID"),
    F.col("u.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("u.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("e.SK").alias("EVT_STAFF_SK")
)

# --------------------------------------------------------------------------------
# WebUserInfoEvtStaff (CSeqFileStage) => Write final file
# Columns (in order):
# 1) JOB_EXCTN_RCRD_ERR_SK
# 2) INSRT_UPDT_CD (char(10))
# 3) DISCARD_IN (char(1))
# 4) PASS_THRU_IN (char(1))
# 5) FIRST_RECYC_DT
# 6) ERR_CT
# 7) RECYCLE_CT
# 8) SRC_SYS_CD
# 9) PRI_KEY_STRING
# 10) EVT_STAFF_SK
# 11) EVT_STAFF_ID
# 12) CRT_RUN_CYC_EXCTN_SK
# 13) LAST_UPDT_RUN_CYC_EXCTN_SK
# 14) STAFF_TYP_ID
# 15) EVT_STAFF_ACTV_IN (char(1))
# 16) EFF_DT_SK (char(10))
# 17) TERM_DT_SK (char(10))
# 18) EVT_STAFF_EMAIL_ADDR
# 19) EVT_STAFF_NM
# 20) EVT_STAFF_PHN_NO
# 21) LAST_UPDT_DTM
# 22) LAST_UPDT_USER_ID
# --------------------------------------------------------------------------------

# Apply rpad for char columns
df_Pkey_padded = df_Pkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),  # char(10)
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),         # char(1)
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),     # char(1)
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("EVT_STAFF_SK").alias("EVT_STAFF_SK"),
    F.col("EVT_STAFF_ID").alias("EVT_STAFF_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("STAFF_TYP_ID").alias("STAFF_TYP_ID"),
    F.rpad(F.col("EVT_STAFF_ACTV_IN"), 1, " ").alias("EVT_STAFF_ACTV_IN"),  # char(1)
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),  # char(10)
    F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),# char(10)
    F.col("EVT_STAFF_EMAIL_ADDR").alias("EVT_STAFF_EMAIL_ADDR"),
    F.col("EVT_STAFF_NM").alias("EVT_STAFF_NM"),
    F.col("EVT_STAFF_PHN_NO").alias("EVT_STAFF_PHN_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

write_files(
    df_Pkey_padded,
    f"{adls_path}/key/WebUserInfoEvtStaffExtr.EvtStaff.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# hf_evt_staff_updt (CHashedFileStage) => Scenario B => Write back to same dummy table via Merge
# --------------------------------------------------------------------------------

# Create a staging table for df_hf_evt_staff_updt
temp_table_name = "STAGING.WebUserInfoEvtStaffExtr_hf_evt_staff_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)

# Write the staging table
df_hf_evt_staff_updt.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props_ids
)

# Merge (upsert) into dummy_hf_evt_staff on PK (EVT_STAFF_ID, SRC_SYS_CD)
merge_sql = (
    f"MERGE dummy_hf_evt_staff as T "
    f"USING {temp_table_name} as S "
    f"ON T.EVT_STAFF_ID = S.EVT_STAFF_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"WHEN MATCHED THEN UPDATE SET "
    f"    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    f"    T.EVT_STAFF_SK = S.EVT_STAFF_SK "
    f"WHEN NOT MATCHED THEN INSERT (EVT_STAFF_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_STAFF_SK) "
    f"    VALUES (S.EVT_STAFF_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.EVT_STAFF_SK);"
)
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)