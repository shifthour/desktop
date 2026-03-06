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
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUMIpRvwLosExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMIR_REVIEW to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                  Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                           ----------------------------------              ---------------------------------               -------------------------
# MAGIC    
# MAGIC Tracy Davis                  03/09/2009              3808                               Originally Programmed                                                      devlIDS                              Steph Goddard                         03/30/2009
# MAGIC Prabhu ES                    2022-03-07              S2S Remediation             MSSQL ODBC conn params added                                 IntegrateDev5		Harsha Ravuri		06-14-2022

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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
TmpOutFile = get_widget_value('TmpOutFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# ---------------------------------------------
# CHashedFileStage: hf_um_ip_rvw_los_lkup (Scenario B: read from a dummy table instead of hashed file)
# ---------------------------------------------
jdbc_url_hf, jdbc_props_hf = get_db_config(facets_secret_name)
query_hf_um_ip_rvw_los = "SELECT SRC_SYS_CD, UMUM_REF_ID, UMIR_SEQ_NO, UMLS_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_IP_RVW_LOS_SK FROM " + FacetsOwner + ".dummy_hf_um_ip_rvw_los"
df_dummy_hf_um_ip_rvw_los = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf)
    .options(**jdbc_props_hf)
    .option("query", query_hf_um_ip_rvw_los)
    .load()
)

# ---------------------------------------------
# ODBCConnector: CMC_UMLS_LOS
# ---------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_UMLS_LOS = (
    "SELECT \n"
    "LOS.UMUM_REF_ID,\n"
    "LOS.UMIR_SEQ_NO,\n"
    "LOS.UMLS_SEQ_NO,\n"
    "LOS.UMLS_LOS_REQ,\n"
    "LOS.UMLS_LOS_AUTH,\n"
    "LOS.UMLS_MCTR_RDNY,\n"
    "LOS.UMLS_USID_RDNY,\n"
    "LOS.UMLS_PAID_DAYS,\n"
    "LOS.UMLS_ALLOW_DAYS \n"
    "FROM " + FacetsOwner + ".CMC_UMLS_LOS AS LOS,\n"
    "     tempdb.." + DriverTable + " as T\n"
    "WHERE LOS.UMUM_REF_ID = T.UM_REF_ID"
)
df_CMC_UMLS_LOS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMLS_LOS)
    .load()
)

# ---------------------------------------------
# CTransformerStage: StripField
# ---------------------------------------------
df_StripField = (
    df_CMC_UMLS_LOS
    .withColumn(
        "UMUM_REF_ID",
        F.trim(F.regexp_replace(F.col("UMUM_REF_ID"), "[\n\r\t]", ""))  # emulate Convert + TRIM
    )
    .select(
        F.col("UMUM_REF_ID").alias("UMUM_REF_ID"),
        F.col("UMIR_SEQ_NO").alias("UMIR_SEQ_NO"),
        F.col("UMLS_SEQ_NO").alias("UMLS_SEQ_NO"),
        F.col("UMLS_LOS_REQ").alias("UMLS_LOS_REQ"),
        F.col("UMLS_LOS_AUTH").alias("UMLS_LOS_AUTH"),
        F.col("UMLS_MCTR_RDNY").alias("UMLS_MCTR_RDNY"),
        F.col("UMLS_USID_RDNY").alias("UMLS_USID_RDNY"),
        F.col("UMLS_PAID_DAYS").alias("UMLS_PAID_DAYS"),
        F.col("UMLS_ALLOW_DAYS").alias("UMLS_ALLOW_DAYS"),
    )
)

# ---------------------------------------------
# CTransformerStage: BusinessRules
# ---------------------------------------------
df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(";", F.lit("FACETS"), F.col("UMUM_REF_ID"), F.col("UMIR_SEQ_NO"), F.col("UMLS_SEQ_NO"))
    )
    .withColumn("UM_IP_RVW_LOS_SK", F.lit(0))
    .withColumn("UMUM_REF_ID", F.col("UMUM_REF_ID"))
    .withColumn("UMIR_SEQ_NO", F.col("UMIR_SEQ_NO"))
    .withColumn("UMLS_SEQ_NO", F.col("UMLS_SEQ_NO"))
    .withColumn(
        "UMLS_LOS_REQ",
        F.when(F.col("UMLS_LOS_REQ").isNull() | (F.length(F.col("UMLS_LOS_REQ")) == 0), F.lit(0))
         .otherwise(F.col("UMLS_LOS_REQ"))
    )
    .withColumn(
        "UMLS_LOS_AUTH",
        F.when(F.col("UMLS_LOS_AUTH").isNull() | (F.length(F.col("UMLS_LOS_AUTH")) == 0), F.lit(0))
         .otherwise(F.col("UMLS_LOS_AUTH"))
    )
    .withColumn(
        "UMLS_MCTR_RDNY",
        F.when(
            F.col("UMLS_MCTR_RDNY").isNull() | (F.length(F.col("UMLS_MCTR_RDNY")) == 0) | (F.upper(F.col("UMLS_MCTR_RDNY")) == "NONE"),
            F.lit("NA")
        ).otherwise(F.col("UMLS_MCTR_RDNY"))
    )
    .withColumn(
        "UMLS_USID_RDNY",
        F.when(
            F.col("UMLS_USID_RDNY").isNull() | (F.length(F.col("UMLS_USID_RDNY")) == 0),
            F.lit("NA")
        ).otherwise(F.col("UMLS_USID_RDNY"))
    )
    .withColumn(
        "UMLS_PAID_DAYS",
        F.when(F.col("UMLS_PAID_DAYS").isNull() | (F.length(F.col("UMLS_PAID_DAYS")) == 0), F.lit(0))
         .otherwise(F.col("UMLS_PAID_DAYS"))
    )
    .withColumn(
        "UMLS_ALLOW_DAYS",
        F.when(F.col("UMLS_ALLOW_DAYS").isNull() | (F.length(F.col("UMLS_ALLOW_DAYS")) == 0), F.lit(0))
         .otherwise(F.col("UMLS_ALLOW_DAYS"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
)

# ---------------------------------------------
# CTransformerStage: PrimaryKey (Join with dummy_hf_um_ip_rvw_los, scenario B logic)
# ---------------------------------------------
df_PrimaryKeyJoin = (
    df_BusinessRules.alias("Transform")
    .join(
        df_dummy_hf_um_ip_rvw_los.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.UMUM_REF_ID") == F.col("lkup.UMUM_REF_ID"),
            F.col("Transform.UMIR_SEQ_NO") == F.col("lkup.UMIR_SEQ_NO"),
            F.col("Transform.UMLS_SEQ_NO") == F.col("lkup.UMLS_SEQ_NO")
        ],
        how="left"
    )
)

df_PrimaryKeyInterim = (
    df_PrimaryKeyJoin
    .withColumn(
        "UM_IP_RVW_LOS_SK",
        F.when(F.col("lkup.UM_IP_RVW_LOS_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.UM_IP_RVW_LOS_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.UM_IP_RVW_LOS_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.lit(CurrRunCycle)
    )
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
    .withColumn("UMIR_SEQ_NO", F.col("Transform.UMIR_SEQ_NO"))
    .withColumn("UMLS_SEQ_NO", F.col("Transform.UMLS_SEQ_NO"))
    .withColumn("UMLS_LOS_REQ", F.col("Transform.UMLS_LOS_REQ"))
    .withColumn("UMLS_LOS_AUTH", F.col("Transform.UMLS_LOS_AUTH"))
    .withColumn("UMLS_MCTR_RDNY", F.col("Transform.UMLS_MCTR_RDNY"))
    .withColumn("UMLS_USID_RDNY", F.col("Transform.UMLS_USID_RDNY"))
    .withColumn("UMLS_PAID_DAYS", F.col("Transform.UMLS_PAID_DAYS"))
    .withColumn("UMLS_ALLOW_DAYS", F.col("Transform.UMLS_ALLOW_DAYS"))
)

# SurrogateKeyGen for UM_IP_RVW_LOS_SK (replacing KeyMgtGetNextValueConcurrent)
df_enriched = df_PrimaryKeyInterim
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"UM_IP_RVW_LOS_SK",<schema>,<secret_name>)

# We now split out the final sets of columns.

# DataFrame for writing to the sequential file "Key" link (IdsUmIpRvwLosExtr):
df_IdsUmIpRvwLosExtr = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "UM_IP_RVW_LOS_SK",
    "UMUM_REF_ID",
    "UMIR_SEQ_NO",
    "UMLS_SEQ_NO",
    "UMLS_LOS_REQ",
    "UMLS_LOS_AUTH",
    "UMLS_MCTR_RDNY",
    "UMLS_USID_RDNY",
    "UMLS_PAID_DAYS",
    "UMLS_ALLOW_DAYS",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# Apply rpad for char/varchar columns with known lengths; if unknown length is needed, use <...>.
df_IdsUmIpRvwLosExtr = (
    df_IdsUmIpRvwLosExtr
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("UMUM_REF_ID", F.rpad(F.col("UMUM_REF_ID"), 9, " "))
    .withColumn("UMLS_MCTR_RDNY", F.rpad(F.col("UMLS_MCTR_RDNY"), 4, " "))
    .withColumn("UMLS_USID_RDNY", F.rpad(F.col("UMLS_USID_RDNY"), 10, " "))
    # The columns below are varchar without known length:
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"),  <...>, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"),  <...>, " "))
)

# Write the "IdsUmIpRvwLosExtr" file
write_files(
    df_IdsUmIpRvwLosExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------
# MERGE logic for the hashed file rewrite (hf_um_ip_rvw_los_write)
# We only insert where lkup.UM_IP_RVW_LOS_SK was null originally
# ---------------------------------------------
df_updt = df_enriched.filter(F.col("lkup.UM_IP_RVW_LOS_SK").isNull()).select(
    F.col("SRC_SYS_CD"),
    F.col("UMUM_REF_ID"),
    F.col("UMIR_SEQ_NO"),
    F.col("UMLS_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("UM_IP_RVW_LOS_SK")
)

temp_table_name = "STAGING.FctsUMIpRvwLosExtr_hf_um_ip_rvw_los_write_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url_hf, jdbc_props_hf)

# Create the staging table via JDBC
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_hf) \
    .options(**jdbc_props_hf) \
    .option("dbtable", temp_table_name) \
    .mode("append") \
    .save()

merge_sql = (
    f"MERGE {FacetsOwner}.dummy_hf_um_ip_rvw_los AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.UMUM_REF_ID = S.UMUM_REF_ID "
    f"AND T.UMIR_SEQ_NO = S.UMIR_SEQ_NO "
    f"AND T.UMLS_SEQ_NO = S.UMLS_SEQ_NO "
    f"WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, UMUM_REF_ID, UMIR_SEQ_NO, UMLS_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_IP_RVW_LOS_SK) "
    f"VALUES (S.SRC_SYS_CD, S.UMUM_REF_ID, S.UMIR_SEQ_NO, S.UMLS_SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.UM_IP_RVW_LOS_SK);"
)
execute_dml(merge_sql, jdbc_url_hf, jdbc_props_hf)

# ---------------------------------------------
# ODBCConnector: CMC_UMLS_LOS_Snapshot
# ---------------------------------------------
extract_query_CMC_UMLS_LOS_Snapshot = (
    "SELECT \n"
    "LOS.UMUM_REF_ID,\n"
    "LOS.UMIR_SEQ_NO,\n"
    "LOS.UMLS_SEQ_NO\n"
    "FROM " + FacetsOwner + ".CMC_UMLS_LOS AS LOS,\n"
    "     tempdb.." + DriverTable + " as T\n"
    "WHERE LOS.UMUM_REF_ID = T.UM_REF_ID"
)
df_CMC_UMLS_LOS_Snapshot = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMLS_LOS_Snapshot)
    .load()
)

# ---------------------------------------------
# CTransformerStage: StripField_Snapshot
# ---------------------------------------------
df_StripField_Snapshot = (
    df_CMC_UMLS_LOS_Snapshot
    .withColumn(
        "UM_REF_ID",
        F.trim(F.regexp_replace(F.col("UMUM_REF_ID"), "[\n\r\t]", ""))
    )
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("UMIR_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
        F.col("UMLS_SEQ_NO").alias("UM_IP_RVW_LOS_SEQ_NO")
    )
)

# ---------------------------------------------
# CSeqFileStage: B_UM_IP_RVW_LOS
# ---------------------------------------------
df_B_UM_IP_RVW_LOS = df_StripField_Snapshot.select(
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "UM_IP_RVW_SEQ_NO",
    "UM_IP_RVW_LOS_SEQ_NO"
)

df_B_UM_IP_RVW_LOS = (
    df_B_UM_IP_RVW_LOS
    .withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), 9, " "))
)

write_files(
    df_B_UM_IP_RVW_LOS,
    f"{adls_path}/load/B_UM_IP_RVW_LOS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)