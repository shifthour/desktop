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
# MAGIC JOB NAME:     FctsUMIpSttusExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMIR_REVIEW to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description				Development Project	Code Reviewer		Date Reviewed
# MAGIC -----------------------	-------------------	-----------------------	---------------------------------------------------------		----------------------------------	---------------------------------	-------------------------
# MAGIC    
# MAGIC Tracy Davis	03/6/2009	3808		Created job				devlIDS			Steph Goddard                        03/30/2009
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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

# Read CMC_UMIT_STATUS (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_UMIT_STATUS = (
    f"SELECT \n"
    f"STTUS.UMUM_REF_ID,\n"
    f"STTUS.UMIT_SEQ_NO,\n"
    f"STTUS.USUS_ID,\n"
    f"STTUS.UMIT_STS,\n"
    f"STTUS.UMIT_STS_DTM,\n"
    f"STTUS.UMIT_MCTR_REAS \n"
    f"FROM {FacetsOwner}.CMC_UMIT_STATUS STTUS,\n"
    f"     tempdb..{DriverTable} as T\n"
    f"WHERE STTUS.UMUM_REF_ID = T.UM_REF_ID"
)
df_CMC_UMIT_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMIT_STATUS)
    .load()
)

# StripField (CTransformerStage)
df_StripField = (
    df_CMC_UMIT_STATUS
    .withColumn("UMUM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UMIT_SEQ_NO", F.col("UMIT_SEQ_NO"))
    .withColumn("USUS_ID", trim(strip_field(F.col("USUS_ID"))))
    .withColumn("UMIT_STS", trim(strip_field(F.col("UMIT_STS"))))
    .withColumn("UMIT_STS_DTM", F.col("UMIT_STS_DTM"))
    .withColumn("UMIT_MCTR_REAS", trim(strip_field(F.col("UMIT_MCTR_REAS"))))
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
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS"), F.lit(";"), F.col("UMUM_REF_ID"), F.lit(";"), F.col("UMIT_SEQ_NO")))
    .withColumn("UMUM_REF_ID", F.col("UMUM_REF_ID"))
    .withColumn("UMIT_SEQ_NO", F.col("UMIT_SEQ_NO"))
    .withColumn("USUS_ID", F.when(F.length(F.col("USUS_ID")) == 0, F.lit("NA")).otherwise(F.col("USUS_ID")))
    .withColumn("UMIT_STS", F.when(F.length(F.col("UMIT_STS")) == 0, F.lit("NA")).otherwise(F.col("UMIT_STS")))
    .withColumn("UMIT_STS_DTM", F.date_format(F.col("UMIT_STS_DTM"), "yyyy-MM-dd"))
    .withColumn("UMIT_MCTR_REAS",
                F.when(
                    (F.length(F.col("UMIT_MCTR_REAS")) == 0) | (F.upper(F.col("UMIT_MCTR_REAS")) == F.lit("NONE")),
                    F.lit("NA")
                ).otherwise(F.col("UMIT_MCTR_REAS")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
)

# Read-Modify-Write Hashed File => scenario B. Replace with dummy_hf_um_ip_sttus table
jdbc_url_ids, jdbc_props_ids = get_db_config(<...>)  # Placeholder for the IDS secret if needed
df_hf_um_ip_sttus_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, UMUM_REF_ID, UMIT_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_IP_STTUS_SK FROM dummy_hf_um_ip_sttus")
    .load()
)

# PrimaryKey (CTransformerStage) left join with hashed file dummy table
df_joined = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_um_ip_sttus_lkup.alias("lkup"),
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
        (F.col("Transform.UMUM_REF_ID") == F.col("lkup.UMUM_REF_ID")) &
        (F.col("Transform.UMIT_SEQ_NO") == F.col("lkup.UMIT_SEQ_NO")),
        how="left"
    )
)

df_pk = (
    df_joined
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.UM_IP_STTUS_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "UM_IP_STTUS_SK",
        F.when(F.col("lkup.UM_IP_STTUS_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.UM_IP_STTUS_SK"))
    )
)

# SurrogateKeyGen for UM_IP_STTUS_SK (replacing KeyMgtGetNextValueConcurrent)
df_enriched = SurrogateKeyGen(df_pk,<DB sequence name>,'UM_IP_STTUS_SK',<schema>,<secret_name>)

# Final output for "Key" link => IdsUmIpSttusExtr
df_IdsUmIpSttusExtr = (
    df_enriched
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
        "UMUM_REF_ID",
        "UMIT_SEQ_NO",
        "USUS_ID",
        "UMIT_STS",
        "UMIT_STS_DTM",
        "UMIT_MCTR_REAS",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "UM_IP_STTUS_SK"
    )
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("UMUM_REF_ID", F.rpad(F.col("UMUM_REF_ID"), 9, " "))
    .withColumn("USUS_ID", F.rpad(F.col("USUS_ID"), 10, " "))
    .withColumn("UMIT_STS", F.rpad(F.col("UMIT_STS"), 2, " "))
    .withColumn("UMIT_MCTR_REAS", F.rpad(F.col("UMIT_MCTR_REAS"), 4, " "))
)

write_files(
    df_IdsUmIpSttusExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Rows for hf_um_ip_sttus_write => only when lkup.UM_IP_STTUS_SK is null => insert
df_newRows = df_enriched.filter(F.col("lkup.UM_IP_STTUS_SK").isNull())

df_hf_um_ip_sttus_write = (
    df_newRows
    .select(
        "SRC_SYS_CD",
        "UMUM_REF_ID",
        "UMIT_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "UM_IP_STTUS_SK"
    )
)

# Merge into dummy_hf_um_ip_sttus
temp_table_write = "STAGING.FctsUMIpSttusExtr_hf_um_ip_sttus_write_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_write}", jdbc_url_ids, jdbc_props_ids)
(
    df_hf_um_ip_sttus_write.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_write)
    .mode("overwrite")
    .save()
)

merge_sql_hf = (
    f"MERGE INTO dummy_hf_um_ip_sttus AS T "
    f"USING {temp_table_write} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.UMUM_REF_ID = S.UMUM_REF_ID "
    f"AND T.UMIT_SEQ_NO = S.UMIT_SEQ_NO "
    f"WHEN MATCHED THEN "
    f"  UPDATE SET "
    f"    -- no updates "
    f"WHEN NOT MATCHED THEN "
    f"  INSERT (SRC_SYS_CD, UMUM_REF_ID, UMIT_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_IP_STTUS_SK) "
    f"  VALUES (S.SRC_SYS_CD, S.UMUM_REF_ID, S.UMIT_SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.UM_IP_STTUS_SK);"
)
execute_dml(merge_sql_hf, jdbc_url_ids, jdbc_props_ids)

# Read CMC_UMIT_STATUS_Snapshot (ODBCConnector)
extract_query_CMC_UMIT_STATUS_Snapshot = (
    f"SELECT \n"
    f"STTUS.UMUM_REF_ID,\n"
    f"STTUS.UMIT_SEQ_NO\n"
    f"FROM {FacetsOwner}.CMC_UMIT_STATUS STTUS,\n"
    f"     tempdb..{DriverTable} as T\n"
    f"WHERE STTUS.UMUM_REF_ID = T.UM_REF_ID"
)
df_CMC_UMIT_STATUS_Snapshot = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMIT_STATUS_Snapshot)
    .load()
)

# StripField_Snapshot (CTransformerStage)
df_StripField_Snapshot = (
    df_CMC_UMIT_STATUS_Snapshot
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("UM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UM_IP_STTUS_SEQ_NO", F.col("UMIT_SEQ_NO"))
)

# B_UM_IP_STTUS (CSeqFileStage)
df_B_UM_IP_STTUS = (
    df_StripField_Snapshot
    .select(
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_IP_STTUS_SEQ_NO"
    )
    .withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), 9, " "))
)

write_files(
    df_B_UM_IP_STTUS,
    f"{adls_path}/load/B_UM_IP_STTUS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)