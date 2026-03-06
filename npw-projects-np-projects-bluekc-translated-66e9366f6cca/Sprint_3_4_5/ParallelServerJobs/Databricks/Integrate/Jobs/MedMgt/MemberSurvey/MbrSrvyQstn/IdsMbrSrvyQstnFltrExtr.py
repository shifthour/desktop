# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsMbrSrvyQstnFltrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Extracts data from UWS MBR_SRVY_QSTN_FLTR and creates a key file to be used by the Fkey job
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam        2012-09-19              4830 & 4735             Initial Programming                                                   IntegrateNewDevl                   Bhoomi Dasari         09/25/2012
# MAGIC Kalyan Neelam         2014-10-15               TFS 9558                Added balancing snap shot file                                IntegrateNewDevl                  Bhoomi Dasari         10/20/2014

# MAGIC MBR_SRVY_QSTN_FLTR Extract Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# ---------------------------------------------------------
# Parameter Retrieval
# ---------------------------------------------------------
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
UWSOwner = get_widget_value('UWSOwner', '')
uws_secret_name = get_widget_value('uws_secret_name', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '100')
CurrDate = get_widget_value('CurrDate', '2011-04-06')

# ---------------------------------------------------------
# Read from ODBC Stage: MBR_SRVY_QSTN_FLTR (CODBCStage)
# (Ignoring the "?" parameter placeholders per instructions, read entire table)
# ---------------------------------------------------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
extract_query_mbr_srvy_qstn_fltr = (
    f"SELECT "
    f"MBR_SRVY_QSTN_FLTR.SRC_SYS_CD, "
    f"MBR_SRVY_QSTN_FLTR.MBR_SRVY_TYP_CD, "
    f"MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_CD_TX, "
    f"MBR_SRVY_QSTN_FLTR.EFF_DT_SK, "
    f"MBR_SRVY_QSTN_FLTR.TERM_DT_SK, "
    f"MBR_SRVY_QSTN_FLTR.INCLD_RSPN_IN, "
    f"MBR_SRVY_QSTN_FLTR.RQRD_QSTN_IN, "
    f"MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_RQRD_RSN_CD, "
    f"MBR_SRVY_QSTN_FLTR.USER_ID, "
    f"MBR_SRVY_QSTN_FLTR.LAST_UPDT_DT_SK "
    f"FROM {UWSOwner}.MBR_SRVY_QSTN_FLTR MBR_SRVY_QSTN_FLTR"
)
df_MBR_SRVY_QSTN_FLTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_mbr_srvy_qstn_fltr)
    .load()
)

# ---------------------------------------------------------
# Dummy table replacement for hashed file (Scenario B):
# hf_mbr_srvy_qstn_fltr_lkup => "dummy_hf_mbr_srvy_qstn_fltr"
# ---------------------------------------------------------
# Reading from the dummy table in IDS schema
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_mbr_srvy_qstn_fltr_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_SRVY_QSTN_CD_TX, EFF_DT_SK, "
        "CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_QUESTON_FLTR_SK "
        "FROM IDS.dummy_hf_mbr_srvy_qstn_fltr"
    )
    .load()
)

# ---------------------------------------------------------
# Read from DB2Connector: CD_MPPNG (IDS)
# ---------------------------------------------------------
extract_query_cd_mppng = (
    f"SELECT CD_MPPNG.SRC_CD as SRC_CD, CD_MPPNG.TRGT_CD as TRGT_CD "
    f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG "
    f"WHERE CD_MPPNG.TRGT_DOMAIN_NM = 'MEMBER SURVEY TYPE'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppng)
    .load()
)

# ---------------------------------------------------------
# hf_mbrsrvyqstnfltr_srvytypcd (Scenario A)
# AnyStage -> CHashedFileStage -> AnyStage with no rewrite
# => Replace with direct DataFrame flow + deduplicate on key columns
# Key column: SRC_CD
# ---------------------------------------------------------
df_hf_mbrsrvyqstnfltr_srvytypcd = dedup_sort(
    df_CD_MPPNG,
    partition_cols=["SRC_CD"],
    sort_cols=[]
)

# ---------------------------------------------------------
# Transformer_169
# Primary input: df_MBR_SRVY_QSTN_FLTR (alias "Extract")
# Lookup link: df_hf_mbrsrvyqstnfltr_srvytypcd (alias "SrvyTyp_lkup")
# Join condition: upper(trim(Extract.MBR_SRVY_TYP_CD)) = SrvyTyp_lkup.SRC_CD (left join)
# Stage variables -> RowPassThru='Y', svMbrSrvyTypCd= if null(...) then 'UNK' else ...
# Two output links: Transform, Snapshot
# ---------------------------------------------------------
df_Transformer_169_input = (
    df_MBR_SRVY_QSTN_FLTR.alias("Extract")
    .join(
        df_hf_mbrsrvyqstnfltr_srvytypcd.alias("SrvyTyp_lkup"),
        (F.upper(F.trim(F.col("Extract.MBR_SRVY_TYP_CD"))) == F.col("SrvyTyp_lkup.SRC_CD")),
        how="left"
    )
)

df_Transformer_169 = (
    df_Transformer_169_input
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn(
        "svMbrSrvyTypCd",
        F.when(F.col("SrvyTyp_lkup.TRGT_CD").isNull(), F.lit("UNK"))
        .otherwise(F.col("SrvyTyp_lkup.TRGT_CD"))
    )
)

# ----- Transform output link -----
df_Transformer_169_Transform = df_Transformer_169.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),            # char(10)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),                # char(1)
    F.rpad(F.col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),    # char(1)
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(
        F.col("Extract.SRC_SYS_CD"),
        F.lit(";"),
        F.col("svMbrSrvyTypCd"),
        F.lit(";"),
        F.col("Extract.MBR_SRVY_QSTN_CD_TX"),
        F.lit(";"),
        F.col("Extract.EFF_DT_SK")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_SRVY_QUESTON_FLTR_SK"),
    F.col("svMbrSrvyTypCd").alias("MBR_SRVY_TYP_CD"),
    F.col("Extract.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    F.rpad(F.col("Extract.EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),   # char(10)
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.upper(F.trim(F.col("Extract.MBR_SRVY_QSTN_RQRD_RSN_CD"))).alias("MBR_SRVY_QSTN_RQRD_RSN_CD"),
    F.rpad(F.col("Extract.RQRD_QSTN_IN"), 1, " ").alias("RQRD_QSTN_IN"),    # char(1)
    F.rpad(F.col("Extract.TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),       # char(10)
    F.rpad(F.col("Extract.LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),  # char(10)
    F.col("Extract.USER_ID").alias("LAST_UPDT_USER_ID"),
    F.rpad(F.col("Extract.INCLD_RSPN_IN"), 1, " ").alias("INCLD_RSPN_IN")   # char(1)
)

# ----- Snapshot output link -----
df_Transformer_169_Snapshot = df_Transformer_169.select(
    F.call_udf(
        "GetFkeyCodes",
        F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.col("Extract.SRC_SYS_CD"), F.lit("X")
    ).alias("SRC_SYS_CD_SK"),
    F.col("svMbrSrvyTypCd").alias("MBR_SRVY_TYP_CD"),
    F.col("Extract.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    F.rpad(F.col("Extract.EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")  # char(10)
)

# ---------------------------------------------------------
# B_MBR_SRVY_QSTN_FLTR (CSeqFileStage)
# Write the "Snapshot" link to a delimited file
# ---------------------------------------------------------
write_files(
    df_Transformer_169_Snapshot.select(
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "MBR_SRVY_QSTN_CD_TX",
        "EFF_DT_SK"
    ),
    f"{adls_path}/load/B_MBR_SRVY_QSTN_FLTR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------------------
# Pkey Transformer
# Primary link: df_Transformer_169_Transform => alias("Transform")
# Lookup link: df_hf_mbr_srvy_qstn_fltr_lkup => alias("lkup") => left join
# Additional logic: KeyMgtGetNextValueConcurrent => SurrogateKeyGen
# ---------------------------------------------------------
df_Pkey_input = (
    df_Transformer_169_Transform.alias("Transform")
    .join(
        df_hf_mbr_srvy_qstn_fltr_lkup.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.MBR_SRVY_TYP_CD") == F.col("lkup.MBR_SRVY_TYP_CD"),
            F.col("Transform.MBR_SRVY_QSTN_CD_TX") == F.col("lkup.MBR_SRVY_QSTN_CD_TX"),
            F.col("Transform.EFF_DT_SK") == F.col("lkup.EFF_DT_SK")
        ],
        how="left"
    )
)

# Stage variable 'SK': if null => KeyMgtGetNextValueConcurrent, else existing
# Implement as SurrogateKeyGen on a temporary placeholder, filling missing keys
# Also handle 'NewCrtRunCycExtcnSk' logic
df_enriched = (
    df_Pkey_input
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK_new",
        F.when(F.col("lkup.MBR_SRVY_QUESTON_FLTR_SK").isNull(), F.lit(CurrRunCycle))
        .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "MBR_SRVY_QUESTON_FLTR_SK_temp",
        F.when(F.col("lkup.MBR_SRVY_QUESTON_FLTR_SK").isNull(), F.lit(None).cast(IntegerType()))
        .otherwise(F.col("lkup.MBR_SRVY_QUESTON_FLTR_SK"))
    )
)

# Call SurrogateKeyGen to fill the null surrogate keys
df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "MBR_SRVY_QUESTON_FLTR_SK_temp",
    <schema>,
    <secret_name>
)

# Rename the surrogate key column to the final name
df_enriched = df_enriched.drop("MBR_SRVY_QUESTON_FLTR_SK")
df_enriched = df_enriched.withColumnRenamed("MBR_SRVY_QUESTON_FLTR_SK_temp", "MBR_SRVY_QUESTON_FLTR_SK")

# ----- "Key" link output -----
df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_SRVY_QUESTON_FLTR_SK").alias("MBR_SRVY_QUESTON_FLTR_SK"),
    F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("Transform.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    F.col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_SRVY_QSTN_RQRD_RSN_CD").alias("MBR_SRVY_QSTN_RQRD_RSN_CD"),
    F.col("Transform.RQRD_QSTN_IN").alias("RQRD_QSTN_IN"),
    F.col("Transform.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Transform.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    F.col("Transform.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("Transform.INCLD_RSPN_IN").alias("INCLD_RSPN_IN")
)

# Write "KeyFile" => IdsMbrSrvyQstnFltr.MbrSrvyQstnFltr.dat
write_files(
    df_Key.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MBR_SRVY_QUESTON_FLTR_SK",
        "MBR_SRVY_TYP_CD",
        "MBR_SRVY_QSTN_CD_TX",
        "EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SRVY_QSTN_RQRD_RSN_CD",
        "RQRD_QSTN_IN",
        "TERM_DT_SK",
        "LAST_UPDT_DT_SK",
        "LAST_UPDT_USER_ID",
        "INCLD_RSPN_IN"
    ),
    f"{adls_path}/key/IdsMbrSrvyQstnFltr.MbrSrvyQstnFltr.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----- "updt" link output => only rows where lookup was null => insert into dummy table
df_updt = (
    df_enriched
    .filter(F.col("lkup.MBR_SRVY_QUESTON_FLTR_SK").isNull())
    .select(
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
        F.col("Transform.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
        F.col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SRVY_QUESTON_FLTR_SK").alias("MBR_SRVY_QUESTON_FLTR_SK")
    )
)

# ---------------------------------------------------------
# Write back to dummy_hf_mbr_srvy_qstn_fltr (Scenario B upsert)
# ---------------------------------------------------------
temp_table_name = "STAGING.IdsMbrSrvyQstnFltr_hf_mbr_srvy_qstn_fltr_updt_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url_ids, jdbc_props_ids)

(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO IDS.dummy_hf_mbr_srvy_qstn_fltr AS T "
    f"USING {temp_table_name} AS S "
    f"ON "
    f"(T.SRC_SYS_CD=S.SRC_SYS_CD "
    f"AND T.MBR_SRVY_TYP_CD=S.MBR_SRVY_TYP_CD "
    f"AND T.MBR_SRVY_QSTN_CD_TX=S.MBR_SRVY_QSTN_CD_TX "
    f"AND T.EFF_DT_SK=S.EFF_DT_SK) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK, "
    f"T.MBR_SRVY_QUESTON_FLTR_SK=S.MBR_SRVY_QUESTON_FLTR_SK "
    f"WHEN NOT MATCHED THEN INSERT ( "
    f"SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_SRVY_QSTN_CD_TX, EFF_DT_SK, "
    f"CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_QUESTON_FLTR_SK ) "
    f"VALUES ( "
    f"S.SRC_SYS_CD, S.MBR_SRVY_TYP_CD, S.MBR_SRVY_QSTN_CD_TX, S.EFF_DT_SK, "
    f"S.CRT_RUN_CYC_EXCTN_SK, S.MBR_SRVY_QUESTON_FLTR_SK );"
)
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)