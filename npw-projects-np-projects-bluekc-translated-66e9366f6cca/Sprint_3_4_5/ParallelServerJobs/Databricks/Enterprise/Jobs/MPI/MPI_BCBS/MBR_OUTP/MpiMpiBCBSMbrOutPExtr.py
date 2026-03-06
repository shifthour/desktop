# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     MpiMpiBCBSBekeyCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     Loads MPI application files into the SQL Server MPI_BCBS MBR_OUTP table.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restore source file(s), if necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                          Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Rick Henry         2012-07-11    4426 MPI    Written                                                                                                         Bhoomi Dasari    08/17/2012
# MAGIC SAndrew            2012-10-12    4426 MPI    Renamed                                                                                                     Bhoomi Dasari    10/14/2012
# MAGIC Hugh Sisson      2012-11-15    4426 MPI    Added parameter for MPI_NTFCTN_IN                                                         
# MAGIC 
# MAGIC SAndrew            2015-08-12      5318         Renamed CurrDtm to CurrDtmSybaseTS to give indication of the format     Kalyan Neelam     2015-09-22
# MAGIC \(9)                                                   changed criteria used to update the MPI_BCBSOwner.MBR_OUTP.   No longer checking that the ENVIRONMNT_ID matchthe ProcessOWner.   
# MAGIC                                                                    set to Only updating WHERE (MPI_MBR_ID = ? AND PRCS_OWNER_SRC_SYS_ENVRN_CK = ?);


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
FileName = get_widget_value("FileName","")
CurrDtmSybaseTS = get_widget_value("CurrDtmSybaseTS","2012-08-29 12:45:23.500")
MpiNtfctnIn = get_widget_value("MpiNtfctnIn","")
MPI_BCBSOwner = get_widget_value("MPI_BCBSOwner","")
mpi_bcbs_secret_name = get_widget_value("mpi_bcbs_secret_name","")

# ----------------------------------------------------------------------------
# Read from MPI_File (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_MPI_File = StructType([
    StructField("MBR_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SSN", StringType(), True),
    StructField("BRTH_DT", DateType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("NM_PFX", StringType(), True),
    StructField("NM_SFX", StringType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("ADDR_LN_3", StringType(), True),
    StructField("CITY_NM", StringType(), True),
    StructField("SUB_ADDR_ST_CD", StringType(), True),
    StructField("CTRY_CD", StringType(), True),
    StructField("POSTAL_CD", StringType(), True),
    StructField("MBR_HOME_PHN_NO", StringType(), True),
    StructField("MBR_MOBLE_PHN_NO", StringType(), True),
    StructField("MBR_WORK_PHN_NO", StringType(), True),
    StructField("EMAIL_ADDR_TX_1", StringType(), True),
    StructField("EMAIL_ADDR_TX_2", StringType(), True),
    StructField("EMAIL_ADDR_TX_3", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("INDV_BE_KEY", DecimalType(10, 0), True),
    StructField("SUB_INDV_BE_KEY", DecimalType(10, 0), True),
    StructField("SUB_UNIQ_KEY", DecimalType(10, 0), True),
    StructField("EDI_VNDR", StringType(), True),
    StructField("CLNT_ID", StringType(), True),
    StructField("MBR_DCSD_DT", DateType(), True),
    StructField("EID", StringType(), True),
    StructField("SOMETIMESTAMP", TimestampType(), True)
])

df_MPIMbrOutp = (
    spark.read.format("csv")
    .schema(schema_MPI_File)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "000")
    .option("escape", "000")
    .load(f"{adls_path_raw}/landing/{FileName}")
)

# ----------------------------------------------------------------------------
# Read from PRCS_OWNER_SRS_SYS_ENVRN_CRSWALK (CODBCStage), removing the WHERE col=?.
# We retrieve all rows, then deduplicate on the key: PRCS_OWNER_SRC_SYS_ENVRN_ID
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(mpi_bcbs_secret_name)
extract_query_PRCS = (
    f"SELECT PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_ID, "
    f"PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PRCS_OWNER_ID, "
    f"PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.SRC_SYS_ID, "
    f"PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.ENVRN_ID, "
    f"PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK "
    f"FROM {MPI_BCBSOwner}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK"
)

df_PRCS_OWNER_SRS_SYS_ENVRN_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PRCS)
    .load()
)

# Deduplicate (Scenario A hashed file replacement)
# Key columns: PRCS_OWNER_SRC_SYS_ENVRN_ID
df_PRCS_OWNER_SRS_SYS_ENVRN_CRSWALK_dedup = dedup_sort(
    df_PRCS_OWNER_SRS_SYS_ENVRN_CRSWALK,
    ["PRCS_OWNER_SRC_SYS_ENVRN_ID"],
    [("PRCS_OWNER_SRC_SYS_ENVRN_ID", "A")]
)

# ----------------------------------------------------------------------------
# Transformer Stage "Trans"
# Left join: MPIMbrOutp.SRC_SYS_CD == lkp_src_sys_cd.PRCS_OWNER_SRC_SYS_ENVRN_ID
# ----------------------------------------------------------------------------
df_joined = df_MPIMbrOutp.alias("m").join(
    df_PRCS_OWNER_SRS_SYS_ENVRN_CRSWALK_dedup.alias("l"),
    F.col("m.SRC_SYS_CD") == F.col("l.PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    how="left"
)

df_enriched = df_joined.select(
    # MPI_MBR_ID
    F.col("m.MBR_ID").alias("MPI_MBR_ID"),
    # PRCS_OWNER_SRC_SYS_ENVRN_CK
    F.when(F.col("l.PRCS_OWNER_SRC_SYS_ENVRN_CK").isNull(), F.lit(0))
     .otherwise(F.col("l.PRCS_OWNER_SRC_SYS_ENVRN_CK"))
     .alias("PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    # PRCS_RUN_DTM -> from parameter
    F.to_timestamp(F.lit(CurrDtmSybaseTS), "yyyy-MM-dd HH:mm:ss.SSS").alias("PRCS_RUN_DTM"),
    # SSN
    F.when(F.col("m.SSN").isNull() | (F.length(trim("m.SSN")) == 0), F.lit("000000000"))
     .otherwise(F.col("m.SSN")).alias("SSN"),
    # BRTH_DT (char(10))
    F.when(F.length(trim("m.BRTH_DT")) == 0, F.lit(None)).otherwise(F.col("m.BRTH_DT")).alias("BRTH_DT"),
    # FIRST_NM
    F.when(
        F.col("m.FIRST_NM").isNull() | (F.length(trim("m.FIRST_NM")) == 0),
        F.lit(" ")
    ).otherwise(F.upper(trim("m.FIRST_NM"))).alias("FIRST_NM"),
    # MIDINIT (char(1))
    F.when(F.length(trim("m.MIDINIT")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.MIDINIT"))).alias("MIDINIT"),
    # LAST_NM
    F.when(
        F.col("m.LAST_NM").isNull() | (F.length(trim("m.LAST_NM")) == 0),
        F.lit(" ")
    ).otherwise(F.upper(trim("m.LAST_NM"))).alias("LAST_NM"),
    # NM_PFX
    F.when(F.length(trim("m.NM_PFX")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.NM_PFX"))).alias("NM_PFX"),
    # NM_SFX
    F.when(F.length(trim("m.NM_SFX")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.NM_SFX"))).alias("NM_SFX"),
    # MBR_GNDR_CD
    F.when(F.length(trim("m.MBR_GNDR_CD")) == 0, F.lit(" "))
     .otherwise(trim("m.MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    # SUB_ID
    F.when(F.length(trim("m.SUB_ID")) == 0, F.lit(" "))
     .otherwise(F.upper(trim("m.SUB_ID"))).alias("SUB_ID"),
    # MBR_SFX_NO (char(2))
    F.when(F.length(trim("m.MBR_SFX_NO")) == 0, F.lit(" "))
     .otherwise(F.upper(trim("m.MBR_SFX_NO"))).alias("MBR_SFX_NO"),
    # ADDR_LN_1
    F.when(F.length(trim("m.ADDR_LN_1")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.ADDR_LN_1"))).alias("ADDR_LN_1"),
    # ADDR_LN_2
    F.when(F.length(trim("m.ADDR_LN_2")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.ADDR_LN_2"))).alias("ADDR_LN_2"),
    # ADDR_LN_3
    F.when(F.length(trim("m.ADDR_LN_3")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.ADDR_LN_3"))).alias("ADDR_LN_3"),
    # CITY_NM
    F.when(F.length(trim("m.CITY_NM")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.CITY_NM"))).alias("CITY_NM"),
    # ST_CD
    F.when(F.length(trim("m.SUB_ADDR_ST_CD")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.SUB_ADDR_ST_CD"))).alias("ST_CD"),
    # POSTAL_CD
    F.when(F.length(trim("m.POSTAL_CD")) == 0, F.lit(None))
     .otherwise(F.col("m.POSTAL_CD")).alias("POSTAL_CD"),
    # CTRY_CD
    F.when(F.length(trim("m.CTRY_CD")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.CTRY_CD"))).alias("CTRY_CD"),
    # MBR_HOME_PHN_NO
    F.when(F.length(trim("m.MBR_HOME_PHN_NO")) == 0, F.lit(None))
     .otherwise(trim("m.MBR_HOME_PHN_NO")).alias("MBR_HOME_PHN_NO"),
    # MBR_MOBL_PHN_NO
    F.when(F.length(trim("m.MBR_MOBLE_PHN_NO")) == 0, F.lit(None))
     .otherwise(trim("m.MBR_MOBLE_PHN_NO")).alias("MBR_MOBL_PHN_NO"),
    # MBR_WORK_PHN_NO
    F.when(F.length(trim("m.MBR_WORK_PHN_NO")) == 0, F.lit(None))
     .otherwise(trim("m.MBR_WORK_PHN_NO")).alias("MBR_WORK_PHN_NO"),
    # EMAIL_ADDR_TX_1
    F.when(F.length(trim("m.EMAIL_ADDR_TX_1")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.EMAIL_ADDR_TX_1"))).alias("EMAIL_ADDR_TX_1"),
    # EMAIL_ADDR_TX_2
    F.when(F.length(trim("m.EMAIL_ADDR_TX_2")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.EMAIL_ADDR_TX_2"))).alias("EMAIL_ADDR_TX_2"),
    # EMAIL_ADDR_TX_3
    F.when(F.length(trim("m.EMAIL_ADDR_TX_3")) == 0, F.lit(None))
     .otherwise(F.upper(trim("m.EMAIL_ADDR_TX_3"))).alias("EMAIL_ADDR_TX_3"),
    # MBR_RELSHP_CD
    F.when(F.col("m.MBR_RELSHP_CD").isNull(), F.lit(" "))
     .otherwise(trim("m.MBR_RELSHP_CD")).alias("MBR_RELSHP_CD"),
    # GRP_ID
    F.when(F.col("m.GRP_ID").isNull(), F.lit(" "))
     .otherwise(trim("m.GRP_ID")).alias("GRP_ID"),
    # GRP_NM
    F.when(
        F.col("m.GRP_NM").isNull() | (F.length(trim("m.GRP_NM")) == 0),
        F.lit(" ")
    ).otherwise(F.upper(trim("m.GRP_NM"))).alias("GRP_NM"),
    # PREV_INDV_BE_KEY
    F.when(
        F.col("m.INDV_BE_KEY").isNull() | (F.length(trim("m.INDV_BE_KEY")) == 0),
        F.lit("0")
    ).otherwise(trim("m.INDV_BE_KEY")).alias("PREV_INDV_BE_KEY"),
    # SUB_INDV_BE_KEY
    F.when(
        F.col("m.SUB_INDV_BE_KEY").isNull() | (F.length(trim("m.SUB_INDV_BE_KEY")) == 0),
        F.lit("0")
    ).otherwise(trim("m.SUB_INDV_BE_KEY")).alias("SUB_INDV_BE_KEY"),
    # MPI_SUB_ID
    F.when(F.col("m.SUB_UNIQ_KEY").isNull(), F.lit(" "))
     .otherwise(trim("m.SUB_UNIQ_KEY")).alias("MPI_SUB_ID"),
    # GRP_EDI_VNDR_NM
    F.when(F.length(trim("m.EDI_VNDR")) == 0, F.lit("UNK"))
     .otherwise(F.upper(F.substring(trim("m.EDI_VNDR"), 1, 20))).alias("GRP_EDI_VNDR_NM"),
    # CLNT_ID
    F.when(F.col("m.CLNT_ID").isNull(), F.lit(" "))
     .otherwise(F.substring(trim("m.CLNT_ID"), 1, 20)).alias("CLNT_ID"),
    # MBR_DCSD_DT (char(10))
    F.when(F.length(trim("m.MBR_DCSD_DT")) == 0, F.lit("2199-12-31"))
     .otherwise(F.col("m.MBR_DCSD_DT")).alias("MBR_DCSD_DT"),
    # ASG_INDV_BE_KEY
    F.when(
        F.col("m.EID").isNull() | (F.length(trim("m.EID")) == 0),
        F.lit("0")
    ).otherwise(trim("m.EID")).alias("ASG_INDV_BE_KEY"),
    # MPI_NTFCTN_IN (char(1)), from parameter MpiNtfctnIn
    F.lit(MpiNtfctnIn).alias("MPI_NTFCTN_IN")
)

# ----------------------------------------------------------------------------
# Apply rpad for columns with known lengths (char type or declared length).
# BRTH_DT: length=10, MIDINIT: length=1, MBR_SFX_NO: length=2, MBR_DCSD_DT: length=10, MPI_NTFCTN_IN: length=1
# ----------------------------------------------------------------------------
df_enriched = df_enriched\
    .withColumn("BRTH_DT", F.rpad(F.col("BRTH_DT"), 10, " "))\
    .withColumn("MIDINIT", F.rpad(F.col("MIDINIT"), 1, " "))\
    .withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " "))\
    .withColumn("MBR_DCSD_DT", F.rpad(F.col("MBR_DCSD_DT"), 10, " "))\
    .withColumn("MPI_NTFCTN_IN", F.rpad(F.col("MPI_NTFCTN_IN"), 1, " "))

# ----------------------------------------------------------------------------
# Write to DB (CODBCStage) with Merge (Upsert)
# Primary Key: (MPI_MBR_ID, PRCS_OWNER_SRC_SYS_ENVRN_CK)
# ----------------------------------------------------------------------------
temp_table_name = "STAGING.MpiMpiBCBSMbrOutPExtr_MBR_OUTP_temp"

# Drop temp table if exists
drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

# Create the temp table by writing df_enriched
df_enriched.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

# Build merge SQL
merge_sql = f"""
MERGE INTO {MPI_BCBSOwner}.MBR_OUTP AS T
USING {temp_table_name} AS S
ON T.MPI_MBR_ID = S.MPI_MBR_ID
   AND T.PRCS_OWNER_SRC_SYS_ENVRN_CK = S.PRCS_OWNER_SRC_SYS_ENVRN_CK
WHEN MATCHED THEN
  UPDATE SET
    T.PRCS_RUN_DTM = S.PRCS_RUN_DTM,
    T.SSN = S.SSN,
    T.BRTH_DT = S.BRTH_DT,
    T.FIRST_NM = S.FIRST_NM,
    T.MIDINIT = S.MIDINIT,
    T.LAST_NM = S.LAST_NM,
    T.NM_PFX = S.NM_PFX,
    T.NM_SFX = S.NM_SFX,
    T.MBR_GNDR_CD = S.MBR_GNDR_CD,
    T.SUB_ID = S.SUB_ID,
    T.MBR_SFX_NO = S.MBR_SFX_NO,
    T.ADDR_LN_1 = S.ADDR_LN_1,
    T.ADDR_LN_2 = S.ADDR_LN_2,
    T.ADDR_LN_3 = S.ADDR_LN_3,
    T.CITY_NM = S.CITY_NM,
    T.ST_CD = S.ST_CD,
    T.POSTAL_CD = S.POSTAL_CD,
    T.CTRY_CD = S.CTRY_CD,
    T.MBR_HOME_PHN_NO = S.MBR_HOME_PHN_NO,
    T.MBR_MOBL_PHN_NO = S.MBR_MOBL_PHN_NO,
    T.MBR_WORK_PHN_NO = S.MBR_WORK_PHN_NO,
    T.EMAIL_ADDR_TX_1 = S.EMAIL_ADDR_TX_1,
    T.EMAIL_ADDR_TX_2 = S.EMAIL_ADDR_TX_2,
    T.EMAIL_ADDR_TX_3 = S.EMAIL_ADDR_TX_3,
    T.MBR_RELSHP_CD = S.MBR_RELSHP_CD,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.PREV_INDV_BE_KEY = S.PREV_INDV_BE_KEY,
    T.SUB_INDV_BE_KEY = S.SUB_INDV_BE_KEY,
    T.MPI_SUB_ID = S.MPI_SUB_ID,
    T.GRP_EDI_VNDR_NM = S.GRP_EDI_VNDR_NM,
    T.CLNT_ID = S.CLNT_ID,
    T.MBR_DCSD_DT = S.MBR_DCSD_DT,
    T.ASG_INDV_BE_KEY = S.ASG_INDV_BE_KEY,
    T.MPI_NTFCTN_IN = S.MPI_NTFCTN_IN
WHEN NOT MATCHED THEN
  INSERT (
    MPI_MBR_ID,
    PRCS_OWNER_SRC_SYS_ENVRN_CK,
    PRCS_RUN_DTM,
    SSN,
    BRTH_DT,
    FIRST_NM,
    MIDINIT,
    LAST_NM,
    NM_PFX,
    NM_SFX,
    MBR_GNDR_CD,
    SUB_ID,
    MBR_SFX_NO,
    ADDR_LN_1,
    ADDR_LN_2,
    ADDR_LN_3,
    CITY_NM,
    ST_CD,
    POSTAL_CD,
    CTRY_CD,
    MBR_HOME_PHN_NO,
    MBR_MOBL_PHN_NO,
    MBR_WORK_PHN_NO,
    EMAIL_ADDR_TX_1,
    EMAIL_ADDR_TX_2,
    EMAIL_ADDR_TX_3,
    MBR_RELSHP_CD,
    GRP_ID,
    GRP_NM,
    PREV_INDV_BE_KEY,
    SUB_INDV_BE_KEY,
    MPI_SUB_ID,
    GRP_EDI_VNDR_NM,
    CLNT_ID,
    MBR_DCSD_DT,
    ASG_INDV_BE_KEY,
    MPI_NTFCTN_IN
  )
  VALUES (
    S.MPI_MBR_ID,
    S.PRCS_OWNER_SRC_SYS_ENVRN_CK,
    S.PRCS_RUN_DTM,
    S.SSN,
    S.BRTH_DT,
    S.FIRST_NM,
    S.MIDINIT,
    S.LAST_NM,
    S.NM_PFX,
    S.NM_SFX,
    S.MBR_GNDR_CD,
    S.SUB_ID,
    S.MBR_SFX_NO,
    S.ADDR_LN_1,
    S.ADDR_LN_2,
    S.ADDR_LN_3,
    S.CITY_NM,
    S.ST_CD,
    S.POSTAL_CD,
    S.CTRY_CD,
    S.MBR_HOME_PHN_NO,
    S.MBR_MOBL_PHN_NO,
    S.MBR_WORK_PHN_NO,
    S.EMAIL_ADDR_TX_1,
    S.EMAIL_ADDR_TX_2,
    S.EMAIL_ADDR_TX_3,
    S.MBR_RELSHP_CD,
    S.GRP_ID,
    S.GRP_NM,
    S.PREV_INDV_BE_KEY,
    S.SUB_INDV_BE_KEY,
    S.MPI_SUB_ID,
    S.GRP_EDI_VNDR_NM,
    S.CLNT_ID,
    S.MBR_DCSD_DT,
    S.ASG_INDV_BE_KEY,
    S.MPI_NTFCTN_IN
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)