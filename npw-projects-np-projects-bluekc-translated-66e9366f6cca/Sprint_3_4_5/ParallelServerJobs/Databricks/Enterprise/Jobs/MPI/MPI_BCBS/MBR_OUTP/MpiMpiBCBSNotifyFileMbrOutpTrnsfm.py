# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     MPIBCBSExtrCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     Converts the MPI app notification file into the format of a MPI daily file
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                       Project/                                                                                                                                                        Code                   Date
# MAGIC Developer                Date              Altiris #         Change Description                                                                                                                  Reviewer            Reviewed
# MAGIC ------------------------------  -------------------   ----------------    -----------------------------------------------------------------------------------------------------------------                                -------------------------  -------------------
# MAGIC Rick Henry              2012-07-11    4426 - MPI   Written                                                                                                                                      Bhoomi Dasari    08/17/2012
# MAGIC Abhiram Dasarathy  2012-11-14    4426 - MPI   Modified job so that it loads into MBR_OUTP table directly 
# MAGIC                                                                           instead of creating a file.
# MAGIC Hugh Sisson            2012-11-15    4426 - MPI   Switched back to writing to a file                                                                  
# MAGIC AbhiramD\(9)\(9)2013-01-08    4426 - MPI   Removed the table lookup, instead now have a hash file lookup with the                               Bhoomi Dasari     1/14/2013
# MAGIC \(9)\(9)\(9)\(9)          hash file being created in another job.
# MAGIC Sharon Andrew       2015-08-17     5318           Changed CurrDtm to CurrDtmSybTS to keep in synch with other programs                                Kalyan Neelam       2015-09-22
# MAGIC SAndrew                  2015-11-19     5318         Changed the input file Notification File's format - it no longer contains a header record.              Kalyan Neelam       2015-12-01

# MAGIC Notification File Converted to Mbr_Outp format and inserted into MBR_OUTP table
# MAGIC Hash File cleared in MpiMpiBCBSFileNotifyMbrOutpTrnsfmSeq job
# MAGIC Reads in the MPI Application file and converts it into a format that is used by the MpiMpiBCBSMbrOutpExtr job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


CurrDtmSybTS = get_widget_value('CurrDtmSybTS','')
RunID = get_widget_value('RunID','')
LoopNo = get_widget_value('LoopNo','')
FileName = get_widget_value('FileName','notification-20130117-01171545_1715.csv')
MPI_BCBSOwner = get_widget_value('$MPI_BCBSOwner','')
mpi_bcbs_secret_name = get_widget_value('mpi_bcbs_secret_name','')
MPI_BCBSEnvrnId = get_widget_value('$MPI_BCBSEnvrnId','')

schema_Notification_File = StructType([
    StructField("MPI_MBR_ID", StringType(), nullable=False),
    StructField("PRCS_OWNER_SRC_SYS_ENVRN_ID", StringType(), nullable=False),
    StructField("PREV_INDV_BE_KEY", DecimalType(38,10), nullable=False),
    StructField("ASG_INDV_BE_KEY", DecimalType(38,10), nullable=True)
])

df_Notification_File = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .schema(schema_Notification_File)
    .load(f"{adls_path_raw}/landing/{FileName}")
)

df_hf_mpi_notify_to_mbr_outp = spark.read.parquet(f"{adls_path}/hf_mpi_notify_to_mbr_outp.parquet")

df_Trans_Joined = df_Notification_File.alias("In").join(
    df_hf_mpi_notify_to_mbr_outp.alias("lkp_src_sys_cd"),
    (
        (F.col("In.MPI_MBR_ID") == F.col("lkp_src_sys_cd.MPI_MBR_ID")) &
        (F.col("In.PRCS_OWNER_SRC_SYS_ENVRN_ID") == F.col("lkp_src_sys_cd.PRCS_OWNER_SRC_SYS_ENVRN_ID"))
    ),
    how="left"
)

df_Trans = df_Trans_Joined.select(
    F.col("In.MPI_MBR_ID").alias("MBR_ID"),
    F.col("In.PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("SRC_SYS_CD"),
    F.when(F.col("lkp_src_sys_cd.SSN").isNull(), "000000000").otherwise(F.col("lkp_src_sys_cd.SSN")).alias("SSN"),
    F.when(F.col("lkp_src_sys_cd.BRTH_DT").isNull(), "1753-01-01").otherwise(F.col("lkp_src_sys_cd.BRTH_DT")).alias("BRTH_DT"),
    F.when(F.col("lkp_src_sys_cd.FIRST_NM").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.FIRST_NM")).alias("FIRST_NM"),
    F.when(F.col("lkp_src_sys_cd.MIDINIT").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.MIDINIT")).alias("MIDINIT"),
    F.when(F.col("lkp_src_sys_cd.LAST_NM").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.LAST_NM")).alias("LAST_NM"),
    F.when(F.col("lkp_src_sys_cd.NM_PFX").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.NM_PFX")).alias("NM_PFX"),
    F.when(F.col("lkp_src_sys_cd.NM_SFX").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.NM_SFX")).alias("NM_SFX"),
    F.when(F.col("lkp_src_sys_cd.MBR_GNDR_CD").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    F.when(F.col("lkp_src_sys_cd.SUB_ID").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.SUB_ID")).alias("SUB_ID"),
    F.when(F.col("lkp_src_sys_cd.MBR_SFX_NO").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.when(F.col("lkp_src_sys_cd.ADDR_LN_1").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.ADDR_LN_1")).alias("ADDR_LN_1"),
    F.when(F.col("lkp_src_sys_cd.ADDR_LN_2").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.ADDR_LN_2")).alias("ADDR_LN_2"),
    F.when(F.col("lkp_src_sys_cd.ADDR_LN_3").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.ADDR_LN_3")).alias("ADDR_LN_3"),
    F.when(F.col("lkp_src_sys_cd.CITY_NM").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.CITY_NM")).alias("CITY_NM"),
    F.when(F.col("lkp_src_sys_cd.ST_CD").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.ST_CD")).alias("SUB_ADDR_ST_CD"),
    F.when(F.col("lkp_src_sys_cd.CTRY_CD").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.CTRY_CD")).alias("CTRY_CD"),
    F.when(F.col("lkp_src_sys_cd.POSTAL_CD").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.POSTAL_CD")).alias("POSTAL_CD"),
    F.when(F.col("lkp_src_sys_cd.MBR_HOME_PHN_NO").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.MBR_HOME_PHN_NO")).alias("MBR_HOME_PHN_NO"),
    F.when(F.col("lkp_src_sys_cd.MBR_MOBL_PHN_NO").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.MBR_MOBL_PHN_NO")).alias("MBR_MOBLE_PHN_NO"),
    F.when(F.col("lkp_src_sys_cd.MBR_WORK_PHN_NO").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.MBR_WORK_PHN_NO")).alias("MBR_WORK_PHN_NO"),
    F.when(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_1").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_1")).alias("EMAIL_ADDR_TX_1"),
    F.when(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_2").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_2")).alias("EMAIL_ADDR_TX_2"),
    F.when(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_3").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.EMAIL_ADDR_TX_3")).alias("EMAIL_ADDR_TX_3"),
    F.when(F.col("lkp_src_sys_cd.MBR_RELSHP_CD").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.MBR_RELSHP_CD")).alias("MBR_RELSHP_CD"),
    F.when(F.col("lkp_src_sys_cd.GRP_ID").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.GRP_ID")).alias("GRP_ID"),
    F.when(F.col("lkp_src_sys_cd.GRP_NM").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.GRP_NM")).alias("GRP_NM"),
    F.col("In.PREV_INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(F.col("lkp_src_sys_cd.SUB_INDV_BE_KEY").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.SUB_INDV_BE_KEY")).alias("SUB_INDV_BE_KEY"),
    F.when(F.col("lkp_src_sys_cd.MPI_SUB_ID").isNull(), F.lit(0)).otherwise(F.col("lkp_src_sys_cd.MPI_SUB_ID")).alias("SUB_UNIQ_KEY"),
    F.when(F.col("lkp_src_sys_cd.GRP_EDI_VNDR_NM").isNull(), F.lit(None)).otherwise(F.col("lkp_src_sys_cd.GRP_EDI_VNDR_NM")).alias("EDI_VNDR"),
    F.when(F.col("lkp_src_sys_cd.CLNT_ID").isNull(), " ").otherwise(F.col("lkp_src_sys_cd.CLNT_ID")).alias("CLNT_ID"),
    F.when(F.col("lkp_src_sys_cd.MBR_DCSD_DT").isNull(), "2199-12-31").otherwise(F.col("lkp_src_sys_cd.MBR_DCSD_DT")).alias("MBR_DCSD_DT"),
    F.col("In.ASG_INDV_BE_KEY").alias("EID"),
    F.lit(CurrDtmSybTS).alias("SOMETIMESTAMP")
)

df_Trans = df_Trans.withColumn("MBR_ID", F.rpad(F.col("MBR_ID"), 255, " ")) \
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " ")) \
    .withColumn("SSN", F.rpad(F.col("SSN"), 255, " ")) \
    .withColumn("BRTH_DT", F.rpad(F.col("BRTH_DT"), 10, " ")) \
    .withColumn("FIRST_NM", F.rpad(F.col("FIRST_NM"), 255, " ")) \
    .withColumn("MIDINIT", F.rpad(F.col("MIDINIT"), 1, " ")) \
    .withColumn("LAST_NM", F.rpad(F.col("LAST_NM"), 255, " ")) \
    .withColumn("NM_PFX", F.rpad(F.col("NM_PFX"), 255, " ")) \
    .withColumn("NM_SFX", F.rpad(F.col("NM_SFX"), 255, " ")) \
    .withColumn("MBR_GNDR_CD", F.rpad(F.col("MBR_GNDR_CD"), 255, " ")) \
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), 255, " ")) \
    .withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " ")) \
    .withColumn("ADDR_LN_1", F.rpad(F.col("ADDR_LN_1"), 255, " ")) \
    .withColumn("ADDR_LN_2", F.rpad(F.col("ADDR_LN_2"), 255, " ")) \
    .withColumn("ADDR_LN_3", F.rpad(F.col("ADDR_LN_3"), 255, " ")) \
    .withColumn("CITY_NM", F.rpad(F.col("CITY_NM"), 255, " ")) \
    .withColumn("SUB_ADDR_ST_CD", F.rpad(F.col("SUB_ADDR_ST_CD"), 255, " ")) \
    .withColumn("CTRY_CD", F.rpad(F.col("CTRY_CD"), 255, " ")) \
    .withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 255, " ")) \
    .withColumn("MBR_HOME_PHN_NO", F.rpad(F.col("MBR_HOME_PHN_NO"), 255, " ")) \
    .withColumn("MBR_MOBLE_PHN_NO", F.rpad(F.col("MBR_MOBLE_PHN_NO"), 255, " ")) \
    .withColumn("MBR_WORK_PHN_NO", F.rpad(F.col("MBR_WORK_PHN_NO"), 255, " ")) \
    .withColumn("EMAIL_ADDR_TX_1", F.rpad(F.col("EMAIL_ADDR_TX_1"), 255, " ")) \
    .withColumn("EMAIL_ADDR_TX_2", F.rpad(F.col("EMAIL_ADDR_TX_2"), 255, " ")) \
    .withColumn("EMAIL_ADDR_TX_3", F.rpad(F.col("EMAIL_ADDR_TX_3"), 255, " ")) \
    .withColumn("MBR_RELSHP_CD", F.rpad(F.col("MBR_RELSHP_CD"), 255, " ")) \
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 255, " ")) \
    .withColumn("GRP_NM", F.rpad(F.col("GRP_NM"), 255, " ")) \
    .withColumn("EDI_VNDR", F.rpad(F.col("EDI_VNDR"), 255, " ")) \
    .withColumn("CLNT_ID", F.rpad(F.col("CLNT_ID"), 255, " ")) \
    .withColumn("MBR_DCSD_DT", F.rpad(F.col("MBR_DCSD_DT"), 10, " "))

df_final = df_Trans.select(
    "MBR_ID",
    "SRC_SYS_CD",
    "SSN",
    "BRTH_DT",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "NM_PFX",
    "NM_SFX",
    "MBR_GNDR_CD",
    "SUB_ID",
    "MBR_SFX_NO",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "SUB_ADDR_ST_CD",
    "CTRY_CD",
    "POSTAL_CD",
    "MBR_HOME_PHN_NO",
    "MBR_MOBLE_PHN_NO",
    "MBR_WORK_PHN_NO",
    "EMAIL_ADDR_TX_1",
    "EMAIL_ADDR_TX_2",
    "EMAIL_ADDR_TX_3",
    "MBR_RELSHP_CD",
    "GRP_ID",
    "GRP_NM",
    "INDV_BE_KEY",
    "SUB_INDV_BE_KEY",
    "SUB_UNIQ_KEY",
    "EDI_VNDR",
    "CLNT_ID",
    "MBR_DCSD_DT",
    "EID",
    "SOMETIMESTAMP"
)

write_files(
    df_final,
    f"{adls_path_raw}/landing/FACETS_MPI_NOTIFICATION_{MPI_BCBSEnvrnId}_{RunID}_{LoopNo}.csv",
    delimiter=";",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)