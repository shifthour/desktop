# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 11/28/07 15:31:13 Batch  14577_56366 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 11/01/07 14:25:46 Batch  14550_51951 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 11/01/07 14:23:00 Batch  14550_51783 INIT bckcett devlIDScur dsadm bls for sa
# MAGIC ^1_2 10/30/07 17:09:53 Batch  14548_61795 INIT bckcett devlIDScur u10157 sa
# MAGIC ^1_1 10/17/07 12:20:35 Batch  14535_44460 PROMOTE bckcett devlIDScur u10157 sa - moved ids_devl DRG code to idscurdevl cause it had balancing code not yet in production
# MAGIC ^1_1 10/17/07 12:18:00 Batch  14535_44325 INIT bckcett devlIDS30 u10157 sa - moving drg code to ids_current_devl with balancing code
# MAGIC ^1_1 05/24/06 13:37:46 Batch  14024_49070 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_13 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_11 12/01/05 15:22:07 Batch  13850_55330 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_10 12/01/05 09:16:26 Batch  13850_33390 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_12 10/05/05 10:21:04 Batch  13793_37270 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_11 09/23/05 09:39:19 Batch  13781_34764 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_10 09/23/05 08:27:20 Batch  13781_30444 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_9 09/23/05 08:22:17 Batch  13781_30141 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_8 09/20/05 15:13:43 Batch  13778_54827 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_7 09/20/05 15:06:38 Batch  13778_54403 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the Foreign key.  
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:  This is run once a year - the flat file used to update the table is only updated once a year.  Users will schedule the job to run when they update the table.  Currently, the CDS table is updated, when the CDS tables are turned off, then the job control will be changed to only run the IDS update.
# MAGIC 
# MAGIC OUTPUTS:  File for DRG table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  06/2004  -   Originally Programmed
# MAGIC             BJ Luce             11/30/2005 - change drg_sk for NA to 1 instead of 0
# MAGIC             BJ Luce              05/19/2006  - take out infile and outfile parameters
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                         
# MAGIC                                                                                                                                                                                                                                                                                                                                                             
# MAGIC Date                 Developer                Project                     Change Description                                                                                                                 environment          Reviewer                Date
# MAGIC ------------------      ----------------------------     --------------------             --------------------------------------------------------------------------------------------------------------------                          -----------------------       ---------------------         -------------------
# MAGIC 2007-10-17     SAndrew                  eproject #3492         DRG conversion of DRG CMS to DRG MS.                                                                            10/25/2007          Steph Goddard      10/25/2007          
# MAGIC                                                                                           Changed default values for parmeters to environmental.                                            
# MAGIC                                                                                           changed drg record adding drg_mthd_cd/SK field in extract and fkey job.   Added new cdma lookup for
# MAGIC                                                                                            "DIAGNOSIS RELATED GROUP METHOD CODE"

# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")

schema_DrgCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(12, 4), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("DRG_SK", IntegerType(), False),
    StructField("DRG_CD", StringType(), False),
    StructField("DRG_METH_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("DRG_DIAG_CAT_CD", StringType(), False),
    StructField("DRG_TYP_CD", StringType(), False),
    StructField("DRG_DESC", StringType(), False),
    StructField("GEOMTRC_AVG_LOS", DecimalType(12, 4), False),
    StructField("ARTHMTC_AVG_LOS", DecimalType(12, 4), False),
    StructField("WT_FCTR", DecimalType(12, 4), False)
])

df_DrgCrf = (
    spark.read
    .option("quote", "\"")
    .option("header", False)
    .schema(schema_DrgCrf)
    .csv(f"{adls_path}/key/IdsDrgExtr.dat")
)

df_enriched = (
    df_DrgCrf
    .withColumn(
        "DrgMethodCdSk",
        GetFkeyCodes("FACETS", col("DRG_SK"), "DIAGNOSIS RELATED GROUP METHOD CODE", trim(col("DRG_METH_CD")), Logging)
    )
    .withColumn(
        "DiagCatCdSk",
        GetFkeyCodes("FACETS", col("DRG_SK"), "DIAGNOSIS CATEGORY", col("DRG_DIAG_CAT_CD"), Logging)
    )
    .withColumn(
        "DrgTypCdSk",
        GetFkeyCodes("FACETS", col("DRG_SK"), "DIAGNOSIS RELATED GROUP TYPE CODE", col("DRG_TYP_CD"), Logging)
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", lit(0))
)

df_Fkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("DRG_SK").alias("DRG_SK"),
        col("DRG_CD").alias("DRG_CD"),
        col("DrgMethodCdSk").alias("DRG_METH_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("DiagCatCdSk").alias("DRG_DIAG_CAT_CD_SK"),
        col("DrgTypCdSk").alias("DRG_TYP_CD_SK"),
        col("DRG_DESC").alias("DRG_DESC"),
        col("GEOMTRC_AVG_LOS").alias("GEOMTRC_AVG_LOS"),
        col("ARTHMTC_AVG_LOS").alias("ARTHMTC_AVG_LOS"),
        col("WT_FCTR").alias("WT_FCTR")
    )
)

df_DefaultUNK = spark.createDataFrame(
    [
        (0, "UNK", 0, 0, 0, 0, 0, "UNK", 0, 0, 0)
    ],
    [
        "DRG_SK",
        "DRG_CD",
        "DRG_METH_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DRG_DIAG_CAT_CD_SK",
        "DRG_TYP_CD_SK",
        "DRG_DESC",
        "GEOMTRC_AVG_LOS",
        "ARTHMTC_AVG_LOS",
        "WT_FCTR"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [
        (1, "NA", 1, 1, 1, 1, 1, "NA", 0, 0, 0)
    ],
    [
        "DRG_SK",
        "DRG_CD",
        "DRG_METH_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DRG_DIAG_CAT_CD_SK",
        "DRG_TYP_CD_SK",
        "DRG_DESC",
        "GEOMTRC_AVG_LOS",
        "ARTHMTC_AVG_LOS",
        "WT_FCTR"
    ]
)

df_Recycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("DRG_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + 1)
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_COUNT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("DRG_SK").alias("DRG_CD_SK"),
        col("DRG_CD").alias("DRG_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("DRG_DIAG_CAT_CD").alias("DRG_DIAG_CAT_CD"),
        col("DRG_TYP_CD").alias("DRG_TYP_CD"),
        col("DRG_DESC").alias("DRG_DESC"),
        col("GEOMTRC_AVG_LOS").alias("GEOMTRC_AVG_LOS"),
        col("ARTHMTC_AVG_LOS").alias("ARTHMTC_AVG_LOS"),
        col("WT_FCTR").alias("WT_FCTR")
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Fkey_sel = df_Fkey.select(
    "DRG_SK",
    "DRG_CD",
    "DRG_METH_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRG_DIAG_CAT_CD_SK",
    "DRG_TYP_CD_SK",
    "DRG_DESC",
    "GEOMTRC_AVG_LOS",
    "ARTHMTC_AVG_LOS",
    "WT_FCTR"
)
df_DefaultUNK_sel = df_DefaultUNK.select(
    "DRG_SK",
    "DRG_CD",
    "DRG_METH_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRG_DIAG_CAT_CD_SK",
    "DRG_TYP_CD_SK",
    "DRG_DESC",
    "GEOMTRC_AVG_LOS",
    "ARTHMTC_AVG_LOS",
    "WT_FCTR"
)
df_DefaultNA_sel = df_DefaultNA.select(
    "DRG_SK",
    "DRG_CD",
    "DRG_METH_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRG_DIAG_CAT_CD_SK",
    "DRG_TYP_CD_SK",
    "DRG_DESC",
    "GEOMTRC_AVG_LOS",
    "ARTHMTC_AVG_LOS",
    "WT_FCTR"
)

df_Collector = df_Fkey_sel.union(df_DefaultUNK_sel).union(df_DefaultNA_sel)

df_final = df_Collector.select(
    col("DRG_SK"),
    rpad(col("DRG_CD"), 255, " ").alias("DRG_CD"),
    col("DRG_METH_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRG_DIAG_CAT_CD_SK"),
    col("DRG_TYP_CD_SK"),
    rpad(col("DRG_DESC"), 255, " ").alias("DRG_DESC"),
    col("GEOMTRC_AVG_LOS"),
    col("ARTHMTC_AVG_LOS"),
    col("WT_FCTR")
)

write_files(
    df_final,
    f"{adls_path}/load/DRG.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)