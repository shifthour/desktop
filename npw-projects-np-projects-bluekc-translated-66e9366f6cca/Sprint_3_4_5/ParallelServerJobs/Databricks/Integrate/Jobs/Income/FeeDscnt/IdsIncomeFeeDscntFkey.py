# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 04/09/07 08:24:30 Batch  14344_30274 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 12/18/06 12:24:28 Batch  14232_44709 INIT bckcetl ids20 dsadm Income Backup for 12/18/2006 install
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:14:49 Batch  14001_45485 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:11:02 Batch  14001_40263 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFeeDscntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsFeeDscntExtr
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Fkey lookups
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker   10/05/2005  -   Originally Programmed
# MAGIC        
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-26                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                             Steph Goddard           10/03/2008

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all required parameter values
InFile = get_widget_value("InFile","FEE_DSCNT.Tmp")
Logging = get_widget_value("Logging","Y")
OutFile = get_widget_value("OutFile","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

# Define schema for input file (IdsFeeDscnt stage)
schema_IdsFeeDscnt = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("FEE_DSCNT_SK", IntegerType(), False),
    StructField("FEE_DSCNT_ID", StringType(), False),
    StructField("FNCL_LOB_CD", StringType(), False),
    StructField("FEE_DSCNT_CD", StringType(), False),
    StructField("FEE_DSCNT_LOB_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])

# Read the incoming sequential file (IdsFeeDscnt)
df_IdsFeeDscnt = (
    spark.read
    .schema(schema_IdsFeeDscnt)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/key/{InFile}")
)

# Transformer: ForeignKey stage
df_foreignKey = (
    df_IdsFeeDscnt
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("FnclLob", GetFkeyFnclLob(lit("PSI"), col("FEE_DSCNT_SK"), col("FNCL_LOB_CD"), lit(Logging)))
    .withColumn("FeeDscnt", GetFkeyCodes(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), lit("FEE DISCOUNT"), col("FEE_DSCNT_CD"), lit(Logging)))
    .withColumn("FeeDscntLob", GetFkeyCodes(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), lit("CLAIM LINE LOB"), col("FEE_DSCNT_LOB_CD"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("FEE_DSCNT_SK")))
)

# Output link "Fkey" (ErrCount = 0 Or PassThru = 'Y')
df_Fkey = (
    df_foreignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("FnclLob").alias("FNCL_LOB_SK"),
        col("FeeDscnt").alias("FEE_DSCNT_CD_SK"),
        col("FeeDscntLob").alias("FEE_DSCNT_LOB_CD_SK")
    )
)

# Output link "DefaultUNK" (@INROWNUM = 1)
df_DefaultUNK = (
    df_foreignKey
    .limit(1)
    .select(
        lit(0).alias("FEE_DSCNT_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("FEE_DSCNT_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FNCL_LOB_SK"),
        lit(0).alias("FEE_DSCNT_CD_SK"),
        lit(0).alias("FEE_DSCNT_LOB_CD_SK")
    )
)

# Output link "DefaultNA" (@INROWNUM = 1)
df_DefaultNA = (
    df_foreignKey
    .limit(1)
    .select(
        lit(1).alias("FEE_DSCNT_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("FEE_DSCNT_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("FNCL_LOB_SK"),
        lit(1).alias("FEE_DSCNT_CD_SK"),
        lit(1).alias("FEE_DSCNT_LOB_CD_SK")
    )
)

# Output link "Recycle" (ErrCount > 0)
df_Recycle = (
    df_foreignKey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("FEE_DSCNT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        col("FEE_DSCNT_CD").alias("FEE_DSCNT_CD"),
        col("FEE_DSCNT_LOB_CD").alias("FEE_DSCNT_LOB_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Write hashed file hf_recycle as parquet (Scenario C)
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Collector stage: union of (Fkey, DefaultUNK, DefaultNA)
df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# FeeDscnt output file
# Apply rpad on character/varchar columns if needed; unknown length -> <...>
df_FeeDscnt = df_Collector.select(
    col("FEE_DSCNT_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("FEE_DSCNT_ID"), <...>, " ").alias("FEE_DSCNT_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FNCL_LOB_SK"),
    col("FEE_DSCNT_CD_SK"),
    col("FEE_DSCNT_LOB_CD_SK")
)

write_files(
    df_FeeDscnt,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)