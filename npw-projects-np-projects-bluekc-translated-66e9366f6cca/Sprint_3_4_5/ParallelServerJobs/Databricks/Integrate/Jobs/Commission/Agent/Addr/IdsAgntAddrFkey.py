# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/02/07 10:26:00 Batch  14428_37563 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 07/02/07 10:19:17 Batch  14428_37158 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_6 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_3 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_3 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_2 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC ^1_1 12/14/05 12:18:44 Batch  13863_44334 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsAgntAddrFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDates
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   AGNT_ADDR.dat
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson  -  10/11/2005  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-05                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          10/03/2008

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    rpad
)
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


OutFile = get_widget_value("OutFile","AGNT_ADDR.dat")
Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","FctsAgntAddrExtr.tmp")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_idsagntaddrextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("AGNT_ADDR_SK", IntegerType(), nullable=False),
    StructField("AGNT_ID", StringType(), nullable=False),
    StructField("ADDR_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("AGNT", IntegerType(), nullable=False),
    StructField("ADDR_LN_1", StringType(), nullable=True),
    StructField("ADDR_LN_2", StringType(), nullable=True),
    StructField("ADDR_LN_3", StringType(), nullable=True),
    StructField("CITY_NM", StringType(), nullable=True),
    StructField("AGNT_ADDR_ST_CD", StringType(), nullable=False),
    StructField("POSTAL_CD", StringType(), nullable=True),
    StructField("CNTY_NM", StringType(), nullable=True),
    StructField("AGNT_ADDR_CTRY_CD", StringType(), nullable=False),
    StructField("PHN_NO", StringType(), nullable=True),
    StructField("PHN_NO_EXT", StringType(), nullable=True),
    StructField("FAX_NO", StringType(), nullable=True),
    StructField("FAX_NO_EXT", StringType(), nullable=True),
    StructField("EMAIL_ADDR_TX", StringType(), nullable=True)
])

df_idsagntaddrextr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_idsagntaddrextr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_transformed = (
    df_idsagntaddrextr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svAgntSK", GetFkeyAgnt(col("SRC_SYS_CD"), col("AGNT_ADDR_SK"), col("AGNT_ID"), Logging))
    .withColumn("svAddrTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("AGNT_ADDR_SK"), lit("ADDRESS TYPE"), col("ADDR_TYP_CD"), Logging))
    .withColumn("svAgntAddrStCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("AGNT_ADDR_SK"), lit("STATE"), col("AGNT_ADDR_ST_CD"), Logging))
    .withColumn("svAgntAddrCtryCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("AGNT_ADDR_SK"), lit("COUNTRY"), col("AGNT_ADDR_CTRY_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("AGNT_ADDR_SK")))
    .withColumn("rownum", row_number().over(Window.orderBy(lit(1))))
)

df_fkey = (
    df_transformed
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("AGNT_ADDR_SK").alias("AGNT_ADDR_SK"),
        lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        col("AGNT_ID").alias("AGNT_ID"),
        col("svAddrTypCdSk").alias("ADDR_TYP_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAgntSK").alias("AGNT_SK"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("ADDR_LN_3").alias("ADDR_LN_3"),
        col("CITY_NM").alias("CITY_NM"),
        col("svAgntAddrStCdSk").alias("AGNT_ADDR_ST_CD_SK"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("CNTY_NM").alias("CNTY_NM"),
        col("svAgntAddrCtryCdSk").alias("AGNT_ADDR_CTRY_CD_SK"),
        col("PHN_NO").alias("PHN_NO"),
        col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        col("FAX_NO").alias("FAX_NO"),
        col("FAX_NO_EXT").alias("FAX_NO_EXT"),
        col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

df_recycle = (
    df_transformed
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("AGNT_ADDR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("AGNT_ADDR_SK").alias("AGNT_ADDR_SK"),
        col("AGNT_ID").alias("AGNT_ID"),
        col("ADDR_TYP_CD").alias("ADDR_TYP_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AGNT").alias("AGNT"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("ADDR_LN_3").alias("ADDR_LN_3"),
        col("CITY_NM").alias("CITY_NM"),
        col("AGNT_ADDR_ST_CD").alias("AGNT_ADDR_ST_CD"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("CNTY_NM").alias("CNTY_NM"),
        col("AGNT_ADDR_CTRY_CD").alias("AGNT_ADDR_CTRY_CD"),
        col("PHN_NO").alias("PHN_NO"),
        col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        col("FAX_NO").alias("FAX_NO"),
        col("FAX_NO_EXT").alias("FAX_NO_EXT"),
        col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

df_recycle_for_write = df_recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    col("AGNT_ADDR_SK"),
    rpad(col("AGNT_ID"), 255, " ").alias("AGNT_ID"),
    rpad(col("ADDR_TYP_CD"), 255, " ").alias("ADDR_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AGNT"),
    rpad(col("ADDR_LN_1"), 255, " ").alias("ADDR_LN_1"),
    rpad(col("ADDR_LN_2"), 255, " ").alias("ADDR_LN_2"),
    rpad(col("ADDR_LN_3"), 255, " ").alias("ADDR_LN_3"),
    rpad(col("CITY_NM"), 255, " ").alias("CITY_NM"),
    rpad(col("AGNT_ADDR_ST_CD"), 255, " ").alias("AGNT_ADDR_ST_CD"),
    rpad(col("POSTAL_CD"), 255, " ").alias("POSTAL_CD"),
    rpad(col("CNTY_NM"), 255, " ").alias("CNTY_NM"),
    rpad(col("AGNT_ADDR_CTRY_CD"), 255, " ").alias("AGNT_ADDR_CTRY_CD"),
    rpad(col("PHN_NO"), 255, " ").alias("PHN_NO"),
    rpad(col("PHN_NO_EXT"), 5, " ").alias("PHN_NO_EXT"),
    rpad(col("FAX_NO"), 255, " ").alias("FAX_NO"),
    rpad(col("FAX_NO_EXT"), 5, " ").alias("FAX_NO_EXT"),
    rpad(col("EMAIL_ADDR_TX"), 255, " ").alias("EMAIL_ADDR_TX")
)

write_files(
    df_recycle_for_write,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultUNK = (
    df_transformed
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("AGNT_ADDR_SK"),
        lit(0).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("AGNT_ID"),
        lit(0).cast(IntegerType()).alias("ADDR_TYP_CD_SK"),
        lit(0).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("AGNT_SK"),
        lit("UNK").alias("ADDR_LN_1"),
        lit("UNK").alias("ADDR_LN_2"),
        lit("UNK").alias("ADDR_LN_3"),
        lit("UNK").alias("CITY_NM"),
        lit(0).cast(IntegerType()).alias("AGNT_ADDR_ST_CD_SK"),
        lit("UNK").alias("POSTAL_CD"),
        lit("UNK").alias("CNTY_NM"),
        lit(0).cast(IntegerType()).alias("AGNT_ADDR_CTRY_CD_SK"),
        lit("UNK").alias("PHN_NO"),
        lit("UNK").alias("PHN_NO_EXT"),
        lit("UNK").alias("FAX_NO"),
        lit("UNK").alias("FAX_NO_EXT"),
        lit("UNK").alias("EMAIL_ADDR_TX")
    )
)

df_defaultNA = (
    df_transformed
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("AGNT_ADDR_SK"),
        lit(1).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("AGNT_ID"),
        lit(1).cast(IntegerType()).alias("ADDR_TYP_CD_SK"),
        lit(1).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("AGNT_SK"),
        lit("NA").alias("ADDR_LN_1"),
        lit("NA").alias("ADDR_LN_2"),
        lit("NA").alias("ADDR_LN_3"),
        lit("NA").alias("CITY_NM"),
        lit(1).cast(IntegerType()).alias("AGNT_ADDR_ST_CD_SK"),
        lit("NA").alias("POSTAL_CD"),
        lit("NA").alias("CNTY_NM"),
        lit(1).cast(IntegerType()).alias("AGNT_ADDR_CTRY_CD_SK"),
        lit("NA").alias("PHN_NO"),
        lit("NA").alias("PHN_NO_EXT"),
        lit("NA").alias("FAX_NO"),
        lit("NA").alias("FAX_NO_EXT"),
        lit("NA").alias("EMAIL_ADDR_TX")
    )
)

df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_final = df_collector.select(
    col("AGNT_ADDR_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("AGNT_ID"), 255, " ").alias("AGNT_ID"),
    col("ADDR_TYP_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AGNT_SK"),
    rpad(col("ADDR_LN_1"), 255, " ").alias("ADDR_LN_1"),
    rpad(col("ADDR_LN_2"), 255, " ").alias("ADDR_LN_2"),
    rpad(col("ADDR_LN_3"), 255, " ").alias("ADDR_LN_3"),
    rpad(col("CITY_NM"), 255, " ").alias("CITY_NM"),
    col("AGNT_ADDR_ST_CD_SK"),
    rpad(col("POSTAL_CD"), 255, " ").alias("POSTAL_CD"),
    rpad(col("CNTY_NM"), 255, " ").alias("CNTY_NM"),
    col("AGNT_ADDR_CTRY_CD_SK"),
    rpad(col("PHN_NO"), 255, " ").alias("PHN_NO"),
    rpad(col("PHN_NO_EXT"), 5, " ").alias("PHN_NO_EXT"),
    rpad(col("FAX_NO"), 255, " ").alias("FAX_NO"),
    rpad(col("FAX_NO_EXT"), 5, " ").alias("FAX_NO_EXT"),
    rpad(col("EMAIL_ADDR_TX"), 255, " ").alias("EMAIL_ADDR_TX")
)

write_files(
    df_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)