# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 02/19/08 12:47:08 Batch  14660_46032 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 02/19/08 12:44:40 Batch  14660_45896 INIT bckcett testIDS dsadm bls for on
# MAGIC ^1_1 02/15/08 12:24:50 Batch  14656_44697 PROMOTE bckcett testIDS u03651 steph for Ollie
# MAGIC ^1_1 02/15/08 12:22:56 Batch  14656_44584 INIT bckcett devlIDS u03651 steffy
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
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsComsnSchdFkey
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
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Oliver Nielsen  -  10/20/2005  -  Originally programmed
# MAGIC             Suzanne Saylor - 10/20/2005 - Modified to IdsComsnSchdFkey
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                     Change Description                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------              ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                   02/14/2008    Production Support                  Fix SrcSysCdSK Stage Variable call                               devlIDS                Steph Goddard             02/15/2008
# MAGIC                                                                                                                  Was passing in wrong argument
# MAGIC 
# MAGIC Bhoomi Dasari                  2008-09-09                                                      Added SrcSysCdSk parame/3567                          devlIDS                       Steph Goddard             09/22/2008
# MAGIC Ralph Tucker                   2012-08-15    4873 - Commissions Reporting  Added new field; updated to latest standards          IntegrateNewDevl         Kalyan Neelam               2012-11-06

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql import Row
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsComsnSchdExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','COMSN_SCHD.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsComsnSchd = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("COMSN_SCHD_SK", IntegerType(), False),
    StructField("COMSN_SCHD_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("COMSN_SCHD_CALC_METH_CD_SK", StringType(), False),
    StructField("SCHD_DESC", StringType(), False),
    StructField("COMSN_MTHDLGY_TYP_CD", StringType(), False)
])

df_IdsComsnSchd = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsComsnSchd)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreign_key = (
    df_IdsComsnSchd
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svComsnSchdCalcMethCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("COMSN_SCHD_SK"),
            F.lit("COMMISSION SCHEDULE CALCULATION METHOD"),
            F.col("COMSN_SCHD_CALC_METH_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svComsnMthdglyTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("COMSN_SCHD_SK"),
            F.lit("COMMISSION METHODOLOGY TYPE"),
            F.col("COMSN_MTHDLGY_TYP_CD"),
            Logging
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_SCHD_SK")))
)

df_fkey = df_foreign_key.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_fkey_sel = df_fkey.select(
    F.col("COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svComsnSchdCalcMethCdSk").alias("COMSN_SCHD_CALC_METH_CD_SK"),
    F.col("SCHD_DESC").alias("SCHD_DESC"),
    F.col("svComsnMthdglyTypCdSk").alias("COMSN_MTHDLGY_TYP_CD_SK")
)

df_recycle = df_foreign_key.filter(F.col("ErrCount") > 0)
df_recycle_sel = df_recycle.select(
    GetRecycleKey(F.col("COMSN_SCHD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
    F.col("COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COMSN_SCHD_CALC_METH_CD_SK").alias("COMSN_SCHD_CALC_METH_CD_SK"),
    F.col("SCHD_DESC").alias("SCHD_DESC"),
    F.col("COMSN_MTHDLGY_TYP_CD").alias("COMSN_MTHDLGY_TYP_CD")
)

df_recycle_final = (
    df_recycle_sel
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("COMSN_SCHD_ID", F.rpad(F.col("COMSN_SCHD_ID"), 20, " "))
    .withColumn("COMSN_SCHD_CALC_METH_CD_SK", F.rpad(F.col("COMSN_SCHD_CALC_METH_CD_SK"), 1, " "))
    .withColumn("SCHD_DESC", F.rpad(F.col("SCHD_DESC"), 70, " "))
    .withColumn("COMSN_MTHDLGY_TYP_CD", F.rpad(F.col("COMSN_MTHDLGY_TYP_CD"), <...>, " "))
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
        "COMSN_SCHD_SK",
        "COMSN_SCHD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_SCHD_CALC_METH_CD_SK",
        "SCHD_DESC",
        "COMSN_MTHDLGY_TYP_CD"
    )
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defUNK = spark.createDataFrame([
    Row(
        COMSN_SCHD_SK=0,
        SRC_SYS_CD_SK=0,
        COMSN_SCHD_ID="0",
        CRT_RUN_CYC_EXCTN_SK=0,
        LAST_UPDT_RUN_CYC_EXCTN_SK=0,
        COMSN_SCHD_CALC_METH_CD_SK="0",
        SCHD_DESC="UNK",
        COMSN_MTHDLGY_TYP_CD_SK=0
    )
])

df_defNA = spark.createDataFrame([
    Row(
        COMSN_SCHD_SK=1,
        SRC_SYS_CD_SK=1,
        COMSN_SCHD_ID="1",
        CRT_RUN_CYC_EXCTN_SK=1,
        LAST_UPDT_RUN_CYC_EXCTN_SK=1,
        COMSN_SCHD_CALC_METH_CD_SK="1",
        SCHD_DESC="NA",
        COMSN_MTHDLGY_TYP_CD_SK=1
    )
])

df_collector = df_fkey_sel.unionByName(df_defUNK).unionByName(df_defNA)

df_collector_final = (
    df_collector
    .withColumn("COMSN_SCHD_ID", F.rpad(F.col("COMSN_SCHD_ID"), 20, " "))
    .withColumn("COMSN_SCHD_CALC_METH_CD_SK", F.rpad(F.col("COMSN_SCHD_CALC_METH_CD_SK"), 1, " "))
    .withColumn("SCHD_DESC", F.rpad(F.col("SCHD_DESC"), 70, " "))
)

df_collector_final_sel = df_collector_final.select(
    "COMSN_SCHD_SK",
    "SRC_SYS_CD_SK",
    "COMSN_SCHD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COMSN_SCHD_CALC_METH_CD_SK",
    "SCHD_DESC",
    "COMSN_MTHDLGY_TYP_CD_SK"
)

write_files(
    df_collector_final_sel,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)