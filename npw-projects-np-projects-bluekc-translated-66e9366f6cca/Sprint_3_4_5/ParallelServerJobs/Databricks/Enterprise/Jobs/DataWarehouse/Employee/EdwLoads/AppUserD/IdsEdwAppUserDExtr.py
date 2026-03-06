# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     EdwAppUserDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                Pulls data from ids creates APP_USER_D load file
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  APP_USER
# MAGIC                   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                hf_app_user_userid
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   
# MAGIC OUTPUTS: 
# MAGIC                     APP_USER_D sequential load file 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                                                                      Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                                                                                             Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   -----------------------------------------------------------------------------------------                                                               ----------------------     -------------------   
# MAGIC Tom Harrocks  08/02/2004                        Originally Programmed
# MAGIC 
# MAGIC Terri O'Bryan    02/17/2010   TTR-724       Changed several items to bring the program up to current                                                                Steph Goddard     02/23/2010
# MAGIC                                                                     design/coding standards.  Eliminated use of IDS/EDW
# MAGIC                                                                    "comparison" to determine what records to send to load,
# MAGIC                                                                     removed hf names as parameters, renamed hash file.
# MAGIC     
# MAGIC 
# MAGIC Archana Palivela   2/27/14        5114     Originally Programmed (In Parallel)                                         EnterpriseWhseDevl                                 Jag Yelavarthi      2014-03-28
# MAGIC 
# MAGIC Jag Yelavarthi        2015-07-23   5407          Added a new column ACTV_DIR_ACCT_NM                  EnterpriseDev1                                         Kalyan Neelam     2015-07-23

# MAGIC Job name:IdsEdwAppUserDExtr
# MAGIC 
# MAGIC 
# MAGIC Extract Employee Application User data from IDS
# MAGIC here, select only APP_USER_ID and APP_USER_SK; populate NASCO userid with APP_USER_ID using lookup REL_NASCO_USER_SK = APP_USER_SK
# MAGIC Read data from source table APP_USER
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC APP_USER_TYP_CD_SK
# MAGIC Write APP_USER_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
APP_USER.USER_SK,
APP_USER.USER_ID,
APP_USER.REL_NASCO_USER_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APP_USER.APP_USER_TYP_CD_SK,
APP_USER.CLERK_ID,
APP_USER.DEPT_ID,
APP_USER.USER_DESC,
APP_USER.ACTV_DIR_ACCT_NM 
FROM {IDSOwner}.APP_USER APP_USER
LEFT JOIN {IDSOwner}.CD_MPPNG CD ON
APP_USER.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_APP_USER_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""SELECT
APP_USER.USER_SK,
APP_USER.USER_ID
FROM
{IDSOwner}.APP_USER APP_USER
"""
df_db2_APP_USER_1_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_Codes = (
    df_db2_APP_USER_Extr.alias("Ink_IdsEdwAppUserDExtr_InABC")
    .join(
        df_db2_APP_USER_1_Extr.alias("Cp_Ink_IdsEdwEvtLocDExtr_InABC"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.REL_NASCO_USER_SK") == F.col("Cp_Ink_IdsEdwEvtLocDExtr_InABC.USER_SK"),
        how="left"
    )
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_BnfVndrTyp"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.APP_USER_TYP_CD_SK") == F.col("Ref_BnfVndrTyp.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("Ref_BnfVndrTyp.TRGT_CD").alias("TRGT_CD"),
        F.col("Ref_BnfVndrTyp.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.USER_SK").alias("USER_SK"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.USER_ID").alias("USER_ID"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.REL_NASCO_USER_SK").alias("REL_NASCO_USER_SK"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.APP_USER_TYP_CD_SK").alias("APP_USER_TYP_CD_SK"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.CLERK_ID").alias("CLERK_ID"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.DEPT_ID").alias("DEPT_ID"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.USER_DESC").alias("USER_DESC"),
        F.col("Cp_Ink_IdsEdwEvtLocDExtr_InABC.USER_ID").alias("USER_ID_1"),
        F.col("Ink_IdsEdwAppUserDExtr_InABC.ACTV_DIR_ACCT_NM").alias("ACTV_DIR_ACCT_NM")
    )
)

df_main_out = (
    df_lkp_Codes.filter((F.col("USER_SK") != 0) & (F.col("USER_SK") != 1))
    .withColumn(
        "SRC_SYS_CD",
        F.when(trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("APP_USER_ID", F.col("USER_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("APP_USER_CLERK_ID", F.col("CLERK_ID"))
    .withColumn("APP_USER_DEPT_ID", F.col("DEPT_ID"))
    .withColumn("APP_USER_DESC", F.col("USER_DESC"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("APP_USER_REL_NASCO_USER_SK", F.col("REL_NASCO_USER_SK"))
    .withColumn("APP_USER_TYP_CD_SK", F.col("APP_USER_TYP_CD_SK"))
    .withColumn("APP_USER_SK", F.col("USER_SK"))
    .withColumn(
        "APP_USER_FIRST_NM",
        F.when(F.col("TRGT_CD").isNull(), "UNK")
        .when(F.col("TRGT_CD") == "I", trim(F.element_at(F.split(F.col("USER_DESC"), ","), 2)))
        .when((F.col("TRGT_CD") == "S") | (F.col("TRGT_CD") == "R"), "")
        .otherwise("")
    )
    .withColumn(
        "APP_USER_LAST_NM",
        F.when(F.col("TRGT_CD").isNull(), "UNK")
        .when(F.col("TRGT_CD") == "I", trim(F.element_at(F.split(F.col("USER_DESC"), ","), 1)))
        .when((F.col("TRGT_CD") == "S") | (F.col("TRGT_CD") == "R"), "")
        .otherwise("")
    )
    .withColumn(
        "APP_USER_REL_NASCO_USER_ID",
        F.when(F.col("USER_ID_1").isNull(), "UNK").otherwise(trim(F.col("USER_ID_1")))
    )
    .withColumn(
        "APP_USER_TYP_CD",
        F.when(F.col("TRGT_CD").isNull(), "UNK").otherwise(trim(F.col("TRGT_CD")))
    )
    .withColumn(
        "APP_USER_TYP_NM",
        F.when(F.col("TRGT_CD_NM").isNull(), "UNK").otherwise(trim(F.col("TRGT_CD_NM")))
    )
    .withColumn("ACTV_DIR_ACCT_NM", F.col("ACTV_DIR_ACCT_NM"))
    .select(
        "APP_USER_SK",
        "SRC_SYS_CD",
        "APP_USER_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "APP_USER_CLERK_ID",
        "APP_USER_DEPT_ID",
        "APP_USER_DESC",
        "APP_USER_FIRST_NM",
        "APP_USER_LAST_NM",
        "APP_USER_REL_NASCO_USER_ID",
        "APP_USER_TYP_CD",
        "APP_USER_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APP_USER_REL_NASCO_USER_SK",
        "APP_USER_TYP_CD_SK",
        "ACTV_DIR_ACCT_NM"
    )
)

schema_unk = StructType([
    StructField("APP_USER_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APP_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APP_USER_CLERK_ID", StringType(), True),
    StructField("APP_USER_DEPT_ID", StringType(), True),
    StructField("APP_USER_DESC", StringType(), True),
    StructField("APP_USER_FIRST_NM", StringType(), True),
    StructField("APP_USER_LAST_NM", StringType(), True),
    StructField("APP_USER_REL_NASCO_USER_ID", StringType(), True),
    StructField("APP_USER_TYP_CD", StringType(), True),
    StructField("APP_USER_TYP_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APP_USER_REL_NASCO_USER_SK", IntegerType(), True),
    StructField("APP_USER_TYP_CD_SK", IntegerType(), True),
    StructField("ACTV_DIR_ACCT_NM", StringType(), True)
])

df_unk_out = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            "1753-01-01",
            "UNK",
            "UNK",
            "",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            100,
            100,
            0,
            0,
            "UNK"
        )
    ],
    schema_unk
)

schema_na = StructType([
    StructField("APP_USER_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APP_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APP_USER_CLERK_ID", StringType(), True),
    StructField("APP_USER_DEPT_ID", StringType(), True),
    StructField("APP_USER_DESC", StringType(), True),
    StructField("APP_USER_FIRST_NM", StringType(), True),
    StructField("APP_USER_LAST_NM", StringType(), True),
    StructField("APP_USER_REL_NASCO_USER_ID", StringType(), True),
    StructField("APP_USER_TYP_CD", StringType(), True),
    StructField("APP_USER_TYP_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APP_USER_REL_NASCO_USER_SK", IntegerType(), True),
    StructField("APP_USER_TYP_CD_SK", IntegerType(), True),
    StructField("ACTV_DIR_ACCT_NM", StringType(), True)
])

df_na_out = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "1753-01-01",
            "1753-01-01",
            "NA",
            "NA",
            "",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            100,
            100,
            1,
            1,
            "NA"
        )
    ],
    schema_na
)

df_funnel = (
    df_main_out.unionByName(df_unk_out)
    .unionByName(df_na_out)
)

df_funnel_processed = (
    df_funnel
    .withColumn("APP_USER_ID", rpad(F.col("APP_USER_ID"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("APP_USER_CLERK_ID", rpad(F.col("APP_USER_CLERK_ID"), 10, " "))
    .withColumn("APP_USER_DEPT_ID", rpad(F.col("APP_USER_DEPT_ID"), 4, " "))
    .select(
        "APP_USER_SK",
        "SRC_SYS_CD",
        "APP_USER_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "APP_USER_CLERK_ID",
        "APP_USER_DEPT_ID",
        "APP_USER_DESC",
        "APP_USER_FIRST_NM",
        "APP_USER_LAST_NM",
        "APP_USER_REL_NASCO_USER_ID",
        "APP_USER_TYP_CD",
        "APP_USER_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APP_USER_REL_NASCO_USER_SK",
        "APP_USER_TYP_CD_SK",
        "ACTV_DIR_ACCT_NM"
    )
)

write_files(
    df_funnel_processed,
    f"{adls_path}/load/APP_USER_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="^",
    nullValue=None
)