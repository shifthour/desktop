# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  04/2004  -   Originally Programmed
# MAGIC              Tom Harrocks   27-Sep-2004  - Changed STATE FK lookup to use "FACETS" as the SRC_SYS. 
# MAGIC             Steph Goddard  12/2005      Changed for sequencer - removed unneeded parameters, renamed stages
# MAGIC             Steph Goddard 02/14/2006  Completed sequencer changes (parameter name)
# MAGIC              Naren Garapaty 03/21/2007 Added New Fields PROV_ID,PROV_NPI,SVC,PROV_NPI
# MAGIC                                                            code review by steph goddard 4/6/07
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                           Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                   --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi D                25/03/2008       ITS HOME           Changed the source from which data was originally coming                 devlIDScur
# MAGIC                                                                                       CMC_CLPP_ITS_PROV (new source). Changed Fkey process 
# MAGIC                                                                                       accordingly                       
# MAGIC                     
# MAGIC Parik                        2008-07-17       3567                    Added Source System Code Surrogate Key as parameter                      devlIDS                           Steph Goddard         07/24/2008
# MAGIC 
# MAGIC Jag Yelavarthi          2012-02-23       TTR#1309          Changed the GetFkey Logging to "X" to ignore 
# MAGIC                                                                                      the code lookup failures to trigger sending a claim ID to hit list             IntegrateCurDevl               SAndrew                  2012-03-01     
# MAGIC                                                                                      
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD    IntegrateCurDevl          Bhoomi Dasari             02/04/2015
# MAGIC                                                                                      in the stage variables and pass it to GetFkeyCodes because code sets are created under BCA for BCBSA
# MAGIC 
# MAGIC Reddy Sanam       2020-10-09                                    changed derivation for this stage variable -svCdMpngSrcSysCd 
# MAGIC                                                                                     to pass'FACETS'   when the source is LUMERIS
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                    Brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC check for foreign keys - write out record to recycle file if errors
# MAGIC Create default rows for UNK and NA
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
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

Source = get_widget_value("Source", "FACETS")
InFile = get_widget_value("InFile", "FctsClmExtrnlProvExtr.FctsClmExtrnlProv.dat.100")
Logging = get_widget_value("Logging", "Y")
RunID = get_widget_value("RunID", "20080326")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

schema_ClmExtrnlProvPkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_EXTRNL_PROV_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=True),
    StructField("ADDR_LN_1", StringType(), nullable=True),
    StructField("ADDR_LN_2", StringType(), nullable=True),
    StructField("ADDR_LN_3", StringType(), nullable=True),
    StructField("CITY_NM", StringType(), nullable=True),
    StructField("CLPP_PR_STATE", StringType(), nullable=False),
    StructField("CLM_EXTRNL_PROV_ST_CD_SK", IntegerType(), nullable=False),
    StructField("POSTAL_CD", StringType(), nullable=True),
    StructField("CNTY_NM", StringType(), nullable=True),
    StructField("CLPP_CNTRY_CD", StringType(), nullable=False),
    StructField("CLM_EXTRNL_PROV_CTRY_CD_SK", IntegerType(), nullable=False),
    StructField("PHN_NO", StringType(), nullable=True),
    StructField("PROV_ID", StringType(), nullable=False),
    StructField("PROV_NPI", StringType(), nullable=False),
    StructField("SVC_PROV_ID", StringType(), nullable=False),
    StructField("SVC_PROV_NPI", StringType(), nullable=False),
    StructField("SVC_PROV_NM", StringType(), nullable=True),
    StructField("TAX_ID", StringType(), nullable=False)
])

df_ClmExtrnlProvPkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ClmExtrnlProvPkey)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_init = df_ClmExtrnlProvPkey.withColumn(
    "svCdMpngSrcSysCd",
    F.when(F.col("SRC_SYS_CD") == "BCBSA", F.lit("BCA"))
    .when(F.col("SRC_SYS_CD") == "LUMERIS", F.lit("FACETS"))
    .otherwise(F.col("SRC_SYS_CD"))
).withColumn(
    "StateCdSk",
    GetFkeyCodes(F.lit("FACETS"), F.col("CLM_EXTRNL_PROV_SK"), F.lit("STATE"), F.col("CLPP_PR_STATE"), F.lit("X"))
).withColumn(
    "ClmSk",
    GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_EXTRNL_PROV_SK"), F.col("CLM_ID"), F.lit(Logging))
).withColumn(
    "CountryCdSk",
    GetFkeyCodes(F.col("svCdMpngSrcSysCd"), F.col("CLM_EXTRNL_PROV_SK"), F.lit("COUNTRY"), F.col("CLPP_CNTRY_CD"), F.lit("X"))
).withColumn(
    "PassThru",
    F.col("PASS_THRU_IN")
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(F.col("CLM_EXTRNL_PROV_SK"))
)

df_ForeignKey = df_ForeignKey_init

df_ForeignKeyFkey = df_ForeignKey.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("CLM_EXTRNL_PROV_SK").alias("CLM_EXTRNL_PROV_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("StateCdSk").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("CountryCdSk").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NPI").alias("PROV_NPI"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NPI").alias("SVC_PROV_NPI"),
    F.col("SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.col("TAX_ID").alias("TAX_ID")
)

df_ForeignKeyRecycle = df_ForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("CLM_EXTRNL_PROV_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_PROV_SK").alias("CLM_EXTRNL_PROV_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("CLPP_PR_STATE").alias("CLPP_PR_STATE"),
    F.col("CLM_EXTRNL_PROV_ST_CD_SK").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("CLPP_CNTRY_CD").alias("CLPP_CNTRY_CD"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD_SK").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NPI").alias("PROV_NPI"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NPI").alias("SVC_PROV_NPI"),
    F.col("SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.col("TAX_ID").alias("TAX_ID")
)

df_ForeignKeyRecycle_Clms = df_ForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_ForeignKeyRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_ForeignKeyRecycle_Clms,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))
df_ForeignKeyWindowed = df_ForeignKey.withColumn("rownum", F.row_number().over(w))

df_ForeignKeyDefaultUNK = df_ForeignKeyWindowed.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("CLM_EXTRNL_PROV_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("UNK").alias("PROV_NM"),
    F.lit("UNK").alias("ADDR_LN_1"),
    F.lit("UNK").alias("ADDR_LN_2"),
    F.lit("UNK").alias("ADDR_LN_3"),
    F.lit("UNK").alias("CITY_NM"),
    F.lit(0).alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.lit("UNK").alias("POSTAL_CD"),
    F.lit("UNK").alias("CNTY_NM"),
    F.lit(0).alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.lit("UNK").alias("PHN_NO"),
    F.lit("UNK").alias("PROV_ID"),
    F.lit("UNK").alias("PROV_NPI"),
    F.lit("UNK").alias("SVC_PROV_ID"),
    F.lit("UNK").alias("SVC_PROV_NPI"),
    F.lit("UNK").alias("SVC_PROV_NM"),
    F.lit("UNK").alias("TAX_ID")
)

df_ForeignKeyDefaultNA = df_ForeignKeyWindowed.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("CLM_EXTRNL_PROV_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit("NA").alias("PROV_NM"),
    F.lit("NA").alias("ADDR_LN_1"),
    F.lit("NA").alias("ADDR_LN_2"),
    F.lit("NA").alias("ADDR_LN_3"),
    F.lit("NA").alias("CITY_NM"),
    F.lit(1).alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.lit("NA").alias("POSTAL_CD"),
    F.lit("NA").alias("CNTY_NM"),
    F.lit(1).alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.lit("NA").alias("PHN_NO"),
    F.lit("NA").alias("PROV_ID"),
    F.lit("NA").alias("PROV_NPI"),
    F.lit("NA").alias("SVC_PROV_ID"),
    F.lit("NA").alias("SVC_PROV_NPI"),
    F.lit("NA").alias("SVC_PROV_NM"),
    F.lit("NA").alias("TAX_ID")
)

df_CollectorFkey = df_ForeignKeyFkey.select(
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

df_CollectorDefaultUNK = df_ForeignKeyDefaultUNK.select(
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

df_CollectorDefaultNA = df_ForeignKeyDefaultNA.select(
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

df_Collector = df_CollectorFkey.union(df_CollectorDefaultUNK).union(df_CollectorDefaultNA)

df_Final = df_Collector \
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " ")) \
    .withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " ")) \
    .withColumn("PHN_NO", F.rpad(F.col("PHN_NO"), 20, " ")) \
    .withColumn("SVC_PROV_ID", F.rpad(F.col("SVC_PROV_ID"), 13, " ")) \
    .withColumn("TAX_ID", F.rpad(F.col("TAX_ID"), 9, " "))

output_path = f"{adls_path}/load/CLM_EXTRNL_PROV.{Source}.dat"

write_files(
    df_Final.select(
        "CLM_EXTRNL_PROV_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "PROV_NM",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "CLM_EXTRNL_PROV_ST_CD_SK",
        "POSTAL_CD",
        "CNTY_NM",
        "CLM_EXTRNL_PROV_CTRY_CD_SK",
        "PHN_NO",
        "PROV_ID",
        "PROV_NPI",
        "SVC_PROV_ID",
        "SVC_PROV_NPI",
        "SVC_PROV_NM",
        "TAX_ID"
    ),
    output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)