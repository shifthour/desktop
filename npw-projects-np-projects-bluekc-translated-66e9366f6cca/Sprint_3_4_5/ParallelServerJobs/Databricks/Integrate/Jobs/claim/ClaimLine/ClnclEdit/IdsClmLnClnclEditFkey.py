# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: Called by multiple jobs.
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Get foreign key as stated above and create file to load to CLM_LN_CLNCL_EDIT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             BJ Luce    6/2004 -   Originally Programmed
# MAGIC             SAndrew     8 /2004-   IDS 2.0 - changed clncl_edit_fmt_chg_cd_sk to CLM_LN_CLNCL_EDIT_FMT_CHG_CD_SK #1158
# MAGIC                                                 Added defualt UNK and NA
# MAGIC             Suzanne Saylor 3/1/2006 - Removed unused parameters, renamed links
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------      
# MAGIC Parikshith Chada      2008-07-15      3567 (Primary key)  Added Source System Code SK as part of the parameter            devlIDS                          Brent Leland              07-18-2008
# MAGIC Dan Long                 2013-11-22      TFS-1114               Added Lookup stage and changed the derivation for the            IntegrateNewDevl          Kalyan Neelam          2013-11-26
# MAGIC                                                                                        REF_CLM_LN_SK and REF_CLM_LN_SEQ_NO in the                                               
# MAGIC                                                                                       for the Fkey sequential file.
# MAGIC Nathan Reynolds	31-oct-2016      tfs-13129                Add new column CLM_LN_CLNCL_EDIT_EXCD_SK
# MAGIC                                                                                          also only load CLM_LN_CLNCL_EDIT_TYP_CD_SK
# MAGIC                                                                                        when claim date is before 26 mar 2016                                      IntegrateDev2                  Jag Yelavarthi            2016-11-16
# MAGIC 
# MAGIC Manasa Andru         2017-03/27      TFS - 18779         Updated the value for the field - COMBND_CHRG_IN                IntegrateDev1                  Nathan Reynolds        2017-3-31
# MAGIC                                                                                        for the default records(NA and UNK)
# MAGIC 
# MAGIC Reddy Sanam       2020-10-10                                      Created stage variable "svSrcSysCd" to map FACETS
# MAGIC                                                                                      when the source is LUMERIS                                                      IntegrateDev2
# MAGIC Reddy Sanam      2020-10-11                                       removed 'select * from' when reading from K table
# MAGIC                                                                                      to bring up to standards
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                           Brought up to standards

# MAGIC Writing Sequential File to /load
# MAGIC Lookup using CLM_ID and CLM_LN_SEQ_NO
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Recycle records with ErrCount > 0
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
Logging = get_widget_value('Logging','Y')
InFile = get_widget_value('InFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_ClmLnClnclEditExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),  
    StructField("DISCARD_IN", StringType(), False),     
    StructField("PASS_THRU_IN", StringType(), False),   
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_CLNCL_EDIT_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_LN_CLNCL_EDIT_ACTN_CD", StringType(), False),
    StructField("CLM_LN_CLNCL_EDIT_FMT_CHG_CD", StringType(), False),
    StructField("CLM_LN_CLNCL_EDIT_TYP_CD", StringType(), False),
    StructField("COMBND_CHRG_IN", StringType(), False),
    StructField("REF_CLM_ID", StringType(), False),
    StructField("REF_CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD", StringType(), False)
])

df_ClmLnClnclEditExtr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_ClmLnClnclEditExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN"
df_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trns1In = df_ClmLnClnclEditExtr.alias("Key").join(
    df_K_CLM_LN.alias("Lookup"),
    (
        (F.col("Lookup.SRC_SYS_CD_SK") == F.lit(SrcSysCdSk)) &
        (F.col("Lookup.CLM_ID") == F.col("Key.REF_CLM_ID")) &
        (F.col("Lookup.CLM_LN_SEQ_NO") == F.col("Key.REF_CLM_LN_SEQ_NO"))
    ),
    "left"
)

df_Trns1 = (
    df_Trns1In
    .withColumn(
        "svSrcSysCd",
        F.when(F.col("Key.SRC_SYS_CD") == F.lit("LUMERIS"), F.lit("FACETS")).otherwise(F.col("Key.SRC_SYS_CD"))
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CLM_LN_CLNCL_EDIT_SK"),
            F.col("Key.CLM_ID"),
            F.when(F.col("Key.CLM_LN_SEQ_NO") == 0, F.lit("NA")).otherwise(F.col("Key.CLM_LN_SEQ_NO")),
            F.lit(Logging)
        )
    )
    .withColumn(
        "EditActCd",
        GetFkeyCodes(
            F.col("svSrcSysCd"),
            F.col("Key.CLM_LN_CLNCL_EDIT_SK"),
            F.lit("CLAIM LINE CLINICAL EDIT ACTION"),
            F.col("Key.CLM_LN_CLNCL_EDIT_ACTN_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "EditFmtChgCd",
        GetFkeyCodes(
            F.col("svSrcSysCd"),
            F.col("Key.CLM_LN_CLNCL_EDIT_SK"),
            F.lit("CLAIM LINE CLINICAL EDIT FORMAT CHANGE"),
            F.col("Key.CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "EditTypeCd",
        GetFkeyCodes(
            F.col("svSrcSysCd"),
            F.col("Key.CLM_LN_CLNCL_EDIT_SK"),
            F.lit("CLAIM LINE CLINICAL EDIT TYPE"),
            F.col("Key.CLM_LN_CLNCL_EDIT_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "PassThru",
        F.col("Key.PASS_THRU_IN")
    )
    .withColumn(
        "ExcdTypeCd",
        GetFkeyExcd(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CLM_LN_CLNCL_EDIT_SK"),
            F.col("Key.CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            F.col("Key.CLM_LN_CLNCL_EDIT_SK")
        )
    )
)

df_recycle_hash = (
    df_Trns1
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("Key.CLM_LN_CLNCL_EDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Key.ERR_CT").alias("ERR_CT"),
        (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Key.CLM_LN_CLNCL_EDIT_SK").alias("CLM_LN_CLNCL_EDIT_SK"),
        F.col("Key.CLM_ID").alias("CLM_ID"),
        F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Key.CLM_LN_CLNCL_EDIT_ACTN_CD").alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
        F.col("Key.CLM_LN_CLNCL_EDIT_FMT_CHG_CD").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
        F.col("Key.CLM_LN_CLNCL_EDIT_TYP_CD").alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
        F.col("Key.COMBND_CHRG_IN").alias("COMBND_CHRG_IN"),
        F.col("Key.REF_CLM_ID").alias("REF_CLM_ID"),
        F.col("Key.REF_CLM_LN_SEQ_NO").alias("REF_CLM_LN_SEQ_NO"),
        F.col("Key.CLM_LN_CLNCL_EDIT_EXCD_TYP_CD").alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
    )
)

write_files(
    df_recycle_hash,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Recycle_Clms = (
    df_Trns1
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.CLM_ID").alias("CLM_ID")
    )
)

write_files(
    df_Recycle_Clms,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))
df_Trns1RowNum = df_Trns1.withColumn("row_number", F.row_number().over(w))

df_DefaultUNK_base = df_Trns1RowNum.filter(F.col("row_number") == 1)
df_DefaultNA_base = df_Trns1RowNum.filter(F.col("row_number") == 1)

df_DefaultUNK = (
    df_DefaultUNK_base
    .select(
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_LN_SK"),
        F.lit(0).alias("REF_CLM_LN_SK"),
        F.lit(0).alias("CLM_LN_CLNCLEDIT_ACTN_CD_SK"),
        F.lit(0).alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
        F.lit(0).alias("CLM_LN_CLNCLEDIT_TYP_CD_SK"),
        F.lit("N").alias("COMBND_CHRG_IN"),
        F.lit("UNK").alias("REF_CLM_ID"),
        F.lit(0).alias("REF_CLM_LN_SEQ_NO"),
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD_SK")
    )
)

df_DefaultNA = (
    df_DefaultNA_base
    .select(
        F.lit(1).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_LN_SK"),
        F.lit(1).alias("REF_CLM_LN_SK"),
        F.lit(1).alias("CLM_LN_CLNCLEDIT_ACTN_CD_SK"),
        F.lit(1).alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
        F.lit(1).alias("CLM_LN_CLNCLEDIT_TYP_CD_SK"),
        F.lit("N").alias("COMBND_CHRG_IN"),
        F.lit("NA").alias("REF_CLM_ID"),
        F.lit(1).alias("REF_CLM_LN_SEQ_NO"),
        F.lit(1).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD_SK")
    )
)

df_Fkey = (
    df_Trns1
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == F.lit('Y')))
    .select(
        F.col("Key.CLM_LN_CLNCL_EDIT_SK").alias("CLM_LN_CLNCL_EDIT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("Key.CLM_ID").alias("CLM_ID"),
        F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmLnSk").alias("CLM_LN_SK"),
        F.when(
            F.col("Lookup.CLM_LN_SK").isNull(),
            F.lit(1)
        ).otherwise(F.col("Lookup.CLM_LN_SK")).alias("REF_CLM_LN_SK"),
        F.col("EditActCd").alias("CLM_LN_CLNCLEDIT_ACTN_CD_SK"),
        F.col("EditFmtChgCd").alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
        F.col("EditTypeCd").alias("CLM_LN_CLNCLEDIT_TYP_CD_SK"),
        F.col("Key.COMBND_CHRG_IN").alias("COMBND_CHRG_IN"),
        F.col("Key.REF_CLM_ID").alias("REF_CLM_ID"),
        F.when(
            F.col("Lookup.CLM_LN_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("Lookup.CLM_LN_SEQ_NO")).alias("REF_CLM_LN_SEQ_NO"),
        F.col("ExcdTypeCd").alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD_SK")
    )
)

df_Collector = df_DefaultUNK.unionByName(df_DefaultNA).unionByName(df_Fkey)

df_Final = (
    df_Collector
    .select(
        "CLM_LN_CLNCL_EDIT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "REF_CLM_LN_SK",
        "CLM_LN_CLNCLEDIT_ACTN_CD_SK",
        "CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK",
        "CLM_LN_CLNCLEDIT_TYP_CD_SK",
        "COMBND_CHRG_IN",
        "REF_CLM_ID",
        "REF_CLM_LN_SEQ_NO",
        "CLM_LN_CLNCL_EDIT_EXCD_TYP_CD_SK"
    )
    .withColumn(
        "CLM_ID",
        F.rpad(F.col("CLM_ID"), 18, " ")
    )
    .withColumn(
        "COMBND_CHRG_IN",
        F.rpad(F.col("COMBND_CHRG_IN"), 1, " ")
    )
)

write_files(
    df_Final,
    f"{adls_path}/load/CLM_LN_CLNCL_EDIT.{Source}.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)