# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari    12/20/2007                Initial program                                                                                   3036                  devlIDS                               Steph Goddard           06/17/2008
# MAGIC Kalyan Neelam     2010-07-19               Added new fields - RISK_SVRTY_CD_SK,                                4297                  RebuildIntNewDevl            Steph Goddard           07/21/2010
# MAGIC                                                              RISK_IDNT_DT_SK, MCSRC_RISK_LVL_NO
# MAGIC                                                              MDCSN_RISK_SCORE_NO, MDCSN_HLTH_STTUS_MESR_NO
# MAGIC                                                              Added sources ALINEO and IHMANALYTICS in stage variables for MedMesrsSk
# MAGIC Kalyan Neelam    2013-06-24                Added Source IDEA in stage variables for MedMesrSk               5056 FEP               IntegrateNewDevl             Bhoomi Dasari            7/24/2013
# MAGIC Santosh Bokka    2013-12-06                Added Source TREO in stage variables for MedMesrSk  
# MAGIC                                                             and added 7 new columns                                                            4917 PCMH           IntegrateNewDevl               Kalyan Neelam              2013-12-26
# MAGIC Santosh Bokka    2014-11-04               Updated Mbr SK column in Foreign Key transformer                    TFS - 8535             IntegrateNewDevl               Kalyan Neelam              2014-04-11
# MAGIC Santosh Bokka    2014-06-11             Added Source BCBSKC in stage variables for MedMesrSk           4917 PCMH           IntegrateNewDevl             Bhoomi Dasari              6/23/2014
# MAGIC Santosh Bokka    2014-07-01             Added Stage Variable svSrcSrsCdRickCat                                   4917 PCMH            IntegrateNewDevl                Kalyan Neelam              2014-07-01
# MAGIC 
# MAGIC Krishnakanth        2016-11-01            Added the four columns                                                                 30001                     IntegrateDev2                      Jag Yelavarthi               2016-11-16
# MAGIC    Manivannan                                    INDV_BE_MARA_RSLT_SK, RISK_MTHDLGY_TYP_SCORE_NO 
# MAGIC                                                           ,RISK_MTHDLGY_CD_SK, RISK_MTHDLGY_TYP_CD_SK

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DoubleType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile', '')
SourceSK = get_widget_value('SourceSK', '')

schema_IdsMbrRiskMesrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_RISK_MESR_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("RISK_CAT_ID", StringType(), nullable=False),
    StructField("PRCS_YR_MO_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_CK", StringType(), nullable=False),
    StructField("MBR_MED_MESRS_SK", IntegerType(), nullable=False),
    StructField("RISK_CAT_SK", StringType(), nullable=False),
    StructField("FTR_RELTV_RISK_NO", DoubleType(), nullable=False),
    StructField("RISK_SVRTY_CD", StringType(), nullable=False),
    StructField("RISK_IDNT_DT", StringType(), nullable=False),
    StructField("MCSRC_RISK_LVL_NO", IntegerType(), nullable=False),
    StructField("MDCSN_RISK_SCORE_NO", DoubleType(), nullable=False),
    StructField("MDCSN_HLTH_STTUS_MESR_NO", IntegerType(), nullable=False),
    StructField("CRG_ID", StringType(), nullable=False),
    StructField("CRG_DESC", StringType(), nullable=False),
    StructField("AGG_CRG_BASE_3_ID", StringType(), nullable=False),
    StructField("AGG_CRG_BASE_3_DESC", StringType(), nullable=False),
    StructField("CRG_WT", DecimalType(38,10), nullable=False),
    StructField("CRG_MDL_ID", StringType(), nullable=False),
    StructField("CRG_VRSN_ID", StringType(), nullable=False),
    StructField("INDV_BE_MARA_RSLT_SK", IntegerType(), nullable=False),
    StructField("RISK_MTHDLGY_CD", StringType(), nullable=False),
    StructField("RISK_MTHDLGY_TYP_CD", StringType(), nullable=False),
    StructField("RISK_MTHDLGY_TYP_SCORE_NO", DecimalType(38,10), nullable=True)
])

df_IdsMbrRiskMesrExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsMbrRiskMesrExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsMbrRiskMesrExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "svSrcSrsCdRickCat",
        when(trim(col("SRC_SYS_CD")) == "COBALTTALON", "UWS")
        .when(col("SRC_SYS_CD") == "BCBSKC", "TREO")
        .otherwise(col("SRC_SYS_CD"))
    )
    .withColumn("svMbrSk", GetFkeyMbr("FACETS", col("MBR_RISK_MESR_SK"), col("MBR_CK"), "X"))
    .withColumn(
        "svMbrMedMesrsSk",
        when(
            (trim(col("SRC_SYS_CD")) == "COBALTTALON")
            | (trim(col("SRC_SYS_CD")) == "MCSOURCE")
            | (trim(col("SRC_SYS_CD")) == "ALINEO")
            | (trim(col("SRC_SYS_CD")) == "IHMANALYTICS")
            | (trim(col("SRC_SYS_CD")) == "IDEA")
            | (trim(col("SRC_SYS_CD")) == "TREO")
            | (trim(col("SRC_SYS_CD")) == "BCBSKC"),
            lit(1)
        ).otherwise(
            GetFkeyMbrMedMesrs(
                col("SRC_SYS_CD"),
                col("MBR_RISK_MESR_SK"),
                col("MBR_UNIQ_KEY"),
                col("PRCS_YR_MO_SK"),
                "X"
            )
        )
    )
    .withColumn("svRiskCatSk", GetFkeyRiskCat(col("svSrcSrsCdRickCat"), col("MBR_RISK_MESR_SK"), col("RISK_CAT_ID"), "X"))
    .withColumn("svRiskSvrtyCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("MBR_RISK_MESR_SK"), "RISK SEVERITY", col("RISK_SVRTY_CD"), "X"))
    .withColumn("svRiskIdntDtSk", GetFkeyDate("IDS", col("MBR_RISK_MESR_SK"), col("RISK_IDNT_DT"), "X"))
    .withColumn("svRiskMthdlgyCdSk", GetFkeyCodes(col("svSrcSrsCdRickCat"), col("MBR_RISK_MESR_SK"), "RISK METHODOLOGY", col("RISK_MTHDLGY_CD"), "X"))
    .withColumn("svRiskMthdlgyTypCdSk", GetFkeyCodes(col("svSrcSrsCdRickCat"), col("MBR_RISK_MESR_SK"), "RISK METHODOLOGY TYPE", col("RISK_MTHDLGY_TYP_CD"), "X"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_RISK_MESR_SK")))
)

df_Fkey = (
    df_foreignKey.filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
        lit(SourceSK).alias("SRC_SYS_CD_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("RISK_CAT_ID").alias("RISK_CAT_ID"),
        col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svMbrSk").alias("MBR_SK"),
        col("svMbrMedMesrsSk").alias("MBR_MED_MESRS_SK"),
        col("svRiskCatSk").alias("RISK_CAT_SK"),
        col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
        col("svRiskSvrtyCdSk").alias("RISK_SVRTY_CD_SK"),
        col("svRiskIdntDtSk").alias("RISK_IDNT_DT_SK"),
        col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
        col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
        col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
        col("CRG_ID").alias("CRG_ID"),
        col("CRG_DESC").alias("CRG_DESC"),
        col("AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
        col("AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
        col("CRG_WT").alias("CRG_WT"),
        col("CRG_MDL_ID").alias("CRG_MDL_ID"),
        col("CRG_VRSN_ID").alias("CRG_VRSN_ID"),
        col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        col("svRiskMthdlgyCdSk").alias("RISK_MTHDLGY_CD_SK"),
        col("svRiskMthdlgyTypCdSk").alias("RISK_MTHDLGY_TYP_CD_SK"),
        col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
    )
)

df_Recycle = (
    df_foreignKey.filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("MBR_RISK_MESR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("RISK_CAT_ID").alias("RISK_CAT_ID"),
        col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_CK").alias("MBR_SK"),
        col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
        col("RISK_CAT_SK").alias("RISK_CAT_SK"),
        col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
        col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
        col("RISK_IDNT_DT").alias("RISK_IDNT_DT"),
        col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
        col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
        col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
        col("CRG_ID").alias("CRG_ID"),
        col("CRG_DESC").alias("CRG_DESC"),
        col("AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
        col("AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
        col("CRG_WT").alias("CRG_WT"),
        col("CRG_MDL_ID").alias("CRG_MDL_ID"),
        col("CRG_VRSN_ID").alias("CRG_VRSN_ID"),
        col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD_SK"),
        col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD_SK"),
        col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
    )
)

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

schema_collector = StructType([
    StructField("MBR_RISK_MESR_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("RISK_CAT_ID", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("MBR_MED_MESRS_SK", IntegerType(), True),
    StructField("RISK_CAT_SK", IntegerType(), True),
    StructField("FTR_RELTV_RISK_NO", DoubleType(), True),
    StructField("RISK_SVRTY_CD_SK", IntegerType(), True),
    StructField("RISK_IDNT_DT_SK", StringType(), True),
    StructField("MCSRC_RISK_LVL_NO", IntegerType(), True),
    StructField("MDCSN_RISK_SCORE_NO", DoubleType(), True),
    StructField("MDCSN_HLTH_STTUS_MESR_NO", IntegerType(), True),
    StructField("CRG_ID", StringType(), True),
    StructField("CRG_DESC", StringType(), True),
    StructField("AGG_CRG_BASE_3_ID", StringType(), True),
    StructField("AGG_CRG_BASE_3_DESC", StringType(), True),
    StructField("CRG_WT", IntegerType(), True),
    StructField("CRG_MDL_ID", IntegerType(), True),
    StructField("CRG_VRSN_ID", IntegerType(), True),
    StructField("INDV_BE_MARA_RSLT_SK", IntegerType(), True),
    StructField("RISK_MTHDLGY_CD_SK", IntegerType(), True),
    StructField("RISK_MTHDLGY_TYP_CD_SK", IntegerType(), True),
    StructField("RISK_MTHDLGY_TYP_SCORE_NO", DoubleType(), True)
])

df_ForeignKey_DefaultUNK = spark.createDataFrame(
    [
        {
            "MBR_RISK_MESR_SK": 0,
            "SRC_SYS_CD_SK": 0,
            "MBR_UNIQ_KEY": 0,
            "RISK_CAT_ID": "UNK",
            "PRCS_YR_MO_SK": "UNK",
            "CRT_RUN_CYC_EXCTN_SK": 0,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": 0,
            "MBR_SK": 0,
            "MBR_MED_MESRS_SK": 0,
            "RISK_CAT_SK": 0,
            "FTR_RELTV_RISK_NO": 0,
            "RISK_SVRTY_CD_SK": 0,
            "RISK_IDNT_DT_SK": "1753-01-01",
            "MCSRC_RISK_LVL_NO": 0,
            "MDCSN_RISK_SCORE_NO": 0,
            "MDCSN_HLTH_STTUS_MESR_NO": 0,
            "CRG_ID": "0",
            "CRG_DESC": "UNK",
            "AGG_CRG_BASE_3_ID": "0",
            "AGG_CRG_BASE_3_DESC": "UNK",
            "CRG_WT": 0,
            "CRG_MDL_ID": 0,
            "CRG_VRSN_ID": 0,
            "INDV_BE_MARA_RSLT_SK": 1,
            "RISK_MTHDLGY_CD_SK": 1,
            "RISK_MTHDLGY_TYP_CD_SK": 1,
            "RISK_MTHDLGY_TYP_SCORE_NO": None
        }
    ],
    schema_collector
)

df_ForeignKey_DefaultNA = spark.createDataFrame(
    [
        {
            "MBR_RISK_MESR_SK": 1,
            "SRC_SYS_CD_SK": 1,
            "MBR_UNIQ_KEY": 1,
            "RISK_CAT_ID": "NA",
            "PRCS_YR_MO_SK": "NA",
            "CRT_RUN_CYC_EXCTN_SK": 1,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": 1,
            "MBR_SK": 1,
            "MBR_MED_MESRS_SK": 1,
            "RISK_CAT_SK": 1,
            "FTR_RELTV_RISK_NO": 0,
            "RISK_SVRTY_CD_SK": 1,
            "RISK_IDNT_DT_SK": "1753-01-01",
            "MCSRC_RISK_LVL_NO": 0,
            "MDCSN_RISK_SCORE_NO": 0,
            "MDCSN_HLTH_STTUS_MESR_NO": 0,
            "CRG_ID": "1",
            "CRG_DESC": "NA",
            "AGG_CRG_BASE_3_ID": "1",
            "AGG_CRG_BASE_3_DESC": "NA",
            "CRG_WT": 1,
            "CRG_MDL_ID": 1,
            "CRG_VRSN_ID": 1,
            "INDV_BE_MARA_RSLT_SK": 1,
            "RISK_MTHDLGY_CD_SK": 1,
            "RISK_MTHDLGY_TYP_CD_SK": 1,
            "RISK_MTHDLGY_TYP_SCORE_NO": None
        }
    ],
    schema_collector
)

df_Collector = (
    df_Fkey
    .unionByName(df_ForeignKey_DefaultUNK)
    .unionByName(df_ForeignKey_DefaultNA)
)

df_final = df_Collector.select(
    "MBR_RISK_MESR_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "RISK_CAT_ID",
    "PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "MBR_MED_MESRS_SK",
    "RISK_CAT_SK",
    "FTR_RELTV_RISK_NO",
    "RISK_SVRTY_CD_SK",
    "RISK_IDNT_DT_SK",
    "MCSRC_RISK_LVL_NO",
    "MDCSN_RISK_SCORE_NO",
    "MDCSN_HLTH_STTUS_MESR_NO",
    "CRG_ID",
    "CRG_DESC",
    "AGG_CRG_BASE_3_ID",
    "AGG_CRG_BASE_3_DESC",
    "CRG_WT",
    "CRG_MDL_ID",
    "CRG_VRSN_ID",
    "INDV_BE_MARA_RSLT_SK",
    "RISK_MTHDLGY_CD_SK",
    "RISK_MTHDLGY_TYP_CD_SK",
    "RISK_MTHDLGY_TYP_SCORE_NO"
)

df_final = df_final.withColumn("PRCS_YR_MO_SK", rpad(col("PRCS_YR_MO_SK"), 6, " ")).withColumn(
    "RISK_IDNT_DT_SK", rpad(col("RISK_IDNT_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_RISK_MESR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)