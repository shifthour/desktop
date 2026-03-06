# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC © Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids fee discount table COMSN_SCHD and loads to EDW.    Does not keep history.
# MAGIC       
# MAGIC PROCESSING:
# MAGIC                   IDS - read from records from the source that have a run cycle greater than the BeginCycle, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC CALLED BY:       EdwCommissionCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                 Date              Project/Altiris #     Change Description                                                                                                          DS Proj          Reviewer            Dt Reviewed
# MAGIC ---------------------------      -------------------   -------------------------    ---------------------------------------------------------------------------------------------------                                    -------------------    -------------------         -------------------------   
# MAGIC Sharon Andrew         2005-11-07                                Original Programming.
# MAGIC SAndrew                  2009-02-25      TTR508             changed load to COMSN_AGMNT from Update to Replace                                           devlEDW        Steph Goddard     06/01/2009
# MAGIC                                                                                    Added select criteria for SK 0 and 1 so always have default records
# MAGIC 
# MAGIC Raj Mangalampally     11/22/2013               5114                        Original Programming                                                                           EnterpriseWrhsDevl    Jag Yelavarthi      2013-12-24   
# MAGIC                                                                                                     (Server to Parallel Conv)

# MAGIC Write COMSN_AGMNT_D Data into a Sequential file for Load Job IdsEdwComsnAgmntDLoad.
# MAGIC Read all the Data from IDS COMSN_AGMNT Table and join on COMSN_ARGMT ; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwSubPrmIncmFExtr
# MAGIC This Job will extract records from COMSN_AGMNT  from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1COMSN_AGMNT_PT_OF_SCHD_CD_SK
# MAGIC 2)COMSN_AGMNT_TERM_RSN_CD_SK
# MAGIC 3)SRC_SYS_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit, when, length, rpad
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


BeginCycle = get_widget_value("BeginCycle","")
EdwRunCycleDate = get_widget_value("EdwRunCycleDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_COMSN_AGMNT_D_in = f"""
SELECT distinct 
AGREE.COMSN_AGMNT_SK,
AGREE.SRC_SYS_CD_SK,
AGREE.COMSN_ARGMT_ID,
AGREE.EFF_DT_SK,
AGREE.AGNT_ID,
AGREE.AGNT_SK,
AGREE.COMSN_ARGMT_SK,
AGREE.COMSN_SCHD_SK,
AGREE.COMSN_AGMNT_PT_OF_SCHD_CD_SK,
AGREE.COMSN_AGMNT_TERM_RSN_CD_SK,
AGREE.TERM_DT_SK,
AGREE.SCHD_FCTR,
ARRANGE.BEG_DT_SK,
ARRANGE.ARGMT_DESC
FROM {IDSOwner}.COMSN_AGMNT AGREE
 left join {IDSOwner}.COMSN_ARGMT ARRANGE
   on ( AGREE.COMSN_ARGMT_SK = ARRANGE.COMSN_ARGMT_SK )
WHERE AGREE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}
      OR AGREE.COMSN_AGMNT_SK = 0
      OR AGREE.COMSN_AGMNT_SK = 1
"""
df_db2_COMSN_AGMNT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_COMSN_AGMNT_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy_cd_mppng_Ref_AgreePTofSchd_Lkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_AgreeTermRsn_Lkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_SrcSysCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_COMSN_AGMNT_D_in.alias("lnk_IdsEdwComsnAgmntDExtr_InABC")
    .join(
        df_cpy_cd_mppng_Ref_AgreeTermRsn_Lkup.alias("Ref_AgreeTermRsn_Lkup"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_AGMNT_TERM_RSN_CD_SK") == col("Ref_AgreeTermRsn_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_AgreePTofSchd_Lkup.alias("Ref_AgreePTofSchd_Lkup"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_AGMNT_PT_OF_SCHD_CD_SK") == col("Ref_AgreePTofSchd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_SrcSysCd_Lkup.alias("Ref_SrcSysCd_Lkup"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.SRC_SYS_CD_SK") == col("Ref_SrcSysCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
        col("Ref_SrcSysCd_Lkup.TRGT_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.EFF_DT_SK").alias("COMSN_AGMNT_EFF_DT_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.AGNT_ID").alias("AGNT_ID"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.AGNT_SK").alias("AGNT_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.BEG_DT_SK").alias("COMSN_ARGMT_BEG_DT_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.ARGMT_DESC").alias("COMSN_ARGMT_DESC"),
        col("Ref_AgreePTofSchd_Lkup.TRGT_CD").alias("COMSN_AGMNT_PT_OF_SCHD_CD"),
        col("Ref_AgreePTofSchd_Lkup.TRGT_CD_NM").alias("COMSN_AGMNT_PT_OF_SCHD_NM"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.SCHD_FCTR").alias("COMSN_AGMNT_SCHD_FCTR"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.TERM_DT_SK").alias("COMSN_AGMNT_TERM_DT_SK"),
        col("Ref_AgreeTermRsn_Lkup.TRGT_CD").alias("COMSN_AGMNT_TERM_RSN_CD"),
        col("Ref_AgreeTermRsn_Lkup.TRGT_CD_NM").alias("COMSN_AGMNT_TERM_RSN_NM"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_AGMNT_PT_OF_SCHD_CD_SK").alias("COMSN_AGMNT_PT_OF_SCHD_CD_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_AGMNT_TERM_RSN_CD_SK").alias("COMSN_AGMNT_TERM_RSN_CD_SK"),
        col("lnk_IdsEdwComsnAgmntDExtr_InABC.COMSN_ARGMT_SK").alias("COMSN_ARGMT_SK")
    )
)

df_xfrm_BusinessLogic_pre = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", lit("UNK")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "COMSN_ARGMT_BEG_DT_SK",
        when(
            col("COMSN_ARGMT_BEG_DT_SK").isNull() | (length(trim(col("COMSN_ARGMT_BEG_DT_SK"))) == 0),
            lit("UNK")
        ).otherwise(col("COMSN_ARGMT_BEG_DT_SK"))
    )
    .withColumn(
        "COMSN_ARGMT_DESC",
        when(
            col("COMSN_ARGMT_DESC").isNull() | (trim(col("COMSN_ARGMT_DESC")) == ""),
            lit("UNK")
        ).otherwise(col("COMSN_ARGMT_DESC"))
    )
    .withColumn(
        "COMSN_AGMNT_PT_OF_SCHD_CD",
        when(trim(col("COMSN_AGMNT_PT_OF_SCHD_CD")) == "", lit("UNK")).otherwise(col("COMSN_AGMNT_PT_OF_SCHD_CD"))
    )
    .withColumn(
        "COMSN_AGMNT_PT_OF_SCHD_NM",
        when(trim(col("COMSN_AGMNT_PT_OF_SCHD_NM")) == "", lit("UNK")).otherwise(col("COMSN_AGMNT_PT_OF_SCHD_NM"))
    )
    .withColumn(
        "COMSN_AGMNT_TERM_RSN_CD",
        when(trim(col("COMSN_AGMNT_TERM_RSN_CD")) == "", lit("UNK")).otherwise(col("COMSN_AGMNT_TERM_RSN_CD"))
    )
    .withColumn(
        "COMSN_AGMNT_TERM_RSN_NM",
        when(trim(col("COMSN_AGMNT_TERM_RSN_NM")) == "", lit("UNK")).otherwise(col("COMSN_AGMNT_TERM_RSN_NM"))
    )
    .withColumn("COMSN_AGMNT_SCHD_PCT", col("COMSN_AGMNT_SCHD_FCTR") / 100)
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EdwRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EdwRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle).cast("int"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle).cast("int"))
)

df_xfrm_BusinessLogic_lnk_IdsEdwComsnAgmntDMainData_Out = (
    df_xfrm_BusinessLogic_pre
    .filter((col("COMSN_AGMNT_SK") != 0) & (col("COMSN_AGMNT_SK") != 1))
    .select(
        "COMSN_AGMNT_SK",
        "SRC_SYS_CD",
        "COMSN_ARGMT_ID",
        "COMSN_AGMNT_EFF_DT_SK",
        "AGNT_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_SK",
        "COMSN_SCHD_SK",
        "COMSN_ARGMT_BEG_DT_SK",
        "COMSN_ARGMT_DESC",
        "COMSN_AGMNT_PT_OF_SCHD_CD",
        "COMSN_AGMNT_PT_OF_SCHD_NM",
        "COMSN_AGMNT_SCHD_FCTR",
        "COMSN_AGMNT_TERM_DT_SK",
        "COMSN_AGMNT_TERM_RSN_CD",
        "COMSN_AGMNT_TERM_RSN_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_AGMNT_PT_OF_SCHD_CD_SK",
        "COMSN_AGMNT_TERM_RSN_CD_SK",
        "COMSN_ARGMT_SK",
        "COMSN_AGMNT_SCHD_PCT"
    )
)

schema_xfrm_BUS = StructType([
    StructField("COMSN_AGMNT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("COMSN_ARGMT_ID", StringType(), True),
    StructField("COMSN_AGMNT_EFF_DT_SK", StringType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("AGNT_SK", IntegerType(), True),
    StructField("COMSN_SCHD_SK", IntegerType(), True),
    StructField("COMSN_ARGMT_BEG_DT_SK", StringType(), True),
    StructField("COMSN_ARGMT_DESC", StringType(), True),
    StructField("COMSN_AGMNT_PT_OF_SCHD_CD", StringType(), True),
    StructField("COMSN_AGMNT_PT_OF_SCHD_NM", StringType(), True),
    StructField("COMSN_AGMNT_SCHD_FCTR", DoubleType(), True),
    StructField("COMSN_AGMNT_TERM_DT_SK", StringType(), True),
    StructField("COMSN_AGMNT_TERM_RSN_CD", StringType(), True),
    StructField("COMSN_AGMNT_TERM_RSN_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("COMSN_AGMNT_PT_OF_SCHD_CD_SK", IntegerType(), True),
    StructField("COMSN_AGMNT_TERM_RSN_CD_SK", IntegerType(), True),
    StructField("COMSN_ARGMT_SK", IntegerType(), True),
    StructField("COMSN_AGMNT_SCHD_PCT", DoubleType(), True)
])

df_xfrm_BusinessLogic_lnk_UNK = spark.createDataFrame([
    (
        0,
        "UNK",
        "UNK",
        "1753-01-01",
        "UNK",
        "1753-01-01",
        "1753-01-01",
        0,
        0,
        "1753-01-01",
        "",
        "UNK",
        "UNK",
        0.0,
        "1753-01-01",
        "UNK",
        "UNK",
        100,
        100,
        0,
        0,
        0,
        0.0
    )
], schema_xfrm_BUS)

df_xfrm_BusinessLogic_lnk_NA = spark.createDataFrame([
    (
        1,
        "NA",
        "NA",
        "1753-01-01",
        "NA",
        "1753-01-01",
        "1753-01-01",
        1,
        1,
        "1753-01-01",
        "",
        "NA",
        "NA",
        0.0,
        "1753-01-01",
        "NA",
        "NA",
        100,
        100,
        1,
        1,
        1,
        0.0
    )
], schema_xfrm_BUS)

df_fnl_UNK_NA = df_xfrm_BusinessLogic_lnk_IdsEdwComsnAgmntDMainData_Out.unionByName(df_xfrm_BusinessLogic_lnk_UNK).unionByName(df_xfrm_BusinessLogic_lnk_NA)

final_df = (
    df_fnl_UNK_NA
    .withColumn("COMSN_AGMNT_EFF_DT_SK", rpad(col("COMSN_AGMNT_EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("COMSN_ARGMT_BEG_DT_SK", rpad(col("COMSN_ARGMT_BEG_DT_SK"), 10, " "))
    .withColumn("COMSN_AGMNT_TERM_DT_SK", rpad(col("COMSN_AGMNT_TERM_DT_SK"), 10, " "))
    .select(
        "COMSN_AGMNT_SK",
        "SRC_SYS_CD",
        "COMSN_ARGMT_ID",
        "COMSN_AGMNT_EFF_DT_SK",
        "AGNT_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_SK",
        "COMSN_SCHD_SK",
        "COMSN_ARGMT_BEG_DT_SK",
        "COMSN_ARGMT_DESC",
        "COMSN_AGMNT_PT_OF_SCHD_CD",
        "COMSN_AGMNT_PT_OF_SCHD_NM",
        "COMSN_AGMNT_SCHD_FCTR",
        "COMSN_AGMNT_TERM_DT_SK",
        "COMSN_AGMNT_TERM_RSN_CD",
        "COMSN_AGMNT_TERM_RSN_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_AGMNT_PT_OF_SCHD_CD_SK",
        "COMSN_AGMNT_TERM_RSN_CD_SK",
        "COMSN_ARGMT_SK",
        "COMSN_AGMNT_SCHD_PCT"
    )
)

write_files(
    final_df,
    f"{adls_path}/load/COMSN_AGMNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)