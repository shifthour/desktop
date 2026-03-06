# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/6/2007           Cust Svc/3028                  Originally Programmed                     devlEDW10                  Steph Goddard             02/21/2007
# MAGIC Steph Goddard              10/09/2009        3556 CDC                     Modified SQL Where Clause                 devlEDWnew             Brent/Steph                  12/29/2009
# MAGIC Steph Goddard              7/15/10               TTR-676                     corrected name when middle init is blank  EnterpriseNewDevl   SAnfrew                        2010-10-01
# MAGIC 
# MAGIC Raj Mangalampally     11/22/2013               5114                        Original Programming                              EnterpriseWrhsDevl   Peter Marshall                1/6/2014  
# MAGIC                                                                                                     (Server to Parallel Conv)

# MAGIC Write PRVCY_EXTRNL_MBR_D Data into a Sequential file for Load Job IdsEdwPrvcyExtrnlMbrDLoad.
# MAGIC Read all the Data from IDS PRVCY_EXTRNL_MBR ; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwPrvcyExtrnlMbrDExtr
# MAGIC This Job will extract records from PRVCY_EXTRNL_MBR from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1PRVCY_EXTRNL_MBR_RELSHP_CD_SK
# MAGIC 2)PRVCY_EXTRNL_MBR_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
EdwRunCycleDate = get_widget_value("EdwRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

# db2_PRVCY_EXTRNL_MBR_D_in
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_db2_PRVCY_EXTRNL_MBR_D_in = (
    f"SELECT \n"
    f"PRVCY_EXTRNL_MBR_SK,\n"
    f"SRC_SYS_LAST_UPDT_DT_SK,\n"
    f"SRC_SYS_LAST_UPDT_USER_SK,\n"
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    f"PRVCY_EXTRNL_MBR_RELSHP_CD_SK,\n"
    f"ALPHA_PFX_TX,\n"
    f"CNTR_NO,\n"
    f"GRP_NO,\n"
    f"MBR_EXT_NO,\n"
    f"MBR_ID,\n"
    f"PRVCY_EXTRNL_MBR_UNIQ_KEY\n"
    f"FROM \n"
    f"{IDSOwner}.PRVCY_EXTRNL_MBR MBR\n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
    f"ON MBR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"WHERE \n"
    f"MBR.LAST_UPDT_RUN_CYC_EXCTN_SK  >=  {IDSRunCycle}\n"
)
df_db2_PRVCY_EXTRNL_MBR_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_PRVCY_EXTRNL_MBR_D_in)
    .load()
)

# db2_CD_MPPNG_Extr
query_db2_CD_MPPNG_Extr = (
    f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"from {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

# db2_PRVCY_EXTRNL_MBR_PRSN_in
query_db2_PRVCY_EXTRNL_MBR_PRSN_in = (
    f"SELECT \n"
    f"PRVCY_EXTRNL_MBR_SK,\n"
    f"PRVCY_EXTRNL_PRSN_GNDR_CD_SK,\n"
    f"PRVCY_EXTL_PRSN_MRTL_STS_CD_SK,\n"
    f"BRTH_DT_SK,FIRST_NM,\n"
    f"MIDINIT,\n"
    f"LAST_NM FROM \n"
    f"{IDSOwner}.PRVCY_EXTRNL_PRSN PRSN,\n"
    f"{IDSOwner}.PRVCY_EXTRNL_MBR MBR \n"
    f"WHERE \n"
    f"MBR.PRVCY_EXTRNL_MBR_UNIQ_KEY=PRSN.PRVCY_EXTRNL_ENTY_UNIQ_KEY"
)
df_db2_PRVCY_EXTRNL_MBR_PRSN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_PRVCY_EXTRNL_MBR_PRSN_in)
    .load()
)

# CD_MPPNG_Extr1
query_CD_MPPNG_Extr1 = (
    f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNKNOWN') TRGT_CD_NM "
    f"from {IDSOwner}.CD_MPPNG"
)
df_CD_MPPNG_Extr1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CD_MPPNG_Extr1)
    .load()
)

# cpy_cd_mppng (PxCopy) from CD_MPPNG_Extr1
df_cpy_cd_mppng = df_CD_MPPNG_Extr1.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
# Two separate references for the two lookup links:
df_Ref_GenderCd_Lkup = df_cpy_cd_mppng
df_Ref_MaritalCD_Lkup = df_cpy_cd_mppng

# lkp_codes (PxLookup)
# primary link: db2_PRVCY_EXTRNL_MBR_PRSN_in
# lookup links: df_Ref_MaritalCD_Lkup (left join on PRVCY_EXTL_PRSN_MRTL_STS_CD_SK = CD_MPPNG_SK)
#               df_Ref_GenderCd_Lkup (left join on PRVCY_EXTRNL_PRSN_GNDR_CD_SK = CD_MPPNG_SK)
df_lkp_codes = (
    df_db2_PRVCY_EXTRNL_MBR_PRSN_in.alias("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup")
    .join(
        df_Ref_MaritalCD_Lkup.alias("Ref_MaritalCD_Lkup"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.PRVCY_EXTL_PRSN_MRTL_STS_CD_SK")
        == F.col("Ref_MaritalCD_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_GenderCd_Lkup.alias("Ref_GenderCd_Lkup"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.PRVCY_EXTRNL_PRSN_GNDR_CD_SK")
        == F.col("Ref_GenderCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.FIRST_NM").alias("FIRST_NM"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.MIDINIT").alias("MIDINIT"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.LAST_NM").alias("LAST_NM"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.PRVCY_EXTRNL_PRSN_GNDR_CD_SK").alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbr_Prsn_lkup.PRVCY_EXTL_PRSN_MRTL_STS_CD_SK").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        F.col("Ref_MaritalCD_Lkup.TRGT_CD").alias("TRGT_CD_MARTL"),
        F.col("Ref_MaritalCD_Lkup.TRGT_CD_NM").alias("TRGT_CD_NM_MRTL"),
        F.col("Ref_GenderCd_Lkup.TRGT_CD").alias("TRGT_CD_GNDR"),
        F.col("Ref_GenderCd_Lkup.TRGT_CD_NM").alias("TRGT_CD_NM_GNDR")
    )
)

# xfm_businessRules1 (CTransformerStage)
# Stage variable: fullname
df_xfm_businessRules1_temp = df_lkp_codes.withColumn(
    "fullname",
    F.when(
        F.col("MIDINIT").isNotNull(),
        F.concat(F.col("FIRST_NM"), F.lit(" "), F.col("MIDINIT"), F.lit(" "), F.col("LAST_NM"))
    ).otherwise(
        F.concat(F.col("FIRST_NM"), F.lit(" "), F.col("LAST_NM"))
    )
)

df_xfm_businessRules1 = df_xfm_businessRules1_temp.select(
    F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("MIDINIT").alias("MIDINIT"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("PRVCY_EXTRNL_PRSN_GNDR_CD_SK").alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
    F.col("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
    F.when(F.trim(F.col("TRGT_CD_MARTL")) == "", F.lit("UNK"))
     .otherwise(F.col("TRGT_CD_MARTL")).alias("TRGT_CD_MARTL"),
    F.when(F.trim(F.col("TRGT_CD_NM_MRTL")) == "", F.lit("UNKNOWN"))
     .otherwise(F.col("TRGT_CD_NM_MRTL")).alias("TRGT_CD_NM_MRTL"),
    F.when(F.trim(F.col("TRGT_CD_GNDR")) == "", F.lit("UNK"))
     .otherwise(F.col("TRGT_CD_GNDR")).alias("TRGT_CD_GNDR"),
    F.when(F.trim(F.col("TRGT_CD_NM_GNDR")) == "", F.lit("UNKOWN"))
     .otherwise(F.col("TRGT_CD_NM_GNDR")).alias("TRGT_CD_NM_GNDR"),
    F.col("fullname").alias("PRVCY_EXTRNL_MBR_FULL_NM")
)

# lkp_Codes1 (PxLookup)
# primary link: df_db2_PRVCY_EXTRNL_MBR_D_in
# lookup link 1: df_xfm_businessRules1 (left join on PRVCY_EXTRNL_MBR_SK)
# lookup link 2: df_db2_CD_MPPNG_Extr (left join on PRVCY_EXTRNL_MBR_RELSHP_CD_SK)
df_lkp_Codes1 = (
    df_db2_PRVCY_EXTRNL_MBR_D_in.alias("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC")
    .join(
        df_xfm_businessRules1.alias("lnk_GndrMrtlData_out"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.PRVCY_EXTRNL_MBR_SK")
        == F.col("lnk_GndrMrtlData_out.PRVCY_EXTRNL_MBR_SK"),
        how="left"
    )
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_ExtrnlMbrRelshpCd_Lkup"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.PRVCY_EXTRNL_MBR_RELSHP_CD_SK")
        == F.col("Ref_ExtrnlMbrRelshpCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.PRVCY_EXTRNL_MBR_UNIQ_KEY").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.ALPHA_PFX_TX").alias("PRVCY_EXTRNL_MBR_ALPHA_PFX_TX"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.CNTR_NO").alias("PRVCY_EXTRNL_MBR_CNTR_NO"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.MBR_EXT_NO").alias("PRVCY_EXTRNL_MBR_EXT_NO"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.GRP_NO").alias("PRVCY_EXTRNL_MBR_GRP_NO"),
        F.col("Ref_ExtrnlMbrRelshpCd_Lkup.TRGT_CD").alias("PRVCY_EXTRNL_MBR_RELSHP_CD"),
        F.col("Ref_ExtrnlMbrRelshpCd_Lkup.TRGT_CD_NM").alias("PRVCY_EXTRNL_MBR_RELSHP_NM"),
        F.col("lnk_GndrMrtlData_out.BRTH_DT_SK").alias("PRVCY_EXTRNL_PRSN_BRTH_DT_SK"),
        F.col("lnk_GndrMrtlData_out.PRVCY_EXTRNL_MBR_FULL_NM").alias("PRVCY_EXTRNL_MBR_FULL_NM"),
        F.col("lnk_GndrMrtlData_out.FIRST_NM").alias("PRVCY_EXTRNL_PRSN_FIRST_NM"),
        F.col("lnk_GndrMrtlData_out.MIDINIT").alias("PRVCY_EXTRNL_PRSN_MIDINIT"),
        F.col("lnk_GndrMrtlData_out.LAST_NM").alias("PRVCY_EXTRNL_PRSN_LAST_NM"),
        F.col("lnk_GndrMrtlData_out.TRGT_CD_GNDR").alias("PRVCY_EXTRNL_PRSN_GNDR_CD"),
        F.col("lnk_GndrMrtlData_out.TRGT_CD_NM_GNDR").alias("PRVCY_EXTRNL_PRSN_GNDR_NM"),
        F.col("lnk_GndrMrtlData_out.TRGT_CD_MARTL").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD"),
        F.col("lnk_GndrMrtlData_out.TRGT_CD_NM_MRTL").alias("PRVCY_EXTL_PRSN_MRTL_STS_NM"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnlMbrDExtr_InABC.PRVCY_EXTRNL_MBR_RELSHP_CD_SK").alias("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
        F.col("lnk_GndrMrtlData_out.PRVCY_EXTL_PRSN_MRTL_STS_CD_SK").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        F.col("lnk_GndrMrtlData_out.PRVCY_EXTRNL_PRSN_GNDR_CD_SK").alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK")
    )
)

# xfrm_BusinessLogic (CTransformerStage)
# We split into three outputs based on constraints
df_main = df_lkp_Codes1.filter(
    (F.col("PRVCY_EXTRNL_MBR_SK") != 0) & (F.col("PRVCY_EXTRNL_MBR_SK") != 1)
)

# Single row for UNK
schema_UNK = StructType([
    StructField("PRVCY_EXTRNL_MBR_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_UNIQ_KEY", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_ALPHA_PFX_TX", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_ID", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_CNTR_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_EXT_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_GRP_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_BRTH_DT_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_FULL_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_FIRST_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_MIDINIT", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_LAST_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_NM", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_CD", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_NM", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_CD_SK", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_CD_SK", StringType(), True),
])
df_UNK = spark.createDataFrame(
    [
        (
            "0","UNK","0","1753-01-01","1753-01-01","",
            "UNK","",None,"","UNK","UNK","1753-01-01","UNK","UNK",None,"UNK","UNK","UNK","UNK",
            "UNK","UNK","1753-01-01","0","100","100","0","0","0"
        )
    ],
    schema_UNK
)

# Single row for NA
schema_NA = StructType([
    StructField("PRVCY_EXTRNL_MBR_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_UNIQ_KEY", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_ALPHA_PFX_TX", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_ID", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_CNTR_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_EXT_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_GRP_NO", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_BRTH_DT_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_FULL_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_FIRST_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_MIDINIT", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_LAST_NM", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_CD", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_NM", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_CD", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_NM", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_CD_SK", StringType(), True),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK", StringType(), True),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_CD_SK", StringType(), True),
])
df_NA = spark.createDataFrame(
    [
        (
            "1","NA","1","1753-01-01","1753-01-01","",
            "NA","",None,"","NA","NA","1753-01-01","NA","NA",None,"NA","NA","NA","NA",
            "NA","NA","1753-01-01","1","100","100","1","1","1"
        )
    ],
    schema_NA
)

# Now we must align df_main with the same columns as the funnel
ordered_cols = [
    "PRVCY_EXTRNL_MBR_SK",
    "SRC_SYS_CD",
    "PRVCY_EXTRNL_MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PRVCY_EXTRNL_MBR_ALPHA_PFX_TX",
    "PRVCY_EXTRNL_MBR_ID",
    "PRVCY_EXTRNL_MBR_CNTR_NO",
    "PRVCY_EXTRNL_MBR_EXT_NO",
    "PRVCY_EXTRNL_MBR_GRP_NO",
    "PRVCY_EXTRNL_MBR_RELSHP_CD",
    "PRVCY_EXTRNL_MBR_RELSHP_NM",
    "PRVCY_EXTRNL_PRSN_BRTH_DT_SK",
    "PRVCY_EXTRNL_MBR_FULL_NM",
    "PRVCY_EXTRNL_PRSN_FIRST_NM",
    "PRVCY_EXTRNL_PRSN_MIDINIT",
    "PRVCY_EXTRNL_PRSN_LAST_NM",
    "PRVCY_EXTRNL_PRSN_GNDR_CD",
    "PRVCY_EXTRNL_PRSN_GNDR_NM",
    "PRVCY_EXTL_PRSN_MRTL_STS_CD",
    "PRVCY_EXTL_PRSN_MRTL_STS_NM",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_EXTRNL_MBR_RELSHP_CD_SK",
    "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
    "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
]

df_main_aligned = df_main.select(
    F.col("PRVCY_EXTRNL_MBR_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PRVCY_EXTRNL_MBR_UNIQ_KEY").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    F.lit(EdwRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EdwRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRVCY_EXTRNL_MBR_ALPHA_PFX_TX").alias("PRVCY_EXTRNL_MBR_ALPHA_PFX_TX"),
    F.col("PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
    F.col("PRVCY_EXTRNL_MBR_CNTR_NO").alias("PRVCY_EXTRNL_MBR_CNTR_NO"),
    F.col("PRVCY_EXTRNL_MBR_EXT_NO").alias("PRVCY_EXTRNL_MBR_EXT_NO"),
    F.col("PRVCY_EXTRNL_MBR_GRP_NO").alias("PRVCY_EXTRNL_MBR_GRP_NO"),
    F.when(F.trim(F.col("PRVCY_EXTRNL_MBR_RELSHP_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PRVCY_EXTRNL_MBR_RELSHP_CD")).alias("PRVCY_EXTRNL_MBR_RELSHP_CD"),
    F.when(F.trim(F.col("PRVCY_EXTRNL_MBR_RELSHP_NM")) == "", F.lit("UNKNOWN"))
     .otherwise(F.col("PRVCY_EXTRNL_MBR_RELSHP_NM")).alias("PRVCY_EXTRNL_MBR_RELSHP_NM"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_BRTH_DT_SK"))
     .otherwise(F.lit("")).alias("PRVCY_EXTRNL_PRSN_BRTH_DT_SK"),
    F.when(
        (F.col("PRVCY_EXTRNL_MBR_SK").isNotNull()) & (F.length(F.col("PRVCY_EXTRNL_MBR_FULL_NM")) > 1),
        F.col("PRVCY_EXTRNL_MBR_FULL_NM")
    ).otherwise(F.lit("")).alias("PRVCY_EXTRNL_MBR_FULL_NM"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_FIRST_NM"))
     .otherwise(F.lit("")).alias("PRVCY_EXTRNL_PRSN_FIRST_NM"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_MIDINIT"))
     .otherwise(F.lit("")).alias("PRVCY_EXTRNL_PRSN_MIDINIT"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_LAST_NM"))
     .otherwise(F.lit("")).alias("PRVCY_EXTRNL_PRSN_LAST_NM"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_GNDR_CD"))
     .otherwise(F.lit("NA")).alias("PRVCY_EXTRNL_PRSN_GNDR_CD"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_GNDR_NM"))
     .otherwise(F.lit("NA")).alias("PRVCY_EXTRNL_PRSN_GNDR_NM"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTL_PRSN_MRTL_STS_CD"))
     .otherwise(F.lit("NA")).alias("PRVCY_EXTL_PRSN_MRTL_STS_CD"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTL_PRSN_MRTL_STS_NM"))
     .otherwise(F.lit("NA")).alias("PRVCY_EXTL_PRSN_MRTL_STS_NM"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTRNL_MBR_RELSHP_CD_SK").alias("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"))
     .otherwise(F.lit("1")).alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
    F.when(F.col("PRVCY_EXTRNL_MBR_SK").isNotNull(), F.col("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"))
     .otherwise(F.lit("1")).alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
)

# Align columns in the same order for the UNK and NA rows as well
df_UNK_aligned = df_UNK.select(ordered_cols)
df_NA_aligned = df_NA.select(ordered_cols)

# Union them
df_funnel = df_main_aligned.unionByName(df_UNK_aligned).unionByName(df_NA_aligned)

# Now apply rpad for columns with char lengths in final output.
# Identify those from the final funnel stage (seq_PRVCY_EXTRNL_MBR_D_csv_load) definition:
# "CRT_RUN_CYC_EXCTN_DT_SK" => char 10
# "LAST_UPDT_RUN_CYC_EXCTN_DT_SK" => char 10
# "PRVCY_EXTRNL_MBR_EXT_NO" => char 2
# "PRVCY_EXTRNL_PRSN_BRTH_DT_SK" => char 10
# "PRVCY_EXTRNL_PRSN_MIDINIT" => char 1
# "SRC_SYS_LAST_UPDT_DT_SK" => char 10
# Add rpad to these columns.
df_final = (
    df_funnel
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PRVCY_EXTRNL_MBR_EXT_NO", F.rpad(F.col("PRVCY_EXTRNL_MBR_EXT_NO"), 2, " "))
    .withColumn("PRVCY_EXTRNL_PRSN_BRTH_DT_SK", F.rpad(F.col("PRVCY_EXTRNL_PRSN_BRTH_DT_SK"), 10, " "))
    .withColumn("PRVCY_EXTRNL_PRSN_MIDINIT", F.rpad(F.col("PRVCY_EXTRNL_PRSN_MIDINIT"), 1, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

# seq_PRVCY_EXTRNL_MBR_D_csv_load (PxSequentialFile)
# Writing the output file .dat with delimiter = ',', quoteChar='^', no header
write_files(
    df_final.select(ordered_cols),
    f"{adls_path}/load/PRVCY_EXTRNL_MBR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)