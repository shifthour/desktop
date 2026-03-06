# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmExtrnMbrshDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from EDW and IDS claim external membership tables.
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_EXTRNL_MBRSH
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_EXTRNL_MBRSH_D
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 hf_CLM_EXTRNL_MBRSH_D_ids - hash file for new edw file
# MAGIC                 hf_CLM_EXTRNL_MBRSH_D_edw - hash file for edw for compare
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC     
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                                                                                                                                                                                                                                                               REVIEW
# MAGIC                                                                                                                                          DESCRIPTION                                                                                                                                 DATASTAGE  ENVIRONMENT               REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------                                                                                          ---------------------------------------------------              ------------------------------          -----------------
# MAGIC  Tom Harrocks                      08/02/2004-                                                                       Originally Programmed
# MAGIC  Oliver Nielsen                      12/06/2004-                                                                        Parameterize hash file names
# MAGIC   Hugh Sisson                       01/10/2006 -                                                                      Replaced single postal code field with 2 new fields - the 5-digit ZIP code and 
# MAGIC                                                                                                                                         the 4-digit ZIP+4 extension
# MAGIC   Brent leland                        03/24/2006                                                                        Took out trim() on SK values, Changed parameters to evironment variables.
# MAGIC   Brent Leland                      05/11/2006                                                                         Removed trim() on codes and IDs
# MAGIC Leandrew Moore                 2013-07-26               5114                                                    Rewrite in parallel                                                                                                                                 EnterpriseWrhsDevl                                Peter Marshall                12/16/2013
# MAGIC SAndrew                               2016-09-26           30001 Data Catalyst                              Added two fields to EDW Clm External Mbrshp that always resided on IDS ClmExternalMbrshp:         EnterpriseDev2                                      Kalyan Neelam               2016-10-27
# MAGIC                                                                                                                                          CLM_EXTRNL_MBR_SUBMT_SUB_ID  and    CLM_EXTRNL_MBR_ACTL_SUB_ID
# MAGIC                                                                                                                                             These fields are set to @NULL on both dbases.
# MAGIC Nikhil Sinha                          2017--02-28           30001 Data Catalyst                              Added  one field to EDW Clm External Mbrshp that always resided on IDS ClmExternalMbrshp:         EnterpriseDev2                                      Kalyan Neelam               2017-03-07

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CLM_EXTRML_MBRSH_D Data into a Sequential file for Load Job IdsEdwClmExtrnlMbrshpDLoad.
# MAGIC Read all the Data from IDS CLM_EXTRNL_MBRSH Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwClmExtrnlMbrshDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as psf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_db2_CLM_EXTRNL_MBRSH_in, jdbc_props_db2_CLM_EXTRNL_MBRSH_in = get_db_config(ids_secret_name)
extract_query_db2_CLM_EXTRNL_MBRSH_in = f"""
SELECT
 CLM_EXTRNL_MBRSH_SK,
 MBRSH.SRC_SYS_CD_SK,
 MBRSH.CLM_ID,
 MBR_BRTH_DT_SK,
 MBR_FIRST_NM,
 MBR_MIDINIT,
 MBR_LAST_NM,
 GRP_NM,
 PCKG_CD_ID,
 PATN_NTWK_SH_NM,
 SUB_GRP_BASE_NO,
 SUB_GRP_SECT_NO,
 SUB_ID,
 SUB_FIRST_NM,
 SUB_MIDINIT,
 SUB_LAST_NM,
 SUB_ADDR_LN_1,
 SUB_ADDR_LN_2,
 SUB_CITY_NM,
 SUB_CNTY_FIPS_ID,
 SUB_POSTAL_CD,
 CLM_SK,
 MBR_GNDR_CD_SK,
 MBR_RELSHP_CD_SK,
 CLM_EXTRNL_MBRSH_SUB_ST_CD_SK,
 CRT_RUN_CYC_EXCTN_SK,
 SUBMT_SUB_ID,
 ACTL_SUB_ID,
 MBR_SK
FROM {IDSOwner}.CLM_EXTRNL_MBRSH MBRSH,
     {IDSOwner}.W_EDW_ETL_DRVR DRVR
WHERE
     MBRSH.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
 AND MBRSH.CLM_ID = DRVR.CLM_ID
"""
df_db2_CLM_EXTRNL_MBRSH_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_EXTRNL_MBRSH_in)
    .options(**jdbc_props_db2_CLM_EXTRNL_MBRSH_in)
    .option("query", extract_query_db2_CLM_EXTRNL_MBRSH_in)
    .load()
)

jdbc_url_db2_CD_MPPNG_in, jdbc_props_db2_CD_MPPNG_in = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_in = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_in)
    .options(**jdbc_props_db2_CD_MPPNG_in)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    psf.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    psf.col("TRGT_CD").alias("TRGT_CD"),
    psf.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_refClmExtrnlMbrGndr = df_cpy_cd_mppng.select(
    psf.col("CD_MPPNG_SK"),
    psf.col("TRGT_CD"),
    psf.col("TRGT_CD_NM")
)

df_lnk_refSrcSys = df_cpy_cd_mppng.select(
    psf.col("CD_MPPNG_SK"),
    psf.col("TRGT_CD"),
    psf.col("TRGT_CD_NM")
)

df_lnk_refClmExtrnlMbrSubSt = df_cpy_cd_mppng.select(
    psf.col("CD_MPPNG_SK"),
    psf.col("TRGT_CD"),
    psf.col("TRGT_CD_NM")
)

df_lnk_refClmExtrnlMbrRelshp = df_cpy_cd_mppng.select(
    psf.col("CD_MPPNG_SK"),
    psf.col("TRGT_CD"),
    psf.col("TRGT_CD_NM")
)

df_lkp_stage = df_db2_CLM_EXTRNL_MBRSH_in.alias("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC") \
    .join(
        df_lnk_refClmExtrnlMbrGndr.alias("lnk_refClmExtrnlMbrGndr"),
        psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_GNDR_CD_SK") == psf.col("lnk_refClmExtrnlMbrGndr.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refSrcSys.alias("lnk_refSrcSys"),
        psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SRC_SYS_CD_SK") == psf.col("lnk_refSrcSys.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refClmExtrnlMbrSubSt.alias("lnk_refClmExtrnlMbrSubSt"),
        psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CLM_EXTRNL_MBRSH_SUB_ST_CD_SK") == psf.col("lnk_refClmExtrnlMbrSubSt.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refClmExtrnlMbrRelshp.alias("lnl_refClmExtrnlMbrRelshp"),
        psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_RELSHP_CD_SK") == psf.col("lnl_refClmExtrnlMbrRelshp.CD_MPPNG_SK"),
        "left"
    )

df_lnk_CodesLkpData_out = df_lkp_stage.select(
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CLM_ID").alias("CLM_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_MIDINIT").alias("MBR_MIDINIT"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_LAST_NM").alias("MBR_LAST_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.GRP_NM").alias("GRP_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.PCKG_CD_ID").alias("PCKG_CD_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_ID").alias("SUB_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_MIDINIT").alias("SUB_MIDINIT"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_LAST_NM").alias("SUB_LAST_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_CITY_NM").alias("SUB_CITY_NM"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CLM_SK").alias("CLM_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CLM_EXTRNL_MBRSH_SUB_ST_CD_SK").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD_SK"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    psf.col("lnk_refClmExtrnlMbrGndr.TRGT_CD").alias("GNDR_CD"),
    psf.col("lnk_refClmExtrnlMbrGndr.TRGT_CD_NM").alias("GNDR_CD_NM"),
    psf.col("lnl_refClmExtrnlMbrRelshp.TRGT_CD").alias("RELATION_CD"),
    psf.col("lnl_refClmExtrnlMbrRelshp.TRGT_CD_NM").alias("RELATION_CD_NM"),
    psf.col("lnk_refClmExtrnlMbrSubSt.TRGT_CD").alias("SUB_CD"),
    psf.col("lnk_refClmExtrnlMbrSubSt.TRGT_CD_NM").alias("SUB_CD_NM"),
    psf.col("lnk_refSrcSys.TRGT_CD").alias("SRC_SYS_CD"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.ACTL_SUB_ID").alias("ACTL_SUB_ID"),
    psf.col("lnk_IdsEdwClmExtrnlMbrshDExtr_InABC.MBR_SK").alias("MBR_SK")
)

df_xfrm = (
    df_lnk_CodesLkpData_out
    .withColumn(
        "SVchecknull",
        psf.when(
            psf.col("SUB_POSTAL_CD").isNull() | (trim(psf.col("SUB_POSTAL_CD")) == ''),
            ''
        ).otherwise(FORMAT_POSTALCD_EE(psf.col("SUB_POSTAL_CD")))
    )
    .withColumn(
        "SVzip5",
        psf.when(
            (psf.length(trim(psf.col("SVchecknull").substr(psf.lit(1), psf.lit(5)))) == 0) |
            (psf.col("SVchecknull") == ''),
            ' '
        ).otherwise(psf.col("SVchecknull").substr(psf.lit(1), psf.lit(5)))
    )
    .withColumn(
        "SVzip4",
        psf.when(
            (psf.length(trim(psf.col("SVchecknull"))) < 6) | (psf.col("SVchecknull") == ''),
            ' '
        ).otherwise(psf.col("SVchecknull").substr(psf.lit(6), psf.lit(4)))
    )
)

df_lnk_Detail = df_xfrm.filter(
    "(CLM_EXTRNL_MBRSH_SK <> 0) AND (CLM_EXTRNL_MBRSH_SK <> 1)"
).select(
    psf.col("CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
    psf.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    psf.col("CLM_ID").alias("CLM_ID"),
    psf.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    psf.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    psf.col("MBR_FIRST_NM").alias("CLM_EXTRNL_MBR_FIRST_NM"),
    psf.when(
        psf.trim(psf.coalesce(psf.col("MBR_MIDINIT"), psf.lit(""))) == ',',
        psf.lit(None)
    ).otherwise(psf.col("MBR_MIDINIT")).alias("CLM_EXTRNL_MBR_MIDINIT"),
    psf.col("MBR_LAST_NM").alias("CLM_EXTRNL_MBR_LAST_NM"),
    psf.col("GNDR_CD").alias("CLM_EXTRNL_MBR_GNDR_CD"),
    psf.col("GNDR_CD_NM").alias("CLM_EXTRNL_MBR_GNDR_NM"),
    psf.col("GRP_NM").alias("CLM_EXTRNL_MBR_GRP_NM"),
    psf.col("PCKG_CD_ID").alias("CLM_EXTRNL_MBR_PCKG_CD_ID"),
    psf.col("PATN_NTWK_SH_NM").alias("CLM_EXTRNL_MBR_PATN_NTWK_SH_NM"),
    psf.col("RELATION_CD").alias("CLM_EXTRNL_MBR_RELSHP_CD"),
    psf.col("RELATION_CD_NM").alias("CLM_EXTRNL_MBR_RELSHP_NM"),
    psf.col("SUB_GRP_BASE_NO").alias("CLM_EXTRNL_MBR_SUB_GRP_BASE_NO"),
    psf.col("SUB_GRP_SECT_NO").alias("CLM_EXTRNL_MBR_SUB_GRP_SECT_NO"),
    psf.col("SUB_ID").alias("CLM_EXTRNL_MBR_SUB_ID"),
    psf.col("SUB_FIRST_NM").alias("CLM_EXTRNL_MBR_SUB_FIRST_NM"),
    psf.when(
        psf.trim(psf.coalesce(psf.col("SUB_MIDINIT"), psf.lit(""))) == ',',
        psf.lit(None)
    ).otherwise(psf.col("SUB_MIDINIT")).alias("CLM_EXTRNL_MBR_SUB_MIDINIT"),
    psf.col("SUB_LAST_NM").alias("CLM_EXTRNL_MBR_SUB_LAST_NM"),
    psf.col("SUB_ADDR_LN_1").alias("CLM_EXTRNL_MBR_SUB_ADDR_LN_1"),
    psf.col("SUB_ADDR_LN_2").alias("CLM_EXTRNL_MBR_SUB_ADDR_LN_2"),
    psf.col("SUB_CITY_NM").alias("CLM_EXTRNL_MBR_SUB_CITY_NM"),
    psf.col("SUB_CD").alias("CLM_EXTRNL_MBR_SUB_ST_CD"),
    psf.col("SUB_CD_NM").alias("CLM_EXTRNL_MBR_SUB_ST_NM"),
    psf.col("SUB_CNTY_FIPS_ID").alias("CLM_EXTR_MBR_SUB_CNTY_FIPS_ID"),
    psf.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    psf.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    psf.col("CLM_SK").alias("CLM_SK"),
    psf.col("MBR_GNDR_CD_SK").alias("CLM_EXTRNL_MBR_GNDR_CD_SK"),
    psf.col("MBR_RELSHP_CD_SK").alias("CLM_EXTRNL_MBR_RELSHP_CD_SK"),
    psf.col("CLM_EXTRNL_MBRSH_SUB_ST_CD_SK").alias("CLM_EXTRNL_MBR_SUB_ST_CD_SK"),
    psf.col("SVzip5").alias("CLM_EXTRNL_MBR_SUB_ZIP_CD_5"),
    psf.col("SVzip4").alias("CLM_EXTRNL_MBR_SUB_ZIP_CD_4"),
    psf.col("MBR_BRTH_DT_SK").alias("CLM_EXTRNL_MBR_BRTH_DT_SK"),
    psf.col("SUBMT_SUB_ID").alias("CLM_EXTRNL_MBR_SUBMT_SUB_ID"),
    psf.col("ACTL_SUB_ID").alias("CLM_EXTRNL_MBR_ACTL_SUB_ID"),
    psf.col("MBR_SK").alias("MBR_SK")
)

schema_xfrm = StructType([
    StructField("CLM_EXTRNL_MBRSH_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_EXTRNL_MBR_FIRST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_MIDINIT", StringType(), True),
    StructField("CLM_EXTRNL_MBR_LAST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_GNDR_CD", StringType(), True),
    StructField("CLM_EXTRNL_MBR_GNDR_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_GRP_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_PCKG_CD_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBR_PATN_NTWK_SH_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_RELSHP_CD", StringType(), True),
    StructField("CLM_EXTRNL_MBR_RELSHP_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_GRP_BASE_NO", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_GRP_SECT_NO", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_FIRST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_MIDINIT", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_LAST_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ADDR_LN_1", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ADDR_LN_2", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_CITY_NM", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ST_CD", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ST_NM", StringType(), True),
    StructField("CLM_EXTR_MBR_SUB_CNTY_FIPS_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CLM_EXTRNL_MBR_GNDR_CD_SK", IntegerType(), True),
    StructField("CLM_EXTRNL_MBR_RELSHP_CD_SK", IntegerType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ST_CD_SK", IntegerType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ZIP_CD_5", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUB_ZIP_CD_4", StringType(), True),
    StructField("CLM_EXTRNL_MBR_BRTH_DT_SK", StringType(), True),
    StructField("CLM_EXTRNL_MBR_SUBMT_SUB_ID", StringType(), True),
    StructField("CLM_EXTRNL_MBR_ACTL_SUB_ID", StringType(), True),
    StructField("MBR_SK", IntegerType(), True)
])

df_lnk_Na = spark.createDataFrame([
    (
        1, 'NA', 'NA', '1753-01-01', '1753-01-01', 'NA', None, 'NA', 'NA', 'NA',
        'NA', 'NA', 'NA', 'NA', 'NA', None, None, 'NA', 'NA', None, 'NA',
        None, None, 'NA', 'NA', 'NA', 100, 100, 1, 1, 1, 1, '', '', '1753-01-01',
        None, None, 1
    )
], schema_xfrm)

df_lnk_Unk = spark.createDataFrame([
    (
        0, 'UNK', 'UNK', '1753-01-01', '1753-01-01', 'UNK', None, 'UNK', 'UNK', 'UNK',
        'UNK', 'UNK', 'UNK', 'UNK', 'UNK', None, None, 'UNK', 'UNK', None, 'UNK',
        None, None, 'UNK', 'UNK', 'UNK', 100, 100, 0, 0, 0, 0, '', '', '1753-01-01',
        None, None, 1
    )
], schema_xfrm)

df_Fnl_CLM_EXTRML_MBRSH_D = df_lnk_Detail.unionByName(df_lnk_Na).unionByName(df_lnk_Unk)

df_funnel_out = df_Fnl_CLM_EXTRML_MBRSH_D

df_final_out = (
    df_funnel_out
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", psf.rpad(psf.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", psf.rpad(psf.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_EXTRNL_MBR_MIDINIT", psf.rpad(psf.col("CLM_EXTRNL_MBR_MIDINIT"), 1, " "))
    .withColumn("CLM_EXTRNL_MBR_SUB_MIDINIT", psf.rpad(psf.col("CLM_EXTRNL_MBR_SUB_MIDINIT"), 1, " "))
    .withColumn("CLM_EXTR_MBR_SUB_CNTY_FIPS_ID", psf.rpad(psf.col("CLM_EXTR_MBR_SUB_CNTY_FIPS_ID"), 3, " "))
    .withColumn("CLM_EXTRNL_MBR_SUB_ZIP_CD_5", psf.rpad(psf.col("CLM_EXTRNL_MBR_SUB_ZIP_CD_5"), 5, " "))
    .withColumn("CLM_EXTRNL_MBR_SUB_ZIP_CD_4", psf.rpad(psf.col("CLM_EXTRNL_MBR_SUB_ZIP_CD_4"), 4, " "))
    .withColumn("CLM_EXTRNL_MBR_BRTH_DT_SK", psf.rpad(psf.col("CLM_EXTRNL_MBR_BRTH_DT_SK"), 10, " "))
    .select(
        "CLM_EXTRNL_MBRSH_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_EXTRNL_MBR_FIRST_NM",
        "CLM_EXTRNL_MBR_MIDINIT",
        "CLM_EXTRNL_MBR_LAST_NM",
        "CLM_EXTRNL_MBR_GNDR_CD",
        "CLM_EXTRNL_MBR_GNDR_NM",
        "CLM_EXTRNL_MBR_GRP_NM",
        "CLM_EXTRNL_MBR_PCKG_CD_ID",
        "CLM_EXTRNL_MBR_PATN_NTWK_SH_NM",
        "CLM_EXTRNL_MBR_RELSHP_CD",
        "CLM_EXTRNL_MBR_RELSHP_NM",
        "CLM_EXTRNL_MBR_SUB_GRP_BASE_NO",
        "CLM_EXTRNL_MBR_SUB_GRP_SECT_NO",
        "CLM_EXTRNL_MBR_SUB_ID",
        "CLM_EXTRNL_MBR_SUB_FIRST_NM",
        "CLM_EXTRNL_MBR_SUB_MIDINIT",
        "CLM_EXTRNL_MBR_SUB_LAST_NM",
        "CLM_EXTRNL_MBR_SUB_ADDR_LN_1",
        "CLM_EXTRNL_MBR_SUB_ADDR_LN_2",
        "CLM_EXTRNL_MBR_SUB_CITY_NM",
        "CLM_EXTRNL_MBR_SUB_ST_CD",
        "CLM_EXTRNL_MBR_SUB_ST_NM",
        "CLM_EXTR_MBR_SUB_CNTY_FIPS_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_EXTRNL_MBR_GNDR_CD_SK",
        "CLM_EXTRNL_MBR_RELSHP_CD_SK",
        "CLM_EXTRNL_MBR_SUB_ST_CD_SK",
        "CLM_EXTRNL_MBR_SUB_ZIP_CD_5",
        "CLM_EXTRNL_MBR_SUB_ZIP_CD_4",
        "CLM_EXTRNL_MBR_BRTH_DT_SK",
        "CLM_EXTRNL_MBR_SUBMT_SUB_ID",
        "CLM_EXTRNL_MBR_ACTL_SUB_ID",
        "MBR_SK"
    )
)

write_files(
    df_final_out,
    f"{adls_path}/load/CLM_EXTRNL_MBRSH_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)