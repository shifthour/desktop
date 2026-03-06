# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2014 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBHIMbrDemoRules56Extr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Final job to determine which of multiple records for some members should be sent to BHI.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiMbrDemoHistExtrSeq
# MAGIC 
# MAGIC HASH FILES:  Parallel datasets used instead of hash files; cleared by control job near end of run.
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                 Date                    Project/Ticket #\(9)                      Change Description\(9)\(9)\(9)    Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------               -------------------         ---------------------------\(9)                      -----------------------------------------------------------------------      --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Abhiram Dasarathy   2014-09-28            5212 Payment Innovation                Original coding                                                      EnterpriseNewDevl                       Kalyan Neelam          2014-11-19
# MAGIC Jaideep Mankala       2015-03-02           5212 Payment Innovation                Removed Submission control file from job             EnterpriseNewDevl                       Kalyan Neelam          2015-03-06 
# MAGIC 
# MAGIC Aishwarya                 2016-08-08            5406 BHI                                         Added 3 fields EFF_DT,EXPRTN_DT,                  EnterpriseDevl                               Jag Yelavarthi           2016-08-11
# MAGIC                                                                                                                        OOA_MBR_CD                                       
# MAGIC 
# MAGIC Shanmugam A        2017-01-17            5406 BHI                                         Added   Min eff_dt and Max eff_dt                           EnterpriseDevl                             Jag Yelavarthi            2017-02-16       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Akhila M                  2017-03-17            5604-BHI updates                     MBR_UNIQUE_Key added in Member File                   EnterpriseNewDevl                            Jag Yelavarthi             2017-03-21        
# MAGIC                                                                                                                                 
# MAGIC 
# MAGIC Madhavan B           2017-07-07            5788-BHI updates                     Modified the transformation rule for                                  EnterpriseDev1                                 Jag Yelavarthi              2017-07-17
# MAGIC                                                                                                                HOST_PLN_OVRD, MBR_PARTCPN_CD and
# MAGIC                                                                                                                OOA_MBR_CD
# MAGIC 
# MAGIC Sruthi M                  2018-03-21               21102                                        Populated HOST_PLN_OVRD                                     EnterpriseDev1                                Jaideep Mankala          03/22/2018
# MAGIC                                                                                                                     field as per the mapping
# MAGIC 
# MAGIC Mohan Karnati        2019-05-16               75586                                     Changed logic for OOA_MBR_CD in Null_Check           EnterpriseDev2                           Kalyan Neelam             2019-05-24              
# MAGIC                                                                                                                 stage
# MAGIC 
# MAGIC Hari Krishna Rao Yadav  2021--03-24   US-361796                            Changed logic for OOA_MBR_CD in Transformer              EnterpriseSITF                                Jeyaprasanna             2021-04-20
# MAGIC                                                                                                             stage
# MAGIC 
# MAGIC Ravi Ranjan           2021--05-24                   US-381576                      Added coulmns MBR_SK and MCARE_BNFCRY_ID       EnterpriseSITF                                Jeyaprasanna             2021-10-12
# MAGIC                                                                                                              and Added lookup for the columns 
# MAGIC                                                                                                              MCARE_CNTR_ID and PLN_BNF_PCKG_ID and
# MAGIC                                                                                                              Added tarnsformation logic for the MCARE_CNTR_ID
# MAGIC                                                                                                                                          
# MAGIC Priyatham               2024-02-26             US-481128                              Removed the constraint for member term date greater     EnterpriseDev2                                Jeyaprasanna            2024-03-06
# MAGIC                                                                                                               than current date so that termed members are not
# MAGIC                                                                                                               excluded from Member demographic file
# MAGIC 
# MAGIC 
# MAGIC Sudhan B              2025-05-09              US649995                              Added NDW25 Release Member Demographic file to         EnterpriseDev2\(9)\(9)Harsha Ravuri\(9)2025-05-20
# MAGIC                                                                                                              populate ITS_SUB_ID with 17 Spaces,  
# MAGIC                                                                                                              MCARE_BNFCRY_ID with 11 Spaces and MCARE_CNTR_ID 
# MAGIC                                                                                                              with 8 Spaces in Xfm_MCARE_CNTR_ID stage.

# MAGIC Final file to be sent to the Association after all the rules have been processed
# MAGIC #$FilePath#/external/std_member_demographic
# MAGIC As part of NDW 25.5 Release Made Change to populate  spaces for ITS_SUB_ID, 
# MAGIC MCARE_BNFCRY_ID and MCARE_CNTR_ID in Xfm_MCARE_CNTR_ID stage.
# MAGIC National Data Warehouse 25.5 Release file std_member_demographic_ndw_25.5
# MAGIC BHI Member Demographic Extract 
# MAGIC 
# MAGIC All duplicate records are filtered out based on ITS Subscriber ID and Term Date.
# MAGIC Sort and remove duplicates based on Subscirber ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ClmStartDate = get_widget_value('ClmStartDate','')
ClmEndDate = get_widget_value('ClmEndDate','')
CurrDate = get_widget_value('CurrDate','')
HstFlgDelta = get_widget_value('HstFlgDelta','N')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)

schema_Member_File = StructType([
    StructField("MBR_SK", IntegerType(), False),
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("CONSIS_MBR_ID", StringType(), False),
    StructField("MBR_PFX", StringType(), False),
    StructField("MBR_LAST_NM", StringType(), False),
    StructField("MBR_FIRST_NM", StringType(), False),
    StructField("MBR_MIDINT", StringType(), False),
    StructField("MBR_SFX", StringType(), False),
    StructField("MBR_PRI_ST_ADDR_1", StringType(), False),
    StructField("MBR_PRI_ST_ADDR_2", StringType(), False),
    StructField("MBR_PRI_CITY", StringType(), False),
    StructField("MBR_PRI_ST", StringType(), False),
    StructField("MBR_PRI_ZIP_CD", StringType(), False),
    StructField("MBR_PRI_ZIP_CD_PLUS_4", StringType(), False),
    StructField("MBR_PRI_PHN_NO", StringType(), False),
    StructField("MBR_PRI_EMAIL", StringType(), False),
    StructField("MBR_SEC_ST_ADDR_1", StringType(), False),
    StructField("MBR_SEC_ST_ADDR_2", StringType(), False),
    StructField("MBR_SEC_CITY", StringType(), False),
    StructField("MBR_SEC_ST", StringType(), False),
    StructField("MBR_SEC_ZIP_CD", StringType(), False),
    StructField("MBR_SEC_ZIP_CD_PLUS_4", StringType(), False),
    StructField("HOST_PLN_OVRD", StringType(), False),
    StructField("MBR_PARTCPN_CD", StringType(), False),
    StructField("ITS_SUB_ID", StringType(), False),
    StructField("MMI_ID", StringType(), False),
    StructField("HOST_PLN_CD", StringType(), False),
    StructField("PI_VOID_IN", StringType(), False),
    StructField("PI_MBR_CONFITY_CD", StringType(), False),
    StructField("MBR_RELSHP_CD", StringType(), False),
    StructField("SUB_UNIQ_KEY_ORIG_EFF_DT", StringType(), False),
    StructField("SUB_MBR_BRTH_DT", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_DP_IN", StringType(), False),
    StructField("SUB_IN_AREA_IN", StringType(), False),
    StructField("PROD_SH_NM", StringType(), False),
    StructField("MCARE_BNFCRY_ID", StringType(), False)
])

df_MBR_MCARE_EVT_D1_query = f"""
SELECT DISTINCT
    MBR_MCARE.MBR_SK,
    MBR_ENR_D.MBR_ENR_EFF_DT_SK,
    MBR_ENR_D.MBR_ENR_TERM_DT_SK,
    MBR_MCARE.MBR_MCARE_EVT_TERM_DT_SK,
    MBR_MCARE.PLN_BNF_PCKG_ID
FROM
    {EDWOwner}.MBR_MCARE_EVT_D as MBR_MCARE
    JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D
       ON MBR_MCARE.MBR_SK = MBR_ENR_D.MBR_SK
       AND MBR_ENR_D.MBR_ENR_EFF_DT_SK between MBR_MCARE.MBR_MCARE_EVT_EFF_DT_SK and MBR_MCARE.MBR_MCARE_EVT_TERM_DT_SK
WHERE
    MBR_MCARE.MBR_MCARE_EVT_CD = 'PBP'
"""

df_MBR_MCARE_EVT_D1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", df_MBR_MCARE_EVT_D1_query)
    .load()
)

df_Transformer_123_tmp = df_MBR_MCARE_EVT_D1.withColumn(
    "MBR_ENR_EFF_DT_SK", Ereplace(F.col("MBR_ENR_EFF_DT_SK"), '-', '')
).withColumn(
    "MBR_ENR_TERM_DT_SK", Ereplace(F.col("MBR_ENR_TERM_DT_SK"), '-', '')
).withColumn(
    "MBR_MCARE_EVT_TERM_DT_SK", Ereplace(F.col("MBR_MCARE_EVT_TERM_DT_SK"), '-', '')
)

df_lnk_PLN_BNF_PCKG_ID = df_Transformer_123_tmp.select(
    F.col("MBR_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_MCARE_EVT_TERM_DT_SK"),
    F.col("PLN_BNF_PCKG_ID")
)

df_MBR_MCARE_EVT_D_query = f"""
SELECT DISTINCT
    MBR_MCARE.MBR_SK,
    MBR_ENR_D.MBR_ENR_EFF_DT_SK,
    MBR_ENR_D.MBR_ENR_TERM_DT_SK,
    MBR_MCARE.MBR_MCARE_EVT_TERM_DT_SK,
    MBR_MCARE.MCARE_CNTR_ID
FROM
    {EDWOwner}.MBR_MCARE_EVT_D as MBR_MCARE
    JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D
       ON MBR_MCARE.MBR_SK = MBR_ENR_D.MBR_SK
       AND MBR_ENR_D.MBR_ENR_EFF_DT_SK between MBR_MCARE.MBR_MCARE_EVT_EFF_DT_SK and MBR_MCARE.MBR_MCARE_EVT_TERM_DT_SK
WHERE
    MBR_MCARE.MBR_MCARE_EVT_CD = 'STCNTRT'
"""

df_MBR_MCARE_EVT_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", df_MBR_MCARE_EVT_D_query)
    .load()
)

df_Transformer_dates_tmp = df_MBR_MCARE_EVT_D.withColumn(
    "MBR_ENR_EFF_DT_SK", Ereplace(F.col("MBR_ENR_EFF_DT_SK"), '-', '')
).withColumn(
    "MBR_ENR_TERM_DT_SK", Ereplace(F.col("MBR_ENR_TERM_DT_SK"), '-', '')
).withColumn(
    "MBR_MCARE_EVT_TERM_DT_SK", Ereplace(F.col("MBR_MCARE_EVT_TERM_DT_SK"), '-', '')
)

df_lnk_MCARE_CNTR_ID = df_Transformer_dates_tmp.select(
    F.col("MBR_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_MCARE_EVT_TERM_DT_SK"),
    F.col("MCARE_CNTR_ID")
)

df_Member_File = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", False)
    .schema(schema_Member_File)
    .csv(f"{adls_path_publish}/external/std_member_demographic_dupes")
)

df_Mem_demo_all = spark.read.parquet(f"{adls_path}/ds/bhi_mbr_demo_all.parquet")

df_Lnk_MinEff = df_Mem_demo_all.select(
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK")
)

df_Lnk_MaxEnd = df_Mem_demo_all.select(
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MaxMBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK")
)

df_RmDup_MbrId = dedup_sort(
    df_Lnk_MinEff,
    ["CONSIS_MBR_ID"],
    [("CONSIS_MBR_ID","A"), ("MBR_ENR_EFF_DT_SK","D")]
)

df_Lnk_Mbr = df_RmDup_MbrId.select(
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK")
)

df_RmDup_MaxEnd = dedup_sort(
    df_Lnk_MaxEnd,
    ["CONSIS_MBR_ID"],
    [("CONSIS_MBR_ID","A"), ("MaxMBR_ENR_TERM_DT_SK","D")]
)

df_Lnk_MaxMb = df_RmDup_MaxEnd.select(
    F.col("CONSIS_MBR_ID"),
    F.col("MaxMBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK")
)

df_Jn_Date_tmp = df_Lnk_Mbr.alias("Lnk_Mbr").join(
    df_Lnk_MaxMb.alias("Lnk_MaxMb"),
    on=["CONSIS_MBR_ID"],
    how="left"
)

df_Jn_Date_all = df_Jn_Date_tmp.select(
    F.col("Lnk_Mbr.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Lnk_Mbr.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Lnk_Mbr.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Lnk_MaxMb.MaxMBR_ENR_TERM_DT_SK").alias("MaxMBR_ENR_TERM_DT_SK")
)

df_Jnr_MaxDates_tmp = df_Member_File.alias("Members").join(
    df_Jn_Date_all.alias("all"),
    on=["CONSIS_MBR_ID"],
    how="inner"
)

df_Jnr_MaxDates = df_Jnr_MaxDates_tmp.select(
    F.col("Members.MBR_SK").alias("MBR_SK"),
    F.col("Members.BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("Members.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Members.MBR_PFX").alias("MBR_PFX"),
    F.col("Members.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Members.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Members.MBR_MIDINT").alias("MBR_MIDINT"),
    F.col("Members.MBR_SFX").alias("MBR_SFX"),
    F.col("Members.MBR_PRI_ST_ADDR_1").alias("MBR_PRI_ST_ADDR_1"),
    F.col("Members.MBR_PRI_ST_ADDR_2").alias("MBR_PRI_ST_ADDR_2"),
    F.col("Members.MBR_PRI_CITY").alias("MBR_PRI_CITY"),
    F.col("Members.MBR_PRI_ST").alias("MBR_PRI_ST"),
    F.col("Members.MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("Members.MBR_PRI_ZIP_CD_PLUS_4").alias("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("Members.MBR_PRI_PHN_NO").alias("MBR_PRI_PHN_NO"),
    F.col("Members.MBR_PRI_EMAIL").alias("MBR_PRI_EMAIL"),
    F.col("Members.MBR_SEC_ST_ADDR_1").alias("MBR_SEC_ST_ADDR_1"),
    F.col("Members.MBR_SEC_ST_ADDR_2").alias("MBR_SEC_ST_ADDR_2"),
    F.col("Members.MBR_SEC_CITY").alias("MBR_SEC_CITY"),
    F.col("Members.MBR_SEC_ST").alias("MBR_SEC_ST"),
    F.col("Members.MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("Members.MBR_SEC_ZIP_CD_PLUS_4").alias("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("Members.HOST_PLN_OVRD").alias("HOST_PLN_OVRD"),
    F.col("Members.MBR_PARTCPN_CD").alias("MBR_PARTCPN_CD"),
    F.col("Members.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("Members.MMI_ID").alias("MMI_ID"),
    F.col("Members.HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("Members.PI_VOID_IN").alias("PI_VOID_IN"),
    F.col("Members.PI_MBR_CONFITY_CD").alias("PI_MBR_CONFITY_CD"),
    F.col("all.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("all.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("all.MaxMBR_ENR_TERM_DT_SK").alias("MaxMBR_ENR_TERM_DT_SK"),
    F.col("Members.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Members.GRP_DP_IN").alias("GRP_DP_IN"),
    F.col("Members.SUB_IN_AREA_IN").alias("SUB_IN_AREA_IN"),
    F.col("Members.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("Members.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    F.col("Members.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)

df_Pass_tmp = df_Jnr_MaxDates.withColumn(
    "MBR_RELSHP",
    F.when(trim(F.col("MBR_RELSHP_CD")) == 'SUB', F.lit(1)).otherwise(F.lit(2))
)

df_sort_ItsSubId = df_Pass_tmp.select(
    F.col("MBR_SK"),
    F.col("BHI_HOME_PLN_ID"),
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_PFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINT"),
    F.col("MBR_SFX"),
    F.col("MBR_PRI_ST_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2"),
    F.col("MBR_PRI_CITY"),
    F.col("MBR_PRI_ST"),
    F.col("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("MBR_PRI_PHN_NO"),
    F.col("MBR_PRI_EMAIL"),
    F.col("MBR_SEC_ST_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2"),
    F.col("MBR_SEC_CITY"),
    F.col("MBR_SEC_ST"),
    F.col("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("HOST_PLN_OVRD"),
    F.col("MBR_PARTCPN_CD"),
    F.col("ITS_SUB_ID"),
    F.col("MMI_ID"),
    F.col("HOST_PLN_CD"),
    F.col("PI_VOID_IN"),
    F.col("PI_MBR_CONFITY_CD"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MaxMBR_ENR_TERM_DT_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_RELSHP"),
    F.col("GRP_DP_IN"),
    F.col("SUB_IN_AREA_IN"),
    F.col("PROD_SH_NM"),
    F.col("MCARE_BNFCRY_ID")
)

df_Remove_Duplicates_91 = dedup_sort(
    df_sort_ItsSubId,
    ["CONSIS_MBR_ID"],
    [
        ("CONSIS_MBR_ID","A"),
        ("MaxMBR_ENR_TERM_DT_SK","D"),
        ("MBR_RELSHP","A"),
        ("ITS_SUB_ID","A")
    ]
)

df_Out = df_Remove_Duplicates_91.select(
    F.col("MBR_SK"),
    F.col("BHI_HOME_PLN_ID"),
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_PFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINT"),
    F.col("MBR_SFX"),
    F.col("MBR_PRI_ST_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2"),
    F.col("MBR_PRI_CITY"),
    F.col("MBR_PRI_ST"),
    F.col("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("MBR_PRI_PHN_NO"),
    F.col("MBR_PRI_EMAIL"),
    F.col("MBR_SEC_ST_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2"),
    F.col("MBR_SEC_CITY"),
    F.col("MBR_SEC_ST"),
    F.col("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("HOST_PLN_OVRD"),
    F.col("MBR_PARTCPN_CD"),
    F.col("ITS_SUB_ID"),
    F.col("MMI_ID"),
    F.col("HOST_PLN_CD"),
    F.col("PI_VOID_IN"),
    F.col("PI_MBR_CONFITY_CD"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MaxMBR_ENR_TERM_DT_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("GRP_DP_IN"),
    F.col("SUB_IN_AREA_IN"),
    F.col("PROD_SH_NM"),
    F.col("MCARE_BNFCRY_ID")
)

df_Null_Check_tmp = df_Out.withColumn(
    "BHI_HOME_PLN_ID",
    F.when(F.col("BHI_HOME_PLN_ID").isNull(), F.lit(" " * 3)).otherwise(F.col("BHI_HOME_PLN_ID"))
).withColumn(
    "CONSIS_MBR_ID",
    F.when(F.col("CONSIS_MBR_ID").isNull(), F.lit(" " * 22)).otherwise(F.col("CONSIS_MBR_ID"))
).withColumn(
    "MBR_PFX",
    F.when(F.col("MBR_PFX").isNull(), F.lit(" " * 20)).otherwise(F.col("MBR_PFX"))
).withColumn(
    "MBR_LAST_NM",
    F.when(F.col("MBR_LAST_NM").isNull(), F.lit(" " * 150)).otherwise(F.col("MBR_LAST_NM"))
).withColumn(
    "MBR_FIRST_NM",
    F.when(F.col("MBR_FIRST_NM").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_FIRST_NM"))
).withColumn(
    "MBR_MIDINT",
    F.when(F.col("MBR_MIDINT").isNull(), F.lit(" " * 2)).otherwise(F.col("MBR_MIDINT"))
).withColumn(
    "MBR_SFX",
    F.when(F.col("MBR_SFX").isNull(), F.lit(" " * 20)).otherwise(F.col("MBR_SFX"))
).withColumn(
    "MBR_PRI_ST_ADDR_1",
    F.when(F.col("MBR_PRI_ST_ADDR_1").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_PRI_ST_ADDR_1"))
).withColumn(
    "MBR_PRI_ST_ADDR_2",
    F.when(F.col("MBR_PRI_ST_ADDR_2").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_PRI_ST_ADDR_2"))
).withColumn(
    "MBR_PRI_CITY",
    F.when(F.col("MBR_PRI_CITY").isNull(), F.lit(" " * 35)).otherwise(F.col("MBR_PRI_CITY"))
).withColumn(
    "MBR_PRI_ST",
    F.when(F.col("MBR_PRI_ST").isNull(), F.lit(" " * 2)).otherwise(F.col("MBR_PRI_ST"))
).withColumn(
    "MBR_PRI_ZIP_CD",
    F.when(F.col("MBR_PRI_ZIP_CD").isNull(), F.lit(" " * 5)).otherwise(F.col("MBR_PRI_ZIP_CD"))
).withColumn(
    "MBR_PRI_ZIP_CD_PLUS_4",
    F.when(F.col("MBR_PRI_ZIP_CD_PLUS_4").isNull(), F.lit(" " * 4)).otherwise(F.col("MBR_PRI_ZIP_CD_PLUS_4"))
).withColumn(
    "MBR_PRI_PHN_NO",
    F.when(F.col("MBR_PRI_PHN_NO").isNull(), F.lit(" " * 10)).otherwise(F.col("MBR_PRI_PHN_NO"))
).withColumn(
    "MBR_PRI_EMAIL",
    F.when(F.col("MBR_PRI_EMAIL").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_PRI_EMAIL"))
).withColumn(
    "MBR_SEC_ST_ADDR_1",
    F.when(F.col("MBR_SEC_ST_ADDR_1").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_SEC_ST_ADDR_1"))
).withColumn(
    "MBR_SEC_ST_ADDR_2",
    F.when(F.col("MBR_SEC_ST_ADDR_2").isNull(), F.lit(" " * 70)).otherwise(F.col("MBR_SEC_ST_ADDR_2"))
).withColumn(
    "MBR_SEC_CITY",
    F.when(F.col("MBR_SEC_CITY").isNull(), F.lit(" " * 35)).otherwise(F.col("MBR_SEC_CITY"))
).withColumn(
    "MBR_SEC_ST",
    F.when(F.col("MBR_SEC_ST").isNull(), F.lit(" " * 2)).otherwise(F.col("MBR_SEC_ST"))
).withColumn(
    "MBR_SEC_ZIP_CD",
    F.when(F.col("MBR_SEC_ZIP_CD").isNull(), F.lit(" " * 5)).otherwise(F.col("MBR_SEC_ZIP_CD"))
).withColumn(
    "MBR_SEC_ZIP_CD_PLUS_4",
    F.when(F.col("MBR_SEC_ZIP_CD_PLUS_4").isNull(), F.lit(" " * 4)).otherwise(F.col("MBR_SEC_ZIP_CD_PLUS_4"))
).withColumn(
    "HOST_PLN_OVRD",
    F.lit(" " * 3)
).withColumn(
    "MBR_PARTCPN_CD",
    F.when(
       (F.col("SUB_IN_AREA_IN") == 'Y') | (F.col("GRP_DP_IN") == 'Y') | (F.col("PROD_SH_NM") == 'BCARE'),
       F.lit('N')
    ).otherwise(
       F.when(F.col("MBR_PARTCPN_CD").isNull(), F.lit(" " * 1)).otherwise(F.col("MBR_PARTCPN_CD"))
    )
).withColumn(
    "ITS_SUB_ID",
    F.when(F.col("ITS_SUB_ID").isNull(), F.lit(" " * 17)).otherwise(F.col("ITS_SUB_ID"))
).withColumn(
    "MMI_ID",
    F.when(F.col("MMI_ID").isNull(), F.lit(" " * 22)).otherwise(F.col("MMI_ID"))
).withColumn(
    "HOST_PLN_CD",
    F.when(F.col("HOST_PLN_CD").isNull(), F.lit(" " * 3)).otherwise(F.col("HOST_PLN_CD"))
).withColumn(
    "PI_VOID_IN",
    F.when(F.col("PI_VOID_IN").isNull(), F.lit(" " * 1)).otherwise(F.col("PI_VOID_IN"))
).withColumn(
    "PI_MBR_CONFITY_CD",
    F.when(F.col("PI_MBR_CONFITY_CD").isNull(), F.lit(" " * 3)).otherwise(F.col("PI_MBR_CONFITY_CD"))
).withColumn(
    "EFF_DT",
    F.concat(
        F.substring(F.col("MBR_ENR_EFF_DT_SK"),1,4),
        F.substring(F.col("MBR_ENR_EFF_DT_SK"),6,2),
        F.substring(F.col("MBR_ENR_EFF_DT_SK"),9,2)
    )
).withColumn(
    "EXPRTN_DT",
    F.concat(
        F.substring(F.col("MaxMBR_ENR_TERM_DT_SK"),1,4),
        F.substring(F.col("MaxMBR_ENR_TERM_DT_SK"),6,2),
        F.substring(F.col("MaxMBR_ENR_TERM_DT_SK"),9,2)
    )
).withColumn(
    "OOA_MBR_CD",
    F.when(
        F.col("PROD_SH_NM") == 'BMADVP',
        F.when(F.col("SUB_IN_AREA_IN") == 'Y', F.lit('02')).otherwise(F.lit('03'))
    ).otherwise(
        F.when(F.col("GRP_DP_IN") == 'Y', F.lit('02')).otherwise(
            F.when((F.col("PROD_SH_NM") == 'BCARE') | (F.col("PROD_SH_NM") == 'BMADVH'), F.lit('02')).otherwise(
                F.when(F.col("SUB_IN_AREA_IN") == 'N', F.lit('01')).otherwise(F.lit('02'))
            )
        )
    )
)

df_Lnk_Lkp_In = df_Null_Check_tmp.select(
    F.col("MBR_SK"),
    F.col("BHI_HOME_PLN_ID"),
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_PFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINT"),
    F.col("MBR_SFX"),
    F.col("MBR_PRI_ST_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2"),
    F.col("MBR_PRI_CITY"),
    F.col("MBR_PRI_ST"),
    F.col("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("MBR_PRI_PHN_NO"),
    F.col("MBR_PRI_EMAIL"),
    F.col("MBR_SEC_ST_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2"),
    F.col("MBR_SEC_CITY"),
    F.col("MBR_SEC_ST"),
    F.col("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("HOST_PLN_OVRD"),
    F.col("MBR_PARTCPN_CD"),
    F.col("ITS_SUB_ID"),
    F.col("MMI_ID"),
    F.col("HOST_PLN_CD"),
    F.col("PI_VOID_IN"),
    F.col("PI_MBR_CONFITY_CD"),
    F.col("EFF_DT"),
    F.col("EXPRTN_DT"),
    F.col("OOA_MBR_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MCARE_BNFCRY_ID"),
    F.col("PROD_SH_NM")
)

df_Lkp_joined = (
    df_Lnk_Lkp_In.alias("Lnk_Lkp_In")
    .join(
        df_lnk_MCARE_CNTR_ID.alias("lnk_MCARE_CNTR_ID"),
        [
          F.col("Lnk_Lkp_In.MBR_SK") == F.col("lnk_MCARE_CNTR_ID.MBR_SK"),
          F.col("Lnk_Lkp_In.EFF_DT") == F.col("lnk_MCARE_CNTR_ID.MBR_ENR_EFF_DT_SK"),
          F.col("Lnk_Lkp_In.EXPRTN_DT") == F.col("lnk_MCARE_CNTR_ID.MBR_ENR_TERM_DT_SK")
        ],
        how="left"
    )
    .join(
        df_lnk_PLN_BNF_PCKG_ID.alias("lnk_PLN_BNF_PCKG_ID"),
        [
          F.col("Lnk_Lkp_In.MBR_SK") == F.col("lnk_PLN_BNF_PCKG_ID.MBR_SK"),
          F.col("Lnk_Lkp_In.EFF_DT") == F.col("lnk_PLN_BNF_PCKG_ID.MBR_ENR_EFF_DT_SK"),
          F.col("Lnk_Lkp_In.EXPRTN_DT") == F.col("lnk_PLN_BNF_PCKG_ID.MBR_ENR_TERM_DT_SK")
        ],
        how="left"
    )
)

df_Lnk_Lkp_Out = df_Lkp_joined.select(
    F.col("Lnk_Lkp_In.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_Lkp_In.BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("Lnk_Lkp_In.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Lnk_Lkp_In.MBR_PFX").alias("MBR_PFX"),
    F.col("Lnk_Lkp_In.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Lnk_Lkp_In.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Lnk_Lkp_In.MBR_MIDINT").alias("MBR_MIDINT"),
    F.col("Lnk_Lkp_In.MBR_SFX").alias("MBR_SFX"),
    F.col("Lnk_Lkp_In.MBR_PRI_ST_ADDR_1").alias("MBR_PRI_ST_ADDR_1"),
    F.col("Lnk_Lkp_In.MBR_PRI_ST_ADDR_2").alias("MBR_PRI_ST_ADDR_2"),
    F.col("Lnk_Lkp_In.MBR_PRI_CITY").alias("MBR_PRI_CITY"),
    F.col("Lnk_Lkp_In.MBR_PRI_ST").alias("MBR_PRI_ST"),
    F.col("Lnk_Lkp_In.MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("Lnk_Lkp_In.MBR_PRI_ZIP_CD_PLUS_4").alias("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("Lnk_Lkp_In.MBR_PRI_PHN_NO").alias("MBR_PRI_PHN_NO"),
    F.col("Lnk_Lkp_In.MBR_PRI_EMAIL").alias("MBR_PRI_EMAIL"),
    F.col("Lnk_Lkp_In.MBR_SEC_ST_ADDR_1").alias("MBR_SEC_ST_ADDR_1"),
    F.col("Lnk_Lkp_In.MBR_SEC_ST_ADDR_2").alias("MBR_SEC_ST_ADDR_2"),
    F.col("Lnk_Lkp_In.MBR_SEC_CITY").alias("MBR_SEC_CITY"),
    F.col("Lnk_Lkp_In.MBR_SEC_ST").alias("MBR_SEC_ST"),
    F.col("Lnk_Lkp_In.MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("Lnk_Lkp_In.MBR_SEC_ZIP_CD_PLUS_4").alias("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("Lnk_Lkp_In.HOST_PLN_OVRD").alias("HOST_PLN_OVRD"),
    F.col("Lnk_Lkp_In.MBR_PARTCPN_CD").alias("MBR_PARTCPN_CD"),
    F.col("Lnk_Lkp_In.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("Lnk_Lkp_In.MMI_ID").alias("MMI_ID"),
    F.col("Lnk_Lkp_In.HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("Lnk_Lkp_In.PI_VOID_IN").alias("PI_VOID_IN"),
    F.col("Lnk_Lkp_In.PI_MBR_CONFITY_CD").alias("PI_MBR_CONFITY_CD"),
    F.col("Lnk_Lkp_In.EFF_DT").alias("EFF_DT"),
    F.col("Lnk_Lkp_In.EXPRTN_DT").alias("EXPRTN_DT"),
    F.col("Lnk_Lkp_In.OOA_MBR_CD").alias("OOA_MBR_CD"),
    F.col("Lnk_Lkp_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_Lkp_In.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    F.col("lnk_MCARE_CNTR_ID.MBR_MCARE_EVT_TERM_DT_SK").alias("MBR_MCARE_EVT_TERM_DT_SK_CNTR_ID"),
    F.col("lnk_MCARE_CNTR_ID.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("lnk_PLN_BNF_PCKG_ID.MBR_MCARE_EVT_TERM_DT_SK").alias("MBR_MCARE_EVT_TERM_DT_SK_PKG_ID"),
    F.col("lnk_PLN_BNF_PCKG_ID.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_Lkp_In.PROD_SH_NM").alias("PROD_SH_NM")
)

df_Xfm_MCARE_CNTR_ID_tmp = df_Lnk_Lkp_Out.withColumn(
    "SvMCARECNTRID",
    F.when(
      F.col("MCARE_CNTR_ID").isNull(),
      F.lit(" " * 5)
    ).otherwise(
      F.when(
        (F.col("PROD_SH_NM") == 'BMADVP') &
        (F.col("MBR_MCARE_EVT_TERM_DT_SK_CNTR_ID") >= F.col("EXPRTN_DT")),
        trim(F.col("MCARE_CNTR_ID"))
      ).otherwise(F.lit(" " * 5))
    )
).withColumn(
    "SvPLNBNFPCKGID",
    F.when(
      F.col("PLN_BNF_PCKG_ID").isNull(),
      F.lit(" " * 3)
    ).otherwise(
      F.when(
        (F.col("PROD_SH_NM") == 'BMADVP') &
        (F.col("MBR_MCARE_EVT_TERM_DT_SK_PKG_ID") >= F.col("EXPRTN_DT")),
        trim(F.col("PLN_BNF_PCKG_ID"))
      ).otherwise(F.lit(" " * 3))
    )
)

df_final_all = df_Xfm_MCARE_CNTR_ID_tmp.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_PFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINT"),
    F.col("MBR_SFX"),
    F.col("MBR_PRI_ST_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2"),
    F.col("MBR_PRI_CITY"),
    F.col("MBR_PRI_ST"),
    F.col("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("MBR_PRI_PHN_NO"),
    F.col("MBR_PRI_EMAIL"),
    F.col("MBR_SEC_ST_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2"),
    F.col("MBR_SEC_CITY"),
    F.col("MBR_SEC_ST"),
    F.col("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("HOST_PLN_OVRD"),
    F.col("MBR_PARTCPN_CD"),
    F.col("ITS_SUB_ID"),
    F.col("MMI_ID"),
    F.col("HOST_PLN_CD"),
    F.col("PI_VOID_IN"),
    F.col("PI_MBR_CONFITY_CD"),
    F.col("EFF_DT"),
    F.col("EXPRTN_DT"),
    F.col("OOA_MBR_CD"),
    F.col("MCARE_BNFCRY_ID"),
    F.when(
        F.col("EXPRTN_DT") < F.lit("20200101"), 
        F.lit(" " * 8)
    ).otherwise(
       F.when(
         F.col("EXPRTN_DT") >= F.lit("20200101"), 
         F.col("SvMCARECNTRID") + F.col("SvPLNBNFPCKGID")
       ).otherwise(F.lit(" " * 8))
    ).alias("MCARE_CNTR_ID"),
    F.col("MBR_UNIQ_KEY")
)

df_error = df_Xfm_MCARE_CNTR_ID_tmp.filter(
    F.col("PROD_SH_NM") == 'BMADVP'
).select(
    F.col("CONSIS_MBR_ID"),
    F.col("EFF_DT"),
    F.col("EXPRTN_DT"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PROD_SH_NM"),
    F.col("MCARE_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID"),
    F.col("SvMCARECNTRID").alias("svMCARE"),
    F.col("SvPLNBNFPCKGID").alias("svPLNBNF")
)

df_final_ndw = df_Xfm_MCARE_CNTR_ID_tmp.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("CONSIS_MBR_ID"),
    F.col("MBR_PFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINT"),
    F.col("MBR_SFX"),
    F.col("MBR_PRI_ST_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2"),
    F.col("MBR_PRI_CITY"),
    F.col("MBR_PRI_ST"),
    F.col("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("MBR_PRI_PHN_NO"),
    F.col("MBR_PRI_EMAIL"),
    F.col("MBR_SEC_ST_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2"),
    F.col("MBR_SEC_CITY"),
    F.col("MBR_SEC_ST"),
    F.col("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("HOST_PLN_OVRD"),
    F.col("MBR_PARTCPN_CD"),
    F.lit(" " * 17).alias("ITS_SUB_ID"),
    F.col("MMI_ID"),
    F.col("HOST_PLN_CD"),
    F.col("PI_VOID_IN"),
    F.col("PI_MBR_CONFITY_CD"),
    F.col("EFF_DT"),
    F.col("EXPRTN_DT"),
    F.col("OOA_MBR_CD"),
    F.lit(" " * 11).alias("MCARE_BNFCRY_ID"),
    F.lit(" " * 8).alias("MCARE_CNTR_ID"),
    F.col("MBR_UNIQ_KEY")
)

df_std_member_demographic = df_final_all.select(
    F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad(F.col("CONSIS_MBR_ID"), 22, " ").alias("CONSIS_MBR_ID"),
    F.rpad(F.col("MBR_PFX"), 20, " ").alias("MBR_PFX"),
    F.rpad(F.col("MBR_LAST_NM"), 150, " ").alias("MBR_LAST_NM"),
    F.rpad(F.col("MBR_FIRST_NM"), 70, " ").alias("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINT"), 2, " ").alias("MBR_MIDINT"),
    F.rpad(F.col("MBR_SFX"), 20, " ").alias("MBR_SFX"),
    F.rpad(F.col("MBR_PRI_ST_ADDR_1"), 70, " ").alias("MBR_PRI_ST_ADDR_1"),
    F.rpad(F.col("MBR_PRI_ST_ADDR_2"), 70, " ").alias("MBR_PRI_ST_ADDR_2"),
    F.rpad(F.col("MBR_PRI_CITY"), 35, " ").alias("MBR_PRI_CITY"),
    F.rpad(F.col("MBR_PRI_ST"), 2, " ").alias("MBR_PRI_ST"),
    F.rpad(F.col("MBR_PRI_ZIP_CD"), 5, " ").alias("MBR_PRI_ZIP_CD"),
    F.rpad(F.col("MBR_PRI_ZIP_CD_PLUS_4"), 4, " ").alias("MBR_PRI_ZIP_CD_PLUS_4"),
    F.rpad(F.col("MBR_PRI_PHN_NO"), 10, " ").alias("MBR_PRI_PHN_NO"),
    F.rpad(F.col("MBR_PRI_EMAIL"), 70, " ").alias("MBR_PRI_EMAIL"),
    F.rpad(F.col("MBR_SEC_ST_ADDR_1"), 70, " ").alias("MBR_SEC_ST_ADDR_1"),
    F.rpad(F.col("MBR_SEC_ST_ADDR_2"), 70, " ").alias("MBR_SEC_ST_ADDR_2"),
    F.rpad(F.col("MBR_SEC_CITY"), 35, " ").alias("MBR_SEC_CITY"),
    F.rpad(F.col("MBR_SEC_ST"), 2, " ").alias("MBR_SEC_ST"),
    F.rpad(F.col("MBR_SEC_ZIP_CD"), 5, " ").alias("MBR_SEC_ZIP_CD"),
    F.rpad(F.col("MBR_SEC_ZIP_CD_PLUS_4"), 4, " ").alias("MBR_SEC_ZIP_CD_PLUS_4"),
    F.rpad(F.col("HOST_PLN_OVRD"), 3, " ").alias("HOST_PLN_OVRD"),
    F.rpad(F.col("MBR_PARTCPN_CD"), 1, " ").alias("MBR_PARTCPN_CD"),
    F.rpad(F.col("ITS_SUB_ID"), 17, " ").alias("ITS_SUB_ID"),
    F.rpad(F.col("MMI_ID"), 22, " ").alias("MMI_ID"),
    F.rpad(F.col("HOST_PLN_CD"), 3, " ").alias("HOST_PLN_CD"),
    F.rpad(F.col("PI_VOID_IN"), 1, " ").alias("PI_VOID_IN"),
    F.rpad(F.col("PI_MBR_CONFITY_CD"), 3, " ").alias("PI_MBR_CONFITY_CD"),
    F.rpad(F.col("EFF_DT"), 8, " ").alias("EFF_DT"),
    F.rpad(F.col("EXPRTN_DT"), 8, " ").alias("EXPRTN_DT"),
    F.rpad(F.col("OOA_MBR_CD"), 2, " ").alias("OOA_MBR_CD"),
    F.rpad(F.col("MCARE_BNFCRY_ID"), 11, " ").alias("MCARE_BNFCRY_ID"),
    F.rpad(F.col("MCARE_CNTR_ID"), 8, " ").alias("MCARE_CNTR_ID"),
    F.col("MBR_UNIQ_KEY")
)

df_std_member_demographic_error = df_error.select(
    F.rpad(F.col("CONSIS_MBR_ID"), 22, " ").alias("CONSIS_MBR_ID"),
    F.rpad(F.col("EFF_DT"), 8, " ").alias("EFF_DT"),
    F.rpad(F.col("EXPRTN_DT"), 8, " ").alias("EXPRTN_DT"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PROD_SH_NM"),
    F.col("MCARE_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID"),
    F.col("svMCARE"),
    F.col("svPLNBNF")
)

df_std_member_demographic_ndw = df_final_ndw.select(
    F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad(F.col("CONSIS_MBR_ID"), 22, " ").alias("CONSIS_MBR_ID"),
    F.rpad(F.col("MBR_PFX"), 20, " ").alias("MBR_PFX"),
    F.rpad(F.col("MBR_LAST_NM"), 150, " ").alias("MBR_LAST_NM"),
    F.rpad(F.col("MBR_FIRST_NM"), 70, " ").alias("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINT"), 2, " ").alias("MBR_MIDINT"),
    F.rpad(F.col("MBR_SFX"), 20, " ").alias("MBR_SFX"),
    F.rpad(F.col("MBR_PRI_ST_ADDR_1"), 70, " ").alias("MBR_PRI_ST_ADDR_1"),
    F.rpad(F.col("MBR_PRI_ST_ADDR_2"), 70, " ").alias("MBR_PRI_ST_ADDR_2"),
    F.rpad(F.col("MBR_PRI_CITY"), 35, " ").alias("MBR_PRI_CITY"),
    F.rpad(F.col("MBR_PRI_ST"), 2, " ").alias("MBR_PRI_ST"),
    F.rpad(F.col("MBR_PRI_ZIP_CD"), 5, " ").alias("MBR_PRI_ZIP_CD"),
    F.rpad(F.col("MBR_PRI_ZIP_CD_PLUS_4"), 4, " ").alias("MBR_PRI_ZIP_CD_PLUS_4"),
    F.rpad(F.col("MBR_PRI_PHN_NO"), 10, " ").alias("MBR_PRI_PHN_NO"),
    F.rpad(F.col("MBR_PRI_EMAIL"), 70, " ").alias("MBR_PRI_EMAIL"),
    F.rpad(F.col("MBR_SEC_ST_ADDR_1"), 70, " ").alias("MBR_SEC_ST_ADDR_1"),
    F.rpad(F.col("MBR_SEC_ST_ADDR_2"), 70, " ").alias("MBR_SEC_ST_ADDR_2"),
    F.rpad(F.col("MBR_SEC_CITY"), 35, " ").alias("MBR_SEC_CITY"),
    F.rpad(F.col("MBR_SEC_ST"), 2, " ").alias("MBR_SEC_ST"),
    F.rpad(F.col("MBR_SEC_ZIP_CD"), 5, " ").alias("MBR_SEC_ZIP_CD"),
    F.rpad(F.col("MBR_SEC_ZIP_CD_PLUS_4"), 4, " ").alias("MBR_SEC_ZIP_CD_PLUS_4"),
    F.rpad(F.col("HOST_PLN_OVRD"), 3, " ").alias("HOST_PLN_OVRD"),
    F.rpad(F.col("MBR_PARTCPN_CD"), 1, " ").alias("MBR_PARTCPN_CD"),
    F.col("ITS_SUB_ID"),
    F.rpad(F.col("MMI_ID"), 22, " ").alias("MMI_ID"),
    F.rpad(F.col("HOST_PLN_CD"), 3, " ").alias("HOST_PLN_CD"),
    F.rpad(F.col("PI_VOID_IN"), 1, " ").alias("PI_VOID_IN"),
    F.rpad(F.col("PI_MBR_CONFITY_CD"), 3, " ").alias("PI_MBR_CONFITY_CD"),
    F.rpad(F.col("EFF_DT"), 8, " ").alias("EFF_DT"),
    F.rpad(F.col("EXPRTN_DT"), 8, " ").alias("EXPRTN_DT"),
    F.rpad(F.col("OOA_MBR_CD"), 2, " ").alias("OOA_MBR_CD"),
    F.col("MCARE_BNFCRY_ID"),
    F.col("MCARE_CNTR_ID"),
    F.col("MBR_UNIQ_KEY")
)

write_files(
    df_std_member_demographic,
    f"{adls_path_publish}/external/std_member_demographic",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_std_member_demographic_error,
    f"{adls_path_publish}/external/std_member_demographic_error",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_std_member_demographic_ndw,
    f"{adls_path_publish}/external/std_member_demographic_ndw_25.5",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)