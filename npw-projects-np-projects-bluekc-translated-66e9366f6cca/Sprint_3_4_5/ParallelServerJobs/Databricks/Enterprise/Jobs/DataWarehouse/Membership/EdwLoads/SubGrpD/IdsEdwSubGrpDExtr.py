# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwMbrNoDriverSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Tom Harrocks     08/02/2004                   Originally Programmed
# MAGIC Hugh Sisson       08/10/2005                   Membership Phase
# MAGIC SAndrew             11/18/2005  1460         Added Rate ID and Risk ID to the EDW SUBGRP_D table.
# MAGIC Suzanne Saylor  04/12/2006                    changed > #BeginCycle# to >=, added RunCycle parameter, 
# MAGIC                                                                   set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC Sharon Andrew   07/06/2006                   Renamed from Edw***Extr to Ids***Extr.  
# MAGIC Ralph Tucker      11/20/2008  3648         Added new field (TAX_ID_NO)                                                                     
# MAGIC Hugh Sisson        2010-08-12   3346         Added  CUR_ANNV_DT_SK, NEXT_ANNV_DT_SK to end of table         Steph Goddard    08/29/2010
# MAGIC Hugh Sisson       2010-12-27    3346         Corrected discrepancies between CDMA and job                                       Steph Goddard    01/18/2011
# MAGIC Pooja Sunkara    07/05/2013   5114        Converted job from server to parallel version. Renamed CurrentDate          Peter Marshall      9/4/2013 
# MAGIC                                                                   to EDWRunCycleDate and  Renamed job to IdsEdwSubGrpDExtr
# MAGIC Santosh Bokka   06-19-2014    TFS-1196  Updated SQL for RATE_CLS_IS and RISK_CLS_ID from
# MAGIC                                                                    db2_SUBGRP_RATE_in  to   lkp_CntySt                                                    Bhoomi Dasari     6/21/2014
# MAGIC Sunitha P         2021-08-12      US 417034    Added new field  SUBGRP_ELIG_PEND_UNTIL_PD_IN                         Goutham K                  8/16/2021

# MAGIC Transform business logic
# MAGIC Extracts all subgroups from IDS source table SUBGRP. (Full Extract)
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SKand built CNTYST
# MAGIC Write SUBGRP_D Data into a Sequential file for Load Ready Job.
# MAGIC Extracts all subgroups from IDS reference table GRP. (Full Extract)
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_SK
# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK
# MAGIC SUBGRP_TERM_RSN_CD_SK
# MAGIC SUBGRP_STTUS_CD_SK
# MAGIC SUBGRP_ST_CD_SK
# MAGIC SUBGRP_TYP_CD_SK
# MAGIC Job name:
# MAGIC IdsEdwSubGrpDExtr
# MAGIC Extracts data from IDS reference tables CD_MPPNG and SUBGRP_RATE
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SUBGRP_SK
# MAGIC CNTY_ST
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
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWCurrDate = get_widget_value('EDWCurrDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CD_MPPNG2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT COALESCE(SRC_DRVD_LKUP_VAL,'UNK') SRC_DRVD_LKUP_VAL, COALESCE(TRGT_CD,'NA') TRGT_CD FROM {IDSOwner}.CD_MPPNG MPPNG WHERE MPPNG.TRGT_DOMAIN_NM='PROVIDER ADDRESS METRO RURAL COVERAGE'"
    )
    .load()
)

df_db2_SUBGRP_RATE_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""Select Distinct
R.SUBGRP_SK,
Max(R.EFF_DT_SK) over (PARTITION BY R.SUBGRP_SK) as EFF_DT_SK,
R.MAX_TERM_DT_SK as TERM_DT_SK,
RA.RATE_CLS_ID,
RA.RISK_CLS_ID
From (
 SELECT DISTINCT
 RATE.SUBGRP_SK,
 RATE.EFF_DT_SK,
 MAX(RATE.EFF_DT_SK) over (PARTITION BY RATE.SUBGRP_SK) as MAX_EFF_DT_SK,
 MAX(RATE.TERM_DT_SK) over (PARTITION BY RATE.SUBGRP_SK) as MAX_TERM_DT_SK,
 RATE.TERM_DT_SK,
 RATE.RATE_CLS_ID,
 RATE.RISK_CLS_ID
 FROM 
  {IDSOwner}.SUBGRP_RATE RATE
 where
 RATE.EFF_DT_SK <= '{EDWCurrDate}'
 and RATE.TERM_DT_SK >= '{EDWCurrDate}'
)  R , {IDSOwner}.SUBGRP_RATE RA
where R.SUBGRP_SK = RA.SUBGRP_SK
and R.MAX_EFF_DT_SK = RA.EFF_DT_SK
and R.MAX_TERM_DT_SK = RA.TERM_DT_SK
and R.EFF_DT_SK <= '{EDWCurrDate}'
and MAX_TERM_DT_SK >= '{EDWCurrDate}'"""
    )
    .load()
)

df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT GRP.GRP_SK, GRP.GRP_ID, GRP.GRP_NM FROM {IDSOwner}.GRP GRP"
    )
    .load()
)

df_db2_SUB_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
SUBGRP.SUBGRP_SK,
SUBGRP.SRC_SYS_CD_SK,
SUBGRP.GRP_ID,
SUBGRP.SUBGRP_ID,
SUBGRP.CRT_RUN_CYC_EXCTN_SK,
SUBGRP.LAST_UPDT_RUN_CYC_EXCTN_SK,
SUBGRP.GRP_SK,
SUBGRP.SUBGRP_UNIQ_KEY,
SUBGRP.SUBGRP_STTUS_CD_SK,
SUBGRP.SUBGRP_TERM_RSN_CD_SK,
SUBGRP.SUBGRP_TYP_CD_SK,
SUBGRP.ORIG_EFF_DT_SK,
SUBGRP.REINST_DT_SK,
SUBGRP.TERM_DT_SK,
SUBGRP.CNTCT_LAST_NM,
SUBGRP.CNTCT_FIRST_NM,
SUBGRP.CNTCT_MIDINIT,
SUBGRP.CNTCT_TTL,
SUBGRP.SUBGRP_NM,
SUBGRP.ADDR_LN_1,
SUBGRP.ADDR_LN_2,
SUBGRP.ADDR_LN_3,
SUBGRP.CITY_NM,
SUBGRP.SUBGRP_ST_CD_SK,
SUBGRP.POSTAL_CD,
SUBGRP.CNTY_NM,
SUBGRP.SUBGRP_CTRY_CD_SK,
SUBGRP.PHN_NO,
SUBGRP.PHN_NO_EXT,
SUBGRP.FAX_NO,
SUBGRP.FAX_NO_EXT,
SUBGRP.EMAIL_ADDR_TX,
SUBGRP.TAX_ID_NO,
SUBGRP.SRC_SYS_LAST_UPDT_DT_SK,
SUBGRP.SRC_SYS_LAST_UPDT_USER_SK,
SUBGRP.CUR_ANNV_DT_SK,
SUBGRP.NEXT_ANNV_DT_SK,
SUBGRP.SUBGRP_ELIG_PEND_UNTIL_PD_IN
FROM {IDSOwner}.SUBGRP SUBGRP"""
    )
    .load()
)

df_db2_CD_MPPNG1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG"""
    )
    .load()
)

df_lkp_GrpSK = (
    df_db2_SUB_GRP_in.alias("Ink_IdsEdwSubGrpDExtr_InABC")
    .join(
        df_db2_GRP_in.alias("ref_GrpSkLkup"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.GRP_SK") == F.col("ref_GrpSkLkup.GRP_SK"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.GRP_ID").alias("GRP_ID"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_STTUS_CD_SK").alias("SUBGRP_STTUS_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_TERM_RSN_CD_SK").alias("SUBGRP_TERM_RSN_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_TYP_CD_SK").alias("SUBGRP_TYP_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.REINST_DT_SK").alias("REINST_DT_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CNTCT_TTL").alias("CNTCT_TTL"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_NM").alias("SUBGRP_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CITY_NM").alias("CITY_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_ST_CD_SK").alias("SUBGRP_ST_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.POSTAL_CD").alias("POSTAL_CD"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CNTY_NM").alias("CNTY_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_CTRY_CD_SK").alias("SUBGRP_CTRY_CD_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.PHN_NO").alias("PHN_NO"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.FAX_NO").alias("FAX_NO"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.TAX_ID_NO").alias("TAX_ID_NO"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.CUR_ANNV_DT_SK").alias("CUR_ANNV_DT_SK"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.NEXT_ANNV_DT_SK").alias("NEXT_ANNV_DT_SK"),
        F.col("ref_GrpSkLkup.GRP_NM").alias("GRP_NM"),
        F.col("Ink_IdsEdwSubGrpDExtr_InABC.SUBGRP_ELIG_PEND_UNTIL_PD_IN").alias("SUBGRP_ELIG_PEND_UNTIL_PD_IN")
    )
)

df_copy_cdMppng = df_db2_CD_MPPNG1_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_StateCode = df_copy_cdMppng.alias("ref_StateCode")
df_ref_SubgrpTermRsn = df_copy_cdMppng.alias("ref_SubgrpTermRsn")
df_ref_SubgrpStatus = df_copy_cdMppng.alias("ref_SubgrpStatus")
df_ref_SrcSys = df_copy_cdMppng.alias("ref_SrcSys")
df_ref_State = df_copy_cdMppng.alias("ref_State")
df_ref_SubgrpType = df_copy_cdMppng.alias("ref_SubgrpType")

df_lkp_Codes = (
    df_lkp_GrpSK.alias("lnk_GrpSKLkpData_out")
    .join(
        df_ref_StateCode,
        F.col("lnk_GrpSKLkpData_out.SUBGRP_ST_CD_SK") == F.col("ref_StateCode.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_SubgrpTermRsn,
        F.col("lnk_GrpSKLkpData_out.SUBGRP_TERM_RSN_CD_SK") == F.col("ref_SubgrpTermRsn.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_SubgrpStatus,
        F.col("lnk_GrpSKLkpData_out.SUBGRP_STTUS_CD_SK") == F.col("ref_SubgrpStatus.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_SrcSys,
        F.col("lnk_GrpSKLkpData_out.SRC_SYS_CD_SK") == F.col("ref_SrcSys.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_State,
        F.col("lnk_GrpSKLkpData_out.SUBGRP_ST_CD_SK") == F.col("ref_State.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_SubgrpType,
        F.col("lnk_GrpSKLkpData_out.SUBGRP_TYP_CD_SK") == F.col("ref_SubgrpType.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_GrpSKLkpData_out.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("ref_SrcSys.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_GrpSKLkpData_out.GRP_ID").alias("GRP_ID"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("lnk_GrpSKLkpData_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_GrpSKLkpData_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_GrpSKLkpData_out.GRP_SK").alias("GRP_SK"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_STTUS_CD_SK").alias("SUBGRP_STTUS_CD_SK"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_TERM_RSN_CD_SK").alias("SUBGRP_TERM_RSN_CD_SK"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_TYP_CD_SK").alias("SUBGRP_TYP_CD_SK"),
        F.col("lnk_GrpSKLkpData_out.ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.REINST_DT_SK").alias("REINST_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
        F.col("lnk_GrpSKLkpData_out.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
        F.col("lnk_GrpSKLkpData_out.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
        F.col("lnk_GrpSKLkpData_out.CNTCT_TTL").alias("CNTCT_TTL"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_NM").alias("SUBGRP_NM"),
        F.col("lnk_GrpSKLkpData_out.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lnk_GrpSKLkpData_out.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lnk_GrpSKLkpData_out.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_ST_CD_SK").alias("SUBGRP_ST_CD_SK"),
        F.col("lnk_GrpSKLkpData_out.CITY_NM").alias("CITY_NM"),
        F.col("lnk_GrpSKLkpData_out.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lnk_GrpSKLkpData_out.CNTY_NM").alias("CNTY_NM"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_CTRY_CD_SK").alias("SUBGRP_CTRY_CD_SK"),
        F.col("lnk_GrpSKLkpData_out.PHN_NO").alias("PHN_NO"),
        F.col("lnk_GrpSKLkpData_out.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("lnk_GrpSKLkpData_out.FAX_NO").alias("FAX_NO"),
        F.col("lnk_GrpSKLkpData_out.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("lnk_GrpSKLkpData_out.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("lnk_GrpSKLkpData_out.TAX_ID_NO").alias("TAX_ID_NO"),
        F.col("lnk_GrpSKLkpData_out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_GrpSKLkpData_out.CUR_ANNV_DT_SK").alias("CUR_ANNV_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.NEXT_ANNV_DT_SK").alias("NEXT_ANNV_DT_SK"),
        F.col("lnk_GrpSKLkpData_out.GRP_NM").alias("GRP_NM"),
        F.col("lnk_GrpSKLkpData_out.SUBGRP_ELIG_PEND_UNTIL_PD_IN").alias("SUBGRP_ELIG_PEND_UNTIL_PD_IN"),
        F.col("ref_StateCode.TRGT_CD").alias("TRGT_CD"),
        F.col("ref_State.TRGT_CD").alias("SUBGRP_ST_CD"),
        F.col("ref_State.TRGT_CD_NM").alias("SUBGRP_ST_NM"),
        F.col("ref_SubgrpStatus.TRGT_CD").alias("SUBGRP_STTUS_CD"),
        F.col("ref_SubgrpStatus.TRGT_CD_NM").alias("SUBGRP_STTUS_NM"),
        F.col("ref_SubgrpTermRsn.TRGT_CD").alias("SUBGRP_TERM_RSN_CD"),
        F.col("ref_SubgrpTermRsn.TRGT_CD_NM").alias("SUBGRP_TERM_RSN_NM"),
        F.col("ref_SubgrpType.TRGT_CD").alias("SUBGRP_TYP_CD"),
        F.col("ref_SubgrpType.TRGT_CD_NM").alias("SUBGRP_TYP_NM"),
    )
)

df_xfrm_businessLogic1 = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.col("SRC_SYS_CD").isNull() | (trim(F.col("SRC_SYS_CD")) == ""), F.lit("UNK"))
         .otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("GRP_NM",
        F.when(F.col("GRP_NM").isNull() | (trim(F.col("GRP_NM")) == ""), F.lit("UNK"))
         .otherwise(F.col("GRP_NM"))
    )
    .withColumn("SUBGRP_ST_CD",
        F.when(F.col("SUBGRP_ST_CD").isNull() | (trim(F.col("SUBGRP_ST_CD")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_ST_CD"))
    )
    .withColumn("SUBGRP_ST_NM",
        F.when(F.col("SUBGRP_ST_NM").isNull() | (trim(F.col("SUBGRP_ST_NM")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_ST_NM"))
    )
    .withColumn("SUBGRP_STTUS_CD",
        F.when(F.col("SUBGRP_STTUS_CD").isNull() | (trim(F.col("SUBGRP_STTUS_CD")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_STTUS_CD"))
    )
    .withColumn("SUBGRP_STTUS_NM",
        F.when(F.col("SUBGRP_STTUS_NM").isNull() | (trim(F.col("SUBGRP_STTUS_NM")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_STTUS_NM"))
    )
    .withColumn("SUBGRP_TERM_RSN_CD",
        F.when(F.col("SUBGRP_TERM_RSN_CD").isNull() | (trim(F.col("SUBGRP_TERM_RSN_CD")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_TERM_RSN_CD"))
    )
    .withColumn("SUBGRP_TERM_RSN_NM",
        F.when(F.col("SUBGRP_TERM_RSN_NM").isNull() | (trim(F.col("SUBGRP_TERM_RSN_NM")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_TERM_RSN_NM"))
    )
    .withColumn("SUBGRP_TYP_CD",
        F.when(F.col("SUBGRP_TYP_CD").isNull() | (trim(F.col("SUBGRP_TYP_CD")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_TYP_CD"))
    )
    .withColumn("SUBGRP_TYP_NM",
        F.when(F.col("SUBGRP_TYP_NM").isNull() | (trim(F.col("SUBGRP_TYP_NM")) == ""), F.lit("UNK"))
         .otherwise(F.col("SUBGRP_TYP_NM"))
    )
    .withColumn(
        "POSTAL_CD",
        F.when(
            F.col("POSTAL_CD").isNull() | (trim(F.col("POSTAL_CD")) == ""),
            F.lit("")
        ).otherwise(
            FORMAT.POSTALCD.EE(F.col("POSTAL_CD"))  # Assume FORMAT.POSTALCD.EE is already defined
        )
    )
    .withColumn(
        "CountyState",
        UpCase(
            trim(
                F.when(F.col("CNTY_NM").isNull(), F.lit(" ")).otherwise(F.col("CNTY_NM")))
        )
        + F.lit(":")
        + trim(
            F.when(F.col("TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("TRGT_CD")))
    )
    .select(
        F.col("SUBGRP_SK"),
        F.col("SRC_SYS_CD"),
        F.col("GRP_ID"),
        F.col("SUBGRP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_SK"),
        F.col("SUBGRP_UNIQ_KEY"),
        F.col("SUBGRP_STTUS_CD_SK"),
        F.col("SUBGRP_TERM_RSN_CD_SK"),
        F.col("SUBGRP_TYP_CD_SK"),
        F.col("ORIG_EFF_DT_SK"),
        F.col("REINST_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("CNTCT_LAST_NM"),
        F.col("CNTCT_FIRST_NM"),
        F.col("CNTCT_MIDINIT"),
        F.col("CNTCT_TTL"),
        F.col("SUBGRP_NM"),
        F.col("ADDR_LN_1"),
        F.col("ADDR_LN_2"),
        F.col("ADDR_LN_3"),
        F.col("CITY_NM"),
        F.col("SUBGRP_ST_CD_SK"),
        F.col("POSTAL_CD"),
        F.col("CNTY_NM"),
        F.col("SUBGRP_CTRY_CD_SK"),
        F.col("PHN_NO"),
        F.col("PHN_NO_EXT"),
        F.col("FAX_NO"),
        F.col("FAX_NO_EXT"),
        F.col("EMAIL_ADDR_TX"),
        F.col("TAX_ID_NO"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("CUR_ANNV_DT_SK"),
        F.col("NEXT_ANNV_DT_SK"),
        F.col("GRP_NM"),
        F.col("SUBGRP_ELIG_PEND_UNTIL_PD_IN"),
        F.col("SUBGRP_ST_CD"),
        F.col("SUBGRP_ST_NM"),
        F.col("SUBGRP_STTUS_CD"),
        F.col("SUBGRP_STTUS_NM"),
        F.col("SUBGRP_TERM_RSN_CD"),
        F.col("SUBGRP_TERM_RSN_NM"),
        F.col("SUBGRP_TYP_CD"),
        F.col("SUBGRP_TYP_NM"),
        F.col("CountyState").alias("CNTY_ST")
    )
)

df_lkp_CntySt = (
    df_xfrm_businessLogic1.alias("lnk_CntyStLkpData_in")
    .join(
        df_db2_SUBGRP_RATE_in.alias("ref_idsRateRisk"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_SK") == F.col("ref_idsRateRisk.SUBGRP_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG2_in.alias("ref_MetroRural"),
        F.col("lnk_CntyStLkpData_in.CNTY_ST") == F.col("ref_MetroRural.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .select(
        F.col("lnk_CntyStLkpData_in.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("lnk_CntyStLkpData_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_CntyStLkpData_in.GRP_ID").alias("GRP_ID"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("lnk_CntyStLkpData_in.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CntyStLkpData_in.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CntyStLkpData_in.GRP_SK").alias("GRP_SK"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_STTUS_CD_SK").alias("SUBGRP_STTUS_CD_SK"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TERM_RSN_CD_SK").alias("SUBGRP_TERM_RSN_CD_SK"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TYP_CD_SK").alias("SUBGRP_TYP_CD_SK"),
        F.col("lnk_CntyStLkpData_in.ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
        F.col("lnk_CntyStLkpData_in.REINST_DT_SK").alias("REINST_DT_SK"),
        F.col("lnk_CntyStLkpData_in.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_CntyStLkpData_in.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
        F.col("lnk_CntyStLkpData_in.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
        F.col("lnk_CntyStLkpData_in.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
        F.col("lnk_CntyStLkpData_in.CNTCT_TTL").alias("CNTCT_TTL"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_NM").alias("SUBGRP_NM"),
        F.col("lnk_CntyStLkpData_in.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lnk_CntyStLkpData_in.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lnk_CntyStLkpData_in.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lnk_CntyStLkpData_in.CITY_NM").alias("CITY_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_ST_CD_SK").alias("SUBGRP_ST_CD_SK"),
        F.col("lnk_CntyStLkpData_in.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lnk_CntyStLkpData_in.CNTY_NM").alias("CNTY_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_CTRY_CD_SK").alias("SUBGRP_CTRY_CD_SK"),
        F.col("lnk_CntyStLkpData_in.PHN_NO").alias("PHN_NO"),
        F.col("lnk_CntyStLkpData_in.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("lnk_CntyStLkpData_in.FAX_NO").alias("FAX_NO"),
        F.col("lnk_CntyStLkpData_in.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("lnk_CntyStLkpData_in.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("lnk_CntyStLkpData_in.TAX_ID_NO").alias("TAX_ID_NO"),
        F.col("lnk_CntyStLkpData_in.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_CntyStLkpData_in.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_CntyStLkpData_in.CUR_ANNV_DT_SK").alias("CUR_ANNV_DT_SK"),
        F.col("lnk_CntyStLkpData_in.NEXT_ANNV_DT_SK").alias("NEXT_ANNV_DT_SK"),
        F.col("lnk_CntyStLkpData_in.GRP_NM").alias("GRP_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_ELIG_PEND_UNTIL_PD_IN").alias("SUBGRP_ELIG_PEND_UNTIL_PD_IN"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_ST_CD").alias("SUBGRP_ST_CD"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_ST_NM").alias("SUBGRP_ST_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_STTUS_CD").alias("SUBGRP_STTUS_CD"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_STTUS_NM").alias("SUBGRP_STTUS_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TERM_RSN_CD").alias("SUBGRP_TERM_RSN_CD"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TERM_RSN_NM").alias("SUBGRP_TERM_RSN_NM"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TYP_CD").alias("SUBGRP_TYP_CD"),
        F.col("lnk_CntyStLkpData_in.SUBGRP_TYP_NM").alias("SUBGRP_TYP_NM"),
        F.col("lnk_CntyStLkpData_in.CNTY_ST").alias("CNTY_ST"),
        F.col("ref_MetroRural.TRGT_CD").alias("SUBGRP_METRO_RURAL_CD"),
        F.col("ref_idsRateRisk.RATE_CLS_ID").alias("SUBGRP_RATE_CLS_ID"),
        F.col("ref_idsRateRisk.RISK_CLS_ID").alias("SUBGRP_RISK_CLS_ID")
    )
)

df_xfrm_businessLogic2 = df_lkp_CntySt.select(
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.lit(EDWCurrDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWCurrDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("CNTCT_FIRST_NM").alias("SUBGRP_CNTCT_FIRST_NM"),
    F.col("CNTCT_MIDINIT").alias("SUBGRP_CNTCT_MIDINIT"),
    F.col("CNTCT_LAST_NM").alias("SUBGRP_CNTCT_LAST_NM"),
    F.col("CNTCT_TTL").alias("SUBGRP_CNTCT_TTL"),
    F.col("SUBGRP_NM").alias("SUBGRP_NM"),
    F.col("ADDR_LN_1").alias("SUBGRP_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("SUBGRP_ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("SUBGRP_ADDR_LN_3"),
    F.col("CITY_NM").alias("SUBGRP_CITY_NM"),
    F.col("SUBGRP_ST_CD").alias("SUBGRP_ST_CD"),
    F.col("SUBGRP_ST_NM").alias("SUBGRP_ST_NM"),
    F.expr(
        """CASE WHEN SUBGRP_ST_CD = 'UNK' OR SUBGRP_ST_CD = 'NA' OR SUBGRP_ST_CD IS NULL 
             THEN NULL 
             ELSE CASE WHEN length(trim(POSTAL_CD)) > 0 THEN substring(POSTAL_CD,1,5) ELSE NULL END END"""
    ).alias("SUBGRP_ZIP_CD_5"),
    F.expr(
        """CASE WHEN SUBGRP_ST_CD = 'UNK' OR SUBGRP_ST_CD = 'NA' OR SUBGRP_ST_CD IS NULL 
             THEN NULL 
             ELSE CASE WHEN length(trim(POSTAL_CD)) = 9 THEN substring(POSTAL_CD,6,4) ELSE NULL END END"""
    ).alias("SUBGRP_ZIP_CD_4"),
    F.col("CNTY_NM").alias("SUBGRP_CNTY_NM"),
    F.col("PHN_NO").alias("SUBGRP_PHN_NO"),
    F.col("PHN_NO_EXT").alias("SUBGRP_PHN_NO_EXT"),
    F.col("FAX_NO").alias("SUBGRP_FAX_NO"),
    F.col("FAX_NO_EXT").alias("SUBGRP_FAX_NO_EXT"),
    trim(F.col("EMAIL_ADDR_TX"), '^', 'A').alias("SUBGRP_EMAIL_ADDR_TX"),
    F.when(F.length(F.col("SUBGRP_METRO_RURAL_CD")) == 5, F.col("SUBGRP_METRO_RURAL_CD")).otherwise(F.lit("NA")).alias("SUBGRP_METRO_RURAL_CD"),
    F.col("ORIG_EFF_DT_SK").alias("SUBGRP_ORIG_EFF_DT_SK"),
    F.when(F.trim(F.col("SUBGRP_RATE_CLS_ID")) == "", F.lit("UNK")).otherwise(F.col("SUBGRP_RATE_CLS_ID")).alias("SUBGRP_RATE_CLS_ID"),
    F.col("REINST_DT_SK").alias("SUBGRP_REINST_DT_SK"),
    F.when(F.trim(F.col("SUBGRP_RISK_CLS_ID")) == "", F.lit("UNK")).otherwise(F.col("SUBGRP_RISK_CLS_ID")).alias("SUBGRP_RISK_CLS_ID"),
    F.col("SUBGRP_STTUS_CD").alias("SUBGRP_STTUS_CD"),
    F.col("SUBGRP_STTUS_NM").alias("SUBGRP_STTUS_NM"),
    F.col("TAX_ID_NO").alias("SUBGRP_TAX_ID_NO"),
    F.col("TERM_DT_SK").alias("SUBGRP_TERM_DT_SK"),
    F.col("SUBGRP_TERM_RSN_CD").alias("SUBGRP_TERM_RSN_CD"),
    F.col("SUBGRP_TERM_RSN_NM").alias("SUBGRP_TERM_RSN_NM"),
    F.col("SUBGRP_TYP_CD").alias("SUBGRP_TYP_CD"),
    F.col("SUBGRP_TYP_NM").alias("SUBGRP_TYP_NM"),
    F.col("SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUBGRP_ST_CD_SK").alias("SUBGRP_ST_CD_SK"),
    F.col("SUBGRP_STTUS_CD_SK").alias("SUBGRP_STTUS_CD_SK"),
    F.col("SUBGRP_TERM_RSN_CD_SK").alias("SUBGRP_TERM_RSN_CD_SK"),
    F.col("SUBGRP_TYP_CD_SK").alias("SUBGRP_TYP_CD_SK"),
    F.col("CUR_ANNV_DT_SK").alias("CUR_ANNV_DT_SK"),
    F.col("NEXT_ANNV_DT_SK").alias("NEXT_ANNV_DT_SK"),
    F.col("SUBGRP_ELIG_PEND_UNTIL_PD_IN").alias("SUBGRP_ELIG_PEND_UNTIL_PD_IN")
)

df_final = (
    df_xfrm_businessLogic2
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("SUBGRP_CNTCT_MIDINIT", F.rpad(F.col("SUBGRP_CNTCT_MIDINIT"), 1, " "))
    .withColumn("SUBGRP_ZIP_CD_5", F.rpad(F.col("SUBGRP_ZIP_CD_5"), 5, " "))
    .withColumn("SUBGRP_ZIP_CD_4", F.rpad(F.col("SUBGRP_ZIP_CD_4"), 4, " "))
    .withColumn("SUBGRP_PHN_NO_EXT", F.rpad(F.col("SUBGRP_PHN_NO_EXT"), 5, " "))
    .withColumn("SUBGRP_FAX_NO_EXT", F.rpad(F.col("SUBGRP_FAX_NO_EXT"), 5, " "))
    .withColumn("SUBGRP_ORIG_EFF_DT_SK", F.rpad(F.col("SUBGRP_ORIG_EFF_DT_SK"), 10, " "))
    .withColumn("SUBGRP_REINST_DT_SK", F.rpad(F.col("SUBGRP_REINST_DT_SK"), 10, " "))
    .withColumn("SUBGRP_TERM_DT_SK", F.rpad(F.col("SUBGRP_TERM_DT_SK"), 10, " "))
    .withColumn("CUR_ANNV_DT_SK", F.rpad(F.col("CUR_ANNV_DT_SK"), 10, " "))
    .withColumn("NEXT_ANNV_DT_SK", F.rpad(F.col("NEXT_ANNV_DT_SK"), 10, " "))
    .withColumn("SUBGRP_ELIG_PEND_UNTIL_PD_IN", F.rpad(F.col("SUBGRP_ELIG_PEND_UNTIL_PD_IN"), 1, " "))
    .select(
        "SUBGRP_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "SUBGRP_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_NM",
        "SUBGRP_CNTCT_FIRST_NM",
        "SUBGRP_CNTCT_MIDINIT",
        "SUBGRP_CNTCT_LAST_NM",
        "SUBGRP_CNTCT_TTL",
        "SUBGRP_NM",
        "SUBGRP_ADDR_LN_1",
        "SUBGRP_ADDR_LN_2",
        "SUBGRP_ADDR_LN_3",
        "SUBGRP_CITY_NM",
        "SUBGRP_ST_CD",
        "SUBGRP_ST_NM",
        "SUBGRP_ZIP_CD_5",
        "SUBGRP_ZIP_CD_4",
        "SUBGRP_CNTY_NM",
        "SUBGRP_PHN_NO",
        "SUBGRP_PHN_NO_EXT",
        "SUBGRP_FAX_NO",
        "SUBGRP_FAX_NO_EXT",
        "SUBGRP_EMAIL_ADDR_TX",
        "SUBGRP_METRO_RURAL_CD",
        "SUBGRP_ORIG_EFF_DT_SK",
        "SUBGRP_RATE_CLS_ID",
        "SUBGRP_REINST_DT_SK",
        "SUBGRP_RISK_CLS_ID",
        "SUBGRP_STTUS_CD",
        "SUBGRP_STTUS_NM",
        "SUBGRP_TAX_ID_NO",
        "SUBGRP_TERM_DT_SK",
        "SUBGRP_TERM_RSN_CD",
        "SUBGRP_TERM_RSN_NM",
        "SUBGRP_TYP_CD",
        "SUBGRP_TYP_NM",
        "SUBGRP_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SUBGRP_ST_CD_SK",
        "SUBGRP_STTUS_CD_SK",
        "SUBGRP_TERM_RSN_CD_SK",
        "SUBGRP_TYP_CD_SK",
        "CUR_ANNV_DT_SK",
        "NEXT_ANNV_DT_SK",
        "SUBGRP_ELIG_PEND_UNTIL_PD_IN"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/SUBGRP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)