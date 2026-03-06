# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called By: IdsRcvdIncmSbsdyDtlFCntl
# MAGIC 
# MAGIC PROCESSING:  Extracting data from RCVD_INCM_SBSDY_DTL_F
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                 	Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------        	 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Santosh Bokka               2014-04-18            5128                        Orginal Program                                           	 EnterpriseNewDevl     
# MAGIC Santosh Bokka               2014-06-17            5128                       Updated with new PROD_ID,
# MAGIC                                                                                            RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD                                          
# MAGIC                                                                                     RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK columns EnterpriseNewDevl  Kalyan Neelam        2014-06-23 
# MAGIC 
# MAGIC Abhiram Dasarathy	      2015-10-26	   5128	    Changed the load file structure and updated the logic   	EntepriseDev1             Bhoomi Dasari            10/28/2015
# MAGIC 					  for columns and added new columns to the process.
# MAGIC Abhiram Dasarathy	      2016-01-04	   5128	  Updated the logic for RCVD_INCM_SBSDY_DTL_LOB_CD EnterpriseDev1	     Kalyan Neelam	        2016-01-04
# MAGIC 					  column in Business_Rules transformer.
# MAGIC Abhiram Dasarathy	      2016-02-10	   5128	  Changed the table structure on the EDW table		 EnterpriseDev1           Kalyan Neelam            2016-02-11
# MAGIC 					Received Income Subsidy Detail Table : Natural Key Change
# MAGIC 					CMS Enrollment Payment Unique Key
# MAGIC  					CMS Enrollment Payment Amount Sequence Number
# MAGIC  					Received Income Subsidy Detail Payment Type Code
# MAGIC  					Received Income Subsidy Account Activity Code
# MAGIC  					Source System Code

# MAGIC Removing duplicate PROD_BILL_CMPNT_ID
# MAGIC Job name:
# MAGIC IdsEdwRcvdIncmSbsdyDtlFExtr
# MAGIC EDW RCVD_INCM_SBSDY_DTL_F extract from IDS
# MAGIC IDS RCVD_INCM_SBSDY_DTL extract from IDS
# MAGIC Add Defaults and NA UNK rows
# MAGIC Sorting and Removing duplicate besed on BILL_ENTY_UNIQ_KEY and LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWOwner = get_widget_value('EDWOwner','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','100')

# Read from IDS PRNT_GRP
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_PRNT_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
    PRNT_GRP_ALIAS.PRNT_GRP_SK,
    PRNT_GRP_ALIAS.PRNT_GRP_BUS_CAT_CD_SK,
    CD_MPPNG_ALIAS.CD_MPPNG_SK,
    CD_MPPNG_ALIAS.TRGT_CD,
    PRNT_GRP_ALIAS.PRNT_GRP_ID
FROM
    {IDSOwner}.PRNT_GRP AS PRNT_GRP_ALIAS
    INNER JOIN
    {IDSOwner}.CD_MPPNG AS CD_MPPNG_ALIAS
        ON PRNT_GRP_ALIAS.PRNT_GRP_BUS_CAT_CD_SK = CD_MPPNG_ALIAS.CD_MPPNG_SK
"""
    )
    .load()
)

# Read from IDS CD_MPPNG
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
  CD_MPPNG_SK,
  TRGT_CD,
  TRGT_CD_NM
FROM
  {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

# Read from IDS GRP
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  GRP_ALIAS.GRP_SK,
  GRP_ALIAS.GRP_BUS_SUB_CAT_SH_NM_CD_SK,
  GRP_ALIAS.DP_IN,
  GRP_ALIAS.GRP_ST_CD_SK,
  GRP_ALIAS.GRP_ID,
  GRP_ALIAS.GRP_UNIQ_KEY,
  GRP_ALIAS.PRNT_GRP_SK
FROM
  {IDSOwner}.GRP AS GRP_ALIAS
"""
    )
    .load()
)

# Read from IDS PROD_SH_NM
df_db2_PROD_SH_NM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  PROD_SH_NM_ALIAS.PROD_SH_NM_SK,
  PROD_SH_NM_ALIAS.PROD_SH_NM
FROM
  {IDSOwner}.PROD_SH_NM AS PROD_SH_NM_ALIAS
"""
    )
    .load()
)

# Read from IDS SUBGRP
df_db2_SUBGRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  SUBGRP_ALIAS.SUBGRP_SK,
  SUBGRP_ALIAS.SUBGRP_ID,
  SUBGRP_ALIAS.SUBGRP_UNIQ_KEY
FROM
  {IDSOwner}.SUBGRP AS SUBGRP_ALIAS
"""
    )
    .load()
)

# Read from IDS SUB
df_db2_SUB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  SUB_ALIAS.SUB_SK,
  SUB_ALIAS.SUB_UNIQ_KEY
FROM
  {IDSOwner}.SUB AS SUB_ALIAS
"""
    )
    .load()
)

# Read from IDS EXPRNC_CAT
df_db2_EXPRNC_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  EXPRNC_CAT_ALIAS.EXPRNC_CAT_SK,
  EXPRNC_CAT_ALIAS.EXPRNC_CAT_CD
FROM
  {IDSOwner}.EXPRNC_CAT AS EXPRNC_CAT_ALIAS
"""
    )
    .load()
)

# Read from IDS SUB_ADDR with join conditions in the SQL
df_db2_SUB_ADDR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
  BILL.BILL_ENTY_SK AS BILL_ENTY_SK,
  ADDR.SUB_ADDR_ST_CD_SK AS SUB_ADDR_ST_CD_SK,
  MAP2.TRGT_CD AS SUB_ADDR_ST_CD
FROM
  {IDSOwner}.BILL_ENTY BILL,
  {IDSOwner}.SUB_ADDR ADDR,
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.CD_MPPNG MAP2
WHERE BILL.SUB_SK = ADDR.SUB_SK
  AND ADDR.SUB_ADDR_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = 'SUBHOME'
  AND ADDR.SUB_ADDR_ST_CD_SK = MAP2.CD_MPPNG_SK
"""
    )
    .load()
)

# Read from IDS RCVD_INCM_SBSDY_DTL
df_db2_RCVD_INCM_SBSDY_DTL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"{IDSOwner}.RCVD_INCM_SBSDY_DTL")
    .load()
)

# Read from IDS PROD
df_db2_PROD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  PROD_ALIAS.PROD_SK,
  PROD_ALIAS.EXPRNC_CAT_SK,
  PROD_ALIAS.PROD_SH_NM_SK,
  PROD_ALIAS.PROD_ID
FROM
  {IDSOwner}.PROD AS PROD_ALIAS
"""
    )
    .load()
)

# Read from IDS BILL_ENTY
df_db2_Bill_Enty_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  BILL_ENTY_ALIAS.BILL_ENTY_SK,
  BILL_ENTY_ALIAS.GRP_SK,
  BILL_ENTY_ALIAS.SUBGRP_SK,
  BILL_ENTY_ALIAS.SUB_SK,
  BILL_ENTY_ALIAS.BILL_ENTY_LVL_CD_SK
FROM
  {IDSOwner}.BILL_ENTY AS BILL_ENTY_ALIAS
"""
    )
    .load()
)

# Read from IDS CLS_PLN
df_db2_CLS_PLN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  CLS_PLN_ALIAS.CLS_PLN_SK,
  CLS_PLN_ALIAS.CLS_PLN_ID
FROM
  {IDSOwner}.CLS_PLN AS CLS_PLN_ALIAS
"""
    )
    .load()
)

# Read from IDS FEE_DSCNT
df_db2_FEE_DSCNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  FEE_DSCNT_ALIAS.FEE_DSCNT_SK,
  FEE_DSCNT_ALIAS.FEE_DSCNT_ID
FROM
  {IDSOwner}.FEE_DSCNT AS FEE_DSCNT_ALIAS
"""
    )
    .load()
)

# Read from IDS FNCL_LOB
df_db2_FNCL_LOB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  FNCL_LOB_ALIAS.FNCL_LOB_SK,
  FNCL_LOB_ALIAS.FNCL_LOB_CD
FROM
  {IDSOwner}.FNCL_LOB AS FNCL_LOB_ALIAS
"""
    )
    .load()
)

# Read from IDS PROD_BILL_CMPNT
df_db2_PROD_BILL_CMPNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  PROD_BILL_CMPNT_ALIAS.PROD_BILL_CMPNT_ID,
  PROD_BILL_CMPNT_ALIAS.PROD_BILL_CMPNT_COV_TYP_CD_SK
FROM
  {IDSOwner}.PROD_BILL_CMPNT AS PROD_BILL_CMPNT_ALIAS
"""
    )
    .load()
)

# Read from IDS BILL_INCM_RCPT
df_db2_BILL_INCM_RCPT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
  BILL_INCM_RCPT_ALIAS.BILL_INCM_RCPT_SK,
  BILL_INCM_RCPT_ALIAS.BILL_ENTY_UNIQ_KEY,
  BILL_INCM_RCPT_ALIAS.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM
  {IDSOwner}.BILL_INCM_RCPT AS BILL_INCM_RCPT_ALIAS
"""
    )
    .load()
)

# Deduplicate Remove_Prod_Bill_Cmpt_Dupes (retain first by key=PROD_BILL_CMPNT_ID)
# We sort ascending on that key so the first is the smallest
df_sorted_prod_bill_cmpnt = df_db2_PROD_BILL_CMPNT.orderBy(
    F.col("PROD_BILL_CMPNT_ID").asc()
)
df_Remove_Prod_Bill_Cmpt_Dupes = dedup_sort(
    df_sorted_prod_bill_cmpnt,
    partition_cols=["PROD_BILL_CMPNT_ID"],
    sort_cols=[("PROD_BILL_CMPNT_ID", "A")]
)

# Deduplicate Remove_Sub_Addr_dupes (retain first by key=BILL_ENTY_SK)
# We sort ascending on that key so the first is the smallest
df_sorted_sub_addr = df_db2_SUB_ADDR_in.orderBy(F.col("BILL_ENTY_SK").asc())
df_Remove_Sub_Addr_dupes = dedup_sort(
    df_sorted_sub_addr,
    partition_cols=["BILL_ENTY_SK"],
    sort_cols=[("BILL_ENTY_SK", "A")]
)

# Sort Srt_RunCyle by [BILL_ENTY_UNIQ_KEY, LAST_UPDT_RUN_CYC_EXCTN_SK] ascending
df_Srt_RunCyle = df_db2_BILL_INCM_RCPT.orderBy(
    F.col("BILL_ENTY_UNIQ_KEY").asc(), F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").asc()
)

# Deduplicate Rmd_Last_Incm_Rcpt_SK (retain last by key=BILL_ENTY_UNIQ_KEY)
# We can do descending on LAST_UPDT_RUN_CYC_EXCTN_SK so the first in each partition is effectively the last
df_sorted_rcpt = df_Srt_RunCyle.orderBy(
    F.col("BILL_ENTY_UNIQ_KEY").asc(), F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").desc()
)
df_Rmd_Last_Incm_Rcpt_SK = dedup_sort(
    df_sorted_rcpt,
    partition_cols=["BILL_ENTY_UNIQ_KEY"],
    sort_cols=[("LAST_UPDT_RUN_CYC_EXCTN_SK", "D")]
)

# Prepare the reference copy stage "Cpy_CdMppng" to create separate dataframes from df_db2_CD_MPPNG_in
# Each is the same but we'll just keep references with renamed columns. We'll do direct selects.
df_Cpy_CdMppng_ref_CdMppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_Cpy_CdMppng_ref_SrcSysCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_BillEntyCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_ProdBillCmpnTypCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_GrpBusShNmCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_RcvdIncmAcctTypCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_RcvdIncmActivitySrcCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_RcvdIncmLobCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_Ref_SubCntrStCd = df_Cpy_CdMppng_ref_CdMppng
df_Cpy_CdMppng_ref_RcvdIncmActvtyTypCd = df_Cpy_CdMppng_ref_CdMppng

# Lkp_Cd: join Source_Data (df_db2_RCVD_INCM_SBSDY_DTL_in) left with {ref_Prod, ref_Bill_Enty, lnk_Billl_Home_Pg_Attrbtn, ref_Cls_pPn, ref_Fee_Dscnt, ref_Fncl_Lob, ref_Prod_Bill_Cmpnt}
df_Lkp_Cd_1 = df_db2_RCVD_INCM_SBSDY_DTL_in.alias("Source_Data") \
    .join(df_db2_PROD_in.alias("ref_Prod"),
          on=[F.col("Source_Data.PROD_SK") == F.col("ref_Prod.PROD_SK")],
          how="left") \
    .join(df_db2_Bill_Enty_in.alias("ref_Bill_Enty"),
          on=[F.col("Source_Data.BILL_ENTY_SK") == F.col("ref_Bill_Enty.BILL_ENTY_SK")],
          how="left") \
    .join(df_Rmd_Last_Incm_Rcpt_SK.alias("lnk_Billl_Home_Pg_Attrbtn"),
          on=[F.col("Source_Data.BILL_ENTY_UNIQ_KEY") == F.col("lnk_Billl_Home_Pg_Attrbtn.BILL_ENTY_UNIQ_KEY")],
          how="left") \
    .join(df_db2_CLS_PLN_in.alias("ref_Cls_pPn"),
          on=[F.col("Source_Data.CLS_PLN_SK") == F.col("ref_Cls_pPn.CLS_PLN_SK")],
          how="left") \
    .join(df_db2_FEE_DSCNT_in.alias("ref_Fee_Dscnt"),
          on=[F.col("Source_Data.FEE_DSCNT_SK") == F.col("ref_Fee_Dscnt.FEE_DSCNT_SK")],
          how="left") \
    .join(df_db2_FNCL_LOB_in.alias("ref_Fncl_Lob"),
          on=[F.col("Source_Data.FNCL_LOB_SK") == F.col("ref_Fncl_Lob.FNCL_LOB_SK")],
          how="left") \
    .join(df_Remove_Prod_Bill_Cmpt_Dupes.alias("ref_Prod_Bill_Cmpnt"),
          on=[F.col("Source_Data.PROD_BILL_CMPNT_ID") == F.col("ref_Prod_Bill_Cmpnt.PROD_BILL_CMPNT_ID")],
          how="left")

df_Lkp_Cd = df_Lkp_Cd_1.select(
    F.col("Source_Data.RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
    F.col("Source_Data.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Source_Data.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Source_Data.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.col("Source_Data.CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("Source_Data.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVDINCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("Source_Data.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("Source_Data.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("Source_Data.PROD_SK").alias("PROD_SK"),
    F.col("Source_Data.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("Source_Data.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("Source_Data.BILL_GRP_BILL_ENTY_UNIQ_KEY").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
    F.col("Source_Data.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("Source_Data.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_ACTV_CD_SK"),
    F.col("Source_Data.RCVD_INCM_SBSDY_ACCT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
    F.col("Source_Data.RCVD_INCM_SBSDY_ACTV_SRC_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
    F.col("Source_Data.RCVD_INCM_SBSDY_ACTV_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
    F.col("Source_Data.RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
    F.col("Source_Data.FIRST_YR_IN").alias("FIRST_YR_IN"),
    F.col("Source_Data.POSTED_DT_SK").alias("POSTED_DT_SK"),
    F.col("Source_Data.ERN_INCM_AMT").alias("ERN_INCM_AMT"),
    F.col("Source_Data.RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
    F.col("Source_Data.GL_NO").alias("GL_NO"),
    F.col("ref_Prod.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("ref_Prod.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("Source_Data.PROD_ID").alias("PROD_ID"),
    F.col("ref_Bill_Enty.GRP_SK").alias("GRP_SK"),
    F.col("ref_Bill_Enty.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("ref_Bill_Enty.SUB_SK").alias("SUB_SK"),
    F.col("ref_Bill_Enty.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_LVL_CD_SK"),
    F.col("lnk_Billl_Home_Pg_Attrbtn.BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
    F.col("lnk_Billl_Home_Pg_Attrbtn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ref_Prod_Bill_Cmpnt.PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("ref_Cls_pPn.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("ref_Fee_Dscnt.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("ref_Fncl_Lob.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("Source_Data.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("Source_Data.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("Source_Data.QHP_SK").alias("QHP_SK"),
    F.col("Source_Data.QHP_ID").alias("QHP_ID"),
    F.col("Source_Data.CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("Source_Data.CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("Source_Data.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("Source_Data.EXCH_POL_ID").alias("EXCH_POL_ID"),
    F.col("Source_Data.EXCH_SUB_ID").alias("EXCH_SUB_ID"),
)

# Lkp_Cd_Next: left join lnk_IdsEdwFirstStageExtr_lkpIn with {df_db2_GRP_in, df_db2_PROD_SH_NM_in, df_db2_SUBGRP_in, df_db2_SUB_in, df_db2_EXPRNC_CAT_in}
df_lnk_IdsEdwFirstStageExtr_lkpIn = df_Lkp_Cd
df_Lkp_Cd_Next_1 = df_lnk_IdsEdwFirstStageExtr_lkpIn.alias("lnk_IdsEdwFirstStageExtr_lkpIn") \
    .join(df_db2_GRP_in.alias("ref_Grp"),
          on=[F.col("lnk_IdsEdwFirstStageExtr_lkpIn.GRP_SK") == F.col("ref_Grp.GRP_SK")],
          how="left") \
    .join(df_db2_PROD_SH_NM_in.alias("ref_Prod_Sh_Nm"),
          on=[F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_SH_NM_SK") == F.col("ref_Prod_Sh_Nm.PROD_SH_NM_SK")],
          how="left") \
    .join(df_db2_SUBGRP_in.alias("ref_SubGrp"),
          on=[F.col("lnk_IdsEdwFirstStageExtr_lkpIn.SUBGRP_SK") == F.col("ref_SubGrp.SUBGRP_SK")],
          how="left") \
    .join(df_db2_SUB_in.alias("ref_Sub"),
          on=[F.col("lnk_IdsEdwFirstStageExtr_lkpIn.SUB_SK") == F.col("ref_Sub.SUB_SK")],
          how="left") \
    .join(df_db2_EXPRNC_CAT_in.alias("Exprnc_cd"),
          on=[F.col("lnk_IdsEdwFirstStageExtr_lkpIn.EXPRNC_CAT_SK") == F.col("Exprnc_cd.EXPRNC_CAT_SK")],
          how="left")

df_Lkp_Cd_Next = df_Lkp_Cd_Next_1.select(
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVDINCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVDINCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_GRP_BILL_ENTY_UNIQ_KEY").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_ACCT_ACTV_CD_SK").alias("RCVDINCM_SBSDY_ACCT_ACTV_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_ACCT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_SRC_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.FIRST_YR_IN").alias("FIRST_YR_IN"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.POSTED_DT_SK").alias("POSTED_DT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.ERN_INCM_AMT").alias("ERN_INCM_AMT"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.GL_NO").alias("GL_NO"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.SUB_SK").alias("SUB_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_LVL_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("ref_Grp.GRP_BUS_SUB_CAT_SH_NM_CD_SK").alias("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("ref_Grp.DP_IN").alias("DP_IN"),
    F.col("ref_Grp.GRP_ST_CD_SK").alias("GRP_ST_CD_SK"),
    F.col("ref_Grp.GRP_ID").alias("GRP_ID"),
    F.col("ref_Grp.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("ref_Grp.PRNT_GRP_SK").alias("PRNT_GRP_SK"),
    F.col("ref_Prod_Sh_Nm.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("ref_SubGrp.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ref_SubGrp.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("ref_Sub.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Exprnc_cd.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.QHP_SK").alias("QHP_SK"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.QHP_ID").alias("QHP_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.EXCH_POL_ID").alias("EXCH_POL_ID"),
    F.col("lnk_IdsEdwFirstStageExtr_lkpIn.EXCH_SUB_ID").alias("EXCH_SUB_ID"),
)

# Lkp_Codes: left join lnk_IdsEdwSecondStageExtr_lkpIn with our cpy_cdMppng references + db2_PRNT_GRP_in + Remove_Sub_Addr_dupes
df_lnk_IdsEdwSecondStageExtr_lkpIn = df_Lkp_Cd_Next

df_Lkp_Codes_1 = (
    df_lnk_IdsEdwSecondStageExtr_lkpIn.alias("lnk_IdsEdwSecondStageExtr_lkpIn")
    .join(
        df_Cpy_CdMppng_ref_SrcSysCd.alias("ref_SrcSysCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SRC_SYS_CD_SK")
            == F.col("ref_SrcSysCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_BillEntyCd.alias("ref_BillEntyCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_ENTY_LVL_CD_SK")
            == F.col("ref_BillEntyCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_ProdBillCmpnTypCd.alias("ref_ProdBillCmpnTypCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_BILL_CMPNT_COV_TYP_CD_SK")
            == F.col("ref_ProdBillCmpnTypCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_GrpBusShNmCd.alias("ref_GrpBusShNmCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_BUS_SUB_CAT_SH_NM_CD_SK")
            == F.col("ref_GrpBusShNmCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_RcvdIncmAcctTypCd.alias("ref_RcvdIncmAcctTypCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACCT_TYP_CD_SK")
            == F.col("ref_RcvdIncmAcctTypCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_RcvdIncmActivitySrcCd.alias("ref_RcvdIncmActivitySrcCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_SRC_CD_SK")
            == F.col("ref_RcvdIncmActivitySrcCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_RcvdIncmLobCd.alias("ref_RcvdIncmLobCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_LOB_CD_SK")
            == F.col("ref_RcvdIncmLobCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_db2_PRNT_GRP_in.alias("ref_Prnt_Grp"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PRNT_GRP_SK")
            == F.col("ref_Prnt_Grp.PRNT_GRP_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_Ref_SubCntrStCd.alias("Ref_SubCntrStCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_ST_CD_SK")
            == F.col("Ref_SubCntrStCd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_Remove_Sub_Addr_dupes.alias("lnk_SubAddr_out"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_ENTY_SK")
            == F.col("lnk_SubAddr_out.BILL_ENTY_SK")
        ],
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_RcvdIncmActvtyTypCd.alias("ref_RcvdIncmActvtyTypCd"),
        on=[
            F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_TYP_CD_SK")
            == F.col("ref_RcvdIncmActvtyTypCd.CD_MPPNG_SK")
        ],
        how="left",
    )
)

df_Lkp_Codes = df_Lkp_Codes_1.select(
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVDINCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.CRT_DT_SK").alias("RCVD_INCM_SBSDY_DTL_CRT_DT_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.QHP_ID").alias("QHP_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_DUE_DT_SK").alias("DUE_YR_MO_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_INCM_RCPT_SK").alias("RCVD_INCM_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.QHP_SK").alias("QHP_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SUB_SK").alias("SUB_SK"),
    F.col("ref_BillEntyCd.TRGT_CD").alias("BILL_ENTY_BILL_LVL_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("ref_RcvdIncmAcctTypCd.TRGT_CD").alias("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD"),
    F.col("ref_RcvdIncmActivitySrcCd.TRGT_CD").alias("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD"),
    F.col("ref_RcvdIncmActvtyTypCd.TRGT_CD").alias("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.FNCL_LOB_CD").alias("RCVD_INCM_SBSDY_DTL_LOB_CD"),
    F.col("Ref_SubCntrStCd.TRGT_CD").alias("SUB_CNTR_ST_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.FIRST_YR_IN").alias("RCVD_INCM_SBSDY_DTL_FIRST_YR_IN"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.ERN_INCM_AMT").alias("RCVD_INCM_SBSDY_DTL_ERN_INCM_AMT"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RVNU_RCVD_AMT").alias("RCVD_INCM_SBSDY_DTL_RVNU_RCVD_AMT"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVDINCM_SBSDY_ACCT_ACTV_CD_SK").alias("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACCT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_SRC_CD_SK").alias("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_ACTV_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_DTL_LOB_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_ST_CD_SK").alias("SUB_CNTR_ST_CD_SK"),
    F.col("ref_GrpBusShNmCd.TRGT_CD").alias("GRP_BUS_SUB_CAT_SH_NM_CD"),
    F.col("ref_Prnt_Grp.TRGT_CD").alias("PRNT_GRP_BUS_CAT_CD"),
    F.col("ref_ProdBillCmpnTypCd.TRGT_CD").alias("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.col("ref_RcvdIncmLobCd.TRGT_CD").alias("RCVD_INCM_SBSDY_LOB_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.POSTED_DT_SK").alias("RCVD_INCM_SBSDY_DTL_DT_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GL_NO").alias("RCVD_INCM_SBSDY_DTL_GL_NO"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_ID").alias("GRP_ID"),
    F.col("ref_Prnt_Grp.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.GRP_BUS_SUB_CAT_SH_NM_CD_SK").alias("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("ref_Prnt_Grp.PRNT_GRP_BUS_CAT_CD_SK").alias("PRNT_GRP_BUS_CAT_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.DP_IN").alias("DP_IN"),
    F.col("lnk_SubAddr_out.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    F.col("lnk_SubAddr_out.SUB_ADDR_ST_CD").alias("SUB_ADDR_ST_CD"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.EXCH_POL_ID").alias("EXCH_POL_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.EXCH_SUB_ID").alias("EXCH_SUB_ID"),
    F.col("lnk_IdsEdwSecondStageExtr_lkpIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

# Business_Rules input => df_Lkp_Codes
df_Business_Rules_in = df_Lkp_Codes

# We now apply the transformations in the Transformer. 
# Because there are 3 output links, we produce 3 dataframes:
# Link1: lnk_IdsEdwRcvdIncmSbsdyFExtr_OutABC (no specific row constraint). 
# Link2: lnk_NA_out => constraint "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
# Link3: lnk_UNK_out => same constraint => 1 => DataStage logic. 
# We replicate them by picking the entire df for link1, then for link2 a single row with overridden columns, then for link3 a single row with overridden columns. 

df_link_main = df_Business_Rules_in  # for lnk_IdsEdwRcvdIncmSbsdyFExtr_OutABC

# We'll create exactly 1 row for lnk_NA_out by taking limit(1) and overwriting columns as specified in the "WhereExpression".
df_temp_NA = df_Business_Rules_in.limit(1) 
# Apply all column "WhereExpression" transformations from the job. 
# We'll do them literally:

df_link_NA_out = (
    df_temp_NA
    .withColumn("RCVD_INCM_SBSDY_DTL_SK", F.lit(0))
    .withColumn("CMS_ENR_PAYMT_UNIQ_KEY", F.lit(0))
    .withColumn("CMS_ENR_PAYMT_AMT_SEQ_NO", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD", F.lit("UNK"))
    .withColumn("SRC_SYS_CD", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("BILL_ENTY_SK", F.lit(0))
    .withColumn("CLS_PLN_SK", F.lit(0))
    .withColumn("EXPRNC_CAT_SK", F.lit(0))
    .withColumn("FEE_DSCNT_SK", F.lit(0))
    .withColumn("FNCL_LOB_SK", F.lit(0))
    .withColumn("GRP_SK", F.lit(0))
    .withColumn("PROD_SK", F.lit(0))
    .withColumn("PROD_SH_NM_SK", F.lit(0))
    .withColumn("QHP_SK", F.lit(0))
    .withColumn("SUBGRP_SK", F.lit(0))
    .withColumn("SUB_SK", F.lit(0))
    .withColumn("BILL_ENTY_BILL_LVL_CD", F.lit("UNK"))
    .withColumn("EXPRNC_CAT_CD", F.lit("UNK"))
    .withColumn("GRP_BUS_SUB_CAT_SH_NM_CD", F.lit("UNK"))
    .withColumn("PRNT_GRP_BUS_CAT_CD", F.lit("UNK"))
    .withColumn("PROD_BILL_CMPNT_COV_TYP_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_LOB_CD", F.lit("UNK"))
    .withColumn("SUB_CNTR_ST_CD", F.lit("UNK"))
    .withColumn("RCVD_INCM_SBSDY_DTL_FIRST_YR_IN", F.lit("N"))
    .withColumn("DUE_YR_MO_SK", F.lit("175301"))
    .withColumn("RCPT_YR_MO_SK", F.lit("175301"))
    .withColumn("RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_CRT_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ERN_INCM_AMT", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_RVNU_RCVD_AMT", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_GL_NO", F.lit("UNK"))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.lit(0))
    .withColumn("GRP_UNIQ_KEY", F.lit(0))
    .withColumn("SUBGRP_UNIQ_KEY", F.lit(0))
    .withColumn("SUB_UNIQ_KEY", F.lit(0))
    .withColumn("CLS_PLN_ID", F.lit("UNK"))
    .withColumn("EXCH_MBR_ID", F.lit("UNK"))
    .withColumn("EXCH_POL_ID", F.lit("UNK"))
    .withColumn("EXCH_SUB_ID", F.lit("UNK"))
    .withColumn("FEE_DSCNT_ID", F.lit("UNK"))
    .withColumn("FNCL_LOB_CD", F.lit("UNK"))
    .withColumn("GRP_ID", F.lit("UNK"))
    .withColumn("PRNT_GRP_ID", F.lit("UNK"))
    .withColumn("PROD_ID", F.lit("UNK"))
    .withColumn("PROD_BILL_CMPNT_ID", F.lit("UNK"))
    .withColumn("PROD_SH_NM", F.lit("UNK"))
    .withColumn("QHP_ID", F.lit("UNK"))
    .withColumn("SUBGRP_ID", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("BILL_ENTY_BILL_LVL_CD_SK", F.lit(0))
    .withColumn("GRP_BUS_SUB_CAT_SH_NM_CD_SK", F.lit(0))
    .withColumn("PRNT_GRP_BUS_CAT_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_LOB_CD_SK", F.lit(0))
    .withColumn("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK", F.lit(0))
    .withColumn("SUB_CNTR_ST_CD_SK", F.lit(0))
)

# We'll create exactly 1 row for lnk_UNK_out with a second single row of forced values:
df_temp_UNK = df_Business_Rules_in.limit(1)
df_link_UNK_out = (
    df_temp_UNK
    .withColumn("RCVD_INCM_SBSDY_DTL_SK", F.lit(1))
    .withColumn("CMS_ENR_PAYMT_UNIQ_KEY", F.lit(1))
    .withColumn("CMS_ENR_PAYMT_AMT_SEQ_NO", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD", F.lit("NA"))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("BILL_ENTY_SK", F.lit(1))
    .withColumn("CLS_PLN_SK", F.lit(1))
    .withColumn("EXPRNC_CAT_SK", F.lit(1))
    .withColumn("FEE_DSCNT_SK", F.lit(1))
    .withColumn("FNCL_LOB_SK", F.lit(1))
    .withColumn("GRP_SK", F.lit(1))
    .withColumn("PROD_SK", F.lit(1))
    .withColumn("PROD_SH_NM_SK", F.lit(1))
    .withColumn("QHP_SK", F.lit(1))
    .withColumn("SUBGRP_SK", F.lit(1))
    .withColumn("SUB_SK", F.lit(1))
    .withColumn("BILL_ENTY_BILL_LVL_CD", F.lit("NA"))
    .withColumn("EXPRNC_CAT_CD", F.lit("NA"))
    .withColumn("GRP_BUS_SUB_CAT_SH_NM_CD", F.lit("NA"))
    .withColumn("PRNT_GRP_BUS_CAT_CD", F.lit("NA"))
    .withColumn("PROD_BILL_CMPNT_COV_TYP_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_LOB_CD", F.lit("NA"))
    .withColumn("SUB_CNTR_ST_CD", F.lit("NA"))
    .withColumn("RCVD_INCM_SBSDY_DTL_FIRST_YR_IN", F.lit("N"))
    .withColumn("DUE_YR_MO_SK", F.lit("175301"))
    .withColumn("RCPT_YR_MO_SK", F.lit("175301"))
    .withColumn("RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_CRT_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_DT_SK", F.lit("1753-01-01"))
    .withColumn("RCVD_INCM_SBSDY_DTL_ERN_INCM_AMT", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_RVNU_RCVD_AMT", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_GL_NO", F.lit("NA"))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.lit(1))
    .withColumn("GRP_UNIQ_KEY", F.lit(1))
    .withColumn("SUBGRP_UNIQ_KEY", F.lit(1))
    .withColumn("SUB_UNIQ_KEY", F.lit(1))
    .withColumn("CLS_PLN_ID", F.lit("NA"))
    .withColumn("EXCH_MBR_ID", F.lit("NA"))
    .withColumn("EXCH_POL_ID", F.lit("NA"))
    .withColumn("EXCH_SUB_ID", F.lit("NA"))
    .withColumn("FEE_DSCNT_ID", F.lit("NA"))
    .withColumn("FNCL_LOB_CD", F.lit("NA"))
    .withColumn("GRP_ID", F.lit("NA"))
    .withColumn("PRNT_GRP_ID", F.lit("NA"))
    .withColumn("PROD_ID", F.lit("NA"))
    .withColumn("PROD_BILL_CMPNT_ID", F.lit("NA"))
    .withColumn("PROD_SH_NM", F.lit("NA"))
    .withColumn("QHP_ID", F.lit("NA"))
    .withColumn("SUBGRP_ID", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("BILL_ENTY_BILL_LVL_CD_SK", F.lit(1))
    .withColumn("GRP_BUS_SUB_CAT_SH_NM_CD_SK", F.lit(1))
    .withColumn("PRNT_GRP_BUS_CAT_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_LOB_CD_SK", F.lit(1))
    .withColumn("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK", F.lit(1))
    .withColumn("SUB_CNTR_ST_CD_SK", F.lit(1))
)

# Funnel stage (fnl_Data) => union these three outputs in the order: lnk_UNK_out, lnk_IdsEdwRcvdIncmSbsdyFExtr_OutABC, lnk_NA_out
df_fnl_Data_in = df_link_UNK_out.unionByName(df_link_main).unionByName(df_link_NA_out)

# cpy_Main_Data => just copy columns as is. According to the job, column order is the same as funnel.
df_cpy_Main_Data = df_fnl_Data_in.select(
    "RCVD_INCM_SBSDY_DTL_SK",
    "CMS_ENR_PAYMT_UNIQ_KEY",
    "CMS_ENR_PAYMT_AMT_SEQ_NO",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD",
    "RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BILL_ENTY_SK",
    "CLS_PLN_SK",
    "EXPRNC_CAT_SK",
    "FEE_DSCNT_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "PROD_SH_NM_SK",
    "QHP_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "BILL_ENTY_BILL_LVL_CD",
    "EXPRNC_CAT_CD",
    "GRP_BUS_SUB_CAT_SH_NM_CD",
    "PRNT_GRP_BUS_CAT_CD",
    "PROD_BILL_CMPNT_COV_TYP_CD",
    "RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD",
    "RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD",
    "RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD",
    "RCVD_INCM_SBSDY_DTL_LOB_CD",
    "SUB_CNTR_ST_CD",
    "RCVD_INCM_SBSDY_DTL_FIRST_YR_IN",
    "DUE_YR_MO_SK",
    "RCPT_YR_MO_SK",
    "RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK",
    "RCVD_INCM_SBSDY_DTL_CRT_DT_SK",
    "RCVD_INCM_SBSDY_DTL_DT_SK",
    "RCVD_INCM_SBSDY_DTL_ERN_INCM_AMT",
    "RCVD_INCM_SBSDY_DTL_RVNU_RCVD_AMT",
    "RCVD_INCM_SBSDY_DTL_GL_NO",
    "BILL_ENTY_UNIQ_KEY",
    "GRP_UNIQ_KEY",
    "SUBGRP_UNIQ_KEY",
    "SUB_UNIQ_KEY",
    "CLS_PLN_ID",
    "EXCH_MBR_ID",
    "EXCH_POL_ID",
    "EXCH_SUB_ID",
    "FEE_DSCNT_ID",
    "FNCL_LOB_CD",
    "GRP_ID",
    "PRNT_GRP_ID",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "PROD_SH_NM",
    "QHP_ID",
    "SUBGRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY_BILL_LVL_CD_SK",
    "GRP_BUS_SUB_CAT_SH_NM_CD_SK",
    "PRNT_GRP_BUS_CAT_CD_SK",
    "RCVD_INCM_SBSDY_DTL_ACCT_ACTVTY_CD_SK",
    "RCVD_INCM_SBSDY_DTL_ACCT_TYP_CD_SK",
    "RCVD_INCM_SBSDY_DTL_ACTVTY_SRC_CD_SK",
    "RCVD_INCM_SBSDY_DTL_ACTVTY_TYP_CD_SK",
    "RCVD_INCM_SBSDY_DTL_LOB_CD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "SUB_CNTR_ST_CD_SK",
)

# Final output stage => RCVD_INCM_SBSDY_DTL_F => write to .dat
# Some columns have SqlType=char => apply rpad. We'll identify them quickly by name or from the job metadata:
# We see fields like: "CRT_RUN_CYC_EXCTN_DT_SK" length=10, "LAST_UPDT_RUN_CYC_EXCTN_DT_SK" length=10, "DUE_YR_MO_SK" length=6, 
# "RCPT_YR_MO_SK" length=6, "RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK" length=10, "RCVD_INCM_SBSDY_DTL_CRT_DT_SK" length=10, 
# "RCVD_INCM_SBSDY_DTL_DT_SK" length=10, "RCVD_INCM_SBSDY_DTL_FIRST_YR_IN" length=1, and possibly others. 
# We'll apply rpad for those that the job explicitly marked "SqlType": "char" with the given length.

df_final = (
    df_cpy_Main_Data
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("DUE_YR_MO_SK", F.rpad(F.col("DUE_YR_MO_SK"), 6, " "))
    .withColumn("RCPT_YR_MO_SK", F.rpad(F.col("RCPT_YR_MO_SK"), 6, " "))
    .withColumn("RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK", F.rpad(F.col("RCVD_INCM_SBSDY_DTL_BILL_DUE_DT_SK"), 10, " "))
    .withColumn("RCVD_INCM_SBSDY_DTL_CRT_DT_SK", F.rpad(F.col("RCVD_INCM_SBSDY_DTL_CRT_DT_SK"), 10, " "))
    .withColumn("RCVD_INCM_SBSDY_DTL_DT_SK", F.rpad(F.col("RCVD_INCM_SBSDY_DTL_DT_SK"), 10, " "))
    .withColumn("RCVD_INCM_SBSDY_DTL_FIRST_YR_IN", F.rpad(F.col("RCVD_INCM_SBSDY_DTL_FIRST_YR_IN"), 1, " "))
)

# Write to the .dat file with required properties
write_files(
    df_final,
    f"{adls_path}/load/RCVD_INCM_SBSDY_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)