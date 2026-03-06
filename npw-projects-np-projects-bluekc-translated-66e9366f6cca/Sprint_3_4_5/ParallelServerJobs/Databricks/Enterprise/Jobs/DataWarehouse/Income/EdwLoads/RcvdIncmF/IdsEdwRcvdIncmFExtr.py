# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids Bill Incom Reciept table BILL_INCM_RCPT and loads to EDW
# MAGIC   Does not keep history.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     BILL_ENTY
# MAGIC                             PROD
# MAGIC                             CLS_PLN
# MAGIC                             FEE_DSCNT
# MAGIC                             FNCL_LOB
# MAGIC                             GRP
# MAGIC                             EXPRNC_CAT
# MAGIC                             PROD_BILL
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from records from the source that have a run cycle greater than the BeginCycle, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen 12/01/2006  - Originally Programmed
# MAGIC               Oliver Nielsen 02/27/2006  -  Change Prod_Bill Query to pull Distinct CMPNT_ID and COV_TYP_CD_SK
# MAGIC               SAndrew        05/29/2007      Renamed  GRP_BUS_TYP_CD to GRP_BUS_SUB_CAT_SH_NM_CD.    Placement of field did not change
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                                                                                         Development          Code                        Date 
# MAGIC Developer            Date                   Project/Altiris #            Change Description                                                                                                                                                                                              Project                    Reviewer                  Reviewed       
# MAGIC ------------------          --------------------       ------------------------            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                    -----------------------        -------------------------       ---------------------------   
# MAGIC SAndrew              2007-10-01         IAD Qrty Rlse 8.1         TTR-142 Corrected Sub Contract State code to be either the grp state or sub state and  not the subscriber family contract code.
# MAGIC                                                                                             Corrected hash.clear signature and added hash file hf_rcvd_incm_sub_addr.                
# MAGIC 
# MAGIC Srikanth Mettpalli  2013-10-01          5114                            Original Programming                                                                                                                                                                                            EnterpriseWrhsDevl  Peter Marshall         12/12/2013           
# MAGIC                                                                                              (Server to Parallel Conversion)      
# MAGIC 
# MAGIC Tim Sieg               2024-04-04           US613791                 adding db2_BILL_ENTY_FutureDate_in lookup and updating SUBGRP_SK, SUBGRP_ID and SUBGRP_UNIQ_KEY logic                              EnterpriseDev2        Jeyaprasanna          2024-04-22
# MAGIC                                                                                             in the xfrm_BusinessLogic2 stage to get correct values for INDVSUB transactions
# MAGIC  
# MAGIC Mohan Karnati      2024-05-02           US613791                 Set First name column to false in seq_RCVD_INCM_F_csv_load stage                                                                                                                 EnterpriseDev2       Jeyaprasanna           2024-05-06

# MAGIC Extract Bill Enty data for future dated bills to populate SUBGRP data
# MAGIC Job Name: IdsEdwRcvdIncmFExtr
# MAGIC Pull From IDS Bill Income Receipt  and Updates  EDW  RCVD_INCM_F
# MAGIC Add Defaults and NA UNK rows
# MAGIC Write RCVD_INCM_F.dat for the IdsEdwRcvdIncmFLoad Job.
# MAGIC Write B_RCVD_INCM_F.dat for the IdsEdwBRcvdIncmFLoad Job.
# MAGIC Code SK lookups for Denormalization
# MAGIC Code SK lookups for Denormalization
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC IDS Grp Data pull
# MAGIC Add Defaults and Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType, TimestampType

IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrentRunDate = get_widget_value('CurrentRunDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ComsnBeginCycle = get_widget_value('ComsnBeginCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_GRP_in = f"""SELECT 
A.GRP_SK,
B.PRNT_GRP_ID,
B.PRNT_GRP_BUS_CAT_CD_SK,
C.TRGT_CD 
FROM {IDSOwner}.GRP A,
 {IDSOwner}.PRNT_GRP B,
 {IDSOwner}.CD_MPPNG C 
 WHERE A.PRNT_GRP_SK=B.PRNT_GRP_SK AND
B.PRNT_GRP_BUS_CAT_CD_SK=C.CD_MPPNG_SK
"""
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_in)
    .load()
)

extract_query_db2_BILL_ENTY_FutureDate_in = f"""SELECT 
BILL.BILL_ENTY_SK,
BILL.GRP_SK,
SUBGRP.SUBGRP_SK,
BILL.SUB_SK,
BILL.BILL_ENTY_LVL_CD_SK,
SUBGRP.SUBGRP_ID,
SUBGRP.SUBGRP_UNIQ_KEY,
SUB.SUB_UNIQ_KEY,
SUB.SUB_ID,
MBRENR.EFF_DT_SK, 
MBRENR.TERM_DT_SK 
FROM 
{IDSOwner}.BILL_ENTY BILL , 
{IDSOwner}.SUB SUB,  
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBRENR,
{IDSOwner}.SUBGRP SUBGRP
WHERE BILL.SUB_SK =  SUB.SUB_SK
AND SUB.SUB_SK = MBR.SUB_SK
AND MBR.MBR_SFX_NO = '00'
AND MBR.MBR_SK = MBRENR.MBR_SK
AND MBRENR.ELIG_IN = 'Y'
AND MBRENR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBRENR.GRP_SK = SUBGRP.GRP_SK
"""
df_db2_BILL_ENTY_FutureDate_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_BILL_ENTY_FutureDate_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

extract_query_db2_BILL_INCM_RCPT_in = f"""SELECT 
BILL_INCM_RCPT_SK,
SRC_SYS_CD_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
BILL_ENTY_UNIQ_KEY,
BILL_DUE_DT_SK,
BILL_CNTR_ID,
CRT_DTM,
CRT_DT_SK,
BILL_ENTY_SK,
CLS_PLN_SK,
FEE_DSCNT_SK,
FNCL_LOB_SK,
PROD_SK,
BILL_INCM_RCPT_ACCT_ACTV_CD_SK,
BILL_INCM_RCPT_ACCT_TYP_CD_SK,
BILL_INCM_RCPT_ACTV_SRC_CD_SK,
BILL_INCM_RCPT_ACTV_TYP_CD_SK,
BILL_INCM_RCPT_LOB_CD_SK,
FIRST_YR_IN,
POSTED_DT_SK,
ERN_INCM_AMT,
RVNU_RCVD_AMT,
GL_NO,
PROD_BILL_CMPNT_ID 
FROM {IDSOwner}.BILL_INCM_RCPT BILL_INCM_RCPT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON BILL_INCM_RCPT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK 
WHERE BILL_INCM_RCPT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ComsnBeginCycle}
"""
df_db2_BILL_INCM_RCPT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_BILL_INCM_RCPT_in)
    .load()
)

extract_query_db2_PROD_SH_NM_in = f"""SELECT A.PROD_SK,
A.PROD_SH_NM_SK,
A.PROD_ID,
B.PROD_SH_NM 
FROM {IDSOwner}.PROD A,
 {IDSOwner}.PROD_SH_NM B 
WHERE A.PROD_SH_NM_SK = B.PROD_SH_NM_SK
"""
df_db2_PROD_SH_NM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_SH_NM_in)
    .load()
)

extract_query_db2_BILL_ENTY_in = f"""SELECT 
BILL.BILL_ENTY_SK,
BILL.GRP_SK,
BILL.SUBGRP_SK,
BILL.SUB_SK,
BILL.BILL_ENTY_LVL_CD_SK,
SUBGRP.SUBGRP_ID,
SUBGRP.SUBGRP_UNIQ_KEY,
SUB.SUB_UNIQ_KEY,
SUB.SUB_ID 
FROM 
{IDSOwner}.BILL_ENTY BILL , 
{IDSOwner}.SUB SUB,  
{IDSOwner}.SUBGRP SUBGRP
WHERE BILL.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND BILL.SUB_SK =  SUB.SUB_SK
"""
df_db2_BILL_ENTY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_BILL_ENTY_in)
    .load()
)

extract_query_db2_CLS_PLN_in = f"""SELECT CLS_PLN_SK,CLS_PLN_ID FROM {IDSOwner}.CLS_PLN"""
df_db2_CLS_PLN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLS_PLN_in)
    .load()
)

extract_query_db2_FEE_DSCNT_in = f"""SELECT FEE_DSCNT_SK,FEE_DSCNT_ID FROM {IDSOwner}.FEE_DSCNT"""
df_db2_FEE_DSCNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_FEE_DSCNT_in)
    .load()
)

extract_query_db2_FNCL_LOB_in = f"""SELECT FNCL_LOB_SK,FNCL_LOB_CD FROM {IDSOwner}.FNCL_LOB"""
df_db2_FNCL_LOB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_FNCL_LOB_in)
    .load()
)

extract_query_db2_GRP_BUS_in = f"""SELECT 
BILL.BILL_ENTY_SK BILL_ENTY_SK,
GRP.GRP_BUS_SUB_CAT_SH_NM_CD_SK GRP_BUS_SUB_CAT_SH_NM_CD_SK,
GRP.DP_IN DP_IN,
GRP.GRP_ST_CD_SK GRP_ST_CD_SK,
MAP1.TRGT_CD GRP_ST_CD,
GRP.GRP_ID GRP_ID,
GRP.GRP_UNIQ_KEY GRP_UNIQ_KEY
FROM 
{IDSOwner}.BILL_ENTY BILL,
{IDSOwner}.GRP GRP,
{IDSOwner}.CD_MPPNG MAP1 
WHERE BILL.GRP_SK = GRP.GRP_SK
AND GRP.GRP_ST_CD_SK = MAP1.CD_MPPNG_SK
"""
df_db2_GRP_BUS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_BUS_in)
    .load()
)

extract_query_db2_EXPRNC_CAT_in = f"""SELECT DISTINCT
A.PROD_SK,
A.SRC_SYS_CD_SK,
B.EXPRNC_CAT_CD,
B.EXPRNC_CAT_SK
FROM  {IDSOwner}.PROD A, {IDSOwner}.EXPRNC_CAT B , {IDSOwner}.BILL_INCM_RCPT C
WHERE 
A.PROD_SK=C.PROD_SK AND 
A.SRC_SYS_CD_SK=C.SRC_SYS_CD_SK AND
A.PROD_EFF_DT_SK<=C.BILL_DUE_DT_SK AND 
A.PROD_TERM_DT_SK>= C.BILL_DUE_DT_SK AND 
A.EXPRNC_CAT_SK=B.EXPRNC_CAT_SK
and C.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ComsnBeginCycle}
"""
df_db2_EXPRNC_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EXPRNC_CAT_in)
    .load()
)

extract_query_db2_PROD_BILL_CMPNT_in = f"""SELECT DISTINCT
Y.PROD_BILL_CMPNT_ID AS PROD_BILL_CMPNT_ID,
Y.PROD_BILL_CMPNT_COV_TYP_CD_SK AS PROD_BILL_CMPNT_COV_TYP_CD_SK
FROM(
   SELECT
   X.PROD_BILL_CMPNT_ID PROD_BILL_CMPNT_ID,
   X.PROD_BILL_CMPNT_COV_TYP_CD_SK1 PROD_BILL_CMPNT_COV_TYP_CD_SK, 
   DENSE_RANK() OVER (PARTITION BY X.PROD_BILL_CMPNT_ID ORDER BY X.PROD_BILL_CMPNT_EFF_DT_SK DESC, X.PROD_BILL_CMPNT_COV_TYP_CD_SK DESC) AS RANK_DATA
   FROM (
     SELECT
     PROD_BILL_CMPNT_ID,
     PROD_BILL_CMPNT_COV_TYP_CD_SK PROD_BILL_CMPNT_COV_TYP_CD_SK1, 
     PROD_BILL_CMPNT_EFF_DT_SK,
     CASE WHEN PROD_BILL_CMPNT_COV_TYP_CD_SK < 0 THEN
       (-1)*PROD_BILL_CMPNT_COV_TYP_CD_SK
       ELSE PROD_BILL_CMPNT_COV_TYP_CD_SK END AS PROD_BILL_CMPNT_COV_TYP_CD_SK
     FROM {IDSOwner}.PROD_BILL_CMPNT
   )X
)Y
WHERE Y.RANK_DATA = 1
"""
df_db2_PROD_BILL_CMPNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_BILL_CMPNT_in)
    .load()
)

extract_query_db2_SUB_ADDR_in = f"""SELECT 
BILL.BILL_ENTY_SK BILL_ENTY_SK,
BILL.SUB_SK SUB_SK,
ADDR.SUB_ADDR_CD_SK SUB_ADDR_CD_SK,
MAP1.TRGT_CD SUB_ADDR_CD,
ADDR.SUB_ADDR_ST_CD_SK SUB_ADDR_ST_CD_SK,
MAP2.TRGT_CD SUB_ADDR_ST_CD
FROM 
{IDSOwner}.BILL_ENTY       BILL, 
{IDSOwner}.SUB_ADDR       ADDR, 
{IDSOwner}.CD_MPPNG      MAP1, 
{IDSOwner}.CD_MPPNG      MAP2  
WHERE BILL.SUB_SK = ADDR.SUB_SK
AND ADDR.SUB_ADDR_CD_SK = MAP1.CD_MPPNG_SK
AND MAP1.TRGT_CD = 'SUBHOME'
AND ADDR.SUB_ADDR_ST_CD_SK = MAP2.CD_MPPNG_SK
"""
df_db2_SUB_ADDR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_SUB_ADDR_in)
    .load()
)

df_cpy_cd_mppng_Ref_AcctActCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_ActSrcCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_LobCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_ActvtyTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_AcctTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_BillEntyCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_CovTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_GrpBus_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_MbrData_joined = df_db2_BILL_INCM_RCPT_in.alias("lnk_IdsEdwRcvdIncmFExtr_InABC") \
    .join(
        df_cpy_cd_mppng_Ref_AcctActCd_Lkup.alias("Ref_AcctActCd_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACCT_ACTV_CD_SK") == F.col("Ref_AcctActCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_AcctTypCd_Lkup.alias("Ref_AcctTypCd_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACCT_TYP_CD_SK") == F.col("Ref_AcctTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_ActSrcCd_Lkup.alias("Ref_ActSrcCd_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACTV_SRC_CD_SK") == F.col("Ref_ActSrcCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_ActvtyTypCd_Lkup.alias("Ref_ActvtyTypCd_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACTV_TYP_CD_SK") == F.col("Ref_ActvtyTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_LobCd_Lkup.alias("Ref_LobCd_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_LOB_CD_SK") == F.col("Ref_LobCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_db2_EXPRNC_CAT_in.alias("Ref_ExprncCat_Lkup"),
        (F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.PROD_SK") == F.col("Ref_ExprncCat_Lkup.PROD_SK")) &
        (F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.SRC_SYS_CD_SK") == F.col("Ref_ExprncCat_Lkup.SRC_SYS_CD_SK")),
        "left"
    ) \
    .join(
        df_db2_PROD_BILL_CMPNT_in.alias("Ref_ProdBillCmpnt_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.PROD_BILL_CMPNT_ID") == F.col("Ref_ProdBillCmpnt_Lkup.PROD_BILL_CMPNT_ID"),
        "left"
    ) \
    .join(
        df_db2_BILL_ENTY_in.alias("Ref_BillEnty_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_ENTY_SK") == F.col("Ref_BillEnty_Lkup.BILL_ENTY_SK"),
        "left"
    ) \
    .join(
        df_db2_PROD_SH_NM_in.alias("Ref_ProdShNm_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.PROD_SK") == F.col("Ref_ProdShNm_Lkup.PROD_SK"),
        "left"
    ) \
    .join(
        df_db2_CLS_PLN_in.alias("Ref_ClsPln_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.CLS_PLN_SK") == F.col("Ref_ClsPln_Lkup.CLS_PLN_SK"),
        "left"
    ) \
    .join(
        df_db2_FEE_DSCNT_in.alias("Ref_FeeDscnt_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.FEE_DSCNT_SK") == F.col("Ref_FeeDscnt_Lkup.FEE_DSCNT_SK"),
        "left"
    ) \
    .join(
        df_db2_FNCL_LOB_in.alias("Ref_FnclLob_Lkup"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.FNCL_LOB_SK") == F.col("Ref_FnclLob_Lkup.FNCL_LOB_SK"),
        "left"
    ) \
    .join(
        # This join references "lnk_Mbr_XfmData_out.GRP_BUS_SUB_CAT_SH_NM_CD_SK" which doesn't exist yet in the DF,
        # but per instructions we do not skip or alter logic:
        df_db2_GRP_BUS_in.alias("Ref_GrpBus_Lkup"),
        (F.col("lnk_Mbr_XfmData_out.GRP_BUS_SUB_CAT_SH_NM_CD_SK") == F.col("Ref_GrpBus_Lkup.CD_MPPNG_SK")) &
        (F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_ENTY_SK") == F.col("Ref_GrpBus_Lkup.BILL_ENTY_SK")),
        "left"
    ) \
    .join(
        df_db2_SUB_ADDR_in.alias("lnk_SubAddr_out"),
        F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_ENTY_SK") == F.col("lnk_SubAddr_out.BILL_ENTY_SK"),
        "left"
    )

df_lkp_MbrData_out = df_lkp_MbrData_joined.select(
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_SK").alias("RCVD_INCM_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_CNTR_ID").alias("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.CRT_DTM").alias("CRT_DTM"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("Ref_ExprncCat_Lkup.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("Ref_BillEnty_Lkup.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.PROD_SK").alias("PROD_SK"),
    F.col("Ref_ProdShNm_Lkup.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("Ref_BillEnty_Lkup.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("Ref_AcctActCd_Lkup.TRGT_CD").alias("BILL_INCM_RCPT_ACCT_ACTVTY_CD"),
    F.col("Ref_AcctTypCd_Lkup.TRGT_CD").alias("BILL_INCM_RCPT_ACCT_TYP_CD"),
    F.col("Ref_ActSrcCd_Lkup.TRGT_CD").alias("BILL_INCM_RCPT_ACTVTY_SRC_CD"),
    F.col("Ref_ActvtyTypCd_Lkup.TRGT_CD").alias("BILL_INCM_RCPT_ACTVTY_TYP_CD"),
    F.col("Ref_LobCd_Lkup.TRGT_CD").alias("BILL_INCM_RCPT_LOB_CD"),
    F.col("Ref_ExprncCat_Lkup.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.FIRST_YR_IN").alias("BILL_INCM_RCPT_FIRST_YR_IN"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.POSTED_DT_SK").alias("BILL_INCM_RCPT_DT_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.ERN_INCM_AMT").alias("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.RVNU_RCVD_AMT").alias("BILL_INCM_RCPT_RVNU_RCVD_AMT"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.GL_NO").alias("BILL_INCM_RCPT_GL_NO"),
    F.col("Ref_ClsPln_Lkup.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Ref_FeeDscnt_Lkup.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("Ref_FnclLob_Lkup.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("Ref_GrpBus_Lkup.GRP_ID").alias("GRP_ID"),
    F.col("Ref_GrpBus_Lkup.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("Ref_ProdShNm_Lkup.PROD_ID").alias("PROD_ID"),
    F.col("Ref_ProdShNm_Lkup.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("Ref_BillEnty_Lkup.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Ref_BillEnty_Lkup.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("Ref_BillEnty_Lkup.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Ref_BillEnty_Lkup.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACCT_ACTV_CD_SK").alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACCT_TYP_CD_SK").alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACTV_SRC_CD_SK").alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_ACTV_TYP_CD_SK").alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
    F.col("lnk_IdsEdwRcvdIncmFExtr_InABC.BILL_INCM_RCPT_LOB_CD_SK").alias("BILL_INCM_RCPT_LOB_CD_SK"),
    F.col("Ref_GrpBus_Lkup.GRP_BUS_SUB_CAT_SH_NM_CD_SK").alias("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("Ref_BillEnty_Lkup.SUB_SK").alias("SUB_SK"),
    F.col("Ref_ProdBillCmpnt_Lkup.PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("Ref_GrpBus_Lkup.DP_IN").alias("DP_IN"),
    F.col("Ref_GrpBus_Lkup.GRP_ST_CD_SK").alias("GRP_ST_CD_SK"),
    F.col("Ref_GrpBus_Lkup.GRP_ST_CD").alias("GRP_ST_CD"),
    F.col("lnk_SubAddr_out.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    F.col("lnk_SubAddr_out.SUB_ADDR_ST_CD").alias("SUB_ADDR_ST_CD")
)

df_xfrm_BusinessLogic1 = df_lkp_MbrData_out.select(
    F.col("RCVD_INCM_SK").alias("RCVD_INCM_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.concat(F.col("BILL_DUE_DT_SK").substr(F.lit(1), F.lit(4)), F.col("BILL_DUE_DT_SK").substr(F.lit(6), F.lit(2))).alias("DUE_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_BILL_CNTR_ID").alias("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.concat(F.col("CRT_DT_SK").substr(F.lit(1), F.lit(4)), F.col("CRT_DT_SK").substr(F.lit(6), F.lit(2))).alias("RCPT_YR_MO_SK"),
    F.col("CRT_DTM").alias("BILL_INCM_RCPT_CRT_DTM"),
    F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.when(F.col("RCVD_INCM_SK") == 1, F.lit(1)).otherwise(F.col("EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.when(F.col("BILL_INCM_RCPT_ACCT_ACTVTY_CD").isNull(), "UNK").otherwise(F.trim(F.col("BILL_INCM_RCPT_ACCT_ACTVTY_CD"))).alias("BILL_INCM_RCPT_ACCT_ACTVTY_CD"),
    F.when(F.col("BILL_INCM_RCPT_ACCT_TYP_CD").isNull(), "UNK").otherwise(F.trim(F.col("BILL_INCM_RCPT_ACCT_TYP_CD"))).alias("BILL_INCM_RCPT_ACCT_TYP_CD"),
    F.when(F.col("BILL_INCM_RCPT_ACTVTY_SRC_CD").isNull(), "UNK").otherwise(F.trim(F.col("BILL_INCM_RCPT_ACTVTY_SRC_CD"))).alias("BILL_INCM_RCPT_ACTVTY_SRC_CD"),
    F.when(F.col("BILL_INCM_RCPT_ACTVTY_TYP_CD").isNull(), "UNK").otherwise(F.trim(F.col("BILL_INCM_RCPT_ACTVTY_TYP_CD"))).alias("BILL_INCM_RCPT_ACTVTY_TYP_CD"),
    F.when(F.col("BILL_INCM_RCPT_LOB_CD").isNull(), "UNK").otherwise(F.trim(F.col("BILL_INCM_RCPT_LOB_CD"))).alias("BILL_INCM_RCPT_LOB_CD"),
    F.col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("BILL_INCM_RCPT_FIRST_YR_IN").alias("BILL_INCM_RCPT_FIRST_YR_IN"),
    F.col("BILL_DUE_DT_SK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("CRT_DTM").alias("BILL_INCM_RCPT_CRT_DT_SK"),
    F.col("BILL_INCM_RCPT_DT_SK").alias("BILL_INCM_RCPT_DT_SK"),
    F.col("BILL_INCM_RCPT_ERN_INCM_AMT").alias("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("BILL_INCM_RCPT_RVNU_RCVD_AMT").alias("BILL_INCM_RCPT_RVNU_RCVD_AMT"),
    F.trim(F.col("BILL_INCM_RCPT_GL_NO")).alias("BILL_INCM_RCPT_GL_NO"),
    F.when(F.trim(F.col("CLS_PLN_ID")) == "", "UNK").otherwise(F.col("CLS_PLN_ID")).alias("CLS_PLN_ID"),
    F.when(F.trim(F.col("FEE_DSCNT_ID")) == "", "NA").otherwise(F.col("FEE_DSCNT_ID")).alias("FEE_DSCNT_ID"),
    F.when(F.trim(F.col("FNCL_LOB_CD")) == "", "NA").otherwise(F.col("FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
    F.when(F.trim(F.col("GRP_ID")) == "", "UNK").otherwise(F.trim(F.col("GRP_ID"))).alias("GRP_ID"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.when(F.trim(F.col("PROD_BILL_CMPNT_ID")) == "", "NA").otherwise(F.col("PROD_BILL_CMPNT_ID")).alias("PROD_BILL_CMPNT_ID"),
    F.when(F.trim(F.col("PROD_ID")) == "", "UNK").otherwise(F.trim(F.col("PROD_ID"))).alias("PROD_ID"),
    F.when(F.trim(F.col("PROD_SH_NM")) == "", "UNK").otherwise(F.trim(F.col("PROD_SH_NM"))).alias("PROD_SH_NM"),
    F.when(F.trim(F.col("SUBGRP_ID")) == "", "UNK").otherwise(F.trim(F.col("SUBGRP_ID"))).alias("SUBGRP_ID"),
    F.col("SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("BILL_ENTY_BILL_LVL_CD_SK").alias("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_ACTV_CD_SK").alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_TYP_CD_SK").alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_SRC_CD_SK").alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_TYP_CD_SK").alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_LOB_CD_SK").alias("BILL_INCM_RCPT_LOB_CD_SK"),
    F.col("GRP_BUS_SUB_CAT_SH_NM_CD_SK").alias("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.when(F.trim(F.col("DP_IN")) == "", F.lit(1)).otherwise(
        F.when(F.col("DP_IN") == "Y",
               F.when(F.col("SUB_ADDR_ST_CD_SK").isNull(), F.lit(0))
               .otherwise(F.col("SUB_ADDR_ST_CD_SK")))
        .otherwise(F.col("GRP_ST_CD_SK"))
    ).alias("SUB_CNTR_ST_CD_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.when(F.trim(F.col("DP_IN")) == "", F.lit("NA")).otherwise(
        F.when(F.col("DP_IN") == "Y",
               F.when(F.col("SUB_ADDR_ST_CD").isNull(), "UNK")
               .otherwise(F.col("SUB_ADDR_ST_CD")))
        .otherwise(
            F.when(F.col("GRP_ST_CD").isNull() | (F.trim(F.col("GRP_ST_CD")) == ""), "UNK")
            .otherwise(F.trim(F.col("GRP_ST_CD")))
        )
    ).alias("SUB_CNTR_ST_CD"),
    F.col("PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK")
)

df_lkp_Codes_joined = df_xfrm_BusinessLogic1.alias("lnk_Mbr_XfmData_out") \
    .join(
        df_cpy_cd_mppng_Ref_BillEntyCd_Lkup.alias("Ref_BillEntyCd_Lkup"),
        F.col("lnk_Mbr_XfmData_out.BILL_ENTY_BILL_LVL_CD_SK") == F.col("Ref_BillEntyCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_CovTypCd_Lkup.alias("Ref_CovTypCd_Lkup"),
        F.col("lnk_Mbr_XfmData_out.PROD_BILL_CMPNT_COV_TYP_CD_SK") == F.col("Ref_CovTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_GrpBus_Lkup.alias("Ref_GrpBus_Lkup"),
        (F.col("lnk_Mbr_XfmData_out.GRP_BUS_SUB_CAT_SH_NM_CD_SK") == F.col("Ref_GrpBus_Lkup.CD_MPPNG_SK")) &
        (F.col("df_db2_BILL_INCM_RCPT_in.BILL_ENTY_SK").isNotNull()),  # Attempt to keep the same logic, though original references an unlinked stage
        "left"
    ) \
    .join(
        df_db2_GRP_in.alias("Ref_Grp_Lkup"),
        F.col("lnk_Mbr_XfmData_out.GRP_SK") == F.col("Ref_Grp_Lkup.GRP_SK"),
        "left"
    ) \
    .join(
        df_db2_BILL_ENTY_FutureDate_in.alias("Ref_BillEntyFutureDate"),
        F.col("lnk_Mbr_XfmData_out.BILL_ENTY_SK") == F.col("Ref_BillEntyFutureDate.BILL_ENTY_SK"),
        "left"
    )

df_lkp_Codes_out = df_lkp_Codes_joined.select(
    F.col("lnk_Mbr_XfmData_out.RCVD_INCM_SK").alias("RCVD_INCM_SK"),
    F.col("lnk_Mbr_XfmData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_Mbr_XfmData_out.DUE_YR_MO_SK").alias("DUE_YR_MO_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_BILL_CNTR_ID").alias("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.col("lnk_Mbr_XfmData_out.RCPT_YR_MO_SK").alias("RCPT_YR_MO_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_CRT_DTM").alias("BILL_INCM_RCPT_CRT_DTM"),
    F.col("lnk_Mbr_XfmData_out.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_Mbr_XfmData_out.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("lnk_Mbr_XfmData_out.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_Mbr_XfmData_out.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_Mbr_XfmData_out.GRP_SK").alias("GRP_SK"),
    F.col("lnk_Mbr_XfmData_out.PROD_SK").alias("PROD_SK"),
    F.col("lnk_Mbr_XfmData_out.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_Mbr_XfmData_out.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("Ref_BillEntyCd_Lkup.TRGT_CD").alias("BILL_ENTY_BILL_LVL_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACCT_ACTVTY_CD").alias("BILL_INCM_RCPT_ACCT_ACTVTY_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACCT_TYP_CD").alias("BILL_INCM_RCPT_ACCT_TYP_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACTVTY_SRC_CD").alias("BILL_INCM_RCPT_ACTVTY_SRC_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACTVTY_TYP_CD").alias("BILL_INCM_RCPT_ACTVTY_TYP_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_LOB_CD").alias("BILL_INCM_RCPT_LOB_CD"),
    F.col("Ref_CovTypCd_Lkup.TRGT_CD").alias("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.col("lnk_Mbr_XfmData_out.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("Ref_GrpBus_Lkup.TRGT_CD").alias("GRP_BUS_SUB_CAT_SH_NM_CD"),
    F.col("Ref_Grp_Lkup.TRGT_CD").alias("PRNT_GRP_BUS_CAT_CD"),
    F.col("lnk_Mbr_XfmData_out.SUB_CNTR_ST_CD").alias("SUB_CNTR_ST_CD"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_FIRST_YR_IN").alias("BILL_INCM_RCPT_FIRST_YR_IN"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_BILL_DUE_DT_SK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_CRT_DT_SK").alias("BILL_INCM_RCPT_CRT_DT_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_DT_SK").alias("BILL_INCM_RCPT_DT_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ERN_INCM_AMT").alias("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_RVNU_RCVD_AMT").alias("BILL_INCM_RCPT_RVNU_RCVD_AMT"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_GL_NO").alias("BILL_INCM_RCPT_GL_NO"),
    F.col("lnk_Mbr_XfmData_out.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("lnk_Mbr_XfmData_out.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("lnk_Mbr_XfmData_out.GRP_ID").alias("GRP_ID"),
    F.col("lnk_Mbr_XfmData_out.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("Ref_Grp_Lkup.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
    F.col("lnk_Mbr_XfmData_out.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_Mbr_XfmData_out.PROD_ID").alias("PROD_ID"),
    F.col("lnk_Mbr_XfmData_out.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("lnk_Mbr_XfmData_out.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_Mbr_XfmData_out.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
    F.col("lnk_Mbr_XfmData_out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_Mbr_XfmData_out.BILL_ENTY_BILL_LVL_CD_SK").alias("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACCT_ACTV_CD_SK").alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACCT_TYP_CD_SK").alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACTV_SRC_CD_SK").alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_ACTV_TYP_CD_SK").alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.BILL_INCM_RCPT_LOB_CD_SK").alias("BILL_INCM_RCPT_LOB_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.GRP_BUS_SUB_CAT_SH_NM_CD_SK").alias("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("Ref_Grp_Lkup.PRNT_GRP_BUS_CAT_CD_SK").alias("PRNT_GRP_BUS_CAT_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.SUB_CNTR_ST_CD_SK").alias("SUB_CNTR_ST_CD_SK"),
    F.col("lnk_Mbr_XfmData_out.SUB_SK").alias("SUB_SK"),
    F.col("Ref_BillEntyFutureDate.SUBGRP_SK").alias("FD_SUBGRP_SK"),
    F.col("Ref_BillEntyFutureDate.SUBGRP_ID").alias("FD_SUBGRP_ID"),
    F.col("Ref_BillEntyFutureDate.SUBGRP_UNIQ_KEY").alias("FD_SUBGRP_UNIQ_KEY"),
    F.col("Ref_BillEntyFutureDate.EFF_DT_SK").alias("FD_EFF_DT_SK"),
    F.col("Ref_BillEntyFutureDate.TERM_DT_SK").alias("FD_TERM_DT_SK")
)

df_xfrm_BusinessLogic2_lnk_Main_Data_in = df_lkp_Codes_out.filter(
    "(RCVD_INCM_SK <> 0 AND RCVD_INCM_SK <> 1)"
)

# DataStage constraint "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1" cannot be directly replicated; 
# we will emulate "only the first row" with a row_number approach:
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

df_xfrm_BusinessLogic2_temp = df_lkp_Codes_out.withColumn("_rownum", F.row_number().over(Window.orderBy(F.lit(1))))
df_xfrm_BusinessLogic2_lnk_UNK_out = df_xfrm_BusinessLogic2_temp.filter(F.col("_rownum") == 1).drop("_rownum")

df_xfrm_BusinessLogic2_temp2 = df_lkp_Codes_out.withColumn("_rownum2", F.row_number().over(Window.orderBy(F.lit(1))))
df_xfrm_BusinessLogic2_lnk_NA_out = df_xfrm_BusinessLogic2_temp2.filter(F.col("_rownum2") == 1).drop("_rownum2")

df_fnl_Data = (
    df_xfrm_BusinessLogic2_lnk_UNK_out.unionByName(df_xfrm_BusinessLogic2_lnk_Main_Data_in, allowMissingColumns=True)
    .unionByName(df_xfrm_BusinessLogic2_lnk_NA_out, allowMissingColumns=True)
)

df_cpy_Main_Data_lnk_IdsEdwBRcvdIncmFExtr_OutABC = df_fnl_Data.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("DUE_YR_MO_SK"), 6, " ").alias("DUE_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.rpad(F.col("RCPT_YR_MO_SK"), 6, " ").alias("RCPT_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_CRT_DTM"),
    F.col("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("FNCL_LOB_CD")
)

df_cpy_Main_Data_lnk_IdsEdwRcvdIncmFExtr_OutABC = df_fnl_Data.select(
    F.col("RCVD_INCM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("DUE_YR_MO_SK"), 6, " ").alias("DUE_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.rpad(F.col("RCPT_YR_MO_SK"), 6, " ").alias("RCPT_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_CRT_DTM"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("GRP_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("SUBGRP_SK"),
    F.col("BILL_ENTY_BILL_LVL_CD"),
    F.col("BILL_INCM_RCPT_ACCT_ACTVTY_CD"),
    F.col("BILL_INCM_RCPT_ACCT_TYP_CD"),
    F.col("BILL_INCM_RCPT_ACTVTY_SRC_CD"),
    F.col("BILL_INCM_RCPT_ACTVTY_TYP_CD"),
    F.col("BILL_INCM_RCPT_LOB_CD"),
    F.col("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.col("EXPRNC_CAT_CD"),
    F.col("GRP_BUS_SUB_CAT_SH_NM_CD"),
    F.col("PRNT_GRP_BUS_CAT_CD"),
    F.col("SUB_CNTR_ST_CD"),
    F.rpad(F.col("BILL_INCM_RCPT_FIRST_YR_IN"), 1, " ").alias("BILL_INCM_RCPT_FIRST_YR_IN"),
    F.rpad(F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.rpad(F.col("BILL_INCM_RCPT_CRT_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_CRT_DT_SK"),
    F.rpad(F.col("BILL_INCM_RCPT_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_DT_SK"),
    F.col("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("BILL_INCM_RCPT_RVNU_RCVD_AMT"),
    F.col("BILL_INCM_RCPT_GL_NO"),
    F.col("CLS_PLN_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("FNCL_LOB_CD"),
    F.col("GRP_ID"),
    F.col("GRP_UNIQ_KEY"),
    F.col("PRNT_GRP_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("PROD_ID"),
    F.col("PROD_SH_NM"),
    F.col("SUBGRP_ID"),
    F.col("SUBGRP_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_LOB_CD_SK"),
    F.col("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("PRNT_GRP_BUS_CAT_CD_SK"),
    F.col("SUB_CNTR_ST_CD_SK"),
    F.col("SUB_SK")
)

df_seq_RCVD_INCM_F_csv_load = df_cpy_Main_Data_lnk_IdsEdwRcvdIncmFExtr_OutABC.select(
    F.rpad(F.col("RCVD_INCM_SK"), 0, " ").alias("RCVD_INCM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("DUE_YR_MO_SK"), 6, " ").alias("DUE_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.rpad(F.col("RCPT_YR_MO_SK"), 6, " ").alias("RCPT_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_CRT_DTM"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("GRP_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("SUBGRP_SK"),
    F.col("BILL_ENTY_BILL_LVL_CD"),
    F.col("BILL_INCM_RCPT_ACCT_ACTVTY_CD"),
    F.col("BILL_INCM_RCPT_ACCT_TYP_CD"),
    F.col("BILL_INCM_RCPT_ACTVTY_SRC_CD"),
    F.col("BILL_INCM_RCPT_ACTVTY_TYP_CD"),
    F.col("BILL_INCM_RCPT_LOB_CD"),
    F.col("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.col("EXPRNC_CAT_CD"),
    F.col("GRP_BUS_SUB_CAT_SH_NM_CD"),
    F.col("PRNT_GRP_BUS_CAT_CD"),
    F.col("SUB_CNTR_ST_CD"),
    F.rpad(F.col("BILL_INCM_RCPT_FIRST_YR_IN"), 1, " ").alias("BILL_INCM_RCPT_FIRST_YR_IN"),
    F.rpad(F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.rpad(F.col("BILL_INCM_RCPT_CRT_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_CRT_DT_SK"),
    F.rpad(F.col("BILL_INCM_RCPT_DT_SK"), 10, " ").alias("BILL_INCM_RCPT_DT_SK"),
    F.col("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("BILL_INCM_RCPT_RVNU_RCVD_AMT"),
    F.col("BILL_INCM_RCPT_GL_NO"),
    F.col("CLS_PLN_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("FNCL_LOB_CD"),
    F.col("GRP_ID"),
    F.col("GRP_UNIQ_KEY"),
    F.col("PRNT_GRP_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("PROD_ID"),
    F.col("PROD_SH_NM"),
    F.col("SUBGRP_ID"),
    F.col("SUBGRP_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_BILL_LVL_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
    F.col("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
    F.col("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
    F.col("BILL_INCM_RCPT_LOB_CD_SK"),
    F.col("GRP_BUS_SUB_CAT_SH_NM_CD_SK"),
    F.col("PRNT_GRP_BUS_CAT_CD_SK"),
    F.col("SUB_CNTR_ST_CD_SK"),
    F.col("SUB_SK")
)

df_seq_B_RCVD_INCM_F_csv_load = df_cpy_Main_Data_lnk_IdsEdwBRcvdIncmFExtr_OutABC.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("DUE_YR_MO_SK"), 6, " ").alias("DUE_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_BILL_CNTR_ID"),
    F.rpad(F.col("RCPT_YR_MO_SK"), 6, " ").alias("RCPT_YR_MO_SK"),
    F.col("BILL_INCM_RCPT_CRT_DTM"),
    F.col("BILL_INCM_RCPT_ERN_INCM_AMT"),
    F.col("FNCL_LOB_CD")
)

write_files(
    df_seq_RCVD_INCM_F_csv_load,
    f"{adls_path}/load/RCVD_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_seq_B_RCVD_INCM_F_csv_load,
    f"{adls_path}/load/B_RCVD_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)