# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwMbrEnrExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls updated records from IDS to create EDW file.  Pull is by run cycle.  A file is created of updated unique keys (BEKeys) so that those keys can be deleted from the EDW before re-extracting.  Because the effective date is part of the key, and effective date can change in Facets, whenever a member enroll record is updated all of the member enroll records for that member are rebuilt.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS: MBR_ENR
# MAGIC                         PROD
# MAGIC                         CLS
# MAGIC                         CLS_PLN
# MAGIC                         GRP
# MAGIC                         PROD_SH_NM
# MAGIC                         SUBGRP
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes
# MAGIC 7:hf_mbr_enrl_extr_sub_sk; hf_mbr_enrl_extr_products;hf_mbr_enrl_extr_grp;hf_mbr_enrl_extr_cls;hf_mbr_enrl_extr_excd;hf_mbr_enrl_extr_cls_pln;hf_mbr_enrl_extr_subgrp
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This program creates an exclusion file.  The member unique keys (bekeys) on this file need to be deleted from EDW before the new records are loaded.          
# MAGIC 
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Update file for IDS
# MAGIC                     Delete file for MBR_ENR_D records
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 8/2/2005       Steph Goddard     Originally programmed
# MAGIC 4/12/2006     Suzanne Saylor    Changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC Sharon Andrew   07/06/2006      Renamed from EdwMbrEnrExtr to IdsMbrEnrExtr.  
# MAGIC                                                            Added driver table processing for extractions
# MAGIC                                                           Seperated all inner joins in sql to each own extraction.
# MAGIC 11/16/2005   Bhoomi Dasari     Added new column from MBR_ENR.SRC_SYS_CRT_DT_SK to MBR_ENR_D
# MAGIC                                                   Renamed to NewIdsMbrEnrExtr
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             2008-02-27          3643                          Made changes to the code lookups                                          devlEDW                 Steph Goddard            03/04/2008
# MAGIC                                                                                                       which look for default non matchable rows
# MAGIC 
# MAGIC SAndrew                           2009-12-21      Mbr360 TTR-621           Added Mbr Enr State Continuation Code, Nm, and SK              devlEDW
# MAGIC 
# MAGIC Bhoomi Dasari                 2010 -09-16      4393/Tennesse table    Added Mbr Famliy Eligibilty Code, Name and Sk                     EnterpriseNewDevl     Steph Goddard          09/21/2010                                  
# MAGIC 
# MAGIC Srikanth Mettpalli             07/3/2013          5114                            Original Programming                                                              EnterpriseWrhsDevl 
# MAGIC                                                                                                        (Server to Parallel Conversion)      
# MAGIC                        
# MAGIC Sunitha P                          2021-08-12      US 417034                    Added new field  BILL_ELIG_IN                                              EnterpriseSITF                Goutham K                  8/16/2021

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC CD_MPPNG_SK
# MAGIC MBR_ENR_CLS_PLN_PROD_CAT_CD_SK
# MAGIC MBR_ENR_ELIG_OVRD_CD_SK
# MAGIC MBR_ENR_ELIG_RSN_CD_SK
# MAGIC MBR_ENR_ELIG_SRC_CD_SK
# MAGIC MBR_ENR_PRCS_STTUS_CD_SK
# MAGIC MBR_ENR_ST_CONT_CD_SK
# MAGIC MBR_ENR_FMLY_ELIG_CD_SK
# MAGIC Write MBR_ENR_D Data into a Sequential file for Load Job IdsEdwMbrEnrDLoad.
# MAGIC Read all the Data from IDS MBR_ENR Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrEnrDExtr
# MAGIC Read all the Data from IDS EGRP
# MAGIC EXCD
# MAGIC SUBGRP
# MAGIC CLS
# MAGIC CLS_PLN
# MAGIC PROD
# MAGIC PROD_SH_NM
# MAGIC W_MBR_DRVR
# MAGIC MBR Tables for Code SK Lookups.
# MAGIC Fkey Lookup for CD SK Mapping.
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC GRP_SK
# MAGIC SUBGRP_SK
# MAGIC CLS_SK
# MAGIC CLS_PLN_SK
# MAGIC PROD_SK
# MAGIC MBR_SK
# MAGIC EXCD_SK
# MAGIC INELGY_EXCD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG")
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_db2_MBR_ENR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT "
        f"ME.MBR_ENR_SK,"
        f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,"
        f"ME.MBR_UNIQ_KEY,"
        f"ME.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,"
        f"ME.EFF_DT_SK,"
        f"ME.CLS_PLN_SK,"
        f"ME.CLS_SK,"
        f"ME.GRP_SK,"
        f"ME.INELGY_EXCD_SK,"
        f"ME.MBR_SK,"
        f"ME.EXCD_SK,"
        f"ME.PROD_SK,"
        f"ME.SUBGRP_SK,"
        f"ME.COBRA_IN,"
        f"ME.ELIG_IN,"
        f"ME.MBR_ENR_ELIG_OVRD_CD_SK,"
        f"ME.MBR_ENR_ELIG_RSN_CD_SK,"
        f"ME.MBR_ENR_ELIG_SRC_CD_SK,"
        f"ME.PLN_ENTRY_DT_SK,"
        f"ME.MBR_ENR_PRCS_STTUS_CD_SK,"
        f"ME.SRC_SYS_CRT_DT_SK,"
        f"ME.TERM_DT_SK,"
        f"ME.MBR_ENR_ST_CONT_CD_SK,"
        f"ME.MBR_ENR_FMLY_ELIG_CD_SK,"
        f"ME.BILL_ELIG_IN "
        f"FROM {IDSOwner}.MBR_ENR ME "
        f"INNER JOIN {IDSOwner}.W_MBR_DRVR DRVR "
        f"ON DRVR.KEY_VAL_INT = ME.MBR_UNIQ_KEY "
        f"LEFT JOIN {IDSOwner}.CD_MPPNG CD "
        f"ON ME.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
        f"WHERE DRVR.SUBJ_NM = 'MBR_UNIQ_KEY'"
    )
    .load()
)

df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT GRP.GRP_SK,GRP.GRP_ID FROM {IDSOwner}.GRP GRP")
    .load()
)

df_db2_SUB_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT SUBGRP.SUBGRP_SK,SUBGRP.SUBGRP_ID FROM {IDSOwner}.SUBGRP SUBGRP")
    .load()
)

df_db2_CLS_PLN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLS_PLN.CLS_PLN_SK,CLS_PLN.CLS_PLN_ID FROM {IDSOwner}.CLS_PLN CLS_PLN")
    .load()
)

df_db2_PROD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT "
        f"PROD.PROD_SK,"
        f"PROD.PROD_ID,"
        f"PROD.PROD_SH_NM_SK,"
        f"PSN.PROD_SH_NM,"
        f"PSN.MCARE_SUPLMT_COV_IN "
        f"FROM {IDSOwner}.PROD PROD, {IDSOwner}.PROD_SH_NM PSN "
        f"WHERE PROD.PROD_SH_NM_SK = PSN.PROD_SH_NM_SK"
    )
    .load()
)

df_db2_CLS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLS.CLS_SK,CLS.CLS_ID FROM {IDSOwner}.CLS CLS")
    .load()
)

df_db2_SUB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT "
        f"MBR.MBR_SK,"
        f"MBR.MBR_UNIQ_KEY,"
        f"MBR.SUB_SK "
        f"FROM {IDSOwner}.W_MBR_DRVR DRVR, {IDSOwner}.MBR MBR "
        f"WHERE DRVR.SUBJ_NM = 'MBR_UNIQ_KEY' "
        f"AND DRVR.KEY_VAL_INT = MBR.MBR_UNIQ_KEY"
    )
    .load()
)

df_db2_EXCD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT EXCD.EXCD_SK,EXCD.EXCD_ID FROM {IDSOwner}.EXCD EXCD")
    .load()
)

df_cp_ExcdData = df_db2_EXCD_in.select(
    col("EXCD_SK"),
    col("EXCD_ID")
)

df_lkp_Codes1 = (
    df_db2_MBR_ENR_in.alias("lnk_IdsEdwMbrEnrDExtr_InABC")
    .join(
        df_db2_GRP_in.alias("Ref_GrpSk_Lkup"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.GRP_SK") == col("Ref_GrpSk_Lkup.GRP_SK"),
        "left"
    )
    .join(
        df_db2_SUB_GRP_in.alias("Ref_SubgrpSk_Lkp"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.SUBGRP_SK") == col("Ref_SubgrpSk_Lkp.SUBGRP_SK"),
        "left"
    )
    .join(
        df_db2_CLS_in.alias("Ref_ClsSk_Lkup"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.CLS_SK") == col("Ref_ClsSk_Lkup.CLS_SK"),
        "left"
    )
    .join(
        df_db2_CLS_PLN_in.alias("Ref_ClsPlnSk_Lkup"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.CLS_PLN_SK") == col("Ref_ClsPlnSk_Lkup.CLS_PLN_SK"),
        "left"
    )
    .join(
        df_db2_PROD_in.alias("Ref_ProdSk_Lkp"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.PROD_SK") == col("Ref_ProdSk_Lkp.PROD_SK"),
        "left"
    )
    .join(
        df_db2_SUB_in.alias("Ref_MbrSk_Lkp"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_SK") == col("Ref_MbrSk_Lkp.MBR_SK"),
        "left"
    )
    .join(
        df_cp_ExcdData.alias("Ref_ExcdSk_Lkp"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.EXCD_SK") == col("Ref_ExcdSk_Lkup.EXCD_SK"),
        "left"
    )
    .join(
        df_cp_ExcdData.alias("Ref_InelgyExcdSk_Lkp"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.INELGY_EXCD_SK") == col("Ref_InelgyExcdSk_Lkp.EXCD_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_SK").alias("MBR_ENR_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.CLS_SK").alias("CLS_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.GRP_SK").alias("GRP_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.INELGY_EXCD_SK").alias("INELGY_EXCD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_SK").alias("MBR_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.EXCD_SK").alias("EXCD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.PROD_SK").alias("PROD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.COBRA_IN").alias("COBRA_IN"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.ELIG_IN").alias("ELIG_IN"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_ELIG_OVRD_CD_SK").alias("MBR_ENR_ELIG_OVRD_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_ELIG_RSN_CD_SK").alias("MBR_ENR_ELIG_RSN_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_ELIG_SRC_CD_SK").alias("MBR_ENR_ELIG_SRC_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.PLN_ENTRY_DT_SK").alias("PLN_ENTRY_DT_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_PRCS_STTUS_CD_SK").alias("MBR_ENR_PRCS_STTUS_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_ST_CONT_CD_SK").alias("MBR_ENR_ST_CONT_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.MBR_ENR_FMLY_ELIG_CD_SK").alias("MBR_ENR_FMLY_ELIG_CD_SK"),
        col("lnk_IdsEdwMbrEnrDExtr_InABC.BILL_ELIG_IN").alias("BILL_ELIG_IN"),
        col("Ref_GrpSk_Lkup.GRP_ID").alias("GRP_ID"),
        col("Ref_SubgrpSk_Lkp.SUBGRP_ID").alias("SUBGRP_ID"),
        col("Ref_ClsSk_Lkup.CLS_ID").alias("CLS_ID"),
        col("Ref_ClsPlnSk_Lkup.CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("Ref_ProdSk_Lkp.PROD_ID").alias("PROD_ID"),
        col("Ref_ProdSk_Lkp.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("Ref_ProdSk_Lkp.PROD_SH_NM").alias("PROD_SH_NM"),
        col("Ref_ProdSk_Lkp.MCARE_SUPLMT_COV_IN").alias("MCARE_SUPLMT_COV_IN"),
        col("Ref_MbrSk_Lkp.SUB_SK").alias("SUB_SK"),
        col("Ref_ExcdSk_Lkup.EXCD_ID").alias("ME_EXCD_ID"),
        col("Ref_InelgyExcdSk_Lkp.EXCD_ID").alias("ME_INELGY_EXCD1_ID")
    )
)

df_lkp_Codes = (
    df_lkp_Codes1.alias("lnk_FkeyLkpData_out")
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrFmlyEligCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_FMLY_ELIG_CD_SK") == col("Ref_MbrEnrFmlyEligCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrEligOvrdCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_OVRD_CD_SK") == col("Ref_MbrEnrEligOvrdCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrPrcsSttusCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_PRCS_STTUS_CD_SK") == col("Ref_MbrEnrPrcsSttusCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrStContCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ST_CONT_CD_SK") == col("Ref_MbrEnrStContCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrEligSrcCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_SRC_CD_SK") == col("Ref_MbrEnrEligSrcCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdSubproMbrEnrEligRsnCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_RSN_CD_SK") == col("Ref_ProdSubproMbrEnrEligRsnCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_MbrEnrClsPlnProdCatCd_Lkup"),
        col("lnk_FkeyLkpData_out.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == col("Ref_MbrEnrClsPlnProdCatCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_FkeyLkpData_out.MBR_ENR_SK").alias("MBR_ENR_SK"),
        col("lnk_FkeyLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_FkeyLkpData_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("Ref_MbrEnrClsPlnProdCatCd_Lkup.TRGT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
        col("lnk_FkeyLkpData_out.EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        col("lnk_FkeyLkpData_out.CLS_SK").alias("CLS_SK"),
        col("lnk_FkeyLkpData_out.CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("lnk_FkeyLkpData_out.GRP_SK").alias("GRP_SK"),
        col("lnk_FkeyLkpData_out.INELGY_EXCD_SK").alias("INELGY_EXCD_SK"),
        col("lnk_FkeyLkpData_out.MBR_SK").alias("MBR_SK"),
        col("lnk_FkeyLkpData_out.EXCD_SK").alias("OVALL_ELIG_EXCD_SK"),
        col("lnk_FkeyLkpData_out.PROD_SK").alias("PROD_SK"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("lnk_FkeyLkpData_out.SUBGRP_SK").alias("SUBGRP_SK"),
        col("lnk_FkeyLkpData_out.CLS_ID").alias("CLS_ID"),
        col("lnk_FkeyLkpData_out.CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("lnk_FkeyLkpData_out.GRP_ID").alias("GRP_ID"),
        col("Ref_MbrEnrClsPlnProdCatCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
        col("lnk_FkeyLkpData_out.COBRA_IN").alias("MBR_ENR_COBRA_IN"),
        col("lnk_FkeyLkpData_out.ELIG_IN").alias("MBR_ENR_ELIG_IN"),
        col("Ref_MbrEnrEligOvrdCd_Lkup.TRGT_CD").alias("MBR_ENR_ELIG_OVRD_CD"),
        col("Ref_MbrEnrEligOvrdCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_ELIG_OVRD_NM"),
        col("Ref_ProdSubproMbrEnrEligRsnCd_Lkup.TRGT_CD").alias("MBR_ENR_ELIG_RSN_CD"),
        col("Ref_ProdSubproMbrEnrEligRsnCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_ELIG_RSN_NM"),
        col("Ref_MbrEnrEligSrcCd_Lkup.TRGT_CD").alias("MBR_ENR_ELIG_SRC_CD"),
        col("Ref_MbrEnrEligSrcCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_ELIG_SRC_NM"),
        col("lnk_FkeyLkpData_out.ME_INELGY_EXCD1_ID").alias("MBR_ENR_INELGY_EXCD"),
        col("lnk_FkeyLkpData_out.ME_EXCD_ID").alias("MBR_ENR_OVALL_ELIG_EXCD"),
        col("lnk_FkeyLkpData_out.PLN_ENTRY_DT_SK").alias("MBR_ENR_PLN_ENTRY_DT_SK"),
        col("Ref_MbrEnrPrcsSttusCd_Lkup.TRGT_CD").alias("MBR_ENR_PRCS_STTUS_CD"),
        col("Ref_MbrEnrPrcsSttusCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_PRCS_STTUS_NM"),
        col("lnk_FkeyLkpData_out.SRC_SYS_CRT_DT_SK").alias("MBR_ENR_SRC_SYS_CRT_DT_SK"),
        col("lnk_FkeyLkpData_out.TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        col("lnk_FkeyLkpData_out.PROD_ID").alias("PROD_ID"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM").alias("PROD_SH_NM"),
        col("lnk_FkeyLkpData_out.MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
        col("lnk_FkeyLkpData_out.SUBGRP_ID").alias("SUBGRP_ID"),
        col("lnk_FkeyLkpData_out.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_OVRD_CD_SK").alias("MBR_ENR_ELIG_OVRD_CD_SK"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_RSN_CD_SK").alias("MBR_ENR_ELIG_RSN_CD_SK"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ELIG_SRC_CD_SK").alias("MBR_ENR_ELIG_SRC_CD_SK"),
        col("lnk_FkeyLkpData_out.MBR_ENR_PRCS_STTUS_CD_SK").alias("MBR_ENR_PRCS_STTUS_CD_SK"),
        col("lnk_FkeyLkpData_out.SUB_SK").alias("SUB_SK"),
        col("lnk_FkeyLkpData_out.MBR_ENR_ST_CONT_CD_SK").alias("MBR_ENR_ST_CONT_CD_SK"),
        col("Ref_MbrEnrStContCd_Lkup.TRGT_CD").alias("MBR_ENR_ST_CONT_CD"),
        col("Ref_MbrEnrStContCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_ST_CONT_NM"),
        col("Ref_MbrEnrFmlyEligCd_Lkup.TRGT_CD").alias("MBR_ENR_FMLY_ELIG_CD"),
        col("Ref_MbrEnrFmlyEligCd_Lkup.TRGT_CD_NM").alias("MBR_ENR_FMLY_ELIG_NM"),
        col("lnk_FkeyLkpData_out.MBR_ENR_FMLY_ELIG_CD_SK").alias("MBR_ENR_FMLY_ELIG_CD_SK"),
        col("lnk_FkeyLkpData_out.BILL_ELIG_IN").alias("BILL_ELIG_IN")
    )
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    col("MBR_ENR_SK"),
    when(trim(col("SRC_SYS_CD")) == "", "NA").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    when(trim(col("MBR_ENR_CLS_PLN_PROD_CAT_CD")) == "", "NA").otherwise(col("MBR_ENR_CLS_PLN_PROD_CAT_CD")).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    col("MBR_ENR_EFF_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("INELGY_EXCD_SK"),
    col("MBR_SK"),
    col("OVALL_ELIG_EXCD_SK"),
    col("PROD_SK"),
    col("PROD_SH_NM_SK"),
    col("SUBGRP_SK"),
    when(trim(col("CLS_ID")) == "", "UNK").otherwise(trim(col("CLS_ID"))).alias("CLS_ID"),
    when(trim(col("CLS_PLN_ID")) == "", "UNK").otherwise(trim(col("CLS_PLN_ID"))).alias("CLS_PLN_ID"),
    when(trim(col("GRP_ID")) == "", "UNK").otherwise(trim(col("GRP_ID"))).alias("GRP_ID"),
    when(trim(col("MBR_ENR_CLS_PLN_PROD_CAT_NM")) == "", "NA").otherwise(col("MBR_ENR_CLS_PLN_PROD_CAT_NM")).alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    col("MBR_ENR_COBRA_IN"),
    col("MBR_ENR_ELIG_IN"),
    when(trim(col("MBR_ENR_ELIG_OVRD_CD")) == "", "NA").otherwise(col("MBR_ENR_ELIG_OVRD_CD")).alias("MBR_ENR_ELIG_OVRD_CD"),
    when(trim(col("MBR_ENR_ELIG_OVRD_NM")) == "", "NA").otherwise(col("MBR_ENR_ELIG_OVRD_NM")).alias("MBR_ENR_ELIG_OVRD_NM"),
    when(trim(col("MBR_ENR_ELIG_RSN_CD")) == "", "NA").otherwise(col("MBR_ENR_ELIG_RSN_CD")).alias("MBR_ENR_ELIG_RSN_CD"),
    when(trim(col("MBR_ENR_ELIG_RSN_NM")) == "", "NA").otherwise(col("MBR_ENR_ELIG_RSN_NM")).alias("MBR_ENR_ELIG_RSN_NM"),
    when(trim(col("MBR_ENR_ELIG_SRC_CD")) == "", "NA").otherwise(col("MBR_ENR_ELIG_SRC_CD")).alias("MBR_ENR_ELIG_SRC_CD"),
    when(trim(col("MBR_ENR_ELIG_SRC_NM")) == "", "NA").otherwise(col("MBR_ENR_ELIG_SRC_NM")).alias("MBR_ENR_ELIG_SRC_NM"),
    when(trim(col("MBR_ENR_INELGY_EXCD")) == "", "UNK").otherwise(trim(col("MBR_ENR_INELGY_EXCD"))).alias("MBR_ENR_INELGY_EXCD"),
    when(trim(col("MBR_ENR_OVALL_ELIG_EXCD")) == "", "UNK").otherwise(trim(col("MBR_ENR_OVALL_ELIG_EXCD"))).alias("MBR_ENR_OVALL_ELIG_EXCD"),
    col("MBR_ENR_PLN_ENTRY_DT_SK"),
    when(trim(col("MBR_ENR_PRCS_STTUS_CD")) == "", "NA").otherwise(col("MBR_ENR_PRCS_STTUS_CD")).alias("MBR_ENR_PRCS_STTUS_CD"),
    when(trim(col("MBR_ENR_PRCS_STTUS_NM")) == "", "NA").otherwise(col("MBR_ENR_PRCS_STTUS_NM")).alias("MBR_ENR_PRCS_STTUS_NM"),
    col("MBR_ENR_SRC_SYS_CRT_DT_SK"),
    col("MBR_ENR_TERM_DT_SK"),
    when(trim(col("PROD_ID")) == "", "UNK").otherwise(col("PROD_ID")).alias("PROD_ID"),
    when(trim(col("PROD_SH_NM")) == "", "UNK").otherwise(col("PROD_SH_NM")).alias("PROD_SH_NM"),
    when(trim(col("PROD_SHNM_MCARE_SUPLMT_COV_IN")) == "", "U").otherwise(col("PROD_SHNM_MCARE_SUPLMT_COV_IN")).alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    when(trim(col("SUBGRP_ID")) == "", "UNK").otherwise(trim(col("SUBGRP_ID"))).alias("SUBGRP_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    col("MBR_ENR_ELIG_OVRD_CD_SK"),
    col("MBR_ENR_ELIG_RSN_CD_SK"),
    col("MBR_ENR_ELIG_SRC_CD_SK"),
    col("MBR_ENR_PRCS_STTUS_CD_SK"),
    col("SUB_SK"),
    col("MBR_ENR_ST_CONT_CD_SK"),
    when(trim(col("MBR_ENR_ST_CONT_CD")) == "", "NA").otherwise(col("MBR_ENR_ST_CONT_CD")).alias("MBR_ENR_ST_CONT_CD"),
    when(trim(col("MBR_ENR_ST_CONT_NM")) == "", "NA").otherwise(col("MBR_ENR_ST_CONT_NM")).alias("MBR_ENR_ST_CONT_NM"),
    when(trim(col("MBR_ENR_FMLY_ELIG_CD")) == "", "UNK").otherwise(col("MBR_ENR_FMLY_ELIG_CD")).alias("MBR_ENR_FMLY_ELIG_CD"),
    when(trim(col("MBR_ENR_FMLY_ELIG_NM")) == "", "UNK").otherwise(col("MBR_ENR_FMLY_ELIG_NM")).alias("MBR_ENR_FMLY_ELIG_NM"),
    col("MBR_ENR_FMLY_ELIG_CD_SK"),
    col("BILL_ELIG_IN")
)

df_seq_MBR_ENR_D_csv_load = df_xfrm_BusinessLogic.select(
    col("MBR_ENR_SK"),
    when(trim(col("SRC_SYS_CD")) == "", "").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    when(trim(col("MBR_ENR_CLS_PLN_PROD_CAT_CD")) == "", "").otherwise(col("MBR_ENR_CLS_PLN_PROD_CAT_CD")).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    rpad(col("MBR_ENR_EFF_DT_SK"), 10, " ").alias("MBR_ENR_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("INELGY_EXCD_SK"),
    col("MBR_SK"),
    col("OVALL_ELIG_EXCD_SK"),
    col("PROD_SK"),
    col("PROD_SH_NM_SK"),
    col("SUBGRP_SK"),
    when(trim(col("CLS_ID")) == "", "").otherwise(trim(col("CLS_ID"))).alias("CLS_ID"),
    when(trim(col("CLS_PLN_ID")) == "", "").otherwise(trim(col("CLS_PLN_ID"))).alias("CLS_PLN_ID"),
    when(trim(col("GRP_ID")) == "", "").otherwise(trim(col("GRP_ID"))).alias("GRP_ID"),
    when(trim(col("MBR_ENR_CLS_PLN_PROD_CAT_NM")) == "", "").otherwise(col("MBR_ENR_CLS_PLN_PROD_CAT_NM")).alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    rpad(col("MBR_ENR_COBRA_IN"), 1, " ").alias("MBR_ENR_COBRA_IN"),
    rpad(col("MBR_ENR_ELIG_IN"), 1, " ").alias("MBR_ENR_ELIG_IN"),
    when(trim(col("MBR_ENR_ELIG_OVRD_CD")) == "", "").otherwise(col("MBR_ENR_ELIG_OVRD_CD")).alias("MBR_ENR_ELIG_OVRD_CD"),
    when(trim(col("MBR_ENR_ELIG_OVRD_NM")) == "", "").otherwise(col("MBR_ENR_ELIG_OVRD_NM")).alias("MBR_ENR_ELIG_OVRD_NM"),
    when(trim(col("MBR_ENR_ELIG_RSN_CD")) == "", "").otherwise(col("MBR_ENR_ELIG_RSN_CD")).alias("MBR_ENR_ELIG_RSN_CD"),
    when(trim(col("MBR_ENR_ELIG_RSN_NM")) == "", "").otherwise(col("MBR_ENR_ELIG_RSN_NM")).alias("MBR_ENR_ELIG_RSN_NM"),
    when(trim(col("MBR_ENR_ELIG_SRC_CD")) == "", "").otherwise(col("MBR_ENR_ELIG_SRC_CD")).alias("MBR_ENR_ELIG_SRC_CD"),
    when(trim(col("MBR_ENR_ELIG_SRC_NM")) == "", "").otherwise(col("MBR_ENR_ELIG_SRC_NM")).alias("MBR_ENR_ELIG_SRC_NM"),
    when(trim(col("MBR_ENR_INELGY_EXCD")) == "", "").otherwise(trim(col("MBR_ENR_INELGY_EXCD"))).alias("MBR_ENR_INELGY_EXCD"),
    when(trim(col("MBR_ENR_OVALL_ELIG_EXCD")) == "", "").otherwise(trim(col("MBR_ENR_OVALL_ELIG_EXCD"))).alias("MBR_ENR_OVALL_ELIG_EXCD"),
    rpad(col("MBR_ENR_PLN_ENTRY_DT_SK"), 10, " ").alias("MBR_ENR_PLN_ENTRY_DT_SK"),
    when(trim(col("MBR_ENR_PRCS_STTUS_CD")) == "", "").otherwise(col("MBR_ENR_PRCS_STTUS_CD")).alias("MBR_ENR_PRCS_STTUS_CD"),
    when(trim(col("MBR_ENR_PRCS_STTUS_NM")) == "", "").otherwise(col("MBR_ENR_PRCS_STTUS_NM")).alias("MBR_ENR_PRCS_STTUS_NM"),
    rpad(col("MBR_ENR_SRC_SYS_CRT_DT_SK"), 10, " ").alias("MBR_ENR_SRC_SYS_CRT_DT_SK"),
    rpad(col("MBR_ENR_TERM_DT_SK"), 10, " ").alias("MBR_ENR_TERM_DT_SK"),
    when(trim(col("PROD_ID")) == "", "").otherwise(col("PROD_ID")).alias("PROD_ID"),
    when(trim(col("PROD_SH_NM")) == "", "").otherwise(col("PROD_SH_NM")).alias("PROD_SH_NM"),
    rpad(
        when(trim(col("PROD_SHNM_MCARE_SUPLMT_COV_IN")) == "", "U")
        .otherwise(col("PROD_SHNM_MCARE_SUPLMT_COV_IN")), 1, " "
    ).alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    when(trim(col("SUBGRP_ID")) == "", "").otherwise(trim(col("SUBGRP_ID"))).alias("SUBGRP_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    col("MBR_ENR_ELIG_OVRD_CD_SK"),
    col("MBR_ENR_ELIG_RSN_CD_SK"),
    col("MBR_ENR_ELIG_SRC_CD_SK"),
    col("MBR_ENR_PRCS_STTUS_CD_SK"),
    col("SUB_SK"),
    col("MBR_ENR_ST_CONT_CD_SK"),
    when(trim(col("MBR_ENR_ST_CONT_CD")) == "", "").otherwise(col("MBR_ENR_ST_CONT_CD")).alias("MBR_ENR_ST_CONT_CD"),
    when(trim(col("MBR_ENR_ST_CONT_NM")) == "", "").otherwise(col("MBR_ENR_ST_CONT_NM")).alias("MBR_ENR_ST_CONT_NM"),
    when(trim(col("MBR_ENR_FMLY_ELIG_CD")) == "", "").otherwise(col("MBR_ENR_FMLY_ELIG_CD")).alias("MBR_ENR_FMLY_ELIG_CD"),
    when(trim(col("MBR_ENR_FMLY_ELIG_NM")) == "", "").otherwise(col("MBR_ENR_FMLY_ELIG_NM")).alias("MBR_ENR_FMLY_ELIG_NM"),
    col("MBR_ENR_FMLY_ELIG_CD_SK"),
    rpad(col("BILL_ELIG_IN"), 1, " ").alias("BILL_ELIG_IN")
)

write_files(
    df_seq_MBR_ENR_D_csv_load,
    f"{adls_path}/load/MBR_ENR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)