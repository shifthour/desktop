# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsMbrEligExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Membership Data Mart Member Eligibility extract from IDS to Data Mart.  
# MAGIC 
# MAGIC INPUTS:
# MAGIC                    IDS:  MBR_PCP
# MAGIC                    IDS:  GRP
# MAGIC                    IDS:  PROD
# MAGIC                    IDS:  CLS
# MAGIC                    IDS:  CLS_PLN
# MAGIC                    IDS:  EXCD
# MAGIC 
# MAGIC HASH FILES:         
# MAGIC                     hf_mbrsh_dm_grp - cleared in IdsMbrCobExtr
# MAGIC                     hf_mbrsh_dm_excd
# MAGIC                     hf_etrnl_cd_mppng
# MAGIC 
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                     Lookups to extract fields
# MAGIC              
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  MBRSH_DM_MBR_ELIG
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    Tao Luo:  Original Programming - 02/22/2006
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tracy Davis             4/17/2008        Web Realignment IAD (3500)            Added new fields:                                                                        devlIDSNew                    Steph Goddard          04/27/2009  
# MAGIC                                                                                                                            PROD_SH_NM
# MAGIC                                                                                                                            MBR_ENR_ACTV_ROW_IN
# MAGIC Ralph Tucker          6/11/2009        3500 - Web Realign                           Added new fields                                                                         devlIDSnew                     Steph Goddard          06/22/2009
# MAGIC 
# MAGIC Bhoomi D               07/08/2009       3500                                                  Added new Prod_lkup                                                                  devlIDSnew                      Steph Goddard         07/09/2009
# MAGIC 
# MAGIC Bhoomi D               07/14/2009      3500                                                   Changed SQL to incorporate LastUpdtRunCycle in                     devlIDSnew                       Steph Goddard         07/15/2009
# MAGIC                                                                                                                    Mbr delete process
# MAGIC 
# MAGIC Bhoomi D               08/20/2009      4113                                                   Added two new fields                                                                   devlIDSnew                       Steph Goddard        10/17/2009
# MAGIC                                                                                                                    MBR_ENR_ELIG_ST_CONT_CD,
# MAGIC                                                                                                                     MBR_ENR_ELIG_ST_CONT_NM
# MAGIC SAndrew               2009-10-27         4113 Mbr 360                            Changes due to production findins                                                    devlIDSnew                         Steph Goddard           10/28/2009
# MAGIC                                                                                                           Changed source of Member State Continous Code from IDS MBR_ELIG to be from IDS MBR_ENR.   This change eliminated some pre-processing lookups.
# MAGIC                                                                                                           Changed hf_mbrsh_dm_grp to be created in the process and not dependent up running a prereq job that pulled in distinct mbrs from mbr_enrl and their group data.  .
# MAGIC                                                                                                           Added group_sk to main extract for more efficient lookup.   Move trans logix that could be immediately resolved at beginning of process and not have two transforms to do business
# MAGIC                                                                                                           logix when possible.   
# MAGIC                                                                                                           Renamed all hash files  from 7;hf_mbrsh_dm_excd;hf_mbr_elig_prod;hf_mbr_elig_sub_grp;hf_mbrdm_elig_max_seqdtm;hf_mbrdm_elig_stcont;hf_mbrdm_elig_exprnccd_curr;hf_mbrdm_elig_exprnccd_term
# MAGIC                                                                                                           Removed IDS Last Run Cycle Parm since not used.
# MAGIC 
# MAGIC SAndrew                2009-12-02    4113 Mbr 360                                In the transformer lookup changed rule for the svMbrEligStatConCd for the state of Missouri from 
# MAGIC                                                            TTR-622                                 IF (svMbrEnrlEligRsnCd = 'MEM5' OR  svMbrEnrlEligRsnCd = 'MEM9' OR svMbrEnrlEligRsnCd = 'SBM5' OR svMbrEnrlEligRsnCd = 'SBM9') then 'MO'  else "  "
# MAGIC                                                                                                           to 
# MAGIC                                                                                                           iF (svMbrEnrlEligRsnCd = 'MEM5' OR svMbrEnrlEligRsnCd = 'MEM8' OR svMbrEnrlEligRsnCd = 'MEM9' OR svMbrEnrlEligRsnCd = 'SBM5' OR svMbrEnrlEligRsnCd = 'SBM8' OR svMbrEnrlEligRsnCd = 'SBM9') then 'MO'  else "  "
# MAGIC Sandrew                2009-12-21     4113 Mbr360                               TTR-621 changes override TTR-622.             Changed source of DM_MBR field State Cont Cd / Nm to be from IDS MBR_ENR.  
# MAGIC                                                         TTR-621                                   Added two stage varliables svMbrEnrlStateContCd and svMbrEnrlStateContNm
# MAGIC                                                                                                          removed ref link st_cont into transfomer TRANS which used to provide the value for this field
# MAGIC                                                                                                         removed build of hash file hf_dm_mbr_elig_st_cont_trgt_cd from ODBC call and to HASH.CLEAR 
# MAGIC 7;hf_dm_mbr_elig_sub_grp;hf_dm_mbr_elig_prod;hf_dm_mbr_elig_exprnccd_curr;hf_dm_mbr_elig_exprnccd_term;hf_dm_mbr_elig_excd;hf_dm_mbr_elig_group;hf_dm_mbr_elig_st_cont_trgt_cd
# MAGIC Shiva Devagiri           07/21/2013        5114                            Create Load File for EDW Table MBRSH_DM_MBR_ELIG       IntegrateWrhseDevl                          Peter Marshall            10/21/2013
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara          08/06/2014          TFS#9537                         Renamed the Load file with  file naming standards                                IntegrateNewDevl            Jag Yelavarthi           2014-08-06
# MAGIC 
# MAGIC Kalyan Neelam           2015-05-06           5398 DPS2E                      Added 3 new columns on the end - PROD_SH_NM_CAT_CD,            IntegrateNewDevl          Bhoomi Dasari           5/11/2015
# MAGIC Neelima Tummala     2021-10-03            US 433707                       Added 1 new columns on the end -BILL_ELIG_IN,                                 Integrate Dev1               Reddy Sanam           10/18/2021     
# MAGIC                                                                                                           PROD_SH_NM_CAT_NM,  PROD_SH_NM_MCARE_SUPLMT_COV_IN

# MAGIC Read from source tables
# MAGIC EXCD AND GRP AND MBR_ENR AND CD_MPPNG AND PROD_CMPNT AND PROD_BILL_CMPNT AND EXPRNC_CAt AND W_MBR_DM_DRVR AND PROD_SH_NM AND PROD AND SUBGRP from IDS.
# MAGIC Job Name: IdsDmMbrshDmMbrEligExtr
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK AND
# MAGIC SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBRSH_DM_MBR_ELIG Data into a Sequential file for Load Job IdsDmMbrshpDmMbrEligLoad.
# MAGIC Reference Data for Code SK lookups
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, rpad, to_date, to_timestamp
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')

# Database config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# db2_CD_MPPING_In
# --------------------------------------------------------------------------------
df_db2_CD_MPPING_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
            CD_MPPNG_SK,
            COALESCE(TRGT_CD,'UNK') TRGT_CD,
            COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
        FROM {IDSOwner}.CD_MPPNG
        """
    )
    .load()
)

# copy_cd_mppng (PxCopy) has multiple output links, each with same columns
df_copy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup = df_db2_CD_MPPING_In.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_copy_cd_mppng_state_cont_cd = df_db2_CD_MPPING_In.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_copy_cd_mppng_prodshnmcatcd = df_db2_CD_MPPING_In.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_PROD_SH_NM_in
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_PROD_SH_NM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
           PROD.PROD_SK,
           PROD_SH_NM.PROD_SH_NM,
           PROD_SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK,
           PROD.PROD_ID,
           PROD.SUBPROD_CD_SK,
           PROD.PROD_DESC,
           PROD_SH_NM.MCARE_SUPLMT_COV_IN,
           PROD_SH_NM.PROD_SH_NM_CAT_CD_SK
        FROM 
           {IDSOwner}.PROD_SH_NM PROD_SH_NM,
           {IDSOwner}.PROD PROD
        WHERE
           PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
        """
    )
    .load()
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_GRP_in
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
          GRP.GRP_SK,
          GRP.GRP_ID,
          GRP.GRP_UNIQ_KEY,
          GRP.GRP_NM
        FROM {IDSOwner}.GRP GRP
        """
    )
    .load()
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_MBR_ELIG_in
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_MBR_ELIG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
          MBR_ENR.MBR_UNIQ_KEY,
          MBR_ENR.MBR_SK,
          MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
          MBR_ENR.PROD_SK,
          MBR_ENR.EFF_DT_SK,
          MBR_ENR.TERM_DT_SK,
          MBR_ENR.GRP_SK,
          MBR_ENR.MBR_ENR_ELIG_RSN_CD_SK,
          MBR_ENR.INELGY_EXCD_SK,
          MBR_ENR.EXCD_SK,
          MBR_ENR.MBR_ENR_PRCS_STTUS_CD_SK,
          MBR_ENR.ELIG_IN,
          MBR_ENR.PLN_ENTRY_DT_SK,
          MBR_ENR.SRC_SYS_CRT_DT_SK,
          MBR_ENR.MBR_ENR_ST_CONT_CD_SK,
          CLS.CLS_ID,
          CLS.CLS_DESC,
          CLS_PLN.CLS_PLN_ID,
          CLS_PLN.CLS_PLN_DESC,
          MBR_ENR.SUBGRP_SK,
          MBR_ENR.BILL_ELIG_IN
        FROM {IDSOwner}.MBR_ENR MBR_ENR
        JOIN {IDSOwner}.W_MBR_DM_DRVR DRVR 
             ON DRVR.KEY_VAL_INT = MBR_ENR.MBR_UNIQ_KEY
        JOIN {IDSOwner}.CLS CLS 
             ON MBR_ENR.CLS_SK = CLS.CLS_SK
        JOIN {IDSOwner}.CLS_PLN CLS_PLN
             ON MBR_ENR.CLS_PLN_SK = CLS_PLN.CLS_PLN_SK
        LEFT JOIN {IDSOwner}.CD_MPPNG CD
             ON MBR_ENR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
        WHERE MBR_ENR.MBR_ENR_SK NOT IN (0, 1)
        """
    )
    .load()
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_SUBGRP_in
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_SUBGRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          SUBGRP_SK,
          SUBGRP_ID,
          SUBGRP_NM
        FROM
          {IDSOwner}.SUBGRP SUBGRP
        """
    )
    .load()
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_MBR_ENR_in
# (references #CurrDate# replaced by {CurrDate})
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_MBR_ENR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
           ENR.MBR_UNIQ_KEY,
           ENR.EFF_DT_SK,
           ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
           ENR.PROD_SK,
           ENR.TERM_DT_SK,
           MPPNG3.TRGT_CD,
           MPPNG3.TRGT_CD_NM,
           ENR.MBR_UNIQ_KEY  MBR_UNIQ_KEY_CURR,
           ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK  MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_CURR,
           ENR.PROD_SK  PROD_SK_CURR
        FROM 
           {IDSOwner}.MBR_ENR ENR,
           {IDSOwner}.CD_MPPNG MPPNG1,
           {IDSOwner}.CD_MPPNG MPPNG2,
           {IDSOwner}.CD_MPPNG MPPNG3,
           {IDSOwner}.PROD_CMPNT CMPNT,
           {IDSOwner}.PROD_BILL_CMPNT BILL,
           {IDSOwner}.EXPRNC_CAT CAT,
           {IDSOwner}.W_MBR_DM_DRVR DRVR
        WHERE 
           DRVR.KEY_VAL_INT = ENR.MBR_UNIQ_KEY
           AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG1.CD_MPPNG_SK
           AND MPPNG1.TRGT_CD IN ('MED', 'DNTL')
           AND ENR.PROD_SK = CMPNT.PROD_SK
           AND CMPNT.PROD_CMPNT_TYP_CD_SK = MPPNG2.CD_MPPNG_SK
           AND MPPNG2.TRGT_CD = 'PDBL'
           AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
           AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
           AND CMPNT.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
           AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurrDate}'
           AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDate}'
           AND BILL.PROD_BILL_CMPNT_ID IN ('MED', 'MED1', 'DEN', 'DEN1')
           AND BILL.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
           AND CAT.EXPRNC_CAT_FUND_CAT_CD_SK = MPPNG3.CD_MPPNG_SK
           AND ENR.EFF_DT_SK <= '{CurrDate}'
           AND ENR.TERM_DT_SK >= '{CurrDate}'
        """
    )
    .load()
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_EXCD_in
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_EXCD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          EXCD.EXCD_SK,
          EXCD.EXCD_ID,
          EXCD.EXCD_SH_TX
        FROM
          {IDSOwner}.EXCD EXCD
        """
    )
    .load()
)

# Cpy_Excd_Extr
df_cpy_excd_extr_inelgy_excd_lookup = df_db2_MBRSHP_DM_EXCD_in.select(
    col("EXCD_SK"),
    col("EXCD_ID"),
    col("EXCD_SH_TX")
)

df_cpy_excd_extr_excd_lookup = df_db2_MBRSHP_DM_EXCD_in.select(
    col("EXCD_SK"),
    col("EXCD_ID"),
    col("EXCD_SH_TX")
)

# --------------------------------------------------------------------------------
# db2_MBRSHP_DM_W_MBR_DM_DRVR_in
# (references #CurrDate# replaced by {CurrDate})
# --------------------------------------------------------------------------------
df_db2_MBRSHP_DM_W_MBR_DM_DRVR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
          ENR.MBR_UNIQ_KEY,
          ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
          ENR.PROD_SK,
          ENR.EFF_DT_SK,
          max(ENR.TERM_DT_SK) TERM_DT_SK,
          MPPNG3.TRGT_CD,
          MPPNG3.TRGT_CD_NM
        FROM 
          {IDSOwner}.MBR_ENR ENR,
          {IDSOwner}.CD_MPPNG MPPNG1,
          {IDSOwner}.CD_MPPNG MPPNG2,
          {IDSOwner}.CD_MPPNG MPPNG3,
          {IDSOwner}.PROD_CMPNT CMPNT,
          {IDSOwner}.PROD_BILL_CMPNT BILL,
          {IDSOwner}.EXPRNC_CAT CAT,
          {IDSOwner}.W_MBR_DM_DRVR DRVR
        WHERE 
          DRVR.KEY_VAL_INT = ENR.MBR_UNIQ_KEY
          AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG1.CD_MPPNG_SK
          AND MPPNG1.TRGT_CD IN ('MED', 'DNTL')
          AND ENR.PROD_SK = CMPNT.PROD_SK
          AND CMPNT.PROD_CMPNT_TYP_CD_SK = MPPNG2.CD_MPPNG_SK
          AND MPPNG2.TRGT_CD = 'PDBL'
          AND CMPNT.PROD_CMPNT_EFF_DT_SK <= ENR.TERM_DT_SK
          AND CMPNT.PROD_CMPNT_TERM_DT_SK >= ENR.TERM_DT_SK
          AND CMPNT.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
          AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= ENR.TERM_DT_SK
          AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= ENR.TERM_DT_SK
          AND BILL.PROD_BILL_CMPNT_ID IN ('MED', 'MED1', 'DEN', 'DEN1')
          AND BILL.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
          AND CAT.EXPRNC_CAT_FUND_CAT_CD_SK = MPPNG3.CD_MPPNG_SK
          AND ENR.TERM_DT_SK < '{CurrDate}'
        GROUP BY 
          ENR.MBR_UNIQ_KEY,
          ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
          ENR.PROD_SK,
          ENR.EFF_DT_SK,
          MPPNG3.TRGT_CD,
          MPPNG3.TRGT_CD_NM
        """
    )
    .load()
)

# ColGen_TermFields (PxColumnGenerator)
df_colgen_termfields = (
    df_db2_MBRSHP_DM_W_MBR_DM_DRVR_in
    .withColumn("MBR_UNIQ_KEY_TERM", col("MBR_UNIQ_KEY"))
    .withColumn("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_TERM", col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"))
    .withColumn("PROD_SK_TERM", col("PROD_SK"))
)

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_In (second occurrence)
# --------------------------------------------------------------------------------
df_db2_CD_MPPNG_In_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
            CD_MPPNG_SK,
            COALESCE(TRGT_CD,'UNK') TRGT_CD,
            COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
        FROM {IDSOwner}.CD_MPPNG
        """
    )
    .load()
)

# cpy_cd_mppng (second occurrence, multiple outputs)
df_cpy_cd_mppng_mbr_enr_elig_rsn_cd_lookup = df_db2_CD_MPPNG_In_2.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_cpy_cd_mppng_mbr_enr_prcs_sttus_cd_lookup = df_db2_CD_MPPNG_In_2.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_cpy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup_2 = df_db2_CD_MPPNG_In_2.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_cpy_cd_mppng_state_cont_cd_2 = df_db2_CD_MPPNG_In_2.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# lkp_Codes (PxLookup) - multi join
# Primary link: df_db2_MBRSHP_DM_MBR_ELIG_in (alias "Extract")
df_lkp_codes = df_db2_MBRSHP_DM_MBR_ELIG_in.alias("Extract")

# Join 1: left join with df_db2_MBRSHP_DM_MBR_ENR_in (alias "Fund_Cat_Curr_Date")
df_lkp_codes = df_lkp_codes.join(
    df_db2_MBRSHP_DM_MBR_ENR_in.alias("Fund_Cat_Curr_Date"),
    (
        (col("Extract.MBR_UNIQ_KEY") == col("Fund_Cat_Curr_Date.MBR_UNIQ_KEY")) &
        (col("Extract.EFF_DT_SK") == col("Fund_Cat_Curr_Date.EFF_DT_SK")) &
        (col("Extract.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == col("Fund_Cat_Curr_Date.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK")) &
        (col("Extract.PROD_SK") == col("Fund_Cat_Curr_Date.PROD_SK"))
    ),
    "left"
)

# Join 2: left join with df_db2_MBRSHP_DM_PROD_SH_NM_in (alias "Prod_Extract")
df_lkp_codes = df_lkp_codes.join(
    df_db2_MBRSHP_DM_PROD_SH_NM_in.alias("Prod_Extract"),
    col("Extract.PROD_SK") == col("Prod_Extract.PROD_SK"),
    "left"
)

# Join 3: left join with df_db2_MBRSHP_DM_SUBGRP_in (alias "Sub_Grp_Extract")
df_lkp_codes = df_lkp_codes.join(
    df_db2_MBRSHP_DM_SUBGRP_in.alias("Sub_Grp_Extract"),
    col("Extract.SUBGRP_SK") == col("Sub_Grp_Extract.SUBGRP_SK"),
    "left"
)

# Join 4: left join with df_cpy_excd_extr_excd_lookup (alias "Excd_Lookup")
df_lkp_codes = df_lkp_codes.join(
    df_cpy_excd_extr_excd_lookup.alias("Excd_Lookup"),
    col("Extract.EXCD_SK") == col("Excd_Lookup.EXCD_SK"),
    "left"
)

# Join 5: left join with df_cpy_excd_extr_inelgy_excd_lookup (alias "Inelgy_Excd_Lookup")
df_lkp_codes = df_lkp_codes.join(
    df_cpy_excd_extr_inelgy_excd_lookup.alias("Inelgy_Excd_Lookup"),
    col("Extract.INELGY_EXCD_SK") == col("Inelgy_Excd_Lookup.EXCD_SK"),
    "left"
)

# Join 6: left join with df_cpy_cd_mppng_mbr_enr_prcs_sttus_cd_lookup (alias "Mbr_Enr_Prcs_Sttus_Cd_Lookup")
df_lkp_codes = df_lkp_codes.join(
    df_cpy_cd_mppng_mbr_enr_prcs_sttus_cd_lookup.alias("Mbr_Enr_Prcs_Sttus_Cd_Lookup"),
    col("Extract.MBR_ENR_PRCS_STTUS_CD_SK") == col("Mbr_Enr_Prcs_Sttus_Cd_Lookup.CD_MPPNG_SK"),
    "left"
)

# Join 7: left join with df_cpy_cd_mppng_mbr_enr_elig_rsn_cd_lookup (alias "Mbr_Enr_Elig_Rsn_Cd_Lookup")
df_lkp_codes = df_lkp_codes.join(
    df_cpy_cd_mppng_mbr_enr_elig_rsn_cd_lookup.alias("Mbr_Enr_Elig_Rsn_Cd_Lookup"),
    col("Extract.MBR_ENR_ELIG_RSN_CD_SK") == col("Mbr_Enr_Elig_Rsn_Cd_Lookup.CD_MPPNG_SK"),
    "left"
)

# Join 8: left join with df_cpy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup_2 (alias "Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup")
# Note: The join conditions reference "lnk_TrnsrmDta.PROD_SH_NM_DLVRY_METH_CD_SK" but that is not yet in this stage. 
# In the JSON, it also references "Extract.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK" => so that is the second set. 
# We'll replicate the condition carefully:
# Actually in the JSON: 
#   "JoinConditions": [
#     {"SourceKeyOrValue": "lnk_TrnsrmDta.PROD_SH_NM_DLVRY_METH_CD_SK", "LookupKey": "Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.CD_MPPNG_SK"},
#     {"SourceKeyOrValue": "Extract.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK", "LookupKey": "Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.CD_MPPNG_SK"}
#   ]
# Because that reference to "lnk_TrnsrmDta" does not exist in this stage yet, we interpret that part as "null" or skip. 
# The JSON itself shows these conditions at a different stage. We'll do only the second part for now, since we only have "Extract".
df_lkp_codes = df_lkp_codes.join(
    df_cpy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup_2.alias("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup"),
    col("Extract.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.CD_MPPNG_SK"),
    "left"
)

# Join 9: left join with df_db2_MBRSHP_DM_GRP_in (alias "Grp_Extract")
df_lkp_codes = df_lkp_codes.join(
    df_db2_MBRSHP_DM_GRP_in.alias("Grp_Extract"),
    col("Extract.GRP_SK") == col("Grp_Extract.GRP_SK"),
    "left"
)

# Join 10: left join with df_colgen_termfields (alias "Fund_Cat_term_Date")
df_lkp_codes = df_lkp_codes.join(
    df_colgen_termfields.alias("Fund_Cat_term_Date"),
    (
        (col("Extract.MBR_UNIQ_KEY") == col("Fund_Cat_term_Date.MBR_UNIQ_KEY")) &
        (col("Extract.EFF_DT_SK") == col("Fund_Cat_term_Date.EFF_DT_SK")) &
        (col("Extract.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == col("Fund_Cat_term_Date.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK")) &
        (col("Extract.PROD_SK") == col("Fund_Cat_term_Date.PROD_SK"))
    ),
    "left"
)

# Now select the columns from the "lkp_Codes" OutputPin
df_lkp_codes_out = df_lkp_codes.select(
    col("Grp_Extract.GRP_ID").alias("GRP_ID"),
    col("Grp_Extract.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.TRGT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.TRGT_CD_NM").alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    col("Mbr_Enr_Elig_Rsn_Cd_Lookup.TRGT_CD").alias("MBR_ENR_ELIG_RSN_CD"),
    col("Mbr_Enr_Elig_Rsn_Cd_Lookup.TRGT_CD_NM").alias("MBR_ENR_ELIG_RSN_NM"),
    col("Mbr_Enr_Prcs_Sttus_Cd_Lookup.TRGT_CD").alias("MBR_ENR_PRCS_STTUS_CD"),
    col("Mbr_Enr_Prcs_Sttus_Cd_Lookup.TRGT_CD_NM").alias("MBR_ENR_PRCS_STTUS_NM"),
    col("Inelgy_Excd_Lookup.EXCD_ID").alias("MBR_ENR_INELGY_EXCD"),
    col("Inelgy_Excd_Lookup.EXCD_SH_TX").alias("MBR_ENR_INELGY_EXCD_SH_TX"),
    col("Excd_Lookup.EXCD_ID").alias("MBR_ENR_OVALL_ELIG_EXCD"),
    col("Excd_Lookup.EXCD_SH_TX").alias("MBR_ENR_OVALL_ELIG_EXCD_SH_TX"),
    col("Sub_Grp_Extract.SUBGRP_ID").alias("SUBGRP_ID"),
    col("Sub_Grp_Extract.SUBGRP_NM").alias("SUBGRP_NM"),
    col("Prod_Extract.PROD_ID").alias("PROD_ID"),
    col("Prod_Extract.SUBPROD_CD_SK").alias("SUBPROD_CD_SK"),
    col("Prod_Extract.PROD_DESC").alias("PROD_DESC"),
    col("Prod_Extract.PROD_SH_NM").alias("PROD_SH_NM"),
    col("Prod_Extract.PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
    col("Fund_Cat_Curr_Date.MBR_UNIQ_KEY_CURR").alias("MBR_UNIQ_KEY_CURR"),
    col("Fund_Cat_Curr_Date.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_CURR").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_CURR"),
    col("Fund_Cat_Curr_Date.PROD_SK_CURR").alias("PROD_SK_CURR"),
    col("Fund_Cat_Curr_Date.TRGT_CD").alias("CURR_TGT_CD"),
    col("Fund_Cat_Curr_Date.TRGT_CD_NM").alias("CURR_TGT_NM"),
    col("Fund_Cat_term_Date.MBR_UNIQ_KEY_TERM").alias("MBR_UNIQ_KEY_TERM"),
    col("Fund_Cat_term_Date.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_TERM").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_TERM"),
    col("Fund_Cat_term_Date.PROD_SK_TERM").alias("PROD_SK_TERM"),
    col("Fund_Cat_term_Date.TRGT_CD").alias("TERM_TRGT_CD"),
    col("Fund_Cat_term_Date.TRGT_CD_NM").alias("TERM_TRGT_CD_NM"),
    col("State_Cont_Cd.TRGT_CD").alias("STATE_CONT_TRGT_CD"),
    col("State_Cont_Cd.TRGT_CD_NM").alias("STATE_CONT_TRGT_CD_NM"),
    col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Extract.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Extract.TERM_DT_SK").alias("MBR_ENR_TERM_DT"),
    col("Extract.ELIG_IN").alias("MBR_ENR_ELIG_IN"),
    col("Extract.PLN_ENTRY_DT_SK").alias("MBR_ENR_PLN_ENTRY_DT"),
    col("Extract.SRC_SYS_CRT_DT_SK").alias("MBR_ENR_SRC_SYS_CRT_DT"),
    col("Extract.CLS_ID").alias("CLS_ID"),
    col("Extract.CLS_DESC").alias("CLS_DESC"),
    col("Extract.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Extract.CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("Prod_Extract.MCARE_SUPLMT_COV_IN").alias("MCARE_SUPLMT_COV_IN"),
    col("Prod_Extract.PROD_SH_NM_CAT_CD_SK").alias("PROD_SH_NM_CAT_CD_SK"),
    col("Extract.BILL_ELIG_IN").alias("BILL_ELIG_IN")
)

# --------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# We'll add stage variables withColumn for clarity, then apply them in final columns
df_xfm_BusinessLogic_1 = df_lkp_codes_out.withColumn(
    "svCurrLkup",
    F.when(
        (F.col("MBR_UNIQ_KEY_CURR").isNotNull()) &
        (F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_CURR").isNotNull()) &
        (F.col("PROD_SK_CURR").isNotNull()), 
        F.lit("Y")
    ).otherwise("N")
).withColumn(
    "svTermLkup",
    F.when(
        (
            (F.col("MBR_UNIQ_KEY_TERM").isNotNull()) | (F.col("MBR_UNIQ_KEY_TERM") != "")
        ) & (
            (F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_TERM").isNotNull()) | (F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK_TERM") != "")
        ) & (
            (F.col("PROD_SK_TERM").isNotNull()) | (F.col("PROD_SK_TERM") != "")
        ),
        F.lit("Y")
    ).otherwise("N")
).withColumn(
    "svFuture",
    F.when(
        to_date(col("EFF_DT_SK"), "yyyy-MM-dd") > to_date(F.lit(CurrDate), "yyyy-MM-dd"),
        F.lit("Y")
    ).otherwise("N")
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic_1.select(
    F.when(
        F.col("SRC_SYS_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD")).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    to_date(col("EFF_DT_SK"), "yyyy-MM-dd").alias("MBR_ENR_EFF_DT"),
    F.when(
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_CLS_PLN_PROD_CAT_NM")).alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    F.when(
        F.col("MBR_ENR_ELIG_RSN_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_ELIG_RSN_CD")).alias("MBR_ENR_ELIG_RSN_CD"),
    F.when(
        F.col("MBR_ENR_ELIG_RSN_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_ELIG_RSN_NM")).alias("MBR_ENR_ELIG_RSN_NM"),
    F.when(
        F.col("MBR_ENR_INELGY_EXCD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_INELGY_EXCD")).alias("MBR_ENR_INELGY_EXCD"),
    F.when(
        F.col("MBR_ENR_INELGY_EXCD_SH_TX").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_INELGY_EXCD_SH_TX")).alias("MBR_ENR_INELGY_EXCD_SH_TX"),
    F.when(
        F.col("MBR_ENR_OVALL_ELIG_EXCD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_OVALL_ELIG_EXCD")).alias("MBR_ENR_OVALL_ELIG_EXCD"),
    F.when(
        F.col("MBR_ENR_OVALL_ELIG_EXCD_SH_TX").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_OVALL_ELIG_EXCD_SH_TX")).alias("MBR_ENR_OVALL_ELIG_EXCD_SH_TX"),
    F.when(
        F.col("MBR_ENR_PRCS_STTUS_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_PRCS_STTUS_CD")).alias("MBR_ENR_PRCS_STTUS_CD"),
    F.when(
        F.col("MBR_ENR_PRCS_STTUS_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_ENR_PRCS_STTUS_NM")).alias("MBR_ENR_PRCS_STTUS_NM"),
    col("MBR_ENR_ELIG_IN").alias("MBR_ENR_ELIG_IN"),
    to_date(col("MBR_ENR_PLN_ENTRY_DT"), "yyyy-MM-dd").alias("MBR_ENR_PLN_ENTRY_DT"),
    to_date(col("MBR_ENR_SRC_SYS_CRT_DT"), "yyyy-MM-dd").alias("MBR_ENR_SRC_SYS_CRT_DT"),
    col("MBR_ENR_TERM_DT").alias("MBR_ENR_TERM_DT"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_DESC").alias("CLS_DESC"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    F.when(
        F.col("GRP_UNIQ_KEY").isNull(),
        F.lit(0)
    ).otherwise(F.col("GRP_UNIQ_KEY")).alias("GRP_UNIQ_KEY"),
    F.when(
        F.col("GRP_ID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.when(
        F.col("PROD_ID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("PROD_ID")).alias("PROD_ID"),
    F.when(
        F.col("PROD_DESC").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("PROD_DESC")).alias("PROD_DESC"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.when(
        F.col("PROD_SH_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("PROD_SH_NM")).alias("PROD_SH_NM"),
    F.when(
        F.col("SUBGRP_ID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_ID"),
    F.when(
        F.col("SUBGRP_NM").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("SUBGRP_NM")).alias("SUBGRP_NM"),
    F.when(
        F.col("PROD_SH_NM_DLVRY_METH_CD_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD_SK")).alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
    F.when(
        F.col("SUBPROD_CD_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("SUBPROD_CD_SK")).alias("SUBPROD_CD_SK"),
    F.when(
        F.col("EFF_DT_SK").isNull() | F.col("MBR_ENR_TERM_DT").isNull() | F.col("MBR_ENR_ELIG_IN").isNull(),
        F.lit("X")
    ).otherwise(
        F.when(
            (col("EFF_DT_SK") <= F.lit(CurrDate)) &
            (col("MBR_ENR_TERM_DT") >= F.lit(CurrDate)) &
            (col("MBR_ENR_ELIG_IN") == F.lit("Y")),
            F.lit("Y")
        ).otherwise("N")
    ).alias("MBR_ENR_ACTV_ROW_IN"),
    F.when(
        (F.col("STATE_CONT_TRGT_CD").isNull()) | (trim(F.col("STATE_CONT_TRGT_CD")) == F.lit("NA")),
        F.lit(" ")
    ).otherwise(trim(F.col("STATE_CONT_TRGT_CD"))).alias("MBR_ENR_ST_CONT_CD"),
    F.when(
        (F.col("STATE_CONT_TRGT_CD").isNull()) | (trim(F.col("STATE_CONT_TRGT_CD")) == F.lit("NA")),
        F.lit(" ")
    ).otherwise(trim(F.col("STATE_CONT_TRGT_CD_NM"))).alias("MBR_ENR_ST_CONT_NM"),
    F.when(col("svFuture") == F.lit("Y"), F.lit(" "))
    .otherwise(
        F.when(col("svCurrLkup") == F.lit("Y"),
           F.when(F.col("CURR_TGT_CD").isNotNull(), F.col("CURR_TGT_CD")).otherwise(F.lit(" "))
        ).otherwise(
           F.when(col("svTermLkup") == F.lit("Y"),
              F.when(F.col("TERM_TRGT_CD").isNotNull(), F.col("TERM_TRGT_CD")).otherwise(F.lit(" "))
           ).otherwise(F.lit(" "))
        )
    ).alias("FUND_CAT_CD"),
    F.when(col("svFuture") == F.lit("Y"), F.lit(" "))
    .otherwise(
        F.when(col("svCurrLkup") == F.lit("Y"),
           F.when(F.col("CURR_TGT_NM").isNotNull(), F.col("CURR_TGT_NM")).otherwise(F.lit(" "))
        ).otherwise(
           F.when(col("svTermLkup") == F.lit("Y"),
              F.when(F.col("TERM_TRGT_CD_NM").isNotNull(), F.col("TERM_TRGT_CD_NM")).otherwise(F.lit(" "))
           ).otherwise(F.lit(" "))
        )
    ).alias("FUND_CAT_NM"),
    F.when(
        (F.col("MCARE_SUPLMT_COV_IN").isNull()) |
        ((F.col("MCARE_SUPLMT_COV_IN") != F.lit("Y")) & (F.col("MCARE_SUPLMT_COV_IN") != F.lit("N"))),
        F.lit("N")
    ).otherwise(F.col("MCARE_SUPLMT_COV_IN")).alias("MCARE_SUPLMT_COV_IN"),
    F.when(
        F.col("PROD_SH_NM_CAT_CD_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("PROD_SH_NM_CAT_CD_SK")).alias("PROD_SH_NM_CAT_CD_SK"),
    col("BILL_ELIG_IN").alias("BILL_ELIG_IN")
)

# --------------------------------------------------------------------------------
# Lookup_Codes (second PxLookup)
# Primary link: df_xfm_BusinessLogic (alias "lnk_TrnsrmDta")
df_lkp2 = df_xfm_BusinessLogic.alias("lnk_TrnsrmDta")

# Left join #1 with df_copy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup
df_lkp2 = df_lkp2.join(
    df_copy_cd_mppng_mbr_enr_cls_pln_prod_cat_cd_lookup.alias("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup"),
    (col("lnk_TrnsrmDta.PROD_SH_NM_DLVRY_METH_CD_SK") == col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.CD_MPPNG_SK")),
    "left"
)

# Left join #2 with df_copy_cd_mppng_state_cont_cd
df_lkp2 = df_lkp2.join(
    df_copy_cd_mppng_state_cont_cd.alias("State_Cont_Cd"),
    (col("lnk_TrnsrmDta.SUBPROD_CD_SK") == col("State_Cont_Cd.CD_MPPNG_SK")),
    "left"
)

# Left join #3 with df_copy_cd_mppng_prodshnmcatcd
df_lkp2 = df_lkp2.join(
    df_copy_cd_mppng_prodshnmcatcd.alias("ProdShNmCatCd"),
    (col("lnk_TrnsrmDta.PROD_SH_NM_CAT_CD_SK") == col("ProdShNmCatCd.CD_MPPNG_SK")),
    "left"
)

df_lkp2_out = df_lkp2.select(
    col("lnk_TrnsrmDta.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_TrnsrmDta.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_TrnsrmDta.MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    col("lnk_TrnsrmDta.MBR_ENR_EFF_DT").alias("MBR_ENR_EFF_DT"),
    col("lnk_TrnsrmDta.MBR_ENR_CLS_PLN_PROD_CAT_NM").alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    col("lnk_TrnsrmDta.MBR_ENR_ELIG_RSN_CD").alias("MBR_ENR_ELIG_RSN_CD"),
    col("lnk_TrnsrmDta.MBR_ENR_ELIG_RSN_NM").alias("MBR_ENR_ELIG_RSN_NM"),
    col("lnk_TrnsrmDta.MBR_ENR_INELGY_EXCD").alias("MBR_ENR_INELGY_EXCD"),
    col("lnk_TrnsrmDta.MBR_ENR_INELGY_EXCD_SH_TX").alias("MBR_ENR_INELGY_EXCD_SH_TX"),
    col("lnk_TrnsrmDta.MBR_ENR_OVALL_ELIG_EXCD").alias("MBR_ENR_OVALL_ELIG_EXCD"),
    col("lnk_TrnsrmDta.MBR_ENR_OVALL_ELIG_EXCD_SH_TX").alias("MBR_ENR_OVALL_ELIG_EXCD_SH_TX"),
    col("lnk_TrnsrmDta.MBR_ENR_PRCS_STTUS_CD").alias("MBR_ENR_PRCS_STTUS_CD"),
    col("lnk_TrnsrmDta.MBR_ENR_PRCS_STTUS_NM").alias("MBR_ENR_PRCS_STTUS_NM"),
    col("lnk_TrnsrmDta.MBR_ENR_ELIG_IN").alias("MBR_ENR_ELIG_IN"),
    col("lnk_TrnsrmDta.MBR_ENR_PLN_ENTRY_DT").alias("MBR_ENR_PLN_ENTRY_DT"),
    col("lnk_TrnsrmDta.MBR_ENR_SRC_SYS_CRT_DT").alias("MBR_ENR_SRC_SYS_CRT_DT"),
    col("lnk_TrnsrmDta.MBR_ENR_TERM_DT").alias("MBR_ENR_TERM_DT"),
    col("lnk_TrnsrmDta.CLS_ID").alias("CLS_ID"),
    col("lnk_TrnsrmDta.CLS_DESC").alias("CLS_DESC"),
    col("lnk_TrnsrmDta.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("lnk_TrnsrmDta.CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("lnk_TrnsrmDta.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("lnk_TrnsrmDta.GRP_ID").alias("GRP_ID"),
    col("lnk_TrnsrmDta.PROD_ID").alias("PROD_ID"),
    col("lnk_TrnsrmDta.PROD_DESC").alias("PROD_DESC"),
    col("lnk_TrnsrmDta.LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("lnk_TrnsrmDta.PROD_SH_NM").alias("PROD_SH_NM"),
    col("lnk_TrnsrmDta.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lnk_TrnsrmDta.SUBGRP_NM").alias("SUBGRP_NM"),
    col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.TRGT_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    col("Mbr_Enr_Cls_Pln_Prod_Cat_Cd_Lookup.TRGT_CD_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    col("State_Cont_Cd.TRGT_CD").alias("PROD_SUBPROD_CD"),
    col("State_Cont_Cd.TRGT_CD_NM").alias("PROD_SUBPROD_NM"),
    col("lnk_TrnsrmDta.MBR_ENR_ACTV_ROW_IN").alias("MBR_ENR_ACTV_ROW_IN"),
    col("lnk_TrnsrmDta.MBR_ENR_ST_CONT_CD").alias("MBR_ENR_ST_CONT_CD"),
    col("lnk_TrnsrmDta.MBR_ENR_ST_CONT_NM").alias("MBR_ENR_ST_CONT_NM"),
    col("lnk_TrnsrmDta.FUND_CAT_CD").alias("FUND_CAT_CD"),
    col("lnk_TrnsrmDta.FUND_CAT_NM").alias("FUND_CAT_NM"),
    col("ProdShNmCatCd.TRGT_CD").alias("PROD_SH_NM_CAT_CD"),
    col("ProdShNmCatCd.TRGT_CD_NM").alias("PROD_SH_NM_CAT_NM"),
    col("lnk_TrnsrmDta.MCARE_SUPLMT_COV_IN").alias("PROD_SH_NM_MCARE_SUPLMT_COV_IN"),
    col("lnk_TrnsrmDta.BILL_ELIG_IN").alias("BILL_ELIG_IN")
)

# --------------------------------------------------------------------------------
# Xfm_Transformer (CTransformerStage)
df_xfm_transformer = df_lkp2_out.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    to_timestamp(col("MBR_ENR_EFF_DT"), "yyyy-MM-dd").alias("MBR_ENR_EFF_DT"),
    col("MBR_ENR_CLS_PLN_PROD_CAT_NM").alias("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    col("MBR_ENR_ELIG_RSN_CD").alias("MBR_ENR_ELIG_RSN_CD"),
    col("MBR_ENR_ELIG_RSN_NM").alias("MBR_ENR_ELIG_RSN_NM"),
    col("MBR_ENR_INELGY_EXCD").alias("MBR_ENR_INELGY_EXCD"),
    col("MBR_ENR_INELGY_EXCD_SH_TX").alias("MBR_ENR_INELGY_EXCD_SH_TX"),
    col("MBR_ENR_OVALL_ELIG_EXCD").alias("MBR_ENR_OVALL_ELIG_EXCD"),
    col("MBR_ENR_OVALL_ELIG_EXCD_SH_TX").alias("MBR_ENR_OVALL_ELIG_EXCD_SH_TX"),
    col("MBR_ENR_PRCS_STTUS_CD").alias("MBR_ENR_PRCS_STTUS_CD"),
    col("MBR_ENR_PRCS_STTUS_NM").alias("MBR_ENR_PRCS_STTUS_NM"),
    col("MBR_ENR_ELIG_IN").alias("MBR_ENR_ELIG_IN"),
    to_timestamp(col("MBR_ENR_PLN_ENTRY_DT"), "yyyy-MM-dd").alias("MBR_ENR_PLN_ENTRY_DT"),
    to_timestamp(col("MBR_ENR_SRC_SYS_CRT_DT"), "yyyy-MM-dd").alias("MBR_ENR_SRC_SYS_CRT_DT"),
    to_timestamp(col("MBR_ENR_TERM_DT"), "yyyy-MM-dd").alias("MBR_ENR_TERM_DT"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_DESC").alias("CLS_DESC"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_DESC").alias("PROD_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("PROD_SH_NM").alias("PROD_SH_NM"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("SUBGRP_NM").alias("SUBGRP_NM"),
    F.when(col("PROD_SH_NM_DLVRY_METH_CD").isNull(), F.lit(None)).otherwise(col("PROD_SH_NM_DLVRY_METH_CD")).alias("PROD_SH_NM_DLVRY_METH_CD"),
    F.when(col("PROD_SH_NM_DLVRY_METH_NM").isNull(), F.lit(None)).otherwise(col("PROD_SH_NM_DLVRY_METH_NM")).alias("PROD_SH_NM_DLVRY_METH_NM"),
    F.when(col("PROD_SUBPROD_CD").isNull(), F.lit(None)).otherwise(col("PROD_SUBPROD_CD")).alias("PROD_SUBPROD_CD"),
    F.when(col("PROD_SUBPROD_NM").isNull(), F.lit(None)).otherwise(col("PROD_SUBPROD_NM")).alias("PROD_SUBPROD_NM"),
    col("MBR_ENR_ACTV_ROW_IN").alias("MBR_ENR_ACTV_ROW_IN"),
    col("MBR_ENR_ST_CONT_CD").alias("MBR_ENR_ELIG_ST_CONT_CD"),
    col("MBR_ENR_ST_CONT_NM").alias("MBR_ENR_ELIG_ST_CONT_NM"),
    col("FUND_CAT_CD").alias("FUND_CAT_CD"),
    col("FUND_CAT_NM").alias("FUND_CAT_NM"),
    F.when(col("PROD_SH_NM_CAT_CD").isNull(), F.lit(" ")).otherwise(col("PROD_SH_NM_CAT_CD")).alias("PROD_SH_NM_CAT_CD"),
    F.when(col("PROD_SH_NM_CAT_NM").isNull(), F.lit(" ")).otherwise(col("PROD_SH_NM_CAT_NM")).alias("PROD_SH_NM_CAT_NM"),
    col("PROD_SH_NM_MCARE_SUPLMT_COV_IN").alias("PROD_SH_NM_MCARE_SUPLMT_COV_IN"),
    col("BILL_ELIG_IN").alias("BILL_ELIG_IN")
)

# --------------------------------------------------------------------------------
# seq_MBRSH_DM_MBR_ELIG_load (PxSequentialFile) - final write
# The final columns in the exact order from that link
df_final = df_xfm_transformer.select(
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "MBR_ENR_EFF_DT",
    "MBR_ENR_CLS_PLN_PROD_CAT_NM",
    "MBR_ENR_ELIG_RSN_CD",
    "MBR_ENR_ELIG_RSN_NM",
    "MBR_ENR_INELGY_EXCD",
    "MBR_ENR_INELGY_EXCD_SH_TX",
    "MBR_ENR_OVALL_ELIG_EXCD",
    "MBR_ENR_OVALL_ELIG_EXCD_SH_TX",
    "MBR_ENR_PRCS_STTUS_CD",
    "MBR_ENR_PRCS_STTUS_NM",
    "MBR_ENR_ELIG_IN",
    "MBR_ENR_PLN_ENTRY_DT",
    "MBR_ENR_SRC_SYS_CRT_DT",
    "MBR_ENR_TERM_DT",
    "CLS_ID",
    "CLS_DESC",
    "CLS_PLN_ID",
    "CLS_PLN_DESC",
    "GRP_UNIQ_KEY",
    "GRP_ID",
    "PROD_ID",
    "PROD_DESC",
    "LAST_UPDT_RUN_CYC_NO",
    "PROD_SH_NM",
    "SUBGRP_ID",
    "SUBGRP_NM",
    "PROD_SH_NM_DLVRY_METH_CD",
    "PROD_SH_NM_DLVRY_METH_NM",
    "PROD_SUBPROD_CD",
    "PROD_SUBPROD_NM",
    "MBR_ENR_ACTV_ROW_IN",
    "MBR_ENR_ELIG_ST_CONT_CD",
    "MBR_ENR_ELIG_ST_CONT_NM",
    "FUND_CAT_CD",
    "FUND_CAT_NM",
    "PROD_SH_NM_CAT_CD",
    "PROD_SH_NM_CAT_NM",
    "PROD_SH_NM_MCARE_SUPLMT_COV_IN",
    "BILL_ELIG_IN"
)

# Apply rpad for columns declared as char with length > 0 in final stage
df_final_padded = (
    df_final
    .withColumn("MBR_ENR_ELIG_IN", rpad(col("MBR_ENR_ELIG_IN"), 1, " "))
    .withColumn("MBR_ENR_INELGY_EXCD", rpad(col("MBR_ENR_INELGY_EXCD"), 4, " "))
    .withColumn("MBR_ENR_OVALL_ELIG_EXCD", rpad(col("MBR_ENR_OVALL_ELIG_EXCD"), 4, " "))
    .withColumn("MBR_ENR_TERM_DT", rpad(F.col("MBR_ENR_TERM_DT").cast("string"), 10, " "))
    .withColumn("MBR_ENR_EFF_DT", rpad(F.col("MBR_ENR_EFF_DT").cast("string"), 10, " "))
    .withColumn("MBR_ENR_PLN_ENTRY_DT", rpad(F.col("MBR_ENR_PLN_ENTRY_DT").cast("string"), 10, " "))
    .withColumn("MBR_ENR_SRC_SYS_CRT_DT", rpad(F.col("MBR_ENR_SRC_SYS_CRT_DT").cast("string"), 10, " "))
    .withColumn("PROD_ID", rpad(col("PROD_ID"), 8, " "))
    .withColumn("BILL_ELIG_IN", rpad(col("BILL_ELIG_IN"), 1, " "))
)

# Write out the final file
write_files(
    df_final_padded,
    f"{adls_path}/load/MBRSH_DM_MBR_ELIG.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)