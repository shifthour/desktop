# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     EdwProdDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw to update edw
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  PROD
# MAGIC                          PROD_SH_NM
# MAGIC                          EXPRNC_CAT
# MAGIC                          FNCL_LOB
# MAGIC                EDW: PROD_D
# MAGIC 
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Brent Leland   07/26/2005    Changed output to be a sequential load file.
# MAGIC               SAndrew         09/01/2005    EDW 2.0 - many fields added to EDW PROD_D.  
# MAGIC                                                             Added Effective/Term dates.   Changed  PROD_LOB_TYP_CD_SK to  PROD_LOB_CD_SK
# MAGIC               SAndrew         02/21/2006    Production Issue.   When PROD_FNCL_LOB_CD_SK was renamed to FNCL_LOB_CD_SK the field moved on the table but the code didn't change.
# MAGIC                                                              The PROD_DNTL_BILL_PFX_CD and FNCL_LOB_CD
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                11/26/2007     Product/3028                  Added PROD_PCKG_CD,                   devlEDW10                  Steph Goddard             12/4/07
# MAGIC                                                                                                        PROD_PCKG_DESC and 
# MAGIC                                                                                                        PROD_PCKG_CD_SK to the  
# MAGIC                                                                                                        existing columns.                                          
# MAGIC 
# MAGIC Srikanth Mettpalli             06/3/2013          5114                            Original Programming                           EnterpriseWrhsDevl     Jag Yelavarthi             2013-08-09
# MAGIC                                                                                                        (Server to Parallel Conversion)      
# MAGIC Kalyan Neelam                 2014-01-09    5235 ACA Data Elements  Added 18 new fields on end                EnterpriseNewDevl      Bhoomi Dasari             01/16/2014
# MAGIC                                                                                                          QHP_SK, QHP_ID,QHP_CSR_VRNT_CD,
# MAGIC                                                                                                  QHP_CSR_VRNT_NM,QHP_CSR_VRNT_CD_SK,
# MAGIC                                                                                                  QHP_ENR_TYP_CD,QHP_ENR_TYP_NM,
# MAGIC                                                                                                  QHP_ENR_TYP_CD_SK,QHP_EXCH_CHAN_CD,
# MAGIC                                                                                                  QHP_EXCH_CHAN_NM,QHP_EXCH_CHAN_CD_SK,
# MAGIC                                                                                                  QHP_EXCH_TYP_CD,QHP_EXCH_TYP_NM,
# MAGIC                                                                                                  QHP_EXCH_TYP_CD_SK,QHP_METAL_LVL_CD,
# MAGIC                                                                                                  QHP_METAL_LVL_NM,QHP_METAL_LVL_CD_SK,ACTURL_VAL_NO
# MAGIC 
# MAGIC Nagesh Bandi               2018-03-29         5832-Spira Care       Added new field SPIRA_BNF_ID                 EnterpriseDev1         Jaideep Mankala         03/30/2018
# MAGIC                                                                                                   for SF Reporting at lkp_Codes1 stage
# MAGIC                                                                                                 Added $EDWAcct, $EDWPW , $EDWDB 
# MAGIC                                                                                                 and $EDWOwner job parameters.
# MAGIC 
# MAGIC Karthik Chintalapani        2020-06-15        US 236339      Added new field EPO_IN                          EnterpriseDev1         Jaideep Mankala         06/19/2020
# MAGIC 
# MAGIC Deepika C                      2023-07-19        US 588189         Updated FNCL_LOB_DESC column length          EnterpriseDevB           Goutham K           2023-08-08
# MAGIC                                                                                              to VARCHAR(255)

# MAGIC Changed the Datatype for ACTURL_VAL_NO to VarChar just for this step because a default value of 0.00 is assinged instead of Null if the datatype is left as Decimal
# MAGIC Write PROD_D Data into a Balacing file to support the needs of Balancing.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC CD_MPPNG_SK
# MAGIC DNTL_BILL_PFX_CD_SK
# MAGIC PROD_LOB_CD_SK
# MAGIC PROD_PCKG_CD_SK
# MAGIC PROD_RATE_TYP_CD_SK
# MAGIC PROD_ST_CD_SK
# MAGIC SUBPROD_CD_SK
# MAGIC PROD_SH_NM_DLVRY_METH_CD_SK
# MAGIC PROD_SH_NM_CAT_CD_SK
# MAGIC Write PROD_D Data into a Sequential file for Load Job IdsEdwProdDLoad.
# MAGIC Read all the Data from IDS PROD Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwProdDExtr
# MAGIC Read all the Data from IDS EXPRNC_CAT, FNCL_LOB, PROD_SH_NM Tables for Code SK Lookups.
# MAGIC Fkey Lookup for CD SK Mapping.
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC EXPRNC_CAT_SK
# MAGIC FNCL_LOB_SK
# MAGIC PROD_SH_NM_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad, coalesce
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# Get DB connection configs
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') AS TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# cpy_cd_mppng (pass-through copy of columns)
df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM"
)

# db2_PROD_in
extract_query_db2_PROD_in = (
    f"SELECT \n"
    f"PROD.PROD_SK,\n"
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    f"PROD.PROD_ID,\n"
    f"PROD.EXPRNC_CAT_SK,\n"
    f"PROD.FNCL_LOB_SK,\n"
    f"PROD.PROD_SH_NM_SK,\n"
    f"PROD.DNTL_BILL_PFX_CD_SK,\n"
    f"PROD.PROD_ACCUM_SFX_CD_SK,\n"
    f"PROD.PROD_LOB_CD_SK,\n"
    f"PROD.PROD_PCKG_CD_SK,\n"
    f"PROD.PROD_RATE_TYP_CD_SK,\n"
    f"PROD.PROD_ST_CD_SK,\n"
    f"PROD.SUBPROD_CD_SK,\n"
    f"PROD.DNTL_LATE_WAIT_IN,\n"
    f"PROD.DRUG_COV_IN,\n"
    f"PROD.HLTH_COV_IN,\n"
    f"PROD.MNL_PRCS_IN,\n"
    f"PROD.MNTL_HLTH_COV_IN,\n"
    f"PROD.MO_HLTH_INSUR_POOL_IN,\n"
    f"PROD.RQRD_PCP_IN,\n"
    f"PROD.VSN_COV_IN,\n"
    f"PROD.PROD_EFF_DT_SK,\n"
    f"PROD.PROD_TERM_DT_SK,\n"
    f"PROD.FCTS_CONV_RATE_PFX,\n"
    f"PROD.LOB_NO,\n"
    f"PROD.PROD_ABBR,\n"
    f"PROD.PROD_DESC,\n"
    f"PROD.QHP_SK\n"
    f"FROM {IDSOwner}.PROD PROD\n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
    f"ON PROD.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"WHERE PROD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
)
df_db2_PROD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_in)
    .load()
)

# db2_EXPRNC_CAT_in
extract_query_db2_EXPRNC_CAT_in = (
    f"SELECT\n"
    f"EXP.EXPRNC_CAT_SK,\n"
    f"EXP.EXPRNC_CAT_CD,\n"
    f"EXP.EXPRNC_CAT_DESC\n"
    f"FROM {IDSOwner}.EXPRNC_CAT EXP"
)
df_db2_EXPRNC_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EXPRNC_CAT_in)
    .load()
)

# db2_PROD_SH_NM_in
extract_query_db2_PROD_SH_NM_in = (
    f"SELECT\n"
    f"PSHNM.PROD_SH_NM_SK,\n"
    f"PSHNM.PROD_SH_NM,\n"
    f"PSHNM.MCARE_SUPLMT_COV_IN,\n"
    f"PSHNM.PROD_SH_NM_DLVRY_METH_CD_SK,\n"
    f"PSHNM.PROD_SH_NM_CAT_CD_SK,\n"
    f"PSHNM.PROD_SH_NM_DESC\n"
    f"FROM {IDSOwner}.PROD_SH_NM PSHNM"
)
df_db2_PROD_SH_NM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_SH_NM_in)
    .load()
)

# db2_FNCL_LOB_in
extract_query_db2_FNCL_LOB_in = (
    f"SELECT\n"
    f"FNCL.FNCL_LOB_SK,\n"
    f"FNCL.FNCL_LOB_CD,\n"
    f"FNCL.EFF_DT_SK,\n"
    f"FNCL.FNCL_LOB_DESC\n"
    f"FROM {IDSOwner}.FNCL_LOB FNCL"
)
df_db2_FNCL_LOB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_FNCL_LOB_in)
    .load()
)

# db2_QHP_in
extract_query_db2_QHP_in = (
    f"SELECT\n"
    f"QHP_SK,\n"
    f"QHP_ID,\n"
    f"QHP_CSR_VRNT_CD_SK,\n"
    f"QHP_ENR_TYP_CD_SK,\n"
    f"QHP_EXCH_CHAN_CD_SK,\n"
    f"QHP_EXCH_TYP_CD_SK,\n"
    f"QHP_METAL_LVL_CD_SK\n"
    f"FROM {IDSOwner}.QHP"
)
df_db2_QHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_QHP_in)
    .load()
)

# db2_ACTURL_in
extract_query_db2_ACTURL_in = (
    f"SELECT\n"
    f"QHP_SK,\n"
    f"ACTURL_VAL_NO\n"
    f"FROM {IDSOwner}.ACTURL_VAL\n"
    f"WHERE\n"
    f"QHP_SK NOT IN (0,1)"
)
df_db2_ACTURL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ACTURL_in)
    .load()
)

# db2_SPIRA_BNF_ID_in (EDW database)
extract_query_db2_SPIRA_BNF_ID_in = (
    f"SELECT distinct PROD_ID, SPIRA_BNF_ID\n"
    f"FROM {EDWOwner}.PROD_DSGN_SUM_F\n"
    f"WHERE ( PROD_CMPNT_EFF_DT_SK <= '{EDWRunCycleDate}' and PROD_CMPNT_TERM_DT_SK >= '{EDWRunCycleDate}' )"
)
df_db2_SPIRA_BNF_ID_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_SPIRA_BNF_ID_in)
    .load()
)

# db2_PROD_CMPNT_in
extract_query_db2_PROD_CMPNT_in = (
    f"SELECT \n"
    f"PROD.PROD_SK,\n"
    f"CD1.TRGT_CD AS PROD_SH_NM_TRGT_CD,\n"
    f"CD2.TRGT_CD AS PROD_CMPNT_TRGT_CD\n"
    f"FROM {IDSOwner}.PROD PROD\n"
    f"INNER JOIN {IDSOwner}.PROD_CMPNT PC\n"
    f"on PROD.PROD_SK = PC.PROD_SK\n"
    f"INNER JOIN {IDSOwner}.PROD_SH_NM PSN\n"
    f"on PROD.PROD_SH_NM_SK = PSN.PROD_SH_NM_SK\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG CD1\n"
    f"ON PSN.PROD_SH_NM_CAT_CD_SK = CD1.CD_MPPNG_SK\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG CD2\n"
    f"ON PC.PROD_CMPNT_TYP_CD_SK = CD2.CD_MPPNG_SK\n"
    f"where (PC.PROD_CMPNT_EFF_DT_SK <= '{EDWRunCycleDate}' AND PC.PROD_CMPNT_TERM_DT_SK >= '{EDWRunCycleDate}')\n"
    f"and CD1.TRGT_CD = 'PPO'\n"
    f"and CD2.TRGT_CD = 'EPO'"
)
df_db2_PROD_CMPNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_CMPNT_in)
    .load()
)

# lkp_Codes1 (multiple left joins)
df_lkp_Codes1 = (
    df_db2_PROD_in.alias("lnk_IdsEdwProdDExtr_InABC")
    .join(
        df_db2_EXPRNC_CAT_in.alias("Ref_ExprncCatSk_Lkup"),
        col("lnk_IdsEdwProdDExtr_InABC.EXPRNC_CAT_SK") == col("Ref_ExprncCatSk_Lkup.EXPRNC_CAT_SK"),
        "left"
    )
    .join(
        df_db2_FNCL_LOB_in.alias("Ref_FnclLobSk_Lkup"),
        col("lnk_IdsEdwProdDExtr_InABC.FNCL_LOB_SK") == col("Ref_FnclLobSk_Lkup.FNCL_LOB_SK"),
        "left"
    )
    .join(
        df_db2_PROD_SH_NM_in.alias("Ref_ProdShNmSk_Lkp"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_SH_NM_SK") == col("Ref_ProdShNmSk_Lkp.PROD_SH_NM_SK"),
        "left"
    )
    .join(
        df_db2_QHP_in.alias("Ref_QhpSk_Lkup"),
        col("lnk_IdsEdwProdDExtr_InABC.QHP_SK") == col("Ref_QhpSk_Lkup.QHP_SK"),
        "left"
    )
    .join(
        df_db2_ACTURL_in.alias("Ref_Acturl_Lkup"),
        col("lnk_IdsEdwProdDExtr_InABC.QHP_SK") == col("Ref_Acturl_Lkup.QHP_SK"),
        "left"
    )
    .join(
        df_db2_SPIRA_BNF_ID_in.alias("Ref_Spira_Bnf_Id_Lkup"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_ID") == col("Ref_Spira_Bnf_Id_Lkup.PROD_ID"),
        "left"
    )
    .join(
        df_db2_PROD_CMPNT_in.alias("Ref_ProdCMPNT_Lkp"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_SK") == col("Ref_ProdCMPNT_Lkp.PROD_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwProdDExtr_InABC.PROD_SK").alias("PROD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_ID").alias("PROD_ID"),
        col("lnk_IdsEdwProdDExtr_InABC.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.DNTL_BILL_PFX_CD_SK").alias("DNTL_BILL_PFX_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_ACCUM_SFX_CD_SK").alias("PROD_ACCUM_SFX_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_PCKG_CD_SK").alias("PROD_PCKG_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_RATE_TYP_CD_SK").alias("PROD_RATE_TYP_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_ST_CD_SK").alias("PROD_ST_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.SUBPROD_CD_SK").alias("SUBPROD_CD_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.DNTL_LATE_WAIT_IN").alias("DNTL_LATE_WAIT_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.DRUG_COV_IN").alias("DRUG_COV_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.HLTH_COV_IN").alias("HLTH_COV_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.MNL_PRCS_IN").alias("MNL_PRCS_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.MNTL_HLTH_COV_IN").alias("MNTL_HLTH_COV_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.MO_HLTH_INSUR_POOL_IN").alias("MO_HLTH_INSUR_POOL_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.RQRD_PCP_IN").alias("RQRD_PCP_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.VSN_COV_IN").alias("VSN_COV_IN"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
        col("lnk_IdsEdwProdDExtr_InABC.FCTS_CONV_RATE_PFX").alias("FCTS_CONV_RATE_PFX"),
        col("lnk_IdsEdwProdDExtr_InABC.LOB_NO").alias("LOB_NO"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_ABBR").alias("PROD_ABBR"),
        col("lnk_IdsEdwProdDExtr_InABC.PROD_DESC").alias("PROD_DESC"),
        col("Ref_ExprncCatSk_Lkup.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        col("Ref_ExprncCatSk_Lkup.EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
        col("Ref_FnclLobSk_Lkup.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        col("Ref_FnclLobSk_Lkup.EFF_DT_SK").alias("EFF_DT_SK"),
        col("Ref_FnclLobSk_Lkup.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        col("Ref_ProdShNmSk_Lkp.PROD_SH_NM").alias("PROD_SH_NM"),
        col("Ref_ProdShNmSk_Lkp.PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
        col("Ref_ProdShNmSk_Lkp.PROD_SH_NM_CAT_CD_SK").alias("PROD_SH_NM_CAT_CD_SK"),
        col("Ref_ProdShNmSk_Lkp.MCARE_SUPLMT_COV_IN").alias("MCARE_SUPLMT_COV_IN"),
        col("Ref_ProdShNmSk_Lkp.PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
        col("lnk_IdsEdwProdDExtr_InABC.QHP_SK").alias("QHP_SK"),
        col("Ref_QhpSk_Lkup.QHP_ID").alias("QHP_ID"),
        col("Ref_QhpSk_Lkup.QHP_CSR_VRNT_CD_SK").alias("QHP_CSR_VRNT_CD_SK"),
        col("Ref_QhpSk_Lkup.QHP_ENR_TYP_CD_SK").alias("QHP_ENR_TYP_CD_SK"),
        col("Ref_QhpSk_Lkup.QHP_EXCH_CHAN_CD_SK").alias("QHP_EXCH_CHAN_CD_SK"),
        col("Ref_QhpSk_Lkup.QHP_EXCH_TYP_CD_SK").alias("QHP_EXCH_TYP_CD_SK"),
        col("Ref_QhpSk_Lkup.QHP_METAL_LVL_CD_SK").alias("QHP_METAL_LVL_CD_SK"),
        col("Ref_Acturl_Lkup.ACTURL_VAL_NO").alias("ACTURL_VAL_NO"),
        col("Ref_Spira_Bnf_Id_Lkup.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
        col("Ref_ProdCMPNT_Lkp.PROD_SH_NM_TRGT_CD").alias("PROD_SH_NM_TRGT_CD"),
        col("Ref_ProdCMPNT_Lkp.PROD_CMPNT_TRGT_CD").alias("PROD_CMPNT_TGRT_CD")
    )
)

# lkp_Codes (multiple left joins with df_cpy_cd_mppng)
df_lkp_Codes = (
    df_lkp_Codes1.alias("lnk_FkeyLkpData_out")
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdShNmDlvryMethCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_DLVRY_METH_CD_SK") == col("Ref_ProdShNmDlvryMethCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdDntlBillPfxCd_Lkup"),
        col("lnk_FkeyLkpData_out.DNTL_BILL_PFX_CD_SK") == col("Ref_ProdDntlBillPfxCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdLobCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_LOB_CD_SK") == col("Ref_ProdLobCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdPckgCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_PCKG_CD_SK") == col("Ref_ProdPckgCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdRateTypCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_RATE_TYP_CD_SK") == col("Ref_ProdRateTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdStCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_ST_CD_SK") == col("Ref_ProdStCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdSubprodCd_Lkup"),
        col("lnk_FkeyLkpData_out.SUBPROD_CD_SK") == col("Ref_ProdSubprodCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_ProdShNmCatCd_Lkup"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_CAT_CD_SK") == col("Ref_ProdShNmCatCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_QhpCsrVrntCd_Lkup"),
        col("lnk_FkeyLkpData_out.QHP_CSR_VRNT_CD_SK") == col("Ref_QhpCsrVrntCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_QhpEnrTypCd_Lkup"),
        col("lnk_FkeyLkpData_out.QHP_ENR_TYP_CD_SK") == col("Ref_QhpEnrTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_QhpExchChanCd_Lkup"),
        col("lnk_FkeyLkpData_out.QHP_EXCH_CHAN_CD_SK") == col("Ref_QhpExchChanCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_QhpExchTypCd_Lkup"),
        col("lnk_FkeyLkpData_out.QHP_EXCH_TYP_CD_SK") == col("Ref_QhpExchTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_QhpMetalLvlCd_Lkup"),
        col("lnk_FkeyLkpData_out.QHP_METAL_LVL_CD_SK") == col("Ref_QhpMetalLvlCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_FkeyLkpData_out.PROD_SK").alias("PROD_SK"),
        col("lnk_FkeyLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_FkeyLkpData_out.PROD_ID").alias("PROD_ID"),
        col("lnk_FkeyLkpData_out.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        col("lnk_FkeyLkpData_out.EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
        col("lnk_FkeyLkpData_out.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        col("lnk_FkeyLkpData_out.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        col("lnk_FkeyLkpData_out.PROD_ABBR").alias("PROD_ABBR"),
        col("Ref_ProdDntlBillPfxCd_Lkup.TRGT_CD").alias("PROD_DNTL_BILL_PFX_CD"),
        col("Ref_ProdDntlBillPfxCd_Lkup.TRGT_CD_NM").alias("PROD_DNTL_BILL_PFX_NM"),
        col("lnk_FkeyLkpData_out.DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
        col("lnk_FkeyLkpData_out.PROD_DESC").alias("PROD_DESC"),
        col("lnk_FkeyLkpData_out.DRUG_COV_IN").alias("PROD_DRUG_COV_IN"),
        col("lnk_FkeyLkpData_out.PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
        col("lnk_FkeyLkpData_out.FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX"),
        col("lnk_FkeyLkpData_out.HLTH_COV_IN").alias("PROD_HLTH_COV_IN"),
        col("lnk_FkeyLkpData_out.LOB_NO").alias("PROD_LOB_NO"),
        col("Ref_ProdLobCd_Lkup.TRGT_CD").alias("PROD_LOB_CD"),
        col("Ref_ProdLobCd_Lkup.TRGT_CD_NM").alias("PROD_LOB_NM"),
        col("Ref_ProdPckgCd_Lkup.TRGT_CD").alias("PROD_PCKG_CD"),
        col("Ref_ProdPckgCd_Lkup.TRGT_CD_NM").alias("PROD_PCKG_DESC"),
        col("lnk_FkeyLkpData_out.MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN"),
        col("lnk_FkeyLkpData_out.MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN"),
        col("Ref_ProdRateTypCd_Lkup.TRGT_CD").alias("PROD_RATE_TYP_CD"),
        col("Ref_ProdRateTypCd_Lkup.TRGT_CD_NM").alias("PROD_RATE_TYP_NM"),
        col("Ref_ProdStCd_Lkup.TRGT_CD").alias("PROD_ST_CD"),
        col("Ref_ProdStCd_Lkup.TRGT_CD_NM").alias("PROD_ST_NM"),
        col("Ref_ProdSubprodCd_Lkup.TRGT_CD").alias("PROD_SUBPROD_CD"),
        col("Ref_ProdSubprodCd_Lkup.TRGT_CD_NM").alias("PROD_SUBPROD_NM"),
        col("lnk_FkeyLkpData_out.PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
        col("lnk_FkeyLkpData_out.VSN_COV_IN").alias("PROD_VSN_COV_IN"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM").alias("PROD_SH_NM"),
        col("Ref_ProdShNmCatCd_Lkup.TRGT_CD").alias("PROD_SH_NM_CAT_CD"),
        col("Ref_ProdShNmCatCd_Lkup.TRGT_CD_NM").alias("PROD_SH_NM_CAT_NM"),
        col("Ref_ProdShNmDlvryMethCd_Lkup.TRGT_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
        col("Ref_ProdShNmDlvryMethCd_Lkup.TRGT_CD_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
        col("lnk_FkeyLkpData_out.MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
        col("lnk_FkeyLkpData_out.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        col("lnk_FkeyLkpData_out.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        col("lnk_FkeyLkpData_out.DNTL_BILL_PFX_CD_SK").alias("PROD_DNTL_BILL_PFX_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_PCKG_CD_SK").alias("PROD_PCKG_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_RATE_TYP_CD_SK").alias("PROD_RATE_TYP_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_CAT_CD_SK").alias("PROD_SH_NM_CAT_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
        col("lnk_FkeyLkpData_out.PROD_ST_CD_SK").alias("PROD_ST_CD_SK"),
        col("lnk_FkeyLkpData_out.SUBPROD_CD_SK").alias("PROD_SUBPROD_CD_SK"),
        col("lnk_FkeyLkpData_out.QHP_SK").alias("QHP_SK"),
        col("lnk_FkeyLkpData_out.QHP_ID").alias("QHP_ID"),
        col("Ref_QhpCsrVrntCd_Lkup.TRGT_CD").alias("QHP_CSR_VRNT_CD"),
        col("Ref_QhpCsrVrntCd_Lkup.TRGT_CD_NM").alias("QHP_CSR_VRNT_NM"),
        col("lnk_FkeyLkpData_out.QHP_CSR_VRNT_CD_SK").alias("QHP_CSR_VRNT_CD_SK"),
        col("Ref_QhpEnrTypCd_Lkup.TRGT_CD").alias("QHP_ENR_TYP_CD"),
        col("Ref_QhpEnrTypCd_Lkup.TRGT_CD_NM").alias("QHP_ENR_TYP_NM"),
        col("lnk_FkeyLkpData_out.QHP_ENR_TYP_CD_SK").alias("QHP_ENR_TYP_CD_SK"),
        col("Ref_QhpExchChanCd_Lkup.TRGT_CD").alias("QHP_EXCH_CHAN_CD"),
        col("Ref_QhpExchChanCd_Lkup.TRGT_CD_NM").alias("QHP_EXCH_CHAN_NM"),
        col("lnk_FkeyLkpData_out.QHP_EXCH_CHAN_CD_SK").alias("QHP_EXCH_CHAN_CD_SK"),
        col("Ref_QhpExchTypCd_Lkup.TRGT_CD").alias("QHP_EXCH_TYP_CD"),
        col("Ref_QhpExchTypCd_Lkup.TRGT_CD_NM").alias("QHP_EXCH_TYP_NM"),
        col("lnk_FkeyLkpData_out.QHP_EXCH_TYP_CD_SK").alias("QHP_EXCH_TYP_CD_SK"),
        col("Ref_QhpMetalLvlCd_Lkup.TRGT_CD").alias("QHP_METAL_LVL_CD"),
        col("Ref_QhpMetalLvlCd_Lkup.TRGT_CD_NM").alias("QHP_METAL_LVL_NM"),
        col("lnk_FkeyLkpData_out.QHP_METAL_LVL_CD_SK").alias("QHP_METAL_LVL_CD_SK"),
        col("lnk_FkeyLkpData_out.ACTURL_VAL_NO").alias("ACTURL_VAL_NO"),
        col("lnk_FkeyLkpData_out.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
        col("lnk_FkeyLkpData_out.PROD_SH_NM_TRGT_CD").alias("PROD_SH_NM_TRGT_CD"),
        col("lnk_FkeyLkpData_out.PROD_CMPNT_TGRT_CD").alias("PROD_CMPNT_TGRT_CD")
    )
)

# xfrm_BusinessLogic: build a DataFrame with transformations
df_xfrm_BusinessLogic = df_lkp_Codes

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "SRC_SYS_CD",
    when((col("SRC_SYS_CD").isNull()) | (trim(col("SRC_SYS_CD")) == ""), "UNK").otherwise(col("SRC_SYS_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_DNTL_BILL_PFX_CD",
    when((col("PROD_DNTL_BILL_PFX_CD").isNull()) | (trim(col("PROD_DNTL_BILL_PFX_CD")) == ""), "UNK").otherwise(col("PROD_DNTL_BILL_PFX_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_DNTL_BILL_PFX_NM",
    when(
        (col("PROD_DNTL_BILL_PFX_NM").isNull()) | (trim(col("PROD_DNTL_BILL_PFX_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_DNTL_BILL_PFX_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_LOB_CD",
    when((col("PROD_LOB_CD").isNull()) | (trim(col("PROD_LOB_CD")) == ""), "UNK").otherwise(col("PROD_LOB_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_LOB_NM",
    when(
        (col("PROD_LOB_NM").isNull()) | (trim(col("PROD_LOB_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_LOB_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_PCKG_CD",
    when((col("PROD_PCKG_CD").isNull()) | (trim(col("PROD_PCKG_CD")) == ""), "UNK").otherwise(col("PROD_PCKG_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_PCKG_DESC",
    when(
        (col("PROD_PCKG_DESC").isNull()) | (trim(col("PROD_PCKG_DESC")) == ""),
        "UNK"
    ).otherwise(col("PROD_PCKG_DESC"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_RATE_TYP_CD",
    when((col("PROD_RATE_TYP_CD").isNull()) | (trim(col("PROD_RATE_TYP_CD")) == ""), "UNK").otherwise(col("PROD_RATE_TYP_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_RATE_TYP_NM",
    when(
        (col("PROD_RATE_TYP_NM").isNull()) | (trim(col("PROD_RATE_TYP_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_RATE_TYP_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_ST_CD",
    when((col("PROD_ST_CD").isNull()) | (trim(col("PROD_ST_CD")) == ""), "UNK").otherwise(col("PROD_ST_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_ST_NM",
    when(
        (col("PROD_ST_NM").isNull()) | (trim(col("PROD_ST_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_ST_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SUBPROD_CD",
    when((col("PROD_SUBPROD_CD").isNull()) | (trim(col("PROD_SUBPROD_CD")) == ""), "UNK").otherwise(col("PROD_SUBPROD_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SUBPROD_NM",
    when(
        (col("PROD_SUBPROD_NM").isNull()) | (trim(col("PROD_SUBPROD_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_SUBPROD_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SH_NM_CAT_CD",
    when((col("PROD_SH_NM_CAT_CD").isNull()) | (trim(col("PROD_SH_NM_CAT_CD")) == ""), "UNK").otherwise(col("PROD_SH_NM_CAT_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SH_NM_CAT_NM",
    when(
        (col("PROD_SH_NM_CAT_NM").isNull()) | (trim(col("PROD_SH_NM_CAT_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_SH_NM_CAT_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SH_NM_DLVRY_METH_CD",
    when((col("PROD_SH_NM_DLVRY_METH_CD").isNull()) | (trim(col("PROD_SH_NM_DLVRY_METH_CD")) == ""), "UNK").otherwise(col("PROD_SH_NM_DLVRY_METH_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PROD_SH_NM_DLVRY_METH_NM",
    when(
        (col("PROD_SH_NM_DLVRY_METH_NM").isNull()) | (trim(col("PROD_SH_NM_DLVRY_METH_NM")) == ""),
        "UNK"
    ).otherwise(col("PROD_SH_NM_DLVRY_METH_NM"))
)

# QHP_ID
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_ID",
    when(col("QHP_ID").isNull(), lit("1"))
    .when(trim(col("QHP_ID")) == "", lit("1"))
    .otherwise(col("QHP_ID"))
)

# QHP_CSR_VRNT_CD
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_CSR_VRNT_CD",
    when(col("QHP_CSR_VRNT_CD").isNull(), lit("NA"))
    .when(trim(col("QHP_CSR_VRNT_CD")) == "", lit("NA"))
    .otherwise(col("QHP_CSR_VRNT_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_CSR_VRNT_NM",
    when(col("QHP_CSR_VRNT_NM").isNull(), lit("NA"))
    .when(trim(col("QHP_CSR_VRNT_NM")) == "", lit("NA"))
    .otherwise(col("QHP_CSR_VRNT_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_CSR_VRNT_CD_SK",
    when(col("QHP_CSR_VRNT_CD_SK").isNull(), lit("1"))
    .when(trim(col("QHP_CSR_VRNT_CD_SK")) == "", lit("1"))
    .otherwise(col("QHP_CSR_VRNT_CD_SK"))
)

# QHP_ENR_TYP_CD
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_ENR_TYP_CD",
    when(col("QHP_ENR_TYP_CD").isNull(), lit("NA"))
    .when(trim(col("QHP_ENR_TYP_CD")) == "", lit("NA"))
    .otherwise(col("QHP_ENR_TYP_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_ENR_TYP_NM",
    when(col("QHP_ENR_TYP_NM").isNull(), lit("NA"))
    .when(trim(col("QHP_ENR_TYP_NM")) == "", lit("NA"))
    .otherwise(col("QHP_ENR_TYP_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_ENR_TYP_CD_SK",
    when(col("QHP_ENR_TYP_CD_SK").isNull(), lit("1"))
    .when(trim(col("QHP_ENR_TYP_CD_SK")) == "", lit("1"))
    .otherwise(col("QHP_ENR_TYP_CD_SK"))
)

# QHP_EXCH_CHAN_CD
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_CHAN_CD",
    when(col("QHP_EXCH_CHAN_CD").isNull(), lit("NA"))
    .when(trim(col("QHP_EXCH_CHAN_CD")) == "", lit("NA"))
    .otherwise(col("QHP_EXCH_CHAN_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_CHAN_NM",
    when(col("QHP_EXCH_CHAN_NM").isNull(), lit("NA"))
    .when(trim(col("QHP_EXCH_CHAN_NM")) == "", lit("NA"))
    .otherwise(col("QHP_EXCH_CHAN_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_CHAN_CD_SK",
    when(col("QHP_EXCH_CHAN_CD_SK").isNull(), lit("1"))
    .when(trim(col("QHP_EXCH_CHAN_CD_SK")) == "", lit("1"))
    .otherwise(col("QHP_EXCH_CHAN_CD_SK"))
)

# QHP_EXCH_TYP_CD
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_TYP_CD",
    when(col("QHP_EXCH_TYP_CD").isNull(), lit("NA"))
    .when(trim(col("QHP_EXCH_TYP_CD")) == "", lit("NA"))
    .otherwise(col("QHP_EXCH_TYP_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_TYP_NM",
    when(col("QHP_EXCH_TYP_NM").isNull(), lit("NA"))
    .when(trim(col("QHP_EXCH_TYP_NM")) == "", lit("NA"))
    .otherwise(col("QHP_EXCH_TYP_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_EXCH_TYP_CD_SK",
    when(col("QHP_EXCH_TYP_CD_SK").isNull(), lit("1"))
    .when(trim(col("QHP_EXCH_TYP_CD_SK")) == "", lit("1"))
    .otherwise(col("QHP_EXCH_TYP_CD_SK"))
)

# QHP_METAL_LVL_CD
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_METAL_LVL_CD",
    when(col("QHP_METAL_LVL_CD").isNull(), lit("NA"))
    .when(trim(col("QHP_METAL_LVL_CD")) == "", lit("NA"))
    .otherwise(col("QHP_METAL_LVL_CD"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_METAL_LVL_NM",
    when(col("QHP_METAL_LVL_NM").isNull(), lit("NA"))
    .when(trim(col("QHP_METAL_LVL_NM")) == "", lit("NA"))
    .otherwise(col("QHP_METAL_LVL_NM"))
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "QHP_METAL_LVL_CD_SK",
    when(col("QHP_METAL_LVL_CD_SK").isNull(), lit("1"))
    .when(trim(col("QHP_METAL_LVL_CD_SK")) == "", lit("1"))
    .otherwise(col("QHP_METAL_LVL_CD_SK"))
)

# ACTURL_VAL_NO
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "ACTURL_VAL_NO",
    when(col("ACTURL_VAL_NO").isNull(), lit(None)).otherwise(col("ACTURL_VAL_NO"))
)

# SPIRA_BNF_ID
# " If Trim(...) = '' Then 'NA' Else If PROD_TERM_DT_SK > EDWRunCycleDate Then SPIRA_BNF_ID Else 'NA'"
# We compare numeric or string? The stage code compares dates as string, so do:
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "SPIRA_BNF_ID",
    when(
        (trim(coalesce(col("SPIRA_BNF_ID"), lit(""))) == ""),
        "NA"
    ).otherwise(
        when(
            col("PROD_TERM_DT_SK") > lit(EDWRunCycleDate),
            col("SPIRA_BNF_ID")
        ).otherwise("NA")
    )
)

# EPO_IN
# "if PROD_SH_NM_TRGT_CD='PPO' AND PROD_CMPNT_TGRT_CD='EPO' then 'Y' else 'N'"
# plus the check if trim is empty => 'N'
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "EPO_IN",
    when(
        (trim(coalesce(col("PROD_SH_NM_TRGT_CD"), lit(""))) == "PPO") &
        (trim(coalesce(col("PROD_CMPNT_TGRT_CD"), lit(""))) == "EPO"),
        lit("Y")
    ).otherwise(lit("N"))
)

# We now create two separate outputs from df_xfrm_BusinessLogic.

# 1) lnk_IdsEdwProdDExtr_OutABC => seq_PROD_D_csv_load
# The column order exactly as in the outputPins:

df_lnk_IdsEdwProdDExtr_OutABC = df_xfrm_BusinessLogic.select(
    "PROD_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    # CRT_RUN_CYC_EXCTN_DT_SK => EDWRunCycleDate
    (lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK")),
    # LAST_UPDT_RUN_CYC_EXCTN_DT_SK => EDWRunCycleDate
    (lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")),
    "EXPRNC_CAT_CD",
    "EXPRNC_CAT_DESC",
    "FNCL_LOB_CD",
    "FNCL_LOB_DESC",
    "PROD_ABBR",
    "PROD_DNTL_BILL_PFX_CD",
    "PROD_DNTL_BILL_PFX_NM",
    col("PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
    "PROD_DESC",
    "PROD_DRUG_COV_IN",
    "PROD_EFF_DT_SK",
    "PROD_FCTS_CONV_RATE_PFX",
    "PROD_HLTH_COV_IN",
    "PROD_LOB_NO",
    "PROD_LOB_CD",
    "PROD_LOB_NM",
    "PROD_PCKG_CD",
    "PROD_PCKG_DESC",
    "PROD_MNL_PRCS_IN",
    "PROD_MNTL_HLTH_COV_IN",
    "PROD_RATE_TYP_CD",
    "PROD_RATE_TYP_NM",
    "PROD_ST_CD",
    "PROD_ST_NM",
    "PROD_SUBPROD_CD",
    "PROD_SUBPROD_NM",
    "PROD_TERM_DT_SK",
    "PROD_VSN_COV_IN",
    "PROD_SH_NM",
    "PROD_SH_NM_CAT_CD",
    "PROD_SH_NM_CAT_NM",
    "PROD_SH_NM_DLVRY_METH_CD",
    "PROD_SH_NM_DLVRY_METH_NM",
    "PROD_SH_NM_DESC",
    "PROD_SHNM_MCARE_SUPLMT_COV_IN",
    # next two columns are EDWRunCycle => numeric, plus we also have them as int? The job uses "CRT_RUN_CYC_EXCTN_SK" => EDWRunCycle, "LAST_UPDT_RUN_CYC_EXCTN_SK" => EDWRunCycle
    (lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")),
    (lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")),
    "EXPRNC_CAT_SK",
    "FNCL_LOB_SK",
    "PROD_DNTL_BILL_PFX_CD_SK",
    "PROD_LOB_CD_SK",
    "PROD_PCKG_CD_SK",
    "PROD_RATE_TYP_CD_SK",
    "PROD_SH_NM_SK",
    "PROD_SH_NM_CAT_CD_SK",
    "PROD_SH_NM_DLVRY_METH_CD_SK",
    "PROD_ST_CD_SK",
    "PROD_SUBPROD_CD_SK",
    "QHP_SK",
    "QHP_ID",
    "QHP_CSR_VRNT_CD",
    "QHP_CSR_VRNT_NM",
    "QHP_CSR_VRNT_CD_SK",
    "QHP_ENR_TYP_CD",
    "QHP_ENR_TYP_NM",
    "QHP_ENR_TYP_CD_SK",
    "QHP_EXCH_CHAN_CD",
    "QHP_EXCH_CHAN_NM",
    "QHP_EXCH_CHAN_CD_SK",
    "QHP_EXCH_TYP_CD",
    "QHP_EXCH_TYP_NM",
    "QHP_EXCH_TYP_CD_SK",
    "QHP_METAL_LVL_CD",
    "QHP_METAL_LVL_NM",
    "QHP_METAL_LVL_CD_SK",
    "ACTURL_VAL_NO",
    "SPIRA_BNF_ID",
    "EPO_IN"
)

# For char columns, apply rpad(...) with the defined length:

df_lnk_IdsEdwProdDExtr_OutABC = df_lnk_IdsEdwProdDExtr_OutABC \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("PROD_DNTL_LATE_WAIT_IN", rpad(col("PROD_DNTL_LATE_WAIT_IN"), 1, " ")) \
    .withColumn("PROD_DRUG_COV_IN", rpad(col("PROD_DRUG_COV_IN"), 1, " ")) \
    .withColumn("PROD_HLTH_COV_IN", rpad(col("PROD_HLTH_COV_IN"), 1, " ")) \
    .withColumn("PROD_MNL_PRCS_IN", rpad(col("PROD_MNL_PRCS_IN"), 1, " ")) \
    .withColumn("PROD_MNTL_HLTH_COV_IN", rpad(col("PROD_MNTL_HLTH_COV_IN"), 1, " ")) \
    .withColumn("PROD_VSN_COV_IN", rpad(col("PROD_VSN_COV_IN"), 1, " ")) \
    .withColumn("PROD_EFF_DT_SK", rpad(col("PROD_EFF_DT_SK"), 10, " ")) \
    .withColumn("PROD_TERM_DT_SK", rpad(col("PROD_TERM_DT_SK"), 10, " ")) \
    .withColumn("PROD_FCTS_CONV_RATE_PFX", rpad(col("PROD_FCTS_CONV_RATE_PFX"), 4, " ")) \
    .withColumn("LOB_NO", rpad(col("LOB_NO"), 4, " ")) \
    .withColumn("PROD_ABBR", rpad(col("PROD_ABBR"), 2, " "))

# 2) lnk_SnapShotFile => seq_B_PROD_csv_bal
df_lnk_SnapShotFile = df_xfrm_BusinessLogic.select(
    when((col("SRC_SYS_CD").isNull()) | (trim(col("SRC_SYS_CD")) == ""), "UNK").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD")
)

# Write seq_PROD_D_csv_load
# "PROD_D.dat" => in "load" => that goes to f"{adls_path}/load/PROD_D.dat"
# ContainsHeader = false, columnDelimiter=",", quoteChar="^", nullValue => not specified => pass None
write_files(
    df_lnk_IdsEdwProdDExtr_OutABC,
    f"{adls_path}/load/PROD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Write seq_B_PROD_csv_bal
# "B_PROD_D.dat" => in "load"
write_files(
    df_lnk_SnapShotFile,
    f"{adls_path}/load/B_PROD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)