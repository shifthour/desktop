# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2015 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmLnExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim Line Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Manisha G                          US 404552                                 Initial programming                                           IntegrateDev2                
# MAGIC 
# MAGIC 2023-08-01      Revathi Boojireddy              US 589700           Added two new fields SNOMED_CT_CD,CVX_VCCN_CD    IntegrateDevB\(9)Harsha Ravuri\(9)2023-08-29
# MAGIC                                                                                                       with a default value in Snapshot  stage and
# MAGIC \(9)                                                                                      mapped it till target MAInboundClmLnExtr file stage     
# MAGIC 2024-02-02     Saranya A                            US 611660              Updated PROC_CD_TYP_CD in Transformer Snapshot     IntegrateDev2          Jeyaprasanna          2024-02-21
# MAGIC                                                                                                     for Dental Claims
# MAGIC 
# MAGIC 2024-04-04     Ediga Maruthi                       US 614444           Added Columns DOC_NO,USER_DEFN_TX_1,                       IntegrateDev1      Jeyaprasanna           2024-04-19
# MAGIC                                                                                                 USER_DEFN_TX_2,USER_DEFN_DT_1,USER_DEFN_DT_2,
# MAGIC                                                                                                 USER_DEFN_AMT_1,USER_DEFN_AMT_2 in MAInboundLanding.

# MAGIC Read the MAInbound Claim Line file created
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC This container is used in:
# MAGIC ESIClmLnExtr
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnMedTrns
# MAGIC MCSourceClmLnExtr
# MAGIC MedicaidClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PcsClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MedtrakClmLnExtr
# MAGIC BCBSSCClmLnExtr
# MAGIC BCBSSCMedClmLnExtr
# MAGIC EyeMedClmLnExtr
# MAGIC DentaQuestClmLnExtr
# MAGIC MAInboundClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
Logging = get_widget_value('Logging','')
InFile_F = get_widget_value('InFile_F','')

schema_MAInboundLanding = StructType([
    StructField("REC_TYPE", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_TYP_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLAIM_STS_CD", StringType(), True),
    StructField("CLM_LN_SEQ_NO", StringType(), True),
    StructField("CLM_ADJ_IN", StringType(), True),
    StructField("CLM_ADJ_FROM_CLM_ID", StringType(), True),
    StructField("CLM_ADJ_TO_CLM_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MCARE_BNFCRY_ID", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("MBR_BRTH_DT", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_1", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_2", StringType(), True),
    StructField("MBR_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("MBR_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("MBR_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NM", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), True),
    StructField("PROV_TXNMY_CD", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_1", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_2", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_3", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("CLM_SVC_PROV_SPEC_CD", StringType(), True),
    StructField("SVC_PROV_ID", StringType(), True),
    StructField("SVC_PROV_NM", StringType(), True),
    StructField("SVC_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("SVC_PROV_TAX_ID", StringType(), True),
    StructField("SVC_PROV_TXNMY_CD", StringType(), True),
    StructField("SVC_PROV_ADDR_LN1", StringType(), True),
    StructField("SVC_PROV_ADDR_LN2", StringType(), True),
    StructField("SVC_PROV_ADDR_LN3", StringType(), True),
    StructField("SVC_PROV_CITY_NM", StringType(), True),
    StructField("SVC_PROV_ST_CD", StringType(), True),
    StructField("SVC_PROV_ZIP_CD_5", StringType(), True),
    StructField("CLM_SVC_STRT_DT", StringType(), True),
    StructField("CLM_SVC_END_DT", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("CLM_PAYE_CD", StringType(), True),
    StructField("CLM_NTWK_STTUS_CD", StringType(), True),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("CLM_LN_CAP_LN_IN", StringType(), True),
    StructField("Claim_Line_Denied_Indicator", StringType(), True),
    StructField("Explanation_Code", StringType(), True),
    StructField("Explanation_Code_Description", StringType(), True),
    StructField("DNTL_CLM_LN_TOOTH_NO", StringType(), True),
    StructField("DNTL_CLM_LN_TOOTH_SRFC_TX", StringType(), True),
    StructField("Adjustment_Reason_Code", StringType(), True),
    StructField("Remittance_Advice_Remark_Code_RARC", StringType(), True),
    StructField("DIAG_CD_TYP_CD", StringType(), True),
    StructField("PROC_CD", StringType(), True),
    StructField("PROC_CD_DESC", StringType(), True),
    StructField("Procedure_Code_Modifier1", StringType(), True),
    StructField("Procedure_Code_Modifier2", StringType(), True),
    StructField("Procedure_Code_Modifier3", StringType(), True),
    StructField("CLM_LN_SVC_STRT_DT", StringType(), True),
    StructField("CLM_LN_SVC_END_DT", StringType(), True),
    StructField("CLM_LN_CHRG_AMT", StringType(), True),
    StructField("CLM_LN_ALW_AMT", StringType(), True),
    StructField("CLM_LN_DSALW_AMT", StringType(), True),
    StructField("CLM_LN_COPAY_AMT", StringType(), True),
    StructField("CLM_LN_COINS_AMT", StringType(), True),
    StructField("CLM_LN_DEDCT_AMT", StringType(), True),
    StructField("Plan_Pay_Amount", StringType(), True),
    StructField("Patient_Responsibility_Amount", StringType(), True),
    StructField("CLM_LN_AGMNT_PRICE_AMT", StringType(), True),
    StructField("CLM_LN_RISK_WTHLD_AMT", StringType(), True),
    StructField("CLM_PROV_ROLE_TYPE_CD", StringType(), True),
    StructField("CLM_LN_PD_AMT", StringType(), True),
    StructField("CLM_COB_PD_AMT", StringType(), True),
    StructField("CLM_COB_ALW_AMT", StringType(), True),
    StructField("UNIT_CT", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("DOC_NO", StringType(), True),
    StructField("USER_DEFN_TX_1", StringType(), True),
    StructField("USER_DEFN_TX_2", StringType(), True),
    StructField("USER_DEFN_DT_1", StringType(), True),
    StructField("USER_DEFN_DT_2", StringType(), True),
    StructField("USER_DEFN_AMT_1", DecimalType(10,2), True),
    StructField("USER_DEFN_AMT_2", DecimalType(10,2), True)
])

df_MAInboundLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_MAInboundLanding)
    .load(f"{adls_path}/verified/{InFile_F}")
)

IDSOwnerParam = IDSOwner
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
SUB.SUB_ID,
PROD.LOB_NO
FROM {IDSOwnerParam}.GRP GRP
INNER JOIN {IDSOwnerParam}.SUB SUB
 ON GRP.GRP_SK = SUB.GRP_SK
INNER JOIN {IDSOwnerParam}.MBR MBR
 ON SUB.SUB_SK=MBR.SUB_SK
INNER JOIN {IDSOwnerParam}.MBR_ENR ENR
 ON MBR.MBR_SK= ENR.MBR_SK
 AND GRP.GRP_SK=ENR.GRP_SK
 AND MBR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
 AND MBR.SRC_SYS_CD_SK = ENR.SRC_SYS_CD_SK
 AND ENR.ELIG_IN ='Y'
INNER JOIN {IDSOwnerParam}.CD_MPPNG CD
 ON ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK
 AND CD.SRC_SYS_CD = 'FACETS'
 AND CD.SRC_CLCTN_CD = 'FACETS DBO'
 AND CD.SRC_DOMAIN_NM = 'CLASS PLAN PRODUCT CATEGORY'
 AND CD.TRGT_CLCTN_CD = 'IDS'
 AND CD.TRGT_DOMAIN_NM = 'CLASS PLAN PRODUCT CATEGORY'
LEFT OUTER JOIN {IDSOwnerParam}.PROD PROD
 ON PROD.PROD_SK = ENR.PROD_SK
 AND ENR.EFF_DT_SK <= PROD.PROD_TERM_DT_SK
 AND ENR.TERM_DT_SK >= PROD.PROD_EFF_DT_SK
"""

df_IDS_MBR_GRP_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_lkp_cls_sk = df_IDS_MBR_GRP_SUB.dropDuplicates(["SUB_ID"])

df_BusinessRules = df_MAInboundLanding.alias("Lnk_Extract").join(
    df_hf_lkp_cls_sk.alias("Lnk_Cls_Sk"),
    F.substring(F.trim(F.col("Lnk_Extract.MBR_ID")), 4, F.length(F.trim(F.col("Lnk_Extract.MBR_ID"))) - 3)
    == F.col("Lnk_Cls_Sk.SUB_ID"),
    "left"
)

df_BusinessRules_filtered = df_BusinessRules.filter(
    F.col("Lnk_Extract.CLM_LN_SEQ_NO") != 0
)

df_Transformcd = df_BusinessRules_filtered.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(0).alias("RECYCLE_CT"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("Lnk_Extract.CLM_ID"), F.lit(";1")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("Lnk_Extract.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Extract.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("CLM_SK"),
    F.col("Lnk_Extract.PROC_CD").alias("PROC_CD"),
    F.when(F.col("Lnk_Extract.SVC_PROV_ID").isNull(), F.lit("NA")).otherwise(F.col("Lnk_Extract.SVC_PROV_ID")).alias("SVC_PROV"),
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    F.lit("UNK").alias("CLM_LN_EOB_EXCD"),
    F.lit("ACPTD").alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.col("Lnk_Cls_Sk.LOB_NO").isNull(), F.lit("NA")).otherwise(F.col("Lnk_Cls_Sk.LOB_NO")).alias("CLM_LN_LOB_CD"),
    F.lit("NA").alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("NA").alias("CLM_LN_UNIT_TYP_CD"),
    F.lit("N").alias("CAP_LN_IN"),
    F.lit("N").alias("PRI_LOB_IN"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_SVC_END_DT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_SVC_END_DT"))) == 0),
        F.lit("2199-12-31")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_SVC_END_DT")).alias("SVC_END_DT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_SVC_STRT_DT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_SVC_STRT_DT"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_SVC_STRT_DT")).alias("SVC_STRT_DT"),
    F.col("Lnk_Extract.CLM_LN_AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_ALW_AMT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_ALW_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_ALW_AMT")).alias("ALW_AMT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_CHRG_AMT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_CHRG_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_CHRG_AMT")).alias("CHRG_AMT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_COINS_AMT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_COINS_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_COINS_AMT")).alias("COINS_AMT"),
    F.lit("0.00").alias("CNSD_CHRG_AMT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_COPAY_AMT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_COPAY_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_COPAY_AMT")).alias("COPAY_AMT"),
    F.when(
        (F.col("Lnk_Extract.CLM_LN_DEDCT_AMT").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.CLM_LN_DEDCT_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.CLM_LN_DEDCT_AMT")).alias("DEDCT_AMT"),
    F.col("Lnk_Extract.CLM_LN_DSALW_AMT").alias("DSALW_AMT"),
    F.lit("0.00").alias("ITS_HOME_DSCNT_AMT"),
    F.lit("0.00").alias("NO_RESP_AMT"),
    F.lit("0.00").alias("MBR_LIAB_BSS_AMT"),
    F.when(
        (F.col("Lnk_Extract.Patient_Responsibility_Amount").isNull()) | (F.length(F.trim(F.col("Lnk_Extract.Patient_Responsibility_Amount"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("Lnk_Extract.Patient_Responsibility_Amount")).alias("PATN_RESP_AMT"),
    F.when(
        F.col("Lnk_Extract.Plan_Pay_Amount").isNotNull(),
        F.col("Lnk_Extract.Plan_Pay_Amount")
    ).otherwise(F.lit("0.00")).alias("PAYBL_AMT"),
    F.lit("0.00").alias("PAYBL_TO_PROV_AMT"),
    F.lit("0.00").alias("PAYBL_TO_SUB_AMT"),
    F.lit("0.00").alias("PROC_TBL_PRICE_AMT"),
    F.lit("0.00").alias("PROFL_PRICE_AMT"),
    F.lit("0.00").alias("PROV_WRT_OFF_AMT"),
    F.col("Lnk_Extract.CLM_LN_RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.lit("0.00").alias("SVC_PRICE_AMT"),
    F.lit("0.00").alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.when(
        (F.col("Lnk_Extract.UNIT_CT").isNull()) | (F.col("Lnk_Extract.UNIT_CT") == ""),
        F.lit("1")
    ).otherwise(F.col("Lnk_Extract.UNIT_CT")).alias("UNIT_CT"),
    F.lit("NA").alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.lit("NA").alias("RFRL_SVC_SEQ_NO"),
    F.lit("NA").alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.lit("NA").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NA").alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.lit("NA").alias("SVC_PRICE_RULE_ID"),
    F.lit("NA").alias("SVC_RULE_TYP_TX"),
    F.lit("NA").alias("CLM_LN_SVC_LOC_TYP_CD"),
    F.lit("NA").alias("CLM_LN_SVC_PRICE_RULE_CD"),
    F.lit("0.00").alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("VBB_RULE"),
    F.lit("NA").alias("VBB_EXCD"),
    F.lit("N").alias("CLM_LN_VBB_IN"),
    F.lit("0.00").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit("0.00").alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID")
)

df_Snapshot = df_Transformcd.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("SVC_PROV").alias("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("PRI_LOB_IN").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("SVC_ID").alias("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("CLM_LN_SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.when(
        F.lit(SrcSysCd) == F.lit("DOMINION"), F.lit("DNTL")
    ).otherwise(
        F.when(F.expr("NUM(trim(PROC_CD))"), F.lit("CPT4")).otherwise(F.lit("HCPCS"))
    ).alias("PROC_CD_TYP_CD"),
    F.lit("MED").alias("PROC_CD_CAT_CD"),
    F.col("VBB_RULE").alias("VBB_RULE_ID"),
    F.col("VBB_EXCD").alias("VBB_EXCD_ID"),
    F.col("CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    F.col("NDC").alias("NDC"),
    F.col("NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT").alias("NDC_UNIT_CT"),
    F.lit("MED").alias("MED_PDX_IND"),
    F.col("APC_ID").alias("APC_ID"),
    F.col("APC_STTUS_ID").alias("APC_STTUS_ID"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}

df_ClmLnPK = ClmLnPK(df_Snapshot, params_ClmLnPK)

df_final_padded = df_ClmLnPK.withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "CLM_LN_ROOM_PRICE_METH_CD", F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"), 2, " ")
).withColumn(
    "CAP_LN_IN", F.rpad(F.col("CAP_LN_IN"), 1, " ")
).withColumn(
    "PRI_LOB_IN", F.rpad(F.col("PRI_LOB_IN"), 1, " ")
).withColumn(
    "DEDCT_AMT_ACCUM_ID", F.rpad(F.col("DEDCT_AMT_ACCUM_ID"), 4, " ")
).withColumn(
    "PREAUTH_SVC_SEQ_NO", F.rpad(F.col("PREAUTH_SVC_SEQ_NO"), 4, " ")
).withColumn(
    "RFRL_SVC_SEQ_NO", F.rpad(F.col("RFRL_SVC_SEQ_NO"), 4, " ")
).withColumn(
    "LMT_PFX_ID", F.rpad(F.col("LMT_PFX_ID"), 4, " ")
).withColumn(
    "PREAUTH_ID", F.rpad(F.col("PREAUTH_ID"), 9, " ")
).withColumn(
    "PROD_CMPNT_DEDCT_PFX_ID", F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"), 4, " ")
).withColumn(
    "PROD_CMPNT_SVC_PAYMT_ID", F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"), 4, " ")
).withColumn(
    "RFRL_ID_TX", F.rpad(F.col("RFRL_ID_TX"), 9, " ")
).withColumn(
    "SVC_ID", F.rpad(F.col("SVC_ID"), 4, " ")
).withColumn(
    "SVC_PRICE_RULE_ID", F.rpad(F.col("SVC_PRICE_RULE_ID"), 4, " ")
).withColumn(
    "SVC_RULE_TYP_TX", F.rpad(F.col("SVC_RULE_TYP_TX"), 3, " ")
).withColumn(
    "SVC_LOC_TYP_CD", F.rpad(F.col("SVC_LOC_TYP_CD"), 20, " ")
).withColumn(
    "CLM_LN_VBB_IN", F.rpad(F.col("CLM_LN_VBB_IN"), 1, " ")
).withColumn(
    "MED_PDX_IND", F.rpad(F.col("MED_PDX_IND"), 3, " ")
)

df_MAInboundClmLnExtr = df_final_padded.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD",
    "SVC_PROV_ID",
    "CLM_LN_DSALW_EXCD",
    "CLM_LN_EOB_EXCD",
    "CLM_LN_FINL_DISP_CD",
    "CLM_LN_LOB_CD",
    "CLM_LN_POS_CD",
    "CLM_LN_PREAUTH_CD",
    "CLM_LN_PREAUTH_SRC_CD",
    "CLM_LN_PRICE_SRC_CD",
    "CLM_LN_RFRL_CD",
    "CLM_LN_RVNU_CD",
    "CLM_LN_ROOM_PRICE_METH_CD",
    "CLM_LN_ROOM_TYP_CD",
    "CLM_LN_TOS_CD",
    "CLM_LN_UNIT_TYP_CD",
    "CAP_LN_IN",
    "PRI_LOB_IN",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "NO_RESP_AMT",
    "MBR_LIAB_BSS_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_PROV_AMT",
    "PAYBL_TO_SUB_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "ALW_PRICE_UNIT_CT",
    "UNIT_CT",
    "DEDCT_AMT_ACCUM_ID",
    "PREAUTH_SVC_SEQ_NO",
    "RFRL_SVC_SEQ_NO",
    "LMT_PFX_ID",
    "PREAUTH_ID",
    "PROD_CMPNT_DEDCT_PFX_ID",
    "PROD_CMPNT_SVC_PAYMT_ID",
    "RFRL_ID_TX",
    "SVC_ID",
    "SVC_PRICE_RULE_ID",
    "SVC_RULE_TYP_TX",
    "SVC_LOC_TYP_CD",
    "NON_PAR_SAV_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "VBB_RULE_ID",
    "VBB_EXCD_ID",
    "CLM_LN_VBB_IN",
    "ITS_SUPLMT_DSCNT_AMT",
    "ITS_SRCHRG_AMT",
    "NDC",
    "NDC_DRUG_FORM_CD",
    "NDC_UNIT_CT",
    "MED_PDX_IND",
    "APC_ID",
    "APC_STTUS_ID",
    "SNOMED_CT_CD",
    "CVX_VCCN_CD"
)

write_files(
    df_MAInboundClmLnExtr,
    f"{adls_path}/key/{SrcSysCd}ClmLnExtr.{SrcSysCd}ClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)