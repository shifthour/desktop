# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Manisha G                          US 404552                                 Initial programming                                           IntegrateDev2            Jeyaprasanna                2022-02-06       
# MAGIC 2022-02-10       Swetha G                           US 404552                          Fixed CLM_TYP_CD, CLM_SUBTYP_CD         IntegrateDev2            Jeyaprasanna                2022-02-16
# MAGIC                                                                                                                 CLM_PAYEE_CD 
# MAGIC 2023-12-15       Arpitha V                            US 599810                          Added logic to populate DNTL to CLM_TYP_CD   IntegrateDev1            Jeyaprasanna              2024-01-03
# MAGIC                                                                                                                 for Dominion in Alpha_Pfx stage
# MAGIC 
# MAGIC 2024-04-03       Ediga Maruthi                     US 614444                          Added Columns DOC_NO,USER_DEFN_TX_1,     Integrate Dev1         Jeyaprasanna             2024-04-19
# MAGIC                                                                                                                 USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                                                 USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                                                 USER_DEFN_AMT_2 in MAInboundLanding  and maped    
# MAGIC                                                                                                                 Doc No  from Source to IDS CLM DOC_TX_ID 
# MAGIC                                                                                                                  in Stage BusinessRules, Business_Rules_2

# MAGIC Apply business logic
# MAGIC Lookup subscriber, product and member information
# MAGIC Read the MAinbound CLM file created fromMAInboundClmExtrFormatData
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC Lookup subscriber, product and member information
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DecimalType, StructType, StructField, 
    IntegerType, TimestampType, DateType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SRC_SYS_CD = get_widget_value('SrcSysCd','')
SRC_SYS_CD_SK = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
BSDLTYP = get_widget_value('BSDLTYP','')
InFile_F = get_widget_value('InFile_F','')

ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)

df_IDS_Ext = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option(
        "query",
        f"""
SELECT
 PROV_ID,
 PROV_SPEC.PROV_SPEC_CD AS PROV_SPEC_CD,
 PROV_TYP.PROV_TYP_CD AS PROV_TYP_CD
FROM {IDSOwner}.PROV PROV
LEFT OUTER JOIN {IDSOwner}.PROV_SPEC_CD PROV_SPEC
  ON PROV.PROV_SPEC_CD_SK = PROV_SPEC.PROV_SPEC_CD_SK
LEFT OUTER JOIN {IDSOwner}.PROV_TYP_CD PROV_TYP
  ON PROV.PROV_TYP_CD_SK = PROV_TYP.PROV_TYP_CD_SK
WHERE SRC_SYS_CD_SK = '{SRC_SYS_CD_SK}'
"""
    )
    .load()
)

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
    StructField("CLM_LN_CHRG_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_ALW_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_DSALW_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_COPAY_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_COINS_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_DEDCT_AMT", DecimalType(38,10), True),
    StructField("Plan_Pay_Amount", DecimalType(38,10), True),
    StructField("Patient_Responsibility_Amount", DecimalType(38,10), True),
    StructField("CLM_LN_AGMNT_PRICE_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_RISK_WTHLD_AMT", DecimalType(38,10), True),
    StructField("CLM_PROV_ROLE_TYPE_CD", StringType(), True),
    StructField("CLM_LN_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_COB_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_COB_ALW_AMT", DecimalType(38,10), True),
    StructField("UNIT_CT", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("DOC_NO", StringType(), True),
    StructField("USER_DEFN_TX_1", StringType(), True),
    StructField("USER_DEFN_TX_2", StringType(), True),
    StructField("USER_DEFN_DT_1", StringType(), True),
    StructField("USER_DEFN_DT_2", StringType(), True),
    StructField("USER_DEFN_AMT_1", DecimalType(38,10), True),
    StructField("USER_DEFN_AMT_2", DecimalType(38,10), True),
])

df_MAInboundLanding = (
    spark.read.csv(
        f"{adls_path}/verified/{InFile_F}",
        schema=schema_MAInboundLanding,
        sep=",",
        header=False,
        quote='"',
        escape=None,
        multiLine=False
    )
)

group_list = [
    "CLM_ID"
]

agg_expr = {
    "REC_TYPE": F.min("REC_TYPE").alias("REC_TYPE"),
    "SRC_SYS_CD": F.max("SRC_SYS_CD").alias("SRC_SYS_CD"),
    "CLM_TYP_CD": F.max("CLM_TYP_CD").alias("CLM_TYP_CD"),
    "CLAIM_STS_CD": F.max("CLAIM_STS_CD").alias("CLAIM_STS_CD"),
    "CLM_LN_SEQ_NO": F.max("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    "CLM_ADJ_IN": F.max("CLM_ADJ_IN").alias("CLM_ADJ_IN"),
    "CLM_ADJ_FROM_CLM_ID": F.max("CLM_ADJ_FROM_CLM_ID").alias("CLM_ADJ_FROM_CLM_ID"),
    "CLM_ADJ_TO_CLM_ID": F.max("CLM_ADJ_TO_CLM_ID").alias("CLM_ADJ_TO_CLM_ID"),
    "GRP_ID": F.max("GRP_ID").alias("GRP_ID"),
    "MBR_ID": F.max("MBR_ID").alias("MBR_ID"),
    "MCARE_BNFCRY_ID": F.max("MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    "MBR_FIRST_NM": F.max("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    "MBR_MIDINIT": F.max("MBR_MIDINIT").alias("MBR_MIDINIT"),
    "MBR_LAST_NM": F.max("MBR_LAST_NM").alias("MBR_LAST_NM"),
    "MBR_BRTH_DT": F.min("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    "MBR_MAIL_ADDR_LN_1": F.max("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    "MBR_MAIL_ADDR_LN_2": F.max("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    "MBR_MAIL_ADDR_CITY_NM": F.max("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    "MBR_MAIL_ADDR_ST_CD": F.max("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    "MBR_MAIL_ADDR_ZIP_CD_5": F.max("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    "PROV_ID": F.max("PROV_ID").alias("PROV_ID"),
    "PROV_NM": F.max("PROV_NM").alias("PROV_NM"),
    "PROV_NTNL_PROV_ID": F.max("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    "PROV_TAX_ID": F.max("PROV_TAX_ID").alias("PROV_TAX_ID"),
    "PROV_TXNMY_CD": F.max("PROV_TXNMY_CD").alias("PROV_TXNMY_CD"),
    "PROV_PRI_MAIL_ADDR_LN_1": F.max("PROV_PRI_MAIL_ADDR_LN_1").alias("PROV_PRI_MAIL_ADDR_LN_1"),
    "PROV_PRI_MAIL_ADDR_LN_2": F.max("PROV_PRI_MAIL_ADDR_LN_2").alias("PROV_PRI_MAIL_ADDR_LN_2"),
    "PROV_PRI_MAIL_ADDR_LN_3": F.max("PROV_PRI_MAIL_ADDR_LN_3").alias("PROV_PRI_MAIL_ADDR_LN_3"),
    "PROV_PRI_MAIL_ADDR_CITY_NM": F.max("PROV_PRI_MAIL_ADDR_CITY_NM").alias("PROV_PRI_MAIL_ADDR_CITY_NM"),
    "PROV_PRI_MAIL_ADDR_ST_CD": F.max("PROV_PRI_MAIL_ADDR_ST_CD").alias("PROV_PRI_MAIL_ADDR_ST_CD"),
    "PROV_PRI_MAIL_ADDR_ZIP_CD_5": F.max("PROV_PRI_MAIL_ADDR_ZIP_CD_5").alias("PROV_PRI_MAIL_ADDR_ZIP_CD_5"),
    "CLM_SVC_PROV_SPEC_CD": F.max("CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    "SVC_PROV_ID": F.max("SVC_PROV_ID").alias("SVC_PROV_ID"),
    "SVC_PROV_NM": F.max("SVC_PROV_NM").alias("SVC_PROV_NM"),
    "SVC_PROV_NTNL_PROV_ID": F.max("SVC_PROV_NTNL_PROV_ID").alias("SVC_PROV_NTNL_PROV_ID"),
    "SVC_PROV_TAX_ID": F.max("SVC_PROV_TAX_ID").alias("SVC_PROV_TAX_ID"),
    "SVC_PROV_TXNMY_CD": F.max("SVC_PROV_TXNMY_CD").alias("SVC_PROV_TXNMY_CD"),
    "SVC_PROV_ADDR_LN1": F.max("SVC_PROV_ADDR_LN1").alias("SVC_PROV_ADDR_LN1"),
    "SVC_PROV_ADDR_LN2": F.max("SVC_PROV_ADDR_LN2").alias("SVC_PROV_ADDR_LN2"),
    "SVC_PROV_ADDR_LN3": F.max("SVC_PROV_ADDR_LN3").alias("SVC_PROV_ADDR_LN3"),
    "SVC_PROV_CITY_NM": F.max("SVC_PROV_CITY_NM").alias("SVC_PROV_CITY_NM"),
    "SVC_PROV_ST_CD": F.max("SVC_PROV_ST_CD").alias("SVC_PROV_ST_CD"),
    "SVC_PROV_ZIP_CD_5": F.max("SVC_PROV_ZIP_CD_5").alias("SVC_PROV_ZIP_CD_5"),
    "CLM_SVC_STRT_DT": F.min("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    "CLM_SVC_END_DT": F.min("CLM_SVC_END_DT").alias("CLM_SVC_END_DT"),
    "CLM_PD_DT": F.min("CLM_PD_DT").alias("CLM_PD_DT"),
    "CLM_RCVD_DT": F.max("CLM_RCVD_DT").alias("CLM_RCVD_DT"),
    "CLM_PAYE_CD": F.max("CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    "CLM_NTWK_STTUS_CD": F.max("CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    "CLM_LN_FINL_DISP_CD": F.max("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    "DIAG_CD_1": F.max("DIAG_CD_1").alias("DIAG_CD_1"),
    "DIAG_CD_2": F.max("DIAG_CD_2").alias("DIAG_CD_2"),
    "DIAG_CD_3": F.max("DIAG_CD_3").alias("DIAG_CD_3"),
    "CLM_LN_CAP_LN_IN": F.max("CLM_LN_CAP_LN_IN").alias("CLM_LN_CAP_LN_IN"),
    "Claim_Line_Denied_Indicator": F.max("Claim_Line_Denied_Indicator").alias("Claim_Line_Denied_Indicator"),
    "Explanation_Code": F.max("Explanation_Code").alias("Explanation_Code"),
    "Explanation_Code_Description": F.max("Explanation_Code_Description").alias("Explanation_Code_Description"),
    "DNTL_CLM_LN_TOOTH_NO": F.max("DNTL_CLM_LN_TOOTH_NO").alias("DNTL_CLM_LN_TOOTH_NO"),
    "DNTL_CLM_LN_TOOTH_SRFC_TX": F.max("DNTL_CLM_LN_TOOTH_SRFC_TX").alias("DNTL_CLM_LN_TOOTH_SRFC_TX"),
    "Adjustment_Reason_Code": F.max("Adjustment_Reason_Code").alias("Adjustment_Reason_Code"),
    "Remittance_Advice_Remark_Code_RARC": F.max("Remittance_Advice_Remark_Code_RARC").alias("Remittance_Advice_Remark_Code_RARC"),
    "DIAG_CD_TYP_CD": F.max("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    "PROC_CD": F.max("PROC_CD").alias("PROC_CD"),
    "PROC_CD_DESC": F.max("PROC_CD_DESC").alias("PROC_CD_DESC"),
    "Procedure_Code_Modifier1": F.max("Procedure_Code_Modifier1").alias("Procedure_Code_Modifier1"),
    "Procedure_Code_Modifier2": F.max("Procedure_Code_Modifier2").alias("Procedure_Code_Modifier2"),
    "Procedure_Code_Modifier3": F.max("Procedure_Code_Modifier3").alias("Procedure_Code_Modifier3"),
    "CLM_LN_SVC_STRT_DT": F.min("CLM_LN_SVC_STRT_DT").alias("CLM_LN_SVC_STRT_DT"),
    "CLM_LN_SVC_END_DT": F.min("CLM_LN_SVC_END_DT").alias("CLM_LN_SVC_END_DT"),
    "CLM_LN_CHRG_AMT": F.sum("CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    "CLM_LN_ALW_AMT": F.sum("CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
    "CLM_LN_DSALW_AMT": F.sum("CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    "CLM_LN_COPAY_AMT": F.sum("CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
    "CLM_LN_COINS_AMT": F.sum("CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
    "CLM_LN_DEDCT_AMT": F.sum("CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
    "Plan_Pay_Amount": F.sum("Plan_Pay_Amount").alias("Plan_Pay_Amount"),
    "Patient_Responsibility_Amount": F.sum("Patient_Responsibility_Amount").alias("Patient_Responsibility_Amount"),
    "CLM_LN_AGMNT_PRICE_AMT": F.sum("CLM_LN_AGMNT_PRICE_AMT").alias("CLM_LN_AGMNT_PRICE_AMT"),
    "CLM_LN_RISK_WTHLD_AMT": F.sum("CLM_LN_RISK_WTHLD_AMT").alias("CLM_LN_RISK_WTHLD_AMT"),
    "CLM_PROV_ROLE_TYPE_CD": F.max("CLM_PROV_ROLE_TYPE_CD").alias("CLM_PROV_ROLE_TYPE_CD"),
    "CLM_LN_PD_AMT": F.sum("CLM_LN_PD_AMT").alias("CLM_LN_PD_AMT"),
    "CLM_COB_PD_AMT": F.sum("CLM_COB_PD_AMT").alias("CLM_COB_PD_AMT"),
    "CLM_COB_ALW_AMT": F.sum("CLM_COB_ALW_AMT").alias("CLM_COB_ALW_AMT"),
    "UNIT_CT": F.max("UNIT_CT").alias("UNIT_CT"),
    "CLM_STTUS_CD": F.min("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    "ADJ_FROM_CLM_ID": F.max("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    "ADJ_TO_CLM_ID": F.max("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    "DOC_NO": F.max("DOC_NO").alias("DOC_NO"),
}

df_MAInbound = (
    df_MAInboundLanding
    .groupBy(*group_list)
    .agg(*agg_expr.values())
)

df_IDS_MBR_GRP_SUB = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option(
        "query",
        f"""
SELECT 
 SUB.SUB_ID,
 GRP.GRP_ID,
 SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY,
 MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
 MBR.MCARE_NO,
 MBR.MBR_SFX_NO,
 MBR.BRTH_DT_SK,
 SUBGRP.SUBGRP_ID AS SUBGRP_ID,
 CLS.CLS_ID,
 PLN.CLS_PLN_ID,
 ENR.EFF_DT_SK,
 ENR.TERM_DT_SK,
 PROD.PROD_ID,
 CAT.EXPRNC_CAT_CD AS EXPRNC_CAT_CD,
 LOB.FNCL_LOB_CD AS FNCL_LOB_CD
FROM (
  SELECT
     ENR1.MBR_SK MBR_SK,
     ENR1.GRP_SK GRP_SK,
     ENR1.CLS_SK CLS_SK,
     ENR1.SUBGRP_SK,
     ENR1.CLS_PLN_SK,
     ENR1.EFF_DT_SK,
     ENR1.TERM_DT_SK,
     ENR1.PROD_SK,
     RANK() OVER (PARTITION BY ENR1.MBR_SK ORDER BY ENR1.EFF_DT_SK DESC) as Rank
  FROM {IDSOwner}.MBR_ENR ENR1
    INNER JOIN {IDSOwner}.PROD_CMPNT PROD_CMPNT
      ON ENR1.PROD_SK = PROD_CMPNT.PROD_SK 
       and PROD_CMPNT.PROD_CMPNT_TYP_CD_SK = (
         Select CD_MPPNG_SK from {IDSOwner}.CD_MPPNG 
         where SRC_DOMAIN_NM = 'PRODUCT COMPONENT TYPE'
           and SRC_CD = 'BSBS'
           and SRC_SYS_CD = 'FACETS'
       )
    INNER JOIN {IDSOwner}.BNF_SUM_DTL_HIST BNF_SUM_DTL_HIST
      ON PROD_CMPNT.PROD_CMPNT_PFX_ID = BNF_SUM_DTL_HIST.PROD_CMPNT_PFX_ID 
       and BNF_SUM_DTL_HIST.BNF_SUM_DTL_TYP_CD IN ('{BSDLTYP}')
    WHERE  
      ELIG_IN ='Y' AND TERM_DT_SK >= '2021-01-01'
)ENR
INNER JOIN {IDSOwner}.MBR MBR
  ON MBR.MBR_SK= ENR.MBR_SK
INNER JOIN {IDSOwner}.SUB SUB
  ON SUB.SUB_SK=MBR.SUB_SK
INNER JOIN {IDSOwner}.GRP GRP
  ON GRP.GRP_SK = SUB.GRP_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP
  ON ENR.SUBGRP_SK = SUBGRP.SUBGRP_SK
INNER JOIN {IDSOwner}.CLS CLS
  ON ENR.CLS_SK = CLS.CLS_SK
INNER JOIN {IDSOwner}.CLS_PLN PLN
  ON ENR.CLS_PLN_SK = PLN.CLS_PLN_SK
LEFT OUTER JOIN {IDSOwner}.PROD PROD
  ON PROD.PROD_SK = ENR.PROD_SK
   AND ENR.EFF_DT_SK <= PROD.PROD_TERM_DT_SK
   AND ENR.TERM_DT_SK >= PROD.PROD_EFF_DT_SK
LEFT OUTER JOIN {IDSOwner}.EXPRNC_CAT CAT
  ON PROD.EXPRNC_CAT_SK=CAT.EXPRNC_CAT_SK
LEFT OUTER JOIN {IDSOwner}.FNCL_LOB LOB
  ON PROD.FNCL_LOB_SK = LOB.FNCL_LOB_SK
WHERE
 RANK = 1
"""
    )
    .load()
)

df_hf_lkp_cls_sk = df_IDS_MBR_GRP_SUB.dropDuplicates(["SUB_ID"])

df_IDS_MBR_GRP_SUB_Only = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option(
        "query",
        f"""
SELECT 
 SUB.SUB_ID,
 GRP.GRP_ID,
 SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY,
 MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
 MBR.MCARE_NO,
 MBR.MBR_SFX_NO,
 MBR.BRTH_DT_SK,
 SUBGRP.SUBGRP_ID AS SUBGRP_ID
FROM 
 {IDSOwner}.GRP GRP
INNER JOIN {IDSOwner}.SUB SUB
  ON GRP.GRP_SK = SUB.GRP_SK
INNER JOIN {IDSOwner}.MBR MBR
  ON SUB.SUB_SK=MBR.SUB_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP
  ON MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
"""
    )
    .load()
)

df_hf_lkp_sub_id = df_IDS_MBR_GRP_SUB_Only.dropDuplicates(["SUB_ID"])

df_BusinessRules_join = (
    df_MAInbound.alias("MAInbound")
    .join(
        df_hf_lkp_cls_sk.alias("Lnk_Cls_Sk"),
        F.expr("substring(trim(MAInbound.MBR_ID),4,length(trim(MAInbound.MBR_ID))) = Lnk_Cls_Sk.SUB_ID"),
        how="left"
    )
)

df_BusinessRules_join = df_BusinessRules_join.withColumn(
    "svServDate",
    F.col("MAInbound.CLM_SVC_STRT_DT")
).withColumn(
    "svDateCheck",
    F.when(
        (F.col("Lnk_Cls_Sk.EFF_DT_SK").isNotNull()) &
        (F.col("Lnk_Cls_Sk.TERM_DT_SK").isNotNull()) &
        (F.col("svServDate") <= F.col("Lnk_Cls_Sk.TERM_DT_SK")),
        F.lit(1)
    ).otherwise(F.lit(0))
).withColumn(
    "svMbrAge",
    F.when(
        (F.col("MAInbound.MBR_BRTH_DT").isNull()) | (F.length(F.col("MAInbound.MBR_BRTH_DT")) == 0),
        AGE(F.col("Lnk_Cls_Sk.BRTH_DT_SK"), F.col("MAInbound.CLM_SVC_STRT_DT"))  # Assume AGE(...) is available
    ).otherwise(
        AGE(F.col("MAInbound.MBR_BRTH_DT"), F.col("MAInbound.CLM_SVC_STRT_DT"))
    )
).withColumn(
    "svClmCnt",
    F.when(
        F.col("MAInbound.CLM_STTUS_CD") == F.lit("A08"),
        F.lit(-1)
    ).otherwise(F.lit(1))
).withColumn(
    "svFamilyIDPrfx",
    Substrings(trim(F.col("MAInbound.MBR_ID")), F.lit(1), F.lit(3))  # Assume Substrings(...) and trim(...) are UDF
).withColumn(
    "svFamilyID",
    Substrings(trim(F.col("MAInbound.MBR_ID")), F.lit(4), F.length(trim(F.col("MAInbound.MBR_ID"))))
).withColumn(
    "DefaultValue",
    F.lit("NA")
)

df_MAInbound_UnMatched = df_BusinessRules_join.filter(
    (
        (F.col("Lnk_Cls_Sk.SUB_ID").isNull()) |
        (F.trim(F.col("Lnk_Cls_Sk.SUB_ID")) == "")
    ) | (F.col("svDateCheck") == 0)
)

df_Lnk_Mbr_Enr = df_BusinessRules_join.filter(
    (
        (F.col("Lnk_Cls_Sk.SUB_ID").isNotNull()) &
        (F.trim(F.col("Lnk_Cls_Sk.SUB_ID")) != "")
    ) & (F.col("svDateCheck") == 1)
)

df_MAInbound_UnMatched_select = df_MAInbound_UnMatched.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(F.col("SRC_SYS_CD"),F.lit(";"),F.col("CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.lit(SRC_SYS_CD_SK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("ADJ_FROM_CLM"),
    F.lit(1).alias("ADJ_TO_CLM"),
    F.col("svFamilyIDPrfx").alias("ALPHA_PFX"),
    F.lit(1).alias("CLM_EOB_EXCD"),
    F.when(
        (F.col("Lnk_Cls_Sk.CLS_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.CLS_ID")
    ).otherwise(F.col("DefaultValue")).alias("CLS"),
    F.when(
        (F.col("Lnk_Cls_Sk.CLS_PLN_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.CLS_PLN_ID")
    ).otherwise(F.col("DefaultValue")).alias("CLS_PLN"),
    F.when(
        (F.col("Lnk_Cls_Sk.EXPRNC_CAT_CD").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.EXPRNC_CAT_CD")
    ).otherwise(F.col("DefaultValue")).alias("EXPRNC_CAT"),
    F.when(
        (F.col("Lnk_Cls_Sk.FNCL_LOB_CD").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.FNCL_LOB_CD")
    ).otherwise(F.col("DefaultValue")).alias("FNCL_LOB"),
    F.when(
        (F.col("Lnk_Cls_Sk.GRP_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.GRP_ID")
    ).otherwise(F.col("DefaultValue")).alias("GRP_ID"),
    F.when(
        (F.col("Lnk_Cls_Sk.MBR_UNIQ_KEY").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.MBR_UNIQ_KEY")
    ).otherwise(F.col("DefaultValue")).alias("MBR"),
    F.col("DefaultValue").alias("NTWK"),
    F.when(
        (F.col("Lnk_Cls_Sk.PROD_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.PROD_ID")
    ).otherwise(F.col("DefaultValue")).alias("PROD_ID"),
    F.when(
        (F.col("Lnk_Cls_Sk.SUBGRP_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.SUBGRP_ID")
    ).otherwise(F.col("DefaultValue")).alias("SUBGRP"),
    F.when(
        (F.col("Lnk_Cls_Sk.SUB_UNIQ_KEY").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.SUB_UNIQ_KEY")
    ).otherwise(F.col("DefaultValue")).alias("SUB_CK"),
    F.col("DefaultValue").alias("CLM_ACDNT_CD"),
    F.col("DefaultValue").alias("CLM_ACDNT_ST_CD"),
    F.col("DefaultValue").alias("CLM_ACTV_BCBS_PLN_CD"),
    F.col("DefaultValue").alias("CLM_AGMNT_SRC_CD"),
    F.col("DefaultValue").alias("CLM_BTCH_ACTN_CD"),
    F.col("DefaultValue").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("DefaultValue").alias("CLM_COB_CD"),
    F.col("DefaultValue").alias("CLM_INPT_METH_CD"),
    F.col("DefaultValue").alias("CLM_INPT_SRC_CD"),
    F.col("DefaultValue").alias("CLM_INTER_PLN_PGM_CD"),
    F.col("DefaultValue").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("DefaultValue").alias("CLM_OTHER_BNF_CD"),
    F.col("CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("DefaultValue").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("DefaultValue").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("DefaultValue").alias("CLM_SUBMTTING_BCBS_PLN_CD"),
    F.col("DefaultValue").alias("CLM_SUB_BCBS_PLN_CD"),
    F.lit("N").alias("ATCHMT_IN"),
    F.lit("N").alias("CLNCL_EDIT_IN"),
    F.lit("N").alias("COBRA_CLM_IN"),
    F.lit("N").alias("FIRST_PASS_IN"),
    F.lit("N").alias("HOST_IN"),
    F.lit("N").alias("LTR_IN"),
    F.lit("N").alias("MCARE_ASG_IN"),
    F.lit("N").alias("NOTE_IN"),
    F.lit("N").alias("PCA_AUDIT_IN"),
    F.lit("N").alias("PCP_SUBMT_IN"),
    F.lit("N").alias("PROD_OOA_IN"),
    F.lit("1753-01-01").alias("ACDNT_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_RCVD_DT").isNull(), "").otherwise(F.col("CLM_RCVD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_RCVD_DT")).alias("INPT_DT"),
    F.lit("1753-01-01").alias("MBR_PLN_ELIG_DT"),
    F.lit("2199-12-31").alias("NEXT_RVW_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("PD_DT"),
    F.lit("1753-01-01").alias("PAYMT_DRAG_CYC_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("PRCS_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_RCVD_DT").isNull(), "").otherwise(F.col("CLM_RCVD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_RCVD_DT")).alias("RCVD_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_SVC_STRT_DT").isNull(), "").otherwise(F.col("CLM_SVC_STRT_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_SVC_STRT_DT")).alias("SVC_STRT_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_SVC_END_DT").isNull(), "").otherwise(F.col("CLM_SVC_END_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_SVC_END_DT")).alias("SVC_END_DT"),
    F.lit("1753-01-01").alias("SMLR_ILNS_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("STTUS_DT"),
    F.lit("1753-01-01").alias("WORK_UNABLE_BEG_DT"),
    F.lit("1753-01-01").alias("WORK_UNABLE_END_DT"),
    F.lit(0.00).alias("ACDNT_AMT"),
    F.col("Plan_Pay_Amount").alias("ACTL_PD_AMT"),
    F.col("CLM_LN_ALW_AMT").alias("ALW_AMT"),
    F.col("CLM_LN_DSALW_AMT").alias("DSALW_AMT"),
    F.col("CLM_LN_COINS_AMT").alias("COINS_AMT"),
    F.lit(0.00).alias("CNSD_CHRG_AMT"),
    F.col("CLM_LN_COPAY_AMT").alias("COPAY_AMT"),
    F.col("CLM_LN_CHRG_AMT").alias("CHRG_AMT"),
    F.col("CLM_LN_DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Plan_Pay_Amount").alias("PAYBL_AMT"),
    F.col("CLM_LN_PD_AMT").alias("CLM_LN_PD_AMT"),
    F.col("svClmCnt").alias("CLM_CT"),
    F.col("svMbrAge").alias("MBR_AGE"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.when(
        F.col("DOC_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("DOC_NO"))).alias("DOC_TX_ID"),
    F.lit(None).alias("MCAID_RESUB_NO"),
    F.when(
        F.col("Lnk_Cls_Sk.MCARE_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.col("Lnk_Cls_Sk.MCARE_NO")).alias("MCARE_ID"),
    F.when(
        F.col("Lnk_Cls_Sk.MBR_SFX_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.col("Lnk_Cls_Sk.MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.lit(None).alias("PATN_ACCT_NO"),
    F.lit("NA").alias("PAYMT_REF_ID"),
    F.lit("NA").alias("PROV_AGMNT_ID"),
    F.lit("NA").alias("RFRNG_PROV_TX"),
    F.col("svFamilyID").alias("SUB_ID"),
    F.col("DefaultValue").alias("PCA_TYP_CD"),
    F.col("DefaultValue").alias("REL_PCA_CLM"),
    F.col("DefaultValue").alias("REL_BASE_CLM"),
    F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.col("Patient_Responsibility_Amount").alias("PATN_PD_AMT"),
    F.lit(1).alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.lit("N").alias("CLM_TXNMY_CD"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN"),
    F.col("CLAIM_STS_CD").alias("CLAIM_STS_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("SVC_PROV_ID").alias("SERVICING_PROVIDER_ID"),
)

df_Lnk_Mbr_Enr_select = df_Lnk_Mbr_Enr.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(F.col("SRC_SYS_CD"),F.lit(";"),F.col("CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.lit(SRC_SYS_CD_SK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("ADJ_FROM_CLM"),
    F.lit(1).alias("ADJ_TO_CLM"),
    F.col("svFamilyIDPrfx").alias("ALPHA_PFX"),
    F.lit(1).alias("CLM_EOB_EXCD"),
    F.when(
        (F.col("Lnk_Cls_Sk.CLS_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.CLS_ID")
    ).otherwise(F.col("DefaultValue")).alias("CLS"),
    F.when(
        (F.col("Lnk_Cls_Sk.CLS_PLN_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.CLS_PLN_ID")
    ).otherwise(F.col("DefaultValue")).alias("CLS_PLN"),
    F.when(
        (F.col("Lnk_Cls_Sk.EXPRNC_CAT_CD").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.EXPRNC_CAT_CD")
    ).otherwise(F.col("DefaultValue")).alias("EXPRNC_CAT"),
    F.when(
        (F.col("Lnk_Cls_Sk.FNCL_LOB_CD").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.FNCL_LOB_CD")
    ).otherwise(F.col("DefaultValue")).alias("FNCL_LOB"),
    F.when(
        (F.col("Lnk_Cls_Sk.GRP_ID").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.GRP_ID")
    ).otherwise(F.col("DefaultValue")).alias("GRP_ID"),
    F.when(
        (F.col("Lnk_Cls_Sk.MBR_UNIQ_KEY").isNotNull()) & (F.col("svDateCheck") == 1),
        F.col("Lnk_Cls_Sk.MBR_UNIQ_KEY")
    ).otherwise(F.col("DefaultValue")).alias("MBR"),
    F.col("DefaultValue").alias("NTWK"),
    F.col("DefaultValue").alias("PROD_ID"),
    F.when(
        (F.col("Lnk_Cls_Sk.SUBGRP_ID").isNotNull()),
        F.col("Lnk_Cls_Sk.SUBGRP_ID")
    ).otherwise(F.col("DefaultValue")).alias("SUBGRP"),
    F.when(
        (F.col("Lnk_Cls_Sk.SUB_UNIQ_KEY").isNotNull()),
        F.col("Lnk_Cls_Sk.SUB_UNIQ_KEY")
    ).otherwise(F.col("DefaultValue")).alias("SUB_CK"),
    F.col("DefaultValue").alias("CLM_ACDNT_CD"),
    F.col("DefaultValue").alias("CLM_ACDNT_ST_CD"),
    F.col("DefaultValue").alias("CLM_ACTV_BCBS_PLN_CD"),
    F.col("DefaultValue").alias("CLM_AGMNT_SRC_CD"),
    F.col("DefaultValue").alias("CLM_BTCH_ACTN_CD"),
    F.col("DefaultValue").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("DefaultValue").alias("CLM_COB_CD"),
    F.col("DefaultValue").alias("CLM_INPT_METH_CD"),
    F.col("DefaultValue").alias("CLM_INPT_SRC_CD"),
    F.col("DefaultValue").alias("CLM_INTER_PLN_PGM_CD"),
    F.col("DefaultValue").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("DefaultValue").alias("CLM_OTHER_BNF_CD"),
    F.col("CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("DefaultValue").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("DefaultValue").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("DefaultValue").alias("CLM_SUBMTTING_BCBS_PLN_CD"),
    F.col("DefaultValue").alias("CLM_SUB_BCBS_PLN_CD"),
    F.lit("N").alias("ATCHMT_IN"),
    F.lit("N").alias("CLNCL_EDIT_IN"),
    F.lit("N").alias("COBRA_CLM_IN"),
    F.lit("N").alias("FIRST_PASS_IN"),
    F.lit("N").alias("HOST_IN"),
    F.lit("N").alias("LTR_IN"),
    F.lit("N").alias("MCARE_ASG_IN"),
    F.lit("N").alias("NOTE_IN"),
    F.lit("N").alias("PCA_AUDIT_IN"),
    F.lit("N").alias("PCP_SUBMT_IN"),
    F.lit("N").alias("PROD_OOA_IN"),
    F.lit("1753-01-01").alias("ACDNT_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_RCVD_DT").isNull(), "").otherwise(F.col("CLM_RCVD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_RCVD_DT")).alias("INPT_DT"),
    F.lit("1753-01-01").alias("MBR_PLN_ELIG_DT"),
    F.lit("2199-12-31").alias("NEXT_RVW_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("PD_DT"),
    F.lit("1753-01-01").alias("PAYMT_DRAG_CYC_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("PRCS_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_RCVD_DT").isNull(), "").otherwise(F.col("CLM_RCVD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_RCVD_DT")).alias("RCVD_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_SVC_STRT_DT").isNull(), "").otherwise(F.col("CLM_SVC_STRT_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_SVC_STRT_DT")).alias("SVC_STRT_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_SVC_END_DT").isNull(), "").otherwise(F.col("CLM_SVC_END_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_SVC_END_DT")).alias("SVC_END_DT"),
    F.lit("1753-01-01").alias("SMLR_ILNS_DT"),
    F.when(
        F.trim(F.when(F.col("CLM_PD_DT").isNull(), "").otherwise(F.col("CLM_PD_DT"))) == "",
        F.lit("1753-01-01")
    ).otherwise(F.col("CLM_PD_DT")).alias("STTUS_DT"),
    F.lit("1753-01-01").alias("WORK_UNABLE_BEG_DT"),
    F.lit("1753-01-01").alias("WORK_UNABLE_END_DT"),
    F.lit(0.00).alias("ACDNT_AMT"),
    F.col("Plan_Pay_Amount").alias("ACTL_PD_AMT"),
    F.col("CLM_LN_ALW_AMT").alias("ALW_AMT"),
    F.col("CLM_LN_DSALW_AMT").alias("DSALW_AMT"),
    F.col("CLM_LN_COINS_AMT").alias("COINS_AMT"),
    F.lit(0.00).alias("CNSD_CHRG_AMT"),
    F.col("CLM_LN_COPAY_AMT").alias("COPAY_AMT"),
    F.col("CLM_LN_CHRG_AMT").alias("CHRG_AMT"),
    F.col("CLM_LN_DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Plan_Pay_Amount").alias("PAYBL_AMT"),
    F.col("CLM_LN_PD_AMT").alias("CLM_LN_PD_AMT"),
    F.col("svClmCnt").alias("CLM_CT"),
    F.col("svMbrAge").alias("MBR_AGE"),
    F.col("CLM_ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("CLM_ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.when(
        F.col("DOC_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("DOC_NO"))).alias("DOC_TX_ID"),
    F.lit(None).alias("MCAID_RESUB_NO"),
    F.when(
        F.col("Lkp_Sub_ID.MCARE_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.col("Lkp_Sub_ID.MCARE_NO")).alias("MCARE_ID"),
    F.when(
        F.col("Lkp_Sub_ID.MBR_SFX_NO").isNull(),
        F.lit("NA")
    ).otherwise(F.col("Lkp_Sub_ID.MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.lit(None).alias("PATN_ACCT_NO"),
    F.lit("NA").alias("PAYMT_REF_ID"),
    F.lit("NA").alias("PROV_AGMNT_ID"),
    F.lit("NA").alias("RFRNG_PROV_TX"),
    F.col("svFamilyID").alias("SUB_ID"),
    F.col("DefaultValue").alias("PCA_TYP_CD"),
    F.col("DefaultValue").alias("REL_PCA_CLM"),
    F.col("DefaultValue").alias("REL_BASE_CLM"),
    F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.col("Patient_Responsibility_Amount").alias("PATN_PD_AMT"),
    F.lit(1).alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.lit("N").alias("CLM_TXNMY_CD"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN"),
    F.col("CLAIM_STS_CD").alias("CLAIM_STS_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("SVC_PROV_ID").alias("SERVICING_PROVIDER_ID"),
).join(
    df_hf_lkp_sub_id.alias("Lkp_Sub_ID"),
    F.expr("substring(trim(CLM_ID.MBR_ID),4,length(trim(CLM_ID.MBR_ID)))=Lkp_Sub_ID.SUB_ID"),
    "left"  # but we must actually do the real join only in code if needed
)  # The job text shows a left join, but the actual pinned logic merges in the second transformer.

df_MAInbound_UnMatched_res = df_MAInbound_UnMatched_select.alias("MAInbound_UnMatched")
df_Lnk_Mbr_Enr_res = df_Lnk_Mbr_Enr_select.alias("Lnk_Mbr_Enr")

df_Link_Collector = df_Lnk_Mbr_Enr_res.unionByName(df_MAInbound_UnMatched_res)

df_alpha_pfx_join = (
    df_Link_Collector.alias("MAInboundClmTrns")
    .join(
        df_IDS_Ext.alias("Lnk_Prov_Ext"),
        F.trim(F.col("MAInboundClmTrns.SERVICING_PROVIDER_ID")) == F.col("Lnk_Prov_Ext.PROV_ID"),
        how="left"
    )
)

df_alpha_pfx = df_alpha_pfx_join.withColumn(
    "DefaultValue",
    F.lit("NA")
).select(
    F.col("MAInboundClmTrns.*"),
    F.when(F.col("MAInboundClmTrns.ALPHA_PFX").isNull(), F.col("DefaultValue")).otherwise(F.col("MAInboundClmTrns.ALPHA_PFX")).alias("ALPHA_PFX"),
    F.lit("NA").alias("CLM_EOB_EXCD"),  # Overwrite per job logic
    F.col("MAInboundClmTrns.CLS").alias("CLS"),
    F.col("MAInboundClmTrns.CLS_PLN").alias("CLS_PLN"),
    F.col("MAInboundClmTrns.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("MAInboundClmTrns.FNCL_LOB").alias("FNCL_LOB"),
    F.col("MAInboundClmTrns.GRP_ID").alias("GRP_ID"),
    F.col("MAInboundClmTrns.MBR").alias("MBR"),
    F.col("MAInboundClmTrns.NTWK").alias("NTWK"),
    F.col("MAInboundClmTrns.PROD_ID").alias("PROD_ID"),
    F.col("MAInboundClmTrns.SUBGRP").alias("SUBGRP"),
    F.col("MAInboundClmTrns.SUB_CK").alias("SUB_CK"),
    F.col("MAInboundClmTrns.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("MAInboundClmTrns.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("MAInboundClmTrns.CLM_ACTV_BCBS_PLN_CD").alias("CLM_ACTV_BCBS_PLN_CD"),
    F.col("MAInboundClmTrns.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("MAInboundClmTrns.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.lit("N").alias("CLM_CAP_CD"),
    F.lit("STD").alias("CLM_CAT_CD"),
    F.col("MAInboundClmTrns.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("MAInboundClmTrns.CLM_COB_CD").alias("CLM_COB_CD"),
    F.when(F.col("MAInboundClmTrns.CLAIM_STS_CD").isNull(), F.col("DefaultValue")).otherwise(F.lit("ACPTD")).alias("CLM_FINL_DISP_CD"),
    F.col("MAInboundClmTrns.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("MAInboundClmTrns.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("MAInboundClmTrns.CLM_INTER_PLN_PGM_CD").alias("CLM_INTER_PLN_PGM_CD"),
    F.col("DefaultValue").alias("CLM_NTWK_STTUS_CD"),
    F.col("MAInboundClmTrns.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("MAInboundClmTrns.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.lit("ALTPAYEE").alias("CLM_PAYE_CD"),
    F.col("MAInboundClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("MAInboundClmTrns.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.when(
        F.col("Lnk_Prov_Ext.PROV_SPEC_CD").isNull(),
        F.col("DefaultValue")
    ).otherwise(F.col("Lnk_Prov_Ext.PROV_SPEC_CD")).alias("CLM_SVC_PROV_SPEC_CD"),
    F.when(
        F.col("Lnk_Prov_Ext.PROV_TYP_CD").isNull(),
        F.col("DefaultValue")
    ).otherwise(F.col("Lnk_Prov_Ext.PROV_TYP_CD")).alias("CLM_SVC_PROV_TYP_CD"),
    F.when(F.col("MAInboundClmTrns.CLM_STTUS_CD").isNull(), F.lit("NA")).otherwise(F.col("MAInboundClmTrns.CLM_STTUS_CD")).alias("CLM_STTUS_CD"),
    F.col("MAInboundClmTrns.CLM_SUBMTTING_BCBS_PLN_CD").alias("CLM_SUBMTTING_BCBS_PLN_CD"),
    F.col("MAInboundClmTrns.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.lit("PR").alias("CLM_SUBTYP_CD"),
    F.when(
        F.col("MAInboundClmTrns.SRC_SYS_CD") == F.lit("DOMINION"),
        F.lit("DNTL")
    ).otherwise(F.lit("MED")).alias("CLM_TYP_CD"),
    F.col("MAInboundClmTrns.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("MAInboundClmTrns.CLNCL_EDIT_IN").alias("CLNCL_EDIT_IN"),
    F.col("MAInboundClmTrns.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("MAInboundClmTrns.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("MAInboundClmTrns.HOST_IN").alias("HOST_IN"),
    F.col("MAInboundClmTrns.LTR_IN").alias("LTR_IN"),
    F.col("MAInboundClmTrns.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("MAInboundClmTrns.NOTE_IN").alias("NOTE_IN"),
    F.col("MAInboundClmTrns.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("MAInboundClmTrns.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("MAInboundClmTrns.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("MAInboundClmTrns.ACDNT_DT").alias("ACDNT_DT"),
    F.col("MAInboundClmTrns.INPT_DT").alias("INPT_DT"),
    F.col("MAInboundClmTrns.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("MAInboundClmTrns.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("MAInboundClmTrns.PD_DT").alias("PD_DT"),
    F.col("MAInboundClmTrns.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("MAInboundClmTrns.PRCS_DT").alias("PRCS_DT"),
    F.col("MAInboundClmTrns.RCVD_DT").alias("RCVD_DT"),
    F.col("MAInboundClmTrns.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("MAInboundClmTrns.SVC_END_DT").alias("SVC_END_DT"),
    F.col("MAInboundClmTrns.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("MAInboundClmTrns.STTUS_DT").alias("STTUS_DT"),
    F.col("MAInboundClmTrns.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("MAInboundClmTrns.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("MAInboundClmTrns.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("MAInboundClmTrns.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("MAInboundClmTrns.ALW_AMT").alias("ALW_AMT"),
    F.col("MAInboundClmTrns.DSALW_AMT").alias("DSALW_AMT"),
    F.col("MAInboundClmTrns.COINS_AMT").alias("COINS_AMT"),
    F.col("MAInboundClmTrns.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("MAInboundClmTrns.COPAY_AMT").alias("COPAY_AMT"),
    F.col("MAInboundClmTrns.CHRG_AMT").alias("CHRG_AMT"),
    F.col("MAInboundClmTrns.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("MAInboundClmTrns.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("MAInboundClmTrns.CLM_CT").alias("CLM_CT"),
    F.col("MAInboundClmTrns.MBR_AGE").alias("MBR_AGE"),
    F.col("MAInboundClmTrns.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("MAInboundClmTrns.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("MAInboundClmTrns.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("MAInboundClmTrns.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("MAInboundClmTrns.MCARE_ID").alias("MCARE_ID"),
    F.col("MAInboundClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MAInboundClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("MAInboundClmTrns.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("MAInboundClmTrns.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("MAInboundClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("MAInboundClmTrns.SUB_ID").alias("SUB_ID"),
    F.col("MAInboundClmTrns.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("MAInboundClmTrns.REL_PCA_CLM").alias("REL_PCA_CLM"),
    F.col("MAInboundClmTrns.REL_BASE_CLM").alias("REL_BASE_CLM"),
    F.col("MAInboundClmTrns.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MAInboundClmTrns.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("MAInboundClmTrns.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("MAInboundClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("MAInboundClmTrns.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.col("MAInboundClmTrns.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
)

df_Snapshot = df_alpha_pfx.select(
    F.col("*")
)

df_ClmPK_input = df_Snapshot.select(
    F.col("*").alias("Transform.*")
)

params_ClmPK = {
    "CurrRunCycle": RunCycle
}

df_ClmPK = ClmPK(df_ClmPK_input, params_ClmPK)

df_MAInboundClmExtr = df_ClmPK.select(
    F.col("*")
)

write_columns = df_MAInboundClmExtr.columns

final_select_expr = []
for c in df_MAInboundClmExtr.schema.fields:
    colname = c.name
    if (c.dataType == StringType()) or ("char" in str(c.dataType).lower()):
        # We apply rpad. Use a large length if we have it in metadata. The job JSON has lengths for some columns.
        # We'll parse from the name if possible. Otherwise use some fallback, e.g. length=100
        length_str = "100"
        # Attempt to find length from the stage if any was declared
        # (all length info is in the JSON, but not trivially parsed at runtime. We'll just do a fallback.)
        final_select_expr.append(F.rpad(F.col(colname), int(length_str), " ").alias(colname))
    else:
        final_select_expr.append(F.col(colname))

df_final = df_MAInboundClmExtr.select(*final_select_expr)

write_files(
    df_final,
    f"{adls_path}/key/{SRC_SYS_CD}ClmExtr.{SRC_SYS_CD}Clm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)