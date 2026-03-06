# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_pfo_claims file
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC HASH FILES:  
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
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen  Annam             2014-07-14              5115 BHI                                       Original programming                                                  EnterpriseNewDevl        Bhoomi Dasari           7/15/2014  
# MAGIC 
# MAGIC Bhoomi Dasari               10/7/2014                 5115-BHI                                     Changes to FTP stage to remove extra bytes              EnterpriseNewDevl         Kalyan Neelam          2014-10-08
# MAGIC 
# MAGIC Madhavan B                 2017-07-06                5788 BHI Updates                       Added new fields to the end                                           EnterpriseDev1             Jag Yelavarthi            2017-07-17
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_PRNCPL,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_1, HDR_ICD_10_DIAG_CD_SEC_2,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_3, HDR_ICD_10_DIAG_CD_SEC_4,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_5, HDR_ICD_10_DIAG_CD_SEC_6,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_7, HDR_ICD_10_DIAG_CD_SEC_8,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_9, HDR_ICD_10_DIAG_CD_SEC_10,
# MAGIC                                                                                                                                             HDR_ICD_10_DIAG_CD_SEC_11, NPI_SITE_PROV_ID
# MAGIC 
# MAGIC Madhavan B                 2018-04-16                 5726                                             Added 2 new fields (BILL_PROV_TXNMY_CD             EnterpriseDev2            Jaideep Mankala        04/18/2018
# MAGIC                                                                                                                               and REND_PROV_TXNMY_CD) at the end of
# MAGIC                                                                                                                               the extract file
# MAGIC 
# MAGIC Madhu Sudhan Thera    2019-08-13            99156                                             Added substance use flag and taxonomy code fields         EnterpriseDev2      Kalyan Neelam             2019-08-14   
# MAGIC                                                                                                                                          in source file

# MAGIC Job Name: std_pfo_claims File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

ProdIn = get_widget_value('ProdIn','')
schema_std_pfo_claims = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_NO", StringType(), False),
    StructField("TRACEABILTY_FLD", StringType(), False),
    StructField("ADJ_SEQ_NO", StringType(), False),
    StructField("HOST_PLN_ID", StringType(), False),
    StructField("HOME_PLN_PROD_ID", StringType(), False),
    StructField("ACCT", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("MBR_ID", StringType(), False),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), False),
    StructField("MBR_CTRY_ON_CLM", StringType(), False),
    StructField("BILL_PROV_ID", StringType(), False),
    StructField("BILL_PROV_SPEC", StringType(), False),
    StructField("BILL_PROV_ZIP_CD", StringType(), False),
    StructField("BILL_PROV_CTRY", StringType(), False),
    StructField("BILL_PROV_TAX_ID", StringType(), False),
    StructField("BILL_PROV_MCARE_ID", StringType(), False),
    StructField("BILL_PROV_LAST_NM", StringType(), False),
    StructField("BILL_PROV_FIRST_NM", StringType(), False),
    StructField("BILL_PROV_NTWK", StringType(), False),
    StructField("PCP_PROV_ID", StringType(), False),
    StructField("PCP_PROV_SPEC", StringType(), False),
    StructField("PCP_PROV_ZIP_CD", StringType(), False),
    StructField("PCP_PROV_CTRY", StringType(), False),
    StructField("REND_PROV_ID", StringType(), False),
    StructField("REND_PROV_SPEC", StringType(), False),
    StructField("REND_PROV_ZIP_CD", StringType(), False),
    StructField("REND_PROV_CTRY", StringType(), False),
    StructField("REND_PROV_TYP", StringType(), False),
    StructField("REND_PROV_MCARE_ID", StringType(), False),
    StructField("REND_PROV_LAST_NM", StringType(), False),
    StructField("REND_PROV_FIRST_NM", StringType(), False),
    StructField("REND_PROV_NTWK", StringType(), False),
    StructField("ICD_9_DIAG_CD_PRNCPL", StringType(), False),
    StructField("ICD_9_DIAG_CD_SECAY_1", StringType(), False),
    StructField("ICD_9_DIAG_CD_SECAY_2", StringType(), False),
    StructField("ICD_9_DIAG_CD_SECAY_3", StringType(), False),
    StructField("CPT_AND_HCPCS_CD", StringType(), False),
    StructField("PROC_MOD", StringType(), False),
    StructField("AUTH_ID", StringType(), False),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), False),
    StructField("CAT_OF_SVC", StringType(), False),
    StructField("CLM_PAYMT_STTUS", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ITS_CLM_IN", StringType(), False),
    StructField("POS", StringType(), False),
    StructField("NON_COV_RSN_CD_PRI", StringType(), False),
    StructField("NON_COV_RSN_CD_2", StringType(), False),
    StructField("NON_COV_RSN_CD_3", StringType(), False),
    StructField("NON_COV_RSN_CD_4", StringType(), False),
    StructField("RMBRMT_TYP_CD", StringType(), False),
    StructField("SUBMSN_TYP", StringType(), False),
    StructField("TOT_UNIT", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("CLM_ENTER_DT", StringType(), False),
    StructField("CLM_PD_DT", StringType(), False),
    StructField("CLM_RCVD_DT", StringType(), False),
    StructField("SVC_END_DT", StringType(), False),
    StructField("SVC_FROM_DT", StringType(), False),
    StructField("SVC_POST_DT", StringType(), False),
    StructField("SUBMT_AMT", StringType(), False),
    StructField("NON_COV_AMT", StringType(), False),
    StructField("ALW_AMT", StringType(), False),
    StructField("PAYMT_AMT", StringType(), False),
    StructField("COB_TPL_AMT", StringType(), False),
    StructField("COINS", StringType(), False),
    StructField("COPAY", StringType(), False),
    StructField("DEDCT", StringType(), False),
    StructField("FFS_EQVLNT", StringType(), False),
    StructField("NPI_BILL_PROV_ID", StringType(), False),
    StructField("NPI_PCP_PROV_ID", StringType(), False),
    StructField("NPI_REND_PROV_ID", StringType(), False),
    StructField("NON_CNTRED_HOLD_HRMLS_NGTNS_IN", StringType(), False),
    StructField("BILL_PROV_CNTRING_STTUS_IN", StringType(), False),
    StructField("REND_PROV_CNTTING_STTUS_IN", StringType(), False),
    StructField("SCCF", StringType(), False),
    StructField("ICD_10_DIAG_CD_PRNCPL", StringType(), False),
    StructField("ICD_10_DIAG_CD_SEC_1", StringType(), False),
    StructField("ICD_10_DIAG_CD_SEC_2", StringType(), False),
    StructField("ICD_10_DIAG_CD_SEC_3", StringType(), False),
    StructField("HDR_ICD_10_DIAG_CD_PRNCPL", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_1", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_2", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_3", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_4", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_5", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_6", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_7", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_8", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_9", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_10", StringType(), True),
    StructField("HDR_ICD_10_DIAG_CD_SEC_11", StringType(), True),
    StructField("NPI_SITE_PROV_ID", StringType(), True),
    StructField("BILL_PROV_TXNMY_CD", StringType(), True),
    StructField("REND_PROV_TXNMY_CD", StringType(), True),
    StructField("SBSTNC_USE_RCRD_IN", StringType(), True)
])
df_std_pfo_claims = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferschema", "false")
    .option("quote", "")
    .option("delimiter", ",")
    .schema(schema_std_pfo_claims)
    .load(f"{adls_path_publish}/external/std_pfo_claims")
)
df_std_pfo_claims = df_std_pfo_claims.withColumn("BHI_HOME_PLN_ID", rpad("BHI_HOME_PLN_ID", 3, " ")) \
.withColumn("CLM_ID", rpad("CLM_ID", 25, " ")) \
.withColumn("CLM_LN_NO", rpad("CLM_LN_NO", 3, " ")) \
.withColumn("TRACEABILTY_FLD", rpad("TRACEABILTY_FLD", 5, " ")) \
.withColumn("ADJ_SEQ_NO", rpad("ADJ_SEQ_NO", 8, " ")) \
.withColumn("HOST_PLN_ID", rpad("HOST_PLN_ID", 3, " ")) \
.withColumn("HOME_PLN_PROD_ID", rpad("HOME_PLN_PROD_ID", 15, " ")) \
.withColumn("ACCT", rpad("ACCT", 14, " ")) \
.withColumn("GRP_ID", rpad("GRP_ID", 14, " ")) \
.withColumn("SUBGRP_ID", rpad("SUBGRP_ID", 10, " ")) \
.withColumn("MBR_ID", rpad("MBR_ID", 22, " ")) \
.withColumn("MBR_ZIP_CD_ON_CLM", rpad("MBR_ZIP_CD_ON_CLM", 5, " ")) \
.withColumn("MBR_CTRY_ON_CLM", rpad("MBR_CTRY_ON_CLM", 2, " ")) \
.withColumn("BILL_PROV_ID", rpad("BILL_PROV_ID", 27, " ")) \
.withColumn("BILL_PROV_SPEC", rpad("BILL_PROV_SPEC", 2, " ")) \
.withColumn("BILL_PROV_ZIP_CD", rpad("BILL_PROV_ZIP_CD", 5, " ")) \
.withColumn("BILL_PROV_CTRY", rpad("BILL_PROV_CTRY", 2, " ")) \
.withColumn("BILL_PROV_TAX_ID", rpad("BILL_PROV_TAX_ID", 12, " ")) \
.withColumn("BILL_PROV_MCARE_ID", rpad("BILL_PROV_MCARE_ID", 20, " ")) \
.withColumn("BILL_PROV_LAST_NM", rpad("BILL_PROV_LAST_NM", 50, " ")) \
.withColumn("BILL_PROV_FIRST_NM", rpad("BILL_PROV_FIRST_NM", 30, " ")) \
.withColumn("BILL_PROV_NTWK", rpad("BILL_PROV_NTWK", 10, " ")) \
.withColumn("PCP_PROV_ID", rpad("PCP_PROV_ID", 27, " ")) \
.withColumn("PCP_PROV_SPEC", rpad("PCP_PROV_SPEC", 2, " ")) \
.withColumn("PCP_PROV_ZIP_CD", rpad("PCP_PROV_ZIP_CD", 5, " ")) \
.withColumn("PCP_PROV_CTRY", rpad("PCP_PROV_CTRY", 2, " ")) \
.withColumn("REND_PROV_ID", rpad("REND_PROV_ID", 27, " ")) \
.withColumn("REND_PROV_SPEC", rpad("REND_PROV_SPEC", 2, " ")) \
.withColumn("REND_PROV_ZIP_CD", rpad("REND_PROV_ZIP_CD", 5, " ")) \
.withColumn("REND_PROV_CTRY", rpad("REND_PROV_CTRY", 2, " ")) \
.withColumn("REND_PROV_TYP", rpad("REND_PROV_TYP", 2, " ")) \
.withColumn("REND_PROV_MCARE_ID", rpad("REND_PROV_MCARE_ID", 20, " ")) \
.withColumn("REND_PROV_LAST_NM", rpad("REND_PROV_LAST_NM", 50, " ")) \
.withColumn("REND_PROV_FIRST_NM", rpad("REND_PROV_FIRST_NM", 30, " ")) \
.withColumn("REND_PROV_NTWK", rpad("REND_PROV_NTWK", 10, " ")) \
.withColumn("ICD_9_DIAG_CD_PRNCPL", rpad("ICD_9_DIAG_CD_PRNCPL", 7, " ")) \
.withColumn("ICD_9_DIAG_CD_SECAY_1", rpad("ICD_9_DIAG_CD_SECAY_1", 7, " ")) \
.withColumn("ICD_9_DIAG_CD_SECAY_2", rpad("ICD_9_DIAG_CD_SECAY_2", 7, " ")) \
.withColumn("ICD_9_DIAG_CD_SECAY_3", rpad("ICD_9_DIAG_CD_SECAY_3", 7, " ")) \
.withColumn("CPT_AND_HCPCS_CD", rpad("CPT_AND_HCPCS_CD", 6, " ")) \
.withColumn("PROC_MOD", rpad("PROC_MOD", 2, " ")) \
.withColumn("AUTH_ID", rpad("AUTH_ID", 15, " ")) \
.withColumn("BNF_PAYMT_STTUS_CD", rpad("BNF_PAYMT_STTUS_CD", 1, " ")) \
.withColumn("CAT_OF_SVC", rpad("CAT_OF_SVC", 14, " ")) \
.withColumn("CLM_PAYMT_STTUS", rpad("CLM_PAYMT_STTUS", 1, " ")) \
.withColumn("CLM_TYP", rpad("CLM_TYP", 2, " ")) \
.withColumn("ITS_CLM_IN", rpad("ITS_CLM_IN", 1, " ")) \
.withColumn("POS", rpad("POS", 2, " ")) \
.withColumn("NON_COV_RSN_CD_PRI", rpad("NON_COV_RSN_CD_PRI", 2, " ")) \
.withColumn("NON_COV_RSN_CD_2", rpad("NON_COV_RSN_CD_2", 2, " ")) \
.withColumn("NON_COV_RSN_CD_3", rpad("NON_COV_RSN_CD_3", 2, " ")) \
.withColumn("NON_COV_RSN_CD_4", rpad("NON_COV_RSN_CD_4", 2, " ")) \
.withColumn("RMBRMT_TYP_CD", rpad("RMBRMT_TYP_CD", 5, " ")) \
.withColumn("SUBMSN_TYP", rpad("SUBMSN_TYP", 2, " ")) \
.withColumn("TOT_UNIT", rpad("TOT_UNIT", 9, " ")) \
.withColumn("ADJDCT_DT", rpad("ADJDCT_DT", 8, " ")) \
.withColumn("CLM_ENTER_DT", rpad("CLM_ENTER_DT", 8, " ")) \
.withColumn("CLM_PD_DT", rpad("CLM_PD_DT", 8, " ")) \
.withColumn("CLM_RCVD_DT", rpad("CLM_RCVD_DT", 8, " ")) \
.withColumn("SVC_END_DT", rpad("SVC_END_DT", 8, " ")) \
.withColumn("SVC_FROM_DT", rpad("SVC_FROM_DT", 8, " ")) \
.withColumn("SVC_POST_DT", rpad("SVC_POST_DT", 8, " ")) \
.withColumn("SUBMT_AMT", rpad("SUBMT_AMT", 10, " ")) \
.withColumn("NON_COV_AMT", rpad("NON_COV_AMT", 10, " ")) \
.withColumn("ALW_AMT", rpad("ALW_AMT", 10, " ")) \
.withColumn("PAYMT_AMT", rpad("PAYMT_AMT", 10, " ")) \
.withColumn("COB_TPL_AMT", rpad("COB_TPL_AMT", 10, " ")) \
.withColumn("COINS", rpad("COINS", 10, " ")) \
.withColumn("COPAY", rpad("COPAY", 10, " ")) \
.withColumn("DEDCT", rpad("DEDCT", 10, " ")) \
.withColumn("FFS_EQVLNT", rpad("FFS_EQVLNT", 10, " ")) \
.withColumn("NPI_BILL_PROV_ID", rpad("NPI_BILL_PROV_ID", 10, " ")) \
.withColumn("NPI_PCP_PROV_ID", rpad("NPI_PCP_PROV_ID", 10, " ")) \
.withColumn("NPI_REND_PROV_ID", rpad("NPI_REND_PROV_ID", 10, " ")) \
.withColumn("NON_CNTRED_HOLD_HRMLS_NGTNS_IN", rpad("NON_CNTRED_HOLD_HRMLS_NGTNS_IN", 1, " ")) \
.withColumn("BILL_PROV_CNTRING_STTUS_IN", rpad("BILL_PROV_CNTRING_STTUS_IN", 1, " ")) \
.withColumn("REND_PROV_CNTTING_STTUS_IN", rpad("REND_PROV_CNTTING_STTUS_IN", 1, " ")) \
.withColumn("SCCF", rpad("SCCF", 17, " ")) \
.withColumn("ICD_10_DIAG_CD_PRNCPL", rpad("ICD_10_DIAG_CD_PRNCPL", 8, " ")) \
.withColumn("ICD_10_DIAG_CD_SEC_1", rpad("ICD_10_DIAG_CD_SEC_1", 8, " ")) \
.withColumn("ICD_10_DIAG_CD_SEC_2", rpad("ICD_10_DIAG_CD_SEC_2", 8, " ")) \
.withColumn("ICD_10_DIAG_CD_SEC_3", rpad("ICD_10_DIAG_CD_SEC_3", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_PRNCPL", rpad("HDR_ICD_10_DIAG_CD_PRNCPL", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_1", rpad("HDR_ICD_10_DIAG_CD_SEC_1", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_2", rpad("HDR_ICD_10_DIAG_CD_SEC_2", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_3", rpad("HDR_ICD_10_DIAG_CD_SEC_3", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_4", rpad("HDR_ICD_10_DIAG_CD_SEC_4", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_5", rpad("HDR_ICD_10_DIAG_CD_SEC_5", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_6", rpad("HDR_ICD_10_DIAG_CD_SEC_6", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_7", rpad("HDR_ICD_10_DIAG_CD_SEC_7", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_8", rpad("HDR_ICD_10_DIAG_CD_SEC_8", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_9", rpad("HDR_ICD_10_DIAG_CD_SEC_9", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_10", rpad("HDR_ICD_10_DIAG_CD_SEC_10", 8, " ")) \
.withColumn("HDR_ICD_10_DIAG_CD_SEC_11", rpad("HDR_ICD_10_DIAG_CD_SEC_11", 8, " ")) \
.withColumn("NPI_SITE_PROV_ID", rpad("NPI_SITE_PROV_ID", 10, " ")) \
.withColumn("BILL_PROV_TXNMY_CD", rpad("BILL_PROV_TXNMY_CD", 15, " ")) \
.withColumn("REND_PROV_TXNMY_CD", rpad("REND_PROV_TXNMY_CD", 15, " ")) \
.withColumn("SBSTNC_USE_RCRD_IN", rpad("SBSTNC_USE_RCRD_IN", 1, " "))
df_std_pfo_claims_final = df_std_pfo_claims.select(
    "BHI_HOME_PLN_ID",
    "CLM_ID",
    "CLM_LN_NO",
    "TRACEABILTY_FLD",
    "ADJ_SEQ_NO",
    "HOST_PLN_ID",
    "HOME_PLN_PROD_ID",
    "ACCT",
    "GRP_ID",
    "SUBGRP_ID",
    "MBR_ID",
    "MBR_ZIP_CD_ON_CLM",
    "MBR_CTRY_ON_CLM",
    "BILL_PROV_ID",
    "BILL_PROV_SPEC",
    "BILL_PROV_ZIP_CD",
    "BILL_PROV_CTRY",
    "BILL_PROV_TAX_ID",
    "BILL_PROV_MCARE_ID",
    "BILL_PROV_LAST_NM",
    "BILL_PROV_FIRST_NM",
    "BILL_PROV_NTWK",
    "PCP_PROV_ID",
    "PCP_PROV_SPEC",
    "PCP_PROV_ZIP_CD",
    "PCP_PROV_CTRY",
    "REND_PROV_ID",
    "REND_PROV_SPEC",
    "REND_PROV_ZIP_CD",
    "REND_PROV_CTRY",
    "REND_PROV_TYP",
    "REND_PROV_MCARE_ID",
    "REND_PROV_LAST_NM",
    "REND_PROV_FIRST_NM",
    "REND_PROV_NTWK",
    "ICD_9_DIAG_CD_PRNCPL",
    "ICD_9_DIAG_CD_SECAY_1",
    "ICD_9_DIAG_CD_SECAY_2",
    "ICD_9_DIAG_CD_SECAY_3",
    "CPT_AND_HCPCS_CD",
    "PROC_MOD",
    "AUTH_ID",
    "BNF_PAYMT_STTUS_CD",
    "CAT_OF_SVC",
    "CLM_PAYMT_STTUS",
    "CLM_TYP",
    "ITS_CLM_IN",
    "POS",
    "NON_COV_RSN_CD_PRI",
    "NON_COV_RSN_CD_2",
    "NON_COV_RSN_CD_3",
    "NON_COV_RSN_CD_4",
    "RMBRMT_TYP_CD",
    "SUBMSN_TYP",
    "TOT_UNIT",
    "ADJDCT_DT",
    "CLM_ENTER_DT",
    "CLM_PD_DT",
    "CLM_RCVD_DT",
    "SVC_END_DT",
    "SVC_FROM_DT",
    "SVC_POST_DT",
    "SUBMT_AMT",
    "NON_COV_AMT",
    "ALW_AMT",
    "PAYMT_AMT",
    "COB_TPL_AMT",
    "COINS",
    "COPAY",
    "DEDCT",
    "FFS_EQVLNT",
    "NPI_BILL_PROV_ID",
    "NPI_PCP_PROV_ID",
    "NPI_REND_PROV_ID",
    "NON_CNTRED_HOLD_HRMLS_NGTNS_IN",
    "BILL_PROV_CNTRING_STTUS_IN",
    "REND_PROV_CNTTING_STTUS_IN",
    "SCCF",
    "ICD_10_DIAG_CD_PRNCPL",
    "ICD_10_DIAG_CD_SEC_1",
    "ICD_10_DIAG_CD_SEC_2",
    "ICD_10_DIAG_CD_SEC_3",
    "HDR_ICD_10_DIAG_CD_PRNCPL",
    "HDR_ICD_10_DIAG_CD_SEC_1",
    "HDR_ICD_10_DIAG_CD_SEC_2",
    "HDR_ICD_10_DIAG_CD_SEC_3",
    "HDR_ICD_10_DIAG_CD_SEC_4",
    "HDR_ICD_10_DIAG_CD_SEC_5",
    "HDR_ICD_10_DIAG_CD_SEC_6",
    "HDR_ICD_10_DIAG_CD_SEC_7",
    "HDR_ICD_10_DIAG_CD_SEC_8",
    "HDR_ICD_10_DIAG_CD_SEC_9",
    "HDR_ICD_10_DIAG_CD_SEC_10",
    "HDR_ICD_10_DIAG_CD_SEC_11",
    "NPI_SITE_PROV_ID",
    "BILL_PROV_TXNMY_CD",
    "REND_PROV_TXNMY_CD",
    "SBSTNC_USE_RCRD_IN"
)
df_FTP_std_pfo_claims = df_std_pfo_claims_final