# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC            
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                             Project/Altiris #\(9)         Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                                   --------------------     \(9)          ------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Lokesh K                                       2021-11-29                     US 404552                Initial programming                                                     IntegrateDev2                          Jeyaprasanna             2022-02-06\(9)
# MAGIC 
# MAGIC Ediga Maruthi                                2024-04-23                     US 614444               Added DOC_NO,USER_DEFN_TX_1                         IntegrateDev1                         Jeyaprasanna             2024-04-23
# MAGIC                                                                                                                              USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                                                            USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                                                           and USER_DEFN_AMT_2  columns to
# MAGIC                                                                                                                            MAInboundClmLanding.

# MAGIC MA Inbound Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSSCClmPKExtr
# MAGIC MAInboundClmPKExtr
# MAGIC DentaQuestClmPKExtr
# MAGIC EYEMEDClmPKExtr
# MAGIC BCBSSCMedClmPKExtr
# MAGIC MAInboundClmPKExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
InFile_F = get_widget_value('InFile_F','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_MAInboundClmLanding = StructType([
    StructField("REC_TYPE", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_TYP_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
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
    StructField("CLAIM_STS_CD", StringType(), True),
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

df_MAInboundClmLanding = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_MAInboundClmLanding)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_MAInboundClmLanding = df_MAInboundClmLanding.select(
    "REC_TYPE",
    "SRC_SYS_CD",
    "CLM_TYP_CD",
    "CLM_ID",
    "CLM_STTUS_CD",
    "CLM_LN_SEQ_NO",
    "CLM_ADJ_IN",
    "CLM_ADJ_FROM_CLM_ID",
    "CLM_ADJ_TO_CLM_ID",
    "GRP_ID",
    "MBR_ID",
    "MCARE_BNFCRY_ID",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "MBR_BRTH_DT",
    "MBR_MAIL_ADDR_LN_1",
    "MBR_MAIL_ADDR_LN_2",
    "MBR_MAIL_ADDR_CITY_NM",
    "MBR_MAIL_ADDR_ST_CD",
    "MBR_MAIL_ADDR_ZIP_CD_5",
    "PROV_ID",
    "PROV_NM",
    "PROV_NTNL_PROV_ID",
    "PROV_TAX_ID",
    "PROV_TXNMY_CD",
    "PROV_PRI_MAIL_ADDR_LN_1",
    "PROV_PRI_MAIL_ADDR_LN_2",
    "PROV_PRI_MAIL_ADDR_LN_3",
    "PROV_PRI_MAIL_ADDR_CITY_NM",
    "PROV_PRI_MAIL_ADDR_ST_CD",
    "PROV_PRI_MAIL_ADDR_ZIP_CD_5",
    "CLM_SVC_PROV_SPEC_CD",
    "SVC_PROV_ID",
    "SVC_PROV_NM",
    "SVC_PROV_NTNL_PROV_ID",
    "SVC_PROV_TAX_ID",
    "SVC_PROV_TXNMY_CD",
    "SVC_PROV_ADDR_LN1",
    "SVC_PROV_ADDR_LN2",
    "SVC_PROV_ADDR_LN3",
    "SVC_PROV_CITY_NM",
    "SVC_PROV_ST_CD",
    "SVC_PROV_ZIP_CD_5",
    "CLM_SVC_STRT_DT",
    "CLM_SVC_END_DT",
    "CLM_PD_DT",
    "CLM_RCVD_DT",
    "CLM_PAYE_CD",
    "CLM_NTWK_STTUS_CD",
    "CLM_LN_FINL_DISP_CD",
    "DIAG_CD_1",
    "DIAG_CD_2",
    "DIAG_CD_3",
    "CLM_LN_CAP_LN_IN",
    "Claim_Line_Denied_Indicator",
    "Explanation_Code",
    "Explanation_Code_Description",
    "DNTL_CLM_LN_TOOTH_NO",
    "DNTL_CLM_LN_TOOTH_SRFC_TX",
    "Adjustment_Reason_Code",
    "Remittance_Advice_Remark_Code_RARC",
    "DIAG_CD_TYP_CD",
    "PROC_CD",
    "PROC_CD_DESC",
    "Procedure_Code_Modifier1",
    "Procedure_Code_Modifier2",
    "Procedure_Code_Modifier3",
    "CLM_LN_SVC_STRT_DT",
    "CLM_LN_SVC_END_DT",
    "CLM_LN_CHRG_AMT",
    "CLM_LN_ALW_AMT",
    "CLM_LN_DSALW_AMT",
    "CLM_LN_COPAY_AMT",
    "CLM_LN_COINS_AMT",
    "CLM_LN_DEDCT_AMT",
    "Plan_Pay_Amount",
    "Patient_Responsibility_Amount",
    "CLM_LN_AGMNT_PRICE_AMT",
    "CLM_LN_RISK_WTHLD_AMT",
    "CLM_PROV_ROLE_TYPE_CD",
    "CLM_LN_PD_AMT",
    "CLM_COB_PD_AMT",
    "CLM_COB_ALW_AMT",
    "UNIT_CT",
    "CLAIM_STS_CD",
    "ADJ_FROM_CLM_ID",
    "ADJ_TO_CLM_ID",
    "DOC_NO",
    "USER_DEFN_TX_1",
    "USER_DEFN_TX_2",
    "USER_DEFN_DT_1",
    "USER_DEFN_DT_2",
    "USER_DEFN_AMT_1",
    "USER_DEFN_AMT_2"
)

df_Trans1 = (
    df_MAInboundClmLanding.filter(F.col("REC_TYPE") == "CLAIM HEADER")
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

df_hf_ma_inbound_clmpk = df_Trans1.dropDuplicates(["SRC_SYS_CD_SK", "CLM_ID"])

params = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}

df_clmloadpk_out, = ClmLoadPK(df_hf_ma_inbound_clmpk, params)

df_clmloadpk_out = (
    df_clmloadpk_out
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("CLM_ID", rpad("CLM_ID", <...>, " "))
    .select("SRC_SYS_CD", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK")
)

write_files(
    df_clmloadpk_out,
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)