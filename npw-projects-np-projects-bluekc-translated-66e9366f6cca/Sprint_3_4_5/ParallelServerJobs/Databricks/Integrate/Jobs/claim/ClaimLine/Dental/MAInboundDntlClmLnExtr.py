# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmLnExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Dental Claim Line Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Manisha G                          US 404552                                 Initial programming                                           IntegrateDev2            Jeyaprasanna             2022-02-06      
# MAGIC   
# MAGIC 2023-12-21      Kshema H K                        US 599810                       Updated CLM_TYP_CD='DENTAL' to 
# MAGIC                                                                                                   CLM_TYP_CD='DNTL' in Transformer(snapshot) stage        IntegrateDev1           Jeyaprasanna             2023-12-21
# MAGIC 
# MAGIC 2024-04-04      Ediga Maruthi                      US 614444              Added Columns DOC_NO,USER_DEFN_TX_1,              IntegrateDev1              Jeyaprasanna             2024-04-19
# MAGIC                                                                                                   USER_DEFN_TX_2,USER_DEFN_DT_1,USER_DEFN_DT_2,
# MAGIC                                                                                                   USER_DEFN_AMT_1,USER_DEFN_AMT_2
# MAGIC                                                                                                    in MAInboundClmln

# MAGIC Balancing
# MAGIC This container is used in:
# MAGIC FctsDntlClmLineExtr
# MAGIC BCBSSCDntlClmLineExtr
# MAGIC MAInboundDntlClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import col, lit, when, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
InFile_F = get_widget_value('InFile_F','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/DntlClmLnPK
# COMMAND ----------

schema_MAInboundClmln = StructType([
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

df_MAInboundClmln = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MAInboundClmln)
    .csv(f"{adls_path}/verified/{InFile_F}")
)

df_Snapshot = (
    df_MAInboundClmln
    .filter((col("REC_TYPE") == "CLAIM LINE") & (col("CLM_TYP_CD") == "DNTL"))
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO")
    )
)

df_Pkey = (
    df_MAInboundClmln
    .filter((col("REC_TYPE") == "CLAIM LINE") & (col("CLM_TYP_CD") == "DNTL"))
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        lit(CurrentDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        concat(col("SRC_SYS_CD"), lit(";"), col("CLM_ID"), lit(";"), col("CLM_LN_SEQ_NO")).alias("PRI_KEY_STRING"),
        lit(0).alias("DNTL_CLM_LN_SK"),
        rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(lit("NA"), 5, " ").alias("ALT_PROC_CD"),
        lit("NA").alias("DNTL_CLM_LN_DNTL_CAT_CD"),
        lit("NA").alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
        rpad(lit("NA"), 3, " ").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
        rpad(lit("NA"), 2, " ").alias("DNTL_CLM_LN_BNF_TYP_CD"),
        when(col("DNTL_CLM_LN_TOOTH_SRFC_TX").isNull(), "UNK").otherwise(col("DNTL_CLM_LN_TOOTH_SRFC_TX")).alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
        rpad(lit("NA"), 3, " ").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
        rpad(lit("NA"), 4, " ").alias("CAT_PAYMT_PFX_ID"),
        rpad(lit("NA"), 4, " ").alias("DNTL_PROC_PAYMT_PFX_ID"),
        rpad(lit("NA"), 4, " ").alias("DNTL_PROC_PRICE_ID"),
        rpad(lit("0"), 2, " ").alias("TOOTH_BEG_NO"),
        rpad(lit("0"), 2, " ").alias("TOOTH_END_NO"),
        when(col("DNTL_CLM_LN_TOOTH_NO").isNull(), "0").otherwise(col("DNTL_CLM_LN_TOOTH_NO")).alias("TOOTH_NO")
    )
)

write_files(
    df_Snapshot.select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO"),
    f"{adls_path}/load/B_DNTL_CLM_LN.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params = {
    "CurrRunCycle": CurrRunCycle
}
df_IdsMAInboundClmLineExtr = DntlClmLnPK(df_Pkey, params)

df_IdsMAInboundClmLineExtr_final = df_IdsMAInboundClmLineExtr.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("DNTL_CLM_LN_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("ALT_PROC_CD"), 5, " ").alias("ALT_PROC_CD"),
    col("DNTL_CLM_LN_DNTL_CAT_CD"),
    col("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    rpad(col("DNTL_CLM_LN_ALT_PROC_EXCD"), 3, " ").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    rpad(col("DNTL_CLM_LN_BNF_TYP_CD"), 2, " ").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    rpad(col("DNTL_CLM_LN_TOOTH_SRFC_CD"), 10, " ").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    rpad(col("DNTL_CLM_LN_UTIL_EDIT_CD"), 3, " ").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    rpad(col("CAT_PAYMT_PFX_ID"), 4, " ").alias("CAT_PAYMT_PFX_ID"),
    rpad(col("DNTL_PROC_PAYMT_PFX_ID"), 4, " ").alias("DNTL_PROC_PAYMT_PFX_ID"),
    rpad(col("DNTL_PROC_PRICE_ID"), 4, " ").alias("DNTL_PROC_PRICE_ID"),
    rpad(col("TOOTH_BEG_NO"), 2, " ").alias("TOOTH_BEG_NO"),
    rpad(col("TOOTH_END_NO"), 2, " ").alias("TOOTH_END_NO"),
    rpad(col("TOOTH_NO"), 2, " ").alias("TOOTH_NO")
)

write_files(
    df_IdsMAInboundClmLineExtr_final,
    f"{adls_path}/key/{SrcSysCd}DntlClmLnExtr.{SrcSysCd}DntlClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)