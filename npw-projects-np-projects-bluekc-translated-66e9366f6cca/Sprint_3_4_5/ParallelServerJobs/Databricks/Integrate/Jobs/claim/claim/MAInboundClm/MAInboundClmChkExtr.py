# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2024 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim Check Extract File to be processed by IDS routines.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2024-09-03      Kshema H K                       US 621773                         Initial programming                                            IntegrateDev1                     Jeyaprasanna            2024-09-04

# MAGIC Read the MAInbound Reversal Format  file created from MAInboundClmExtrRvrsl
# MAGIC This container is used in:
# MAGIC MAInboundClmChkExtr
# MAGIC FctsClmChkExtr
# MAGIC NascoClmChkTrns
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile_F = get_widget_value('InFile_F','')

MAInboundClm_schema = T.StructType([
    T.StructField("REC_TYPE", T.StringType(), True),
    T.StructField("SRC_SYS_CD", T.StringType(), True),
    T.StructField("CLM_TYP_CD", T.StringType(), True),
    T.StructField("CLM_ID", T.StringType(), True),
    T.StructField("CLM_STTUS_CD", T.StringType(), True),
    T.StructField("CLM_LN_SEQ_NO", T.StringType(), True),
    T.StructField("CLM_ADJ_IN", T.StringType(), True),
    T.StructField("CLM_ADJ_FROM_CLM_ID", T.StringType(), True),
    T.StructField("CLM_ADJ_TO_CLM_ID", T.StringType(), True),
    T.StructField("GRP_ID", T.StringType(), True),
    T.StructField("MBR_ID", T.StringType(), True),
    T.StructField("MCARE_BNFCRY_ID", T.StringType(), True),
    T.StructField("MBR_FIRST_NM", T.StringType(), True),
    T.StructField("MBR_MIDINIT", T.StringType(), True),
    T.StructField("MBR_LAST_NM", T.StringType(), True),
    T.StructField("MBR_BRTH_DT", T.StringType(), True),
    T.StructField("MBR_MAIL_ADDR_LN_1", T.StringType(), True),
    T.StructField("MBR_MAIL_ADDR_LN_2", T.StringType(), True),
    T.StructField("MBR_MAIL_ADDR_CITY_NM", T.StringType(), True),
    T.StructField("MBR_MAIL_ADDR_ST_CD", T.StringType(), True),
    T.StructField("MBR_MAIL_ADDR_ZIP_CD_5", T.StringType(), True),
    T.StructField("PROV_ID", T.StringType(), True),
    T.StructField("PROV_NM", T.StringType(), True),
    T.StructField("PROV_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("PROV_TAX_ID", T.StringType(), True),
    T.StructField("PROV_TXNMY_CD", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_LN_1", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_LN_2", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_LN_3", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_CITY_NM", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_ST_CD", T.StringType(), True),
    T.StructField("PROV_PRI_MAIL_ADDR_ZIP_CD_5", T.StringType(), True),
    T.StructField("CLM_SVC_PROV_SPEC_CD", T.StringType(), True),
    T.StructField("SVC_PROV_ID", T.StringType(), True),
    T.StructField("SVC_PROV_NM", T.StringType(), True),
    T.StructField("SVC_PROV_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("SVC_PROV_TAX_ID", T.StringType(), True),
    T.StructField("SVC_PROV_TXNMY_CD", T.StringType(), True),
    T.StructField("SVC_PROV_ADDR_LN1", T.StringType(), True),
    T.StructField("SVC_PROV_ADDR_LN2", T.StringType(), True),
    T.StructField("SVC_PROV_ADDR_LN3", T.StringType(), True),
    T.StructField("SVC_PROV_CITY_NM", T.StringType(), True),
    T.StructField("SVC_PROV_ST_CD", T.StringType(), True),
    T.StructField("SVC_PROV_ZIP_CD_5", T.StringType(), True),
    T.StructField("CLM_SVC_STRT_DT", T.StringType(), True),
    T.StructField("CLM_SVC_END_DT", T.StringType(), True),
    T.StructField("CLM_PD_DT", T.StringType(), True),
    T.StructField("CLM_RCVD_DT", T.StringType(), True),
    T.StructField("CLM_PAYE_CD", T.StringType(), True),
    T.StructField("CLM_NTWK_STTUS_CD", T.StringType(), True),
    T.StructField("CLM_LN_FINL_DISP_CD", T.StringType(), True),
    T.StructField("DIAG_CD_1", T.StringType(), True),
    T.StructField("DIAG_CD_2", T.StringType(), True),
    T.StructField("DIAG_CD_3", T.StringType(), True),
    T.StructField("CLM_LN_CAP_LN_IN", T.StringType(), True),
    T.StructField("Claim_Line_Denied_Indicator", T.StringType(), True),
    T.StructField("Explanation_Code", T.StringType(), True),
    T.StructField("Explanation_Code_Description", T.StringType(), True),
    T.StructField("DNTL_CLM_LN_TOOTH_NO", T.StringType(), True),
    T.StructField("DNTL_CLM_LN_TOOTH_SRFC_TX", T.StringType(), True),
    T.StructField("Adjustment_Reason_Code", T.StringType(), True),
    T.StructField("Remittance_Advice_Remark_Code_RARC", T.StringType(), True),
    T.StructField("DIAG_CD_TYP_CD", T.StringType(), True),
    T.StructField("PROC_CD", T.StringType(), True),
    T.StructField("PROC_CD_DESC", T.StringType(), True),
    T.StructField("Procedure_Code_Modifier1", T.StringType(), True),
    T.StructField("Procedure_Code_Modifier2", T.StringType(), True),
    T.StructField("Procedure_Code_Modifier3", T.StringType(), True),
    T.StructField("CLM_LN_SVC_STRT_DT", T.StringType(), True),
    T.StructField("CLM_LN_SVC_END_DT", T.StringType(), True),
    T.StructField("CLM_LN_CHRG_AMT", T.StringType(), True),
    T.StructField("CLM_LN_ALW_AMT", T.StringType(), True),
    T.StructField("CLM_LN_DSALW_AMT", T.StringType(), True),
    T.StructField("CLM_LN_COPAY_AMT", T.StringType(), True),
    T.StructField("CLM_LN_COINS_AMT", T.StringType(), True),
    T.StructField("CLM_LN_DEDCT_AMT", T.StringType(), True),
    T.StructField("Plan_Pay_Amount", T.DecimalType(38, 10), True),
    T.StructField("Patient_Responsibility_Amount", T.StringType(), True),
    T.StructField("CLM_LN_AGMNT_PRICE_AMT", T.StringType(), True),
    T.StructField("CLM_LN_RISK_WTHLD_AMT", T.StringType(), True),
    T.StructField("CLM_PROV_ROLE_TYPE_CD", T.StringType(), True),
    T.StructField("CLM_LN_PD_AMT", T.StringType(), True),
    T.StructField("CLM_COB_PD_AMT", T.StringType(), True),
    T.StructField("CLM_COB_ALW_AMT", T.StringType(), True),
    T.StructField("UNIT_CT", T.StringType(), True),
    T.StructField("CLAIM_STS_CD", T.StringType(), True),
    T.StructField("ADJ_FROM_CLM_ID", T.StringType(), True),
    T.StructField("ADJ_TO_CLM_ID", T.StringType(), True),
    T.StructField("DOC_NO", T.StringType(), True),
    T.StructField("USER_DEFN_TX_1", T.StringType(), True),
    T.StructField("USER_DEFN_TX_2", T.StringType(), True),
    T.StructField("USER_DEFN_DT_1", T.StringType(), True),
    T.StructField("USER_DEFN_DT_2", T.StringType(), True),
    T.StructField("USER_DEFN_AMT_1", T.DecimalType(38, 10), True),
    T.StructField("USER_DEFN_AMT_2", T.DecimalType(38, 10), True)
])

df_MAInboundClm = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(MAInboundClm_schema)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_agg = (
    df_MAInboundClm
    .groupBy("CLM_ID")
    .agg(
        F.min("REC_TYPE").alias("REC_TYPE"),
        F.max("SVC_PROV_NM").alias("SVC_PROV_NM"),
        F.min("CLM_PD_DT").alias("CLM_PD_DT"),
        F.sum("Plan_Pay_Amount").alias("Plan_Pay_Amount"),
        F.max("USER_DEFN_TX_1").alias("USER_DEFN_TX_1")
    )
)

df_xfm_1 = df_agg.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    df_agg["REC_TYPE"].alias("REC_TYPE"),
    df_agg["CLM_ID"].alias("CLM_ID"),
    F.lit(" ").alias("CLM_CHK_PAYE_TYP_CD"),
    F.lit(" ").alias("CLM_CHK_LOB_CD"),
    F.lit("1").alias("CLM_CHK_PAYMT_METH_CD"),
    F.lit("1").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    F.lit("N").alias("EXTRNL_CHK_IN"),
    F.lit("N").alias("PCA_CHK_IN"),
    df_agg["CLM_PD_DT"].alias("CHK_PD_DT"),
    df_agg["Plan_Pay_Amount"].alias("CHK_NET_PAYMT_AMT"),
    df_agg["Plan_Pay_Amount"].alias("CHK_ORIG_AMT"),
    trim(df_agg["USER_DEFN_TX_1"]).alias("CHK_NO"),
    F.lit(0).alias("CHK_SEQ_NO"),
    df_agg["SVC_PROV_NM"].alias("CHK_PAYE_NM"),
    F.lit("NA").alias("CHK_PAYMT_REF_ID")
)

df_logic_allcol = df_xfm_1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    df_xfm_1["CLM_ID"].alias("CLM_ID"),
    df_xfm_1["CLM_CHK_PAYE_TYP_CD"].alias("CLM_CHK_PAYE_TYP_CD"),
    df_xfm_1["CLM_CHK_LOB_CD"].alias("CLM_CHK_LOB_CD"),
    df_xfm_1["JOB_EXCTN_RCRD_ERR_SK"].alias("JOB_EXCTN_RCRD_ERR_SK"),
    df_xfm_1["INSRT_UPDT_CD"].alias("INSRT_UPDT_CD"),
    df_xfm_1["DISCARD_IN"].alias("DISCARD_IN"),
    df_xfm_1["PASS_THRU_IN"].alias("PASS_THRU_IN"),
    df_xfm_1["FIRST_RECYC_DT"].alias("FIRST_RECYC_DT"),
    df_xfm_1["ERR_CT"].alias("ERR_CT"),
    df_xfm_1["RECYCLE_CT"].alias("RECYCLE_CT"),
    df_xfm_1["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), df_xfm_1["CLM_ID"]).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_CHK_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_xfm_1["CLM_ID"].alias("CLM_ID_1"),
    df_xfm_1["CLM_CHK_PAYMT_METH_CD"].alias("CLM_CHK_PAYMT_METH_CD"),
    df_xfm_1["CLM_REMIT_HIST_PAYMTOVRD_CD"].alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    df_xfm_1["EXTRNL_CHK_IN"].alias("EXTRNL_CHK_IN"),
    df_xfm_1["PCA_CHK_IN"].alias("PCA_CHK_IN"),
    df_xfm_1["CHK_PD_DT"].alias("CHK_PD_DT"),
    df_xfm_1["CHK_NET_PAYMT_AMT"].alias("CHK_NET_PAYMT_AMT"),
    df_xfm_1["CHK_ORIG_AMT"].alias("CHK_ORIG_AMT"),
    df_xfm_1["CHK_NO"].alias("CHK_NO"),
    df_xfm_1["CHK_SEQ_NO"].alias("CHK_SEQ_NO"),
    df_xfm_1["CHK_PAYE_NM"].alias("CHK_PAYE_NM"),
    df_xfm_1["CHK_PAYMT_REF_ID"].alias("CHK_PAYMT_REF_ID")
)

df_logic_transform = df_xfm_1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    df_xfm_1["CLM_ID"].alias("CLM_ID"),
    df_xfm_1["CLM_CHK_PAYE_TYP_CD"].alias("CLM_CHK_PAYE_TYP_CD"),
    df_xfm_1["CLM_CHK_LOB_CD"].alias("CLM_CHK_LOB_CD")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmChkPK
# COMMAND ----------

params = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner,
    "CurrentDate": CurrentDate
}
df_ClmChkPK = ClmChkPK(df_logic_allcol, df_logic_transform, params)

df_final = df_ClmChkPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_CHK_SK",
    "CLM_ID",
    "CLM_CHK_PAYE_TYP_CD",
    "CLM_CHK_LOB_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_ID_1",
    "CLM_CHK_PAYMT_METH_CD",
    "CLM_REMIT_HIST_PAYMTOVRD_CD",
    "EXTRNL_CHK_IN",
    "PCA_CHK_IN",
    "CHK_PD_DT",
    "CHK_NET_PAYMT_AMT",
    "CHK_ORIG_AMT",
    "CHK_NO",
    "CHK_SEQ_NO",
    "CHK_PAYE_NM",
    "CHK_PAYMT_REF_ID"
)

df_final = df_final.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")) \
                   .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")) \
                   .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")) \
                   .withColumn("CLM_ID_1", F.rpad(F.col("CLM_ID_1"), 18, " ")) \
                   .withColumn("EXTRNL_CHK_IN", F.rpad(F.col("EXTRNL_CHK_IN"), 1, " ")) \
                   .withColumn("PCA_CHK_IN", F.rpad(F.col("PCA_CHK_IN"), 1, " "))

write_files(
    df_final,
    f"{adls_path}/key/{SrcSysCd}ClmChkExtr.{SrcSysCd}ClmChk.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)