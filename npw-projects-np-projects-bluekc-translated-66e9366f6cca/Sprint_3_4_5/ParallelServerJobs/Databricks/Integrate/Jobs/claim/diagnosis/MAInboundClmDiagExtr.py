# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim Diagnosis Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Lokesh K                          US 404552                                 Initial programming                                           IntegrateDev2                Jeyaprasanna           2022-02-06
# MAGIC 
# MAGIC 2024-02-02       Arpitha                              US 599810                           Added logic to read 4 Character for DIAG_CD      IntegrateDev1                Jeyaprasanna           2024-02-05
# MAGIC 
# MAGIC 2024-04-04       Ediga Maruthi                   US 614444                           Added columns DOC_NO,USER_DEFN_TX_1,     IntegrateDev1              Jeyaprasanna            2024-04-19
# MAGIC                                                                                                                USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                                                USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                                                USER_DEFN_AMT_2 in MAInboundClm.

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Diag Cods file is used in 
# MAGIC BCAFEPClmDRGExtr 
# MAGIC EyeMedClmExtr job
# MAGIC Process to extract the Diag Codes
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC BCBSAClmDiagExtr
# MAGIC EyeMedClmExtr 
# MAGIC EyeMedMAClmDiagExtr
# MAGIC MAInboundClmDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
InFile_F = get_widget_value('InFile_F','')

schema_MAInboundClm = StructType([
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
    StructField("USER_DEFN_AMT_1", DecimalType(38,10), True),
    StructField("USER_DEFN_AMT_2", DecimalType(38,10), True)
])

df_MAClm = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_MAInboundClm)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_Xfm_Diag_cd = (
    df_MAClm
    .filter(F.col("REC_TYPE") == "CLAIM LINE")
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
        F.col("DIAG_CD_TYP_CD").alias("ICD_CD_SET"),
        F.substring(F.col("DIAG_CD_1"), 1, 4).alias("DIAG_CD_1"),
        F.substring(F.col("DIAG_CD_2"), 1, 4).alias("DIAG_CD_2"),
        F.substring(F.col("DIAG_CD_3"), 1, 4).alias("DIAG_CD_3"),
        F.lit("1").alias("DIAGNO1"),
        F.lit("2").alias("DIAGNO2"),
        F.lit("3").alias("DIAGNO3")
    )
)

df_DiagCd_Pivot = (
    df_Xfm_Diag_cd
    .select(
        F.col("CLM_ID"),
        F.col("CLM_LN_NO"),
        F.col("ICD_CD_SET"),
        F.expr("stack(3, DIAG_CD_1, DIAGNO1, DIAG_CD_2, DIAGNO2, DIAG_CD_3, DIAGNO3) as (DIAG_CD, DIAG_NO)")
    )
)

df_DiagCds = (
    df_DiagCd_Pivot
    .filter(F.col("DIAG_CD").isNotNull() & (F.length(trim(F.col("DIAG_CD"))) > 0))
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("DIAG_NO").alias("DIAG_NO"),
        F.col("ICD_CD_SET").alias("ICD_VRSN_CD"),
        F.lit("NA").alias("DIAG_POA_IN")
    )
)

df_SortDiagCds1 = df_DiagCds.sort(
    F.col("CLM_ID").asc(),
    F.col("CLM_LN_NO").desc(),
    F.col("DIAG_NO").desc()
)

df_hf_mainbound_clm_diagcd_dedupe = dedup_sort(
    df_SortDiagCds1,
    partition_cols=["CLM_ID","DIAG_CD"],
    sort_cols=[("CLM_LN_NO","D"),("DIAG_NO","D")]
)

df_SortDiagCds2 = df_hf_mainbound_clm_diagcd_dedupe.sort(
    F.col("CLM_ID").asc(),
    F.col("CLM_LN_NO").asc(),
    F.col("DIAG_NO").asc()
)

windowSpec = Window.partitionBy("CLM_ID").orderBy("CLM_LN_NO","DIAG_NO")

df_BusinessRules = (
    df_SortDiagCds2
    .withColumn("svOrdnlNo", F.row_number().over(windowSpec))
    .withColumn("ClmId", F.col("CLM_ID"))
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.lit("1")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_DIAG_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.col("svOrdnlNo").alias("CLM_DIAG_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        Change(F.col("DIAG_CD"), '.', '').alias("DIAG_CD"),
        F.lit("NA").alias("CLM_DIAG_POA_CD"),
        F.when(F.col("ICD_VRSN_CD") == '9', 'ICD9')
         .when(F.col("ICD_VRSN_CD") == '0', 'ICD10')
         .otherwise('UNK')
         .alias("DIAG_CD_TYP_CD")
    )
)

df_Snapshot_AllCol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    F.col("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)

df_Snapshot_Snapshot = df_BusinessRules.select(
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD")
)

df_Snapshot_Transform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_ClmDiagPK = ClmDiagPK(df_Snapshot_AllCol, df_Snapshot_Transform, params)

df_MAInboundClmDiag = df_ClmDiagPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("CLM_DIAG_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    F.rpad(F.col("CLM_DIAG_POA_CD"), 2, " ").alias("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)

write_files(
    df_MAInboundClmDiag,
    f"{adls_path}/key/{SrcSysCd}ClmDiagExtr.{SrcSysCd}ClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Transformer = (
    df_Snapshot_Snapshot
    .withColumn(
        "CLM_DIAG_ORDNL_CD",
        GetFkeyCodes(F.lit(SrcSysCd), F.lit(0), F.lit("DIAGNOSIS ORDINAL"), F.col("CLM_DIAG_ORDNL_CD"), F.lit("X"))
    )
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("CLM_DIAG_ORDNL_CD")
    )
)

df_B_CLM_DIAG_out = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD")
)

write_files(
    df_B_CLM_DIAG_out,
    f"{adls_path}/load/B_CLM_DIAG.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)