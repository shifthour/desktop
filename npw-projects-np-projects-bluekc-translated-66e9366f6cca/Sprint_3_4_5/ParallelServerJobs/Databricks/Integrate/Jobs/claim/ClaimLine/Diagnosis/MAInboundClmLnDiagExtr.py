# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmLnExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Diagonis Claim Line Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Lokesh K                          US 404552                                 Initial programming                                           IntegrateDev2              Jeyaprasanna            2022-02-06   
# MAGIC 
# MAGIC 2024-02-02       Arpitha                              US 599810                          Added logic to read 4 characters for DIAG_CD      IntegrateDev1              Jeyaprasanna            2024-02-06
# MAGIC 
# MAGIC 2024-04-04       Ediga Maruthi                    US 614444                         Added Columns DOC_NO,USER_DEFN_TX_1,       IntegrateDev1           Jeyaprasanna             2024-04-19
# MAGIC                                                                                            USER_DEFN_TX_2,USER_DEFN_DT_1,USER_DEFN_DT_2,
# MAGIC                                                                                            USER_DEFN_AMT_1,USER_DEFN_AMT_2 in MAInboundClmLnLanding.

# MAGIC Process to extract the Diag Codes
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC BCBSSCClmLnDiagExtr
# MAGIC EyeMedClmLnDiagExtr
# MAGIC EyeMedMAClmLnDiagExtr
# MAGIC MAInboundClmLnDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col, lit, when, regexp_replace, trim, length, row_number
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
InFile_F = get_widget_value("InFile_F","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrentDate = get_widget_value("CurrentDate","")

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

df_MAInboundClm = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_MAInboundClm)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_Xfm_Diag_cd = (
    df_MAInboundClm
    .filter(col("CLM_LN_SEQ_NO") != lit("0"))
    .select(
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
        col("DIAG_CD_TYP_CD").alias("ICD_CD_SET"),
        regexp_replace(col("DIAG_CD_1").substr(1,4), "\r\n|\n|\r", "").alias("DIAG_CD_1"),
        regexp_replace(col("DIAG_CD_2").substr(1,4), "\r\n|\n|\r", "").alias("DIAG_CD_2"),
        regexp_replace(col("DIAG_CD_3").substr(1,4), "\r\n|\n|\r", "").alias("DIAG_CD_3"),
        lit("1").alias("DIAGNO1"),
        lit("2").alias("DIAGNO2"),
        lit("3").alias("DIAGNO3")
    )
)

df_DiagCd_Pivot = df_Xfm_Diag_cd.selectExpr(
    "CLM_ID",
    "CLM_LN_NO",
    "ICD_CD_SET",
    "stack(3, DIAG_CD_1, DIAGNO1, DIAG_CD_2, DIAGNO2, DIAG_CD_3, DIAGNO3) as (DIAG_CD, DIAG_NO)"
).select("CLM_ID", "CLM_LN_NO", "DIAG_CD", "DIAG_NO", "ICD_CD_SET")

df_DiagCds = (
    df_DiagCd_Pivot
    .filter(col("DIAG_CD").isNotNull() & (length(trim(col("DIAG_CD"))) > 0))
    .select(
        col("CLM_ID"),
        col("CLM_LN_NO"),
        col("DIAG_CD"),
        col("DIAG_NO"),
        col("ICD_CD_SET").alias("ICD_VRSN_CD")
    )
)

df_SortDiagCds1 = df_DiagCds  # Represents the sorted stage "SortDiagCds1"

df_SortDiagCds1_dedup = df_SortDiagCds1.drop_duplicates(["CLM_ID","CLM_LN_NO","DIAG_CD"])

df_SortDiagCds2_input = df_SortDiagCds1_dedup  # This replaces the hashed file "hf_mainbound_clm_diagcd_dedupe"

df_SortDiagCds2 = df_SortDiagCds2_input.sort(col("CLM_ID"), col("CLM_LN_NO"), col("DIAG_NO"))

windowSpec = Window.partitionBy("CLM_ID","CLM_LN_NO").orderBy("DIAG_NO")

df_BusinessRules = (
    df_SortDiagCds2
    .withColumn("svOrdnlCd", row_number().over(windowSpec))
    .select(
        col("CLM_ID"),
        col("CLM_LN_NO"),
        regexp_replace(col("DIAG_CD"), "\.", "").alias("DIAG_CD"),
        col("ICD_VRSN_CD"),
        when(col("ICD_VRSN_CD") == lit("9"), lit("ICD9"))
         .when(col("ICD_VRSN_CD") == lit("0"), lit("ICD10"))
         .otherwise(lit("UNK")).alias("DIAG_CD_TYP_CD"),
    )
    .select(
        col("CLM_ID"),
        col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
        col("DIAG_CD"),
        col("DIAG_CD_TYP_CD"),
        col("svOrdnlCd").alias("CLM_LN_DIAG_ORDNL_CD")
    )
    .select(
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("DIAG_CD"),
        col("CLM_LN_DIAG_ORDNL_CD"),
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        lit("Y").alias("PASS_THRU_IN"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        (
            lit(SrcSysCd)
            .concat(lit(";"))
            .concat(col("CLM_ID"))
            .concat(lit(";"))
            .concat(col("CLM_LN_SEQ_NO"))
            .concat(lit(";1"))
        ).alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_LN_DIAG_SK"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("DIAG_CD_TYP_CD").alias("BUSRULES_DIAG_CD_TYP_CD")
    )
)

df_Snapshot = df_BusinessRules

df_SnapshotAllCol = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD").alias("DIAG_CD"),
    col("BUSRULES_DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

df_SnapshotSnapshot = df_Snapshot.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

df_SnapshotTransform = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

params_ClmLnDiagPK = {
    "DriverTable": "NA",
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrentDate,
    "FacetsDB": "NA",
    "FacetsOwner": "NA",
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "IDSOwner": IDSOwner
}

df_MAInboundClmLnDiagExtr = ClmLnDiagPK(
    df_SnapshotAllCol,
    df_SnapshotTransform,
    params_ClmLnDiagPK
)

df_MAInboundClmLnDiagExtr_final = df_MAInboundClmLnDiagExtr.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    rpad(col("FIRST_RECYC_DT"), 10, " ").alias("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 10, " ").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("DIAG_CD"), 8, " ").alias("DIAG_CD"),
    rpad(col("DIAG_CD_TYP_CD"), 10, " ").alias("DIAG_CD_TYP_CD")
)

write_files(
    df_MAInboundClmLnDiagExtr_final,
    f"{adls_path}/key/{SrcSysCd}ClmLnDiagExtr.{SrcSysCd}ClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_TransformerInput = df_SnapshotSnapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

df_Transformer = df_TransformerInput.withColumn(
    "CLM_LN_DIAG_ORDNL_CD_SK",
    GetFkeyCodes(SrcSysCd, lit(0), "DIAGNOSIS ORDINAL", col("CLM_LN_DIAG_ORDNL_CD"), "X")
).select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD_SK")
)

df_B_CLM_LN_DIAG_final = df_Transformer.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD_SK")
)

write_files(
    df_B_CLM_LN_DIAG_final,
    f"{adls_path}/load/B_CLM_LN_DIAG.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)