# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim COB Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Manisha G                          US 404552                                 Initial programming                                       IntegrateDev2                   Jeyaprasanna          2022-02-06
# MAGIC 
# MAGIC 2024-04-04       Ediga Maruthi                      US 614444                       Added Columns DOC_NO,USER_DEFN_TX_1,    IntegrateDev1              Jeyaprasanna             2024-04-19
# MAGIC                                                                                                               USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                                               USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                                               USER_DEFN_AMT_2 in MAInboundClm.

# MAGIC Read the MAInbound Reversal Format  file created from MAInboundClmExtrRvrsl
# MAGIC Writing Sequential File to /key
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmCobPK
# COMMAND ----------

RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile_F = get_widget_value('InFile_F','')

schema_MAInboundClm = StructType([
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

df_MAInboundClm = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .schema(schema_MAInboundClm)
    .option("quote", "\"")
    .csv(f"{adls_path}/verified/{InFile_F}")
)

df_filtered = df_MAInboundClm.filter(F.col("REC_TYPE") == 'CLAIM LINE')

df_MA_hf = df_filtered.select(
    F.col("CLM_ID").alias("CLAIM_ID")
)

df_pd_amt = df_filtered.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_COB_PD_AMT").alias("PD_AMT")
)

df_MA_hf_dedup = df_MA_hf.drop_duplicates(["CLAIM_ID"])
df_MA = df_MA_hf_dedup

df_agg = df_pd_amt.groupBy("CLM_ID").agg(F.sum("PD_AMT").alias("PD_AMT"))
df_agg_dedup = df_agg.drop_duplicates(["CLM_ID"])
df_pdamtlkp = df_agg_dedup

df_joined = df_MA.alias("MA").join(
    df_pdamtlkp.alias("Pdamtlkp"),
    F.col("MA.CLAIM_ID") == F.col("Pdamtlkp.CLM_ID"),
    how="left"
)

df_joined_filtered = df_joined.filter(F.col("Pdamtlkp.PD_AMT") != F.lit("0.00"))

df_Trans = df_joined_filtered.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    (F.lit(SrcSysCd) + F.lit(";") + F.col("MA.CLAIM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_COB_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("MA.CLAIM_ID").alias("CLM_ID"),
    F.lit("NA").alias("CLM_COB_TYP_CD"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("NA").alias("CLM_COB_LIAB_TYP_CD"),
    F.lit("0.00").alias("ALW_AMT"),
    F.lit("0.00").alias("COPAY_AMT"),
    F.lit("0.00").alias("DEDCT_AMT"),
    F.lit("0.00").alias("DSALW_AMT"),
    F.lit("0.00").alias("MED_COINS_AMT"),
    F.lit("0.00").alias("MNTL_HLTH_COINS_AMT"),
    F.col("Pdamtlkp.PD_AMT").alias("PD_AMT"),
    F.lit("0.00").alias("SANC_AMT"),
    F.lit(None).alias("COB_CAR_RSN_CD_TX"),
    F.lit(None).alias("COB_CAR_RSN_TX")
)

df_RowCount = df_Trans.select(
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.col("ALW_AMT"),
    F.col("PD_AMT")
)

df_AllCol = df_Trans.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_COB_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("CLM_COB_LIAB_TYP_CD"),
    F.col("ALW_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT"),
    F.col("PD_AMT"),
    F.col("SANC_AMT"),
    F.col("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX")
)

df_Transform = df_Trans.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD")
)

df_stageVars = df_RowCount.withColumn(
    "ITSHost",
    F.when(F.substring(F.col("CLM_COB_TYP_CD"), 1, 1) == '*', F.lit('Y')).otherwise(F.lit('N'))
).withColumn(
    "ITSCOBType",
    F.when(F.col("ITSHost") == 'Y', F.substring(F.col("CLM_COB_TYP_CD"), 2, 1)).otherwise(F.lit('NA'))
).withColumn(
    "svClmCobTypCdSk",
    F.when(
        F.col("ITSHost") == 'N',
        GetFkeyCodes(F.lit("FACETS"), F.lit(0), F.lit("CLAIM COB"), F.col("CLM_COB_TYP_CD"), F.lit("X"))
    ).otherwise(
        GetFkeyCodes(F.lit("FIT"), F.lit(0), F.lit("CLAIM COB"), F.col("ITSCOBType"), F.lit("X"))
    )
)

df_Load = df_stageVars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

write_files(
    df_Load,
    f"{adls_path}/load/B_CLM_COB.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmCobPK = {
    "CurrRunCycle": RunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "CurrentDate": CurrentDate,
    "IDSOwner": IDSOwner
}

df_key = ClmCobPK(df_AllCol, df_Transform, params_ClmCobPK)

df_key_final = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_COB_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("CLM_COB_LIAB_TYP_CD"), 20, " ").alias("CLM_COB_LIAB_TYP_CD"),
    F.col("ALW_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT"),
    F.col("PD_AMT"),
    F.col("SANC_AMT"),
    F.col("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX")
)

write_files(
    df_key_final,
    f"{adls_path}/key/{SrcSysCd}ClmCOBExtr.{SrcSysCd}ClmCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)