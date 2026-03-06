# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    OPTUMRXACADrugInvoiceUpdateSeq 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from OPTUMRX ACA Invoice file to a landing file used by all other invoices processes Member information is applied to records.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                      Date                 Prjoect / TTR                                                                           Change Description                                                                                     Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------                 --------------------      -----------------------                                                             -----------------------------------------------------------------------------------------------------                             --------------------------------                   -----------------------------     ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs             Initial Development                                                                                                    IntegrateDev2                            Reddy Sanam            12/10/2020

# MAGIC Used in IDS, EDW, and Web DM update jobs
# MAGIC OPTUMRX Invoice Landing
# MAGIC This OPTUMRX Claim Invoice Landing job is for the Invoice process only.    The daily and the Bi Monthly (ie the Invoice file) are not similarily structured.   Daily file has more fields than Invoice file.
# MAGIC Very Important - this landing program will assign the Claim Id and the amount fields based on the claim status.  if an adjustment claim, the claimR will be given as the ClaimID and the amount fields are negated.   Some fields are Absolute(OPTUMRX.Field) in order to stay true to Paid / Adjust claim calulations.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, DecimalType, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_OPTUMRX = StructType([
    StructField("INVC_NO", DecimalType(38,10), True),
    StructField("INVC_DT", DecimalType(38,10), True),
    StructField("BILL_ENTY_ID", StringType(), True),
    StructField("BILL_ENTY_NM", StringType(), True),
    StructField("CAR_ID", StringType(), True),
    StructField("CAR_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True),
    StructField("ACCT_NM", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("RXCLM_NO", DecimalType(38,10), True),
    StructField("CLM_SEQ_NO", DecimalType(38,10), True),
    StructField("CLM_STTUS", StringType(), True),
    StructField("CLM_CLS_SUB_CD", StringType(), True),
    StructField("CLM_SUBMSN_TYP", StringType(), True),
    StructField("TAX_CD", StringType(), True),
    StructField("NET_CLM_CT", DecimalType(38,10), True),
    StructField("PDX_NCPDP_ID", DecimalType(38,10), True),
    StructField("PDX_NPI_ID", DecimalType(38,10), True),
    StructField("SUBMT_PDX_ID", DecimalType(38,10), True),
    StructField("RX_NO", StringType(), True),
    StructField("DT_RX_WRTN", DecimalType(38,10), True),
    StructField("MNL_DT_SUBMSN", DecimalType(38,10), True),
    StructField("FILL_DT", DecimalType(38,10), True),
    StructField("SUBMT_DT", DecimalType(38,10), True),
    StructField("RFL_CD", StringType(), True),
    StructField("RFL_STTUS", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("CARDHLDR_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("SEX", StringType(), True),
    StructField("BRTH_DT", DecimalType(38,10), True),
    StructField("MBR_CLNT_RIDER_CD", StringType(), True),
    StructField("MBR_DUR_KEY", StringType(), True),
    StructField("CARE_FCLTY_ID", StringType(), True),
    StructField("CARE_NTWK_ID", StringType(), True),
    StructField("CARE_QLFR_ID", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("REP_NDC", StringType(), True),
    StructField("GNRC_PROD_IN_NO", StringType(), True),
    StructField("DRUG_NM", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("CMPND_IN", StringType(), True),
    StructField("GNRC_OVRD_IN", StringType(), True),
    StructField("PROD_MULTI_SRC_IN", StringType(), True),
    StructField("GNRC_IN", StringType(), True),
    StructField("MAIL_ORDER_IN", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("DRUG_MNFCTR_ID", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("PRSCRBR_FIRST_NM", StringType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("QTY_DISPNS", DecimalType(38,10), True),
    StructField("DAYS_SUPL", DecimalType(38,10), True),
    StructField("INGRS_CST", DecimalType(38,10), True),
    StructField("DISPENSE_FEE", DecimalType(38,10), True),
    StructField("SLS_TAX", DecimalType(38,10), True),
    StructField("PATN_CST", DecimalType(38,10), True),
    StructField("PLN_CST", DecimalType(38,10), True),
    StructField("TOT_CST", DecimalType(38,10), True),
    StructField("BILL_CLM_CST", DecimalType(38,10), True),
    StructField("CLM_ADM_FEE", DecimalType(38,10), True),
    StructField("BILL_CLSIFIER_CD_DESC", StringType(), True),
    StructField("REJ_CD", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("PDX_CITY", StringType(), True),
    StructField("PDX_ST", StringType(), True),
    StructField("PDX_ZIP_CD", StringType(), True),
    StructField("PDX_NTWK_PRTY", StringType(), True),
    StructField("PDX_NTWK_ID", StringType(), True),
    StructField("SUPER_NTWK_ID", StringType(), True),
    StructField("NO_BILL_IN", StringType(), True),
    StructField("CLM_SK", DecimalType(38,10), True),
    StructField("AWP_AMT", DecimalType(38,10), True),
    StructField("LICS_AMT", DecimalType(38,10), True),
    StructField("CGAP_AMT", DecimalType(38,10), True),
    StructField("MI_HICA_TAX_IN", StringType(), True),
    StructField("MI_HICA_TAX_AMT", DecimalType(38,10), True),
    StructField("HLTH_PLN_AMT", DecimalType(38,10), True),
    StructField("MCARE_PLN_TYP", StringType(), True),
    StructField("DUAL_EGWP_AMT", DecimalType(38,10), True)
])

df_optumrx = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_OPTUMRX)
    .load(f"{adls_path}/verified/OPTUMRX_DrugInvoice.dat.{RunID}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = (
    f"SELECT SUB.SUB_ID||MBR.MBR_SFX_NO as MEMBERID, "
    f"MBR_UNIQ_KEY, MBR_SFX_NO, SUB_ID, GRP_ID, SUB_UNIQ_KEY "
    f"FROM {IDSOwner}.MBR MBR, {IDSOwner}.SUB SUB, {IDSOwner}.GRP GRP "
    f"WHERE MBR.SUB_SK = SUB.SUB_SK "
    f"And SUB.GRP_SK = GRP.GRP_SK"
)
df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

df_trans1 = df_ids.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MEMBERID").alias("MEMBER_ID")
)

df_mbr_ck_lkup = df_trans1.dropDuplicates(["GRP_ID","MEMBER_ID"])

df_businesslogic = (
    df_optumrx.alias("OPTUMRX")
    .join(
        df_mbr_ck_lkup.alias("find_mbr_ck_lkup"),
        (
            (F.col("OPTUMRX.PRSN_CD") == F.col("find_mbr_ck_lkup.MBR_SFX_NO"))
            & (F.col("OPTUMRX.CARDHLDR_ID") == F.col("find_mbr_ck_lkup.SUB_ID"))
            & (trim(F.col("OPTUMRX.ACCT_ID")) == F.col("find_mbr_ck_lkup.GRP_ID"))
            & (ereplace(F.col("OPTUMRX.MBR_ID"), " ", "") == F.col("find_mbr_ck_lkup.MEMBER_ID"))
        ),
        how="left"
    )
    .withColumn(
        "svAdjustedClaim",
        F.when(trim(F.col("OPTUMRX.CLM_STTUS")) == F.lit("X"), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "svCLMID",
        F.when(
            F.col("svAdjustedClaim") == True,
            F.concat_ws("", F.col("OPTUMRX.RXCLM_NO"), F.col("OPTUMRX.CLM_SEQ_NO"), F.lit("R"))
        ).otherwise(
            F.concat_ws("", F.col("OPTUMRX.RXCLM_NO"), F.col("OPTUMRX.CLM_SEQ_NO"))
        )
    )
    .withColumn(
        "svMbrCk",
        F.when(
            F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)
        ).otherwise(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY"))
    )
    .withColumn(
        "svGrpID",
        F.when(
            F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY").isNotNull(),
            F.col("find_mbr_ck_lkup.GRP_ID")
        ).otherwise(F.col("OPTUMRX.GRP_ID"))
    )
    .withColumn(
        "svFillDate",
        F.when(
            F.length(F.col("OPTUMRX.FILL_DT")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            FORMAT.DATE(F.col("OPTUMRX.FILL_DT"), "CHAR", "CCYYMMDD", "CCYY-MM-DD")
        )
    )
    .withColumn(
        "svOPTUMRXBillDate",
        F.when(
            F.length(F.col("OPTUMRX.INVC_DT")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            FORMAT.DATE(F.col("OPTUMRX.INVC_DT"), "CHAR", "CCYYMMDD", "CCYY-MM-DD")
        )
    )
    .withColumn(
        "svINVDT",
        Field(F.col("svOPTUMRXBillDate"), F.lit("-"), F.lit(3))
    )
    .withColumn(
        "svINVCDT1",
        F.when(
            F.col("svINVDT") == F.lit(16),
            OConv(
                Iconv(F.col("OPTUMRX.INVC_DT"), "D") - F.lit(1),
                "D-YMD[4,2,2]"
            )
        ).otherwise(
            FIND.DATE(F.col("svOPTUMRXBillDate"), F.lit(-1), "M", "L", "CCYY-MM-DD")
        )
    )
    .withColumn("CLM_ID", F.col("svCLMID"))
    .withColumn("INVC_DT", F.col("svINVCDT1"))
    .withColumn("TOT_CST", F.col("OPTUMRX.TOT_CST"))
    .withColumn("CLM_ADM_FEE", F.col("OPTUMRX.CLM_ADM_FEE"))
    .withColumn("BILL_CLM_CST", F.col("OPTUMRX.BILL_CLM_CST"))
    .withColumn("MBR_ID", F.col("svMbrCk"))
    .withColumn("GRP_ID", F.col("svGrpID"))
    .withColumn("DT_FILLED", F.col("svFillDate"))
    .withColumn("CLM_STTUS", F.col("OPTUMRX.CLM_STTUS"))
    .withColumn("GRP_NM", F.col("OPTUMRX.GRP_NM"))
    .withColumn("ACCT_ID", F.col("OPTUMRX.ACCT_ID"))
)

df_final = df_businesslogic.select(
    rpad(F.col("CLM_ID"), 20, " ").alias("CLM_ID"),
    F.col("INVC_DT").alias("INVC_DT"),
    F.col("TOT_CST").alias("TOT_CST"),
    F.col("CLM_ADM_FEE").alias("CLM_ADM_FEE"),
    F.col("BILL_CLM_CST").alias("BILL_CLM_CST"),
    rpad(F.col("MBR_ID"), 20, " ").alias("MBR_ID"),
    rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
    rpad(F.col("DT_FILLED"), 10, " ").alias("DT_FILLED"),
    rpad(F.col("CLM_STTUS"), 8, " ").alias("CLM_STTUS"),
    rpad(F.col("GRP_NM"), 30, " ").alias("GRP_NM"),
    rpad(F.col("ACCT_ID"), 15, " ").alias("ACCT_ID")
)

write_files(
    df_final,
    f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)