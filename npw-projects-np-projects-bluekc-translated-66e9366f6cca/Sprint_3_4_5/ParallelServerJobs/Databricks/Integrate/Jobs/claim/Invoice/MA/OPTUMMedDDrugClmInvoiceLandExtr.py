# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    OPTUMRXDrugInvoiceUpdateSeq 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from OPTUMRX Invoice file to a landing file used by all other invoices processes Member information is applied to records.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                Date                 Prjoect / TTR               Change Description                                                                                     Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------              --------------------    -----------------------            -----------------------------------------------------------------------------------------------------                  --------------------------------                   -----------------------------     ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development                       IntegrateDev2		Abhiram Dasarathy	2020-12-11

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# Define schema for OPTUMRX_DrugInvoice (.dat) file
schema_OPTUMRX = StructType([
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_ENTY_ID", StringType(), True),
    StructField("BILL_ENTY_NM", StringType(), True),
    StructField("CAR_ID", StringType(), True),
    StructField("CAR_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True),
    StructField("ACCT_NM", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("RXCLM_NO", StringType(), True),
    StructField("CLM_SEQ_NO", StringType(), True),
    StructField("CLM_STTUS", StringType(), True),
    StructField("CLM_CLS_SUB_CD", StringType(), True),
    StructField("CLM_SUBMSN_TYP", StringType(), True),
    StructField("TAX_CD", StringType(), True),
    StructField("NET_CLM_CT", StringType(), True),
    StructField("PDX_NCPDP_ID", StringType(), True),
    StructField("PDX_NPI_ID", StringType(), True),
    StructField("SUBMT_PDX_ID", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("DT_RX_WRTN", StringType(), True),
    StructField("MNL_DT_SUBMSN", StringType(), True),
    StructField("FILL_DT", StringType(), True),
    StructField("SUBMT_DT", StringType(), True),
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
    StructField("BRTH_DT", StringType(), True),
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
    StructField("QTY_DISPNS", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("INGRS_CST", StringType(), True),
    StructField("DISPENSE_FEE", StringType(), True),
    StructField("SLS_TAX", StringType(), True),
    StructField("PATN_CST", StringType(), True),
    StructField("PLN_CST", StringType(), True),
    StructField("TOT_CST", StringType(), True),
    StructField("BILL_CLM_CST", StringType(), True),
    StructField("CLM_ADM_FEE", StringType(), True),
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
    StructField("CLM_SK", StringType(), True),
    StructField("AWP_AMT", StringType(), True),
    StructField("LICS_AMT", StringType(), True),
    StructField("CGAP_AMT", StringType(), True),
    StructField("MI_HICA_TAX_IN", StringType(), True),
    StructField("MI_HICA_TAX_AMT", StringType(), True),
    StructField("HLTH_PLN_AMT", StringType(), True),
    StructField("MCARE_PLN_TYP", StringType(), True),
    StructField("DUAL_EGWP_AMT", StringType(), True)
])

# Read input file for OPTUMRX_DrugInvoice
df_OPTUMRX_DrugInvoice = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRX)
    .csv(f"{adls_path}/verified/OPTUMRX_DrugInvoice.dat.{RunID}")
)

# Read from IDS database (DB2Connector stage)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT SUB.SUB_ID||MBR.MBR_SFX_NO as MEMBERID, "
    f"MBR_UNIQ_KEY, MBR_SFX_NO, SUB_ID, GRP_ID, SUB_UNIQ_KEY "
    f"FROM {IDSOwner}.MBR MBR, {IDSOwner}.SUB SUB, {IDSOwner}.GRP GRP "
    f"WHERE MBR.SUB_SK = SUB.SUB_SK "
    f"And SUB.GRP_SK = GRP.GRP_SK"
)
df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Trans1 stage
df_Trans1 = df_ids.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MEMBERID").alias("MEMBER_ID")
)

# Write (merge) to dummy table replacing the hashed file "hf_optum_clm_invc_land"
# Prepare a staging table
temp_table_name = "STAGING.OPTUMMedDDrugClmInvoiceLandExtr_hf_optum_clm_invc_land_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_Trans1.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql_hashed = """
MERGE INTO dummy_hf_optum_clm_invc_land AS T
USING STAGING.OPTUMMedDDrugClmInvoiceLandExtr_hf_optum_clm_invc_land_temp AS S
ON (T.GRP_ID = S.GRP_ID AND T.MEMBER_ID = S.MEMBER_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_SFX_NO = S.MBR_SFX_NO,
    T.SUB_ID = S.SUB_ID,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
WHEN NOT MATCHED THEN
  INSERT (MBR_UNIQ_KEY, MBR_SFX_NO, SUB_ID, GRP_ID, SUB_UNIQ_KEY, MEMBER_ID)
  VALUES (S.MBR_UNIQ_KEY, S.MBR_SFX_NO, S.SUB_ID, S.GRP_ID, S.SUB_UNIQ_KEY, S.MEMBER_ID);
"""
execute_dml(merge_sql_hashed, jdbc_url, jdbc_props)

# Read from dummy table (hashed file lookup)
df_hf_optum_clm_invc_land = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT MBR_UNIQ_KEY, MBR_SFX_NO, SUB_ID, GRP_ID, SUB_UNIQ_KEY, MEMBER_ID FROM dummy_hf_optum_clm_invc_land")
    .load()
)

# BusinessLogic - Transformer with a left join lookup to the dummy hashed file
df_BL_join = (
    df_OPTUMRX_DrugInvoice.alias("OPTUMRX")
    .join(
        df_hf_optum_clm_invc_land.alias("find_mbr_ck_lkup"),
        (
            (F.col("Esi.PRSN_CD") == F.col("find_mbr_ck_lkup.MBR_SFX_NO"))
            & (F.col("Esi.CARDHLDR_ID_NO") == F.col("find_mbr_ck_lkup.SUB_ID"))
            & (trim(F.col("OPTUMRX.ACCT_ID")) == F.col("find_mbr_ck_lkup.GRP_ID"))
            & (F.expr("Ereplace(OPTUMRX.MBR_ID,' ','')") == F.col("find_mbr_ck_lkup.MEMBER_ID"))
        ),
        how="left"
    )
)

# Apply stage variables and final columns
df_BL_vars = (
    df_BL_join
    .withColumn(
        "svAdjustedClaim",
        F.when(trim(F.col("OPTUMRX.CLM_STTUS")) == "X", F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "svCLMID",
        F.when(
            F.col("svAdjustedClaim") == True,
            F.concat(F.col("OPTUMRX.RXCLM_NO"), F.col("OPTUMRX.CLM_SEQ_NO"), F.lit("R"))
        ).otherwise(
            F.concat(F.col("OPTUMRX.RXCLM_NO"), F.col("OPTUMRX.CLM_SEQ_NO"))
        )
    )
    .withColumn(
        "svMbrCk",
        F.when(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY").isNull(), F.lit("0")).otherwise(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY"))
    )
    .withColumn(
        "svGrpID",
        F.when(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY").isNotNull(), F.col("find_mbr_ck_lkup.GRP_ID")).otherwise(F.col("OPTUMRX.GRP_ID"))
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
        Field(F.col("svOPTUMRXBillDate"), F.lit("-"), F.lit("3"))
    )
    .withColumn(
        "svINVCDT1",
        F.when(
            F.col("svINVDT") == "16",
            OConv(Iconv(F.col("OPTUMRX.INVC_DT"), "D") - F.lit("1"), "D-YMD[4,2,2]")
        ).otherwise(
            FIND.DATE(F.col("svOPTUMRXBillDate"), F.lit("-1"), "M", "L", "CCYY-MM-DD")
        )
    )
)

df_MbrVerified = df_BL_vars.select(
    F.col("svCLMID").alias("CLM_ID"),
    F.col("svINVCDT1").alias("INVC_DT"),
    F.col("OPTUMRX.TOT_CST").alias("TOT_CST"),
    F.col("OPTUMRX.CLM_ADM_FEE").alias("CLM_ADM_FEE"),
    F.col("OPTUMRX.BILL_CLM_CST").alias("BILL_CLM_CST"),
    F.col("svMbrCk").alias("MBR_ID"),
    F.col("svGrpID").alias("GRP_ID"),
    F.col("svFillDate").alias("DT_FILLED"),
    F.col("OPTUMRX.CLM_STTUS").alias("CLM_STTUS"),
    F.col("OPTUMRX.GRP_NM").alias("GRP_NM"),
    F.col("OPTUMRX.ACCT_ID").alias("ACCT_ID")
)

# Apply rpad for char/varchar columns in final DataFrame
df_final = (
    df_MbrVerified
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 20, " "))
    .withColumn("MBR_ID", F.rpad(F.col("MBR_ID"), 20, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("DT_FILLED", F.rpad(F.col("DT_FILLED"), 10, " "))
    .withColumn("CLM_STTUS", F.rpad(F.col("CLM_STTUS"), 8, " "))
    .withColumn("GRP_NM", F.rpad(F.col("GRP_NM"), 30, " "))
    .withColumn("ACCT_ID", F.rpad(F.col("ACCT_ID"), 15, " "))
)

# Write the final file
write_files(
    df_final,
    f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)