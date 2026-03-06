# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
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
# MAGIC Ramu                      2019-10-23       6131- PBM Replacement      Originally Programmed                                                                         IntegrateDev2                               Kalyan Neelam         2019-11-27  
# MAGIC Peter Gichiri            2020-01-21      6131- PBM Replacement    Changed INVC_DT logic to -  IF svINVDT=16 THEN OConv(Iconv(O  IntergrateDev2                              Kalyan Neelam         2020-01-22
# MAGIC                                                         PTUMRX.INVC_DT,"D")-1,"D-YMD[4,2,2]") ELSE FIND.DATE(svOPTUMRXBillDate, -1, 'M', 'L', 
# MAGIC                                                         'CCYY-MM-DD') 
# MAGIC Peter Gichiri           2020-01-28       6131 - PBM Replacement    Added a filter condition "OPTUMRX.ACCT_ID<>'28549000' And        IntegrateDev2                               Kalyan Neelam         2020-01-28
# MAGIC                                                                                                     OPTUMRX.PDX_NPI_ID <>'1699120170'"   to filter out BURNS & 
# MAGIC                                                                                                     MCDONNELL PHARMACY records
# MAGIC 
# MAGIC Peter Gichiri           2020-01-29       6131 - PBM Replacement   Revised the  filter condition to output all the records that do not meet 
# MAGIC                                                                                                    the Filter condition  "OPTUMRX.ACCT_ID=28549000' And                  IntegrateDev2                               Kalyan Neelam         2020-01-29
# MAGIC                                                                                                    OPTUMRX.PDX_NPI_ID =1699120170'"  to the link MbrVerified
# MAGIC 
# MAGIC Velmani Kondappan    2020-04-02   6131- PBM Replacement           Removed the errror warnings coming from Lkup_Fkey lookup       IntegrateDev2                                                                    
# MAGIC 
# MAGIC Velmani Kondappan    2020-04-22   6131- PBM Replacement           Removed the Filter condition for BURNS &                                    IntegrateDev2                              Kalyan Neelam         2020-04-23                                  
# MAGIC                                                                                                            MCDONNELL PHARMACY

# MAGIC Used in IDS, EDW, and Web DM update jobs
# MAGIC OPTUMRX Invoice Landing
# MAGIC This OPTUMRX Claim Invoice Landing job is for the Invoice process only.    The daily and the Bi Monthly (ie the Invoice file) are not similarily structured.   Daily file has more fields than Invoice file.
# MAGIC Very Important - this landing program will assign the Claim Id and the amount fields based on the claim status.  if an adjustment claim, the claimR will be given as the ClaimID and the amount fields are negated.   Some fields are Absolute(OPTUMRX.Field) in order to stay true to Paid / Adjust claim calulations.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','10')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read from OPTUMRX_DrugInvoice (CSeqFileStage)
schema_OPTUMRX_DrugInvoice = StructType([
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

df_OPTUMRX_DrugInvoice = (
    spark.read.format("csv")
    .schema(schema_OPTUMRX_DrugInvoice)
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(f"{adls_path}/verified/OPTUMRX_DrugInvoice.dat.{RunID}")
)

# Read from IDS (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT 
 SUB.SUB_ID||MBR.MBR_SFX_NO as MEMBERID,
 MBR_UNIQ_KEY,
 MBR_SFX_NO,
 SUB_ID,
 GRP_ID,
 SUB_UNIQ_KEY
FROM
 {IDSOwner}.MBR MBR,
 {IDSOwner}.SUB SUB,
 {IDSOwner}.GRP GRP
WHERE
 MBR.SUB_SK = SUB.SUB_SK
 AND SUB.GRP_SK = GRP.GRP_SK
"""

df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# Trans1 (CTransformerStage)
df_Trans1 = df_ids.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_SFX_NO"),
    F.col("SUB_ID"),
    F.col("GRP_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("MEMBERID").alias("MEMBER_ID")
)

# hf_optum_clm_invc_land (CHashedFileStage) - Scenario A -> Deduplicate on keys GRP_ID, MEMBER_ID
df_hf_optum_clm_invc_land = dedup_sort(
    df_Trans1,
    partition_cols=["GRP_ID", "MEMBER_ID"],
    sort_cols=[]
)

# BusinessLogic (CTransformerStage) with left join on hashed-file data
df_bizlogic_joined = df_OPTUMRX_DrugInvoice.alias("OPTUMRX").join(
    df_hf_optum_clm_invc_land.alias("find_mbr_ck_lkup"),
    on=[
        F.col("OPTUMRX.PRSN_CD") == F.col("find_mbr_ck_lkup.MBR_SFX_NO"),
        F.col("OPTUMRX.CARDHLDR_ID") == F.col("find_mbr_ck_lkup.SUB_ID"),
        trim(F.col("OPTUMRX.ACCT_ID")) == F.col("find_mbr_ck_lkup.GRP_ID"),
        eReplace(F.col("OPTUMRX.MBR_ID"), ' ', '') == F.col("find_mbr_ck_lkup.MEMBER_ID")
    ],
    how="left"
)

df_bizlogic = (
    df_bizlogic_joined
    .withColumn(
        "svAdjustedClaim",
        F.when(trim(F.col("OPTUMRX.CLM_STTUS")) == F.lit("X"), F.lit(True)).otherwise(F.lit(False))
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
        F.when(F.isnull(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY")), F.lit(0))
         .otherwise(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY"))
    )
    .withColumn(
        "svGrpID",
        F.when(
            F.isnull(F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY")) == False,
            F.col("find_mbr_ck_lkup.GRP_ID")
        ).otherwise(
            F.col("OPTUMRX.GRP_ID")
        )
    )
    .withColumn(
        "svFillDate",
        F.when(
            F.length(F.col("OPTUMRX.FILL_DT")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            FORMAT_DATE(
                F.col("OPTUMRX.FILL_DT"),
                F.lit("CHAR"),
                F.lit("CCYYMMDD"),
                F.lit("CCYY-MM-DD")
            )
        )
    )
    .withColumn(
        "svOPTUMRXBillDate",
        F.when(
            F.length(F.col("OPTUMRX.FILL_DT")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            FORMAT_DATE(
                F.col("OPTUMRX.INVC_DT"),
                F.lit("CHAR"),
                F.lit("CCYYMMDD"),
                F.lit("CCYY-MM-DD")
            )
        )
    )
    .withColumn(
        "svINVDT",
        Field(F.col("svOPTUMRXBillDate"), F.lit("-"), F.lit(3))
    )
    .withColumn(
        "svINVCDT1",
        F.when(
            F.col("svINVDT") == F.lit("16"),
            OConv(
                Iconv(F.col("OPTUMRX.INVC_DT"), F.lit("D")) - F.lit(1),
                F.lit("D-YMD[4,2,2]")
            )
        ).otherwise(
            FIND_DATE(
                F.col("svOPTUMRXBillDate"),
                F.lit(-1),
                F.lit("M"),
                F.lit("L"),
                F.lit("CCYY-MM-DD")
            )
        )
    )
)

df_MbrVerified = df_bizlogic.select(
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

# Apply rpad for final "char"/"varchar" columns per DataStage definitions
df_MbrVerified_rpad = (
    df_MbrVerified
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 20, " "))
    .withColumn("INVC_DT", F.rpad(F.col("INVC_DT"), 10, " "))
    .withColumn("MBR_ID", F.rpad(F.col("MBR_ID"), 20, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("DT_FILLED", F.rpad(F.col("DT_FILLED"), 10, " "))
    .withColumn("CLM_STTUS", F.rpad(F.col("CLM_STTUS"), 8, " "))
    .withColumn("GRP_NM", F.rpad(F.col("GRP_NM"), 30, " "))
    .withColumn("ACCT_ID", F.rpad(F.col("ACCT_ID"), 15, " "))
)

df_MbrVerified_final = df_MbrVerified_rpad.select(
    "CLM_ID",
    "INVC_DT",
    "TOT_CST",
    "CLM_ADM_FEE",
    "BILL_CLM_CST",
    "MBR_ID",
    "GRP_ID",
    "DT_FILLED",
    "CLM_STTUS",
    "GRP_NM",
    "ACCT_ID"
)

# Write to OPTUMRXDrugClmInvoicePaidUpdt.dat.#RunID#
write_files(
    df_MbrVerified_final,
    f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)