# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     OpsDashboardClmInvtryCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restart, no other steps necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC ADasarathy      07/20/2015     5407             Original program                                                             Kalyan Neelam    2015-07-20  
# MAGIC ADasarathy      07/21/2015     5407\(9)     Added Sort and Dedupe stage                                       Kalyan Neelam    2015-07-21
# MAGIC 
# MAGIC Jag Yelavarthi  09/02/2015     5407            File format changes are applied                                      Kalyan Neelam    2015-09-04

# MAGIC Reads in source file from ImageNow, cleanse the data and then apply lookups to derive codes as needed
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmInvtryPkey
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, trim, rpad, length
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','1245')
CurrDate = get_widget_value('CurrDate','2015-07-23')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: IMAGENOWLandingFile
schema_IMAGENOWLandingFile = StructType([
    StructField("QUEUE_NM", StringType(), False),
    StructField("BLUE_KC_DOC_ID", StringType(), False),
    StructField("CLM_WORK_TYP", StringType(), False),
    StructField("PHP_OCR_TYP", StringType(), True),
    StructField("PDX_CLM_TOT", IntegerType(), True),
    StructField("USER_NM", StringType(), False),
    StructField("RCVD_DT", StringType(), True),
    StructField("SCANNED_DT", StringType(), True),
    StructField("QUEUE_STRT_TM", StringType(), True)
])

df_IMAGENOWLandingFile = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IMAGENOWLandingFile)
    .csv(f"{adls_path_raw}/landing/LEXMARK.CLM.WKFLW.csv")
)

# Stage: xfrmStripFields
# Create intermediate columns for stage variables, then final output columns
df_xfrmStripFields_stagevars = (
    df_IMAGENOWLandingFile
    .withColumn("svRemoveSprcialCharsQueue", STRIP_SPECIAL(col("QUEUE_NM")))
    .withColumn("svTrimSpaceQueue", Ereplace(col("svRemoveSprcialCharsQueue"), " ", ""))
    .withColumn(
        "svRemoveSprcialCharsWorkTyp",
        when(
            col("CLM_WORK_TYP").isNull() | (trim(col("CLM_WORK_TYP")) == ""),
            lit("NA")
        ).otherwise(STRIP_SPECIAL(col("CLM_WORK_TYP")))
    )
    .withColumn("svTrimSpaceWorkTyp", Ereplace(col("svRemoveSprcialCharsWorkTyp"), " ", ""))
)

df_xfrmStripFields = df_xfrmStripFields_stagevars.select(
    col("QUEUE_NM").alias("QUEUE_NM"),
    col("BLUE_KC_DOC_ID").alias("DOC_ID"),
    col("USER_NM").alias("USER_NM"),
    when(
        col("QUEUE_STRT_TM").isNull() | (trim(col("QUEUE_STRT_TM")) == ""),
        lit("1753-01-01")
    ).otherwise(
        FORMAT_DATE(col("QUEUE_STRT_TM"), lit("DB2"), lit("TIMESTAMP"), lit("SYBTIMESTAMP"))
    ).alias("QUEUE_STRT_TM"),
    when(
        col("RCVD_DT").isNull() | (trim(col("RCVD_DT")) == ""),
        lit("1753-01-01")
    ).otherwise(
        FORMAT_DATE(col("RCVD_DT"), lit("DB2"), lit("TIMESTAMP"), lit("SYBTIMESTAMP"))
    ).alias("RECEIVED_DT"),
    when(
        col("SCANNED_DT").isNull() | (trim(col("SCANNED_DT")) == ""),
        lit("1753-01-01")
    ).otherwise(
        FORMAT_DATE(col("SCANNED_DT"), lit("DB2"), lit("TIMESTAMP"), lit("SYBTIMESTAMP"))
    ).alias("SCANNED_DT"),
    col("svTrimSpaceQueue").alias("DRVD_QUEUE_NM"),
    col("svTrimSpaceWorkTyp").alias("WORK_TYP"),
    when(
        (col("PHP_OCR_TYP") == "FBKCI") | (col("PHP_OCR_TYP") == "HBKCI") |
        (col("PHP_OCR_TYP") == "FBKCP") | (col("PHP_OCR_TYP") == "HBKCP") |
        (col("PHP_OCR_TYP") == "BKCD"),
        col("PHP_OCR_TYP")
    ).otherwise(lit("NA")).alias("CLM_SUB_TYP"),
    when(
        col("PDX_CLM_TOT").isNull() | (trim(col("PDX_CLM_TOT").cast("string")) == ""),
        lit(1)
    ).otherwise(col("PDX_CLM_TOT")).alias("WORK_ITEM_CT")
)

# Stage: P_SRC_DOMAIN_TRNSLTN (DB2Connector - IDS)
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_src_domain = (
    "SELECT SRC_DOMAIN_TX as SRC_DOMAIN_TX,TRGT_DOMAIN_TX as TRGT_DOMAIN_TX "
    f"FROM {IDSOwner}.P_SRC_DOMAIN_TRNSLTN "
    "WHERE SRC_SYS_CD = 'IMAGENOW' AND DOMAIN_ID = 'CLMINVPENDCAT'"
)
df_P_SRC_DOMAIN_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_src_domain)
    .load()
)

# Stage: hf_imgnw_clm_invtry_sdt (Scenario A - intermediate hashed file)
# Key columns: SRC_DOMAIN_TX
df_hf_imgnw_clm_invtry_sdt = dedup_sort(
    df_P_SRC_DOMAIN_TRNSLTN,
    ["SRC_DOMAIN_TX"],
    [("SRC_DOMAIN_TX","A")]
)

# Stage: APP_USER (DB2Connector - IDS)
extract_query_app_user = (
    "SELECT ACTV_DIR_ACCT_NM,USER_SK,USER_ID "
    f"FROM {IDSOwner}.APP_USER WHERE ACTV_DIR_ACCT_NM IS NOT NULL"
)
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_app_user)
    .load()
)

# Stage: hf_imgnw_actvdirnm (Scenario A - intermediate hashed file)
# Key columns: ACTV_DIR_ACCT_NM
df_hf_imgnw_actvdirnm = dedup_sort(
    df_APP_USER,
    ["ACTV_DIR_ACCT_NM"],
    [("ACTV_DIR_ACCT_NM","A")]
)

# Stage: P_WORK_UNIT_XREF (DB2Connector - IDS)
extract_query_work_unit_xref = (
    "SELECT \n"
    "XREF.SRC_VAL_TX,\n"
    "XREF.OPS_WORK_UNIT_SK,\n"
    "OPS_WORK_UNIT.OPS_WORK_UNIT_ID\n"
    "\nFROM \n\n"
    f"{IDSOwner}.P_OPS_WORK_UNIT_XREF XREF,\n"
    f"{IDSOwner}.OPS_WORK_UNIT OPS_WORK_UNIT\n\n"
    "WHERE \n\n"
    "XREF.SRC_SYS_CD = 'IMAGENOW' AND XREF.OPS_WORK_UNIT_SK NOT IN (1,0)\n"
    "AND XREF.OPS_WORK_UNIT_SK = OPS_WORK_UNIT.OPS_WORK_UNIT_SK"
)
df_P_WORK_UNIT_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_work_unit_xref)
    .load()
)

# Stage: Transformer_107
df_Transformer_107 = df_P_WORK_UNIT_XREF.select(
    col("SRC_VAL_TX").alias("SRC_VAL_TX"),
    col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID")
)

# Stage: hf_imgnw_clm_invtry_powux (Scenario A)
# Key columns: SRC_VAL_TX
df_hf_imgnw_clm_invtry_powux = dedup_sort(
    df_Transformer_107,
    ["SRC_VAL_TX"],
    [("SRC_VAL_TX","A")]
)

# Stage: BusinessRules
# Left joins with df_hf_imgnw_clm_invtry_sdt, df_hf_imgnw_clm_invtry_powux, df_hf_imgnw_actvdirnm
df_BusinessRules_in = (
    df_xfrmStripFields.alias("Extract")
    .join(
        df_hf_imgnw_clm_invtry_sdt.alias("src_dmn_tx_lkup"),
        col("Extract.DRVD_QUEUE_NM") == col("src_dmn_tx_lkup.SRC_DOMAIN_TX"),
        "left"
    )
    .join(
        df_hf_imgnw_clm_invtry_powux.alias("WorkUnitlkup"),
        col("Extract.WORK_TYP") == col("WorkUnitlkup.SRC_VAL_TX"),
        "left"
    )
    .join(
        df_hf_imgnw_actvdirnm.alias("user_nm_lkup"),
        col("Extract.USER_NM") == col("user_nm_lkup.ACTV_DIR_ACCT_NM"),
        "left"
    )
)

df_BusinessRules_vars = (
    df_BusinessRules_in
    .withColumn("PassThru", lit("Y"))
    .withColumn("svSrcSysCd", lit("IMAGENOW"))
    .withColumn("svClmId", col("Extract.DOC_ID"))
    .withColumn("svSttusDt", FORMAT_DATE(col("Extract.QUEUE_STRT_TM"), lit("DB2"), lit("TIMESTAMP"), lit("CCYY-MM-DD")))
    .withColumn("svRcvdDt", FORMAT_DATE(col("Extract.RECEIVED_DT"), lit("DB2"), lit("TIMESTAMP"), lit("CCYY-MM-DD")))
    .withColumn("svInptDt", FORMAT_DATE(col("Extract.SCANNED_DT"), lit("DB2"), lit("TIMESTAMP"), lit("CCYY-MM-DD")))
)

df_BusinessRules = df_BusinessRules_vars.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("PassThru").alias("PASS_THRU_IN"),
    lit("CurrDate").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    (col("svSrcSysCd") + lit(";") + col("svClmId")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_INVTRY_SK"),
    col("svClmId").alias("CLM_INVTRY_KEY_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("NA").alias("GRP_ID"),
    when(
        col("WorkUnitlkup.SRC_VAL_TX").isNull(),
        lit("NA")
    ).otherwise(col("WorkUnitlkup.OPS_WORK_UNIT_ID")).alias("OPS_WORK_UNIT_ID"),
    lit("NA").alias("PDPD_ID"),
    lit("NA").alias("PRPR_ID"),
    when(
        col("src_dmn_tx_lkup.SRC_DOMAIN_TX").isNull(),
        lit("NA")
    ).otherwise(col("src_dmn_tx_lkup.TRGT_DOMAIN_TX")).alias("CLM_INVTRY_PEND_CAT_CD"),
    lit("NA").alias("CLM_STTUS_CHG_RSN_CD"),
    lit("NA").alias("CLST_STS"),
    col("Extract.CLM_SUB_TYP").alias("CLCL_CL_SUB_TYPE"),
    when(
        col("src_dmn_tx_lkup.SRC_DOMAIN_TX").isNull(),
        lit("NA")
    ).otherwise(col("src_dmn_tx_lkup.TRGT_DOMAIN_TX")).alias("CLCL_CL_TYPE"),
    col("svInptDt").alias("INPT_DT_SK"),
    col("svRcvdDt").alias("RCVD_DT_SK"),
    lit("CurrDate").alias("EXTR_DT_SK"),
    col("svSttusDt").alias("STTUS_DT_SK"),
    lit(1).alias("INVTRY_CT"),
    when(
        col("user_nm_lkup.ACTV_DIR_ACCT_NM").isNull(),
        lit(0)
    ).otherwise(col("user_nm_lkup.USER_SK")).alias("ASG_USER_SK"),
    col("Extract.WORK_ITEM_CT").alias("WORK_ITEM_CT")
)

# Stage: Key_Trans
df_Key_Trans = df_BusinessRules.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_INVTRY_SK").alias("CLM_INVTRY_SK"),
    when(
        col("CLM_INVTRY_KEY_ID").isNull() | (length(trim(col("CLM_INVTRY_KEY_ID"))) == 0),
        lit("UNK")
    ).otherwise(col("CLM_INVTRY_KEY_ID")).alias("CLM_INVTRY_KEY_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    col("PDPD_ID").alias("PDPD_ID"),
    col("PRPR_ID").alias("PRPR_ID"),
    col("CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
    col("CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
    col("CLST_STS").alias("CLST_STS"),
    col("CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE"),
    col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    when(
        col("INPT_DT_SK").isNull() | (length(trim(col("INPT_DT_SK"))) == 0),
        lit("1753-01-01")
    ).otherwise(col("INPT_DT_SK")).alias("INPT_DT_SK"),
    when(
        col("RCVD_DT_SK").isNull() | (length(trim(col("RCVD_DT_SK"))) == 0),
        lit("1753-01-01")
    ).otherwise(col("RCVD_DT_SK")).alias("RCVD_DT_SK"),
    when(
        col("EXTR_DT_SK").isNull() | (length(trim(col("EXTR_DT_SK"))) == 0),
        lit("1753-01-01")
    ).otherwise(col("EXTR_DT_SK")).alias("EXTR_DT_SK"),
    when(
        col("STTUS_DT_SK").isNull() | (length(trim(col("STTUS_DT_SK"))) == 0),
        lit("1753-01-01")
    ).otherwise(col("STTUS_DT_SK")).alias("STTUS_DT_SK"),
    col("INVTRY_CT").alias("INVTRY_CT"),
    col("ASG_USER_SK").alias("ASG_USER_SK"),
    col("WORK_ITEM_CT").alias("WORK_ITEM_CT")
)

# Stage: ClmInvtryPkey (Shared Container)
params_ClmInvtryPkey = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": "#CurrRunCycle#",
    "RunID": "#RunID#",
    "CurrDate": "#CurrDate#",
    "$FacetsDB": "#$FacetsDB#",
    "$FacetsOwner": "#$FacetsOwner#"
}
df_ClmInvtryPkey = ClmInvtryPkey(df_Key_Trans, params_ClmInvtryPkey)

# Stage: IdsClmInvtryExtr (CSeqFileStage)
# Final select with rpad for columns of type char or varchar where length is known
df_IdsClmInvtryExtr = df_ClmInvtryPkey.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_INVTRY_SK"),
    col("CLM_INVTRY_KEY_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_ID"),
    col("OPS_WORK_UNIT_ID"),
    rpad(col("PDPD_ID"), 8, " ").alias("PDPD_ID"),
    rpad(col("PRPR_ID"), 12, " ").alias("PRPR_ID"),
    col("CLM_INVTRY_PEND_CAT_CD"),
    col("CLM_STTUS_CHG_RSN_CD"),
    rpad(col("CLST_STS"), 2, " ").alias("CLST_STS"),
    rpad(col("CLCL_CL_SUB_TYPE"), 1, " ").alias("CLCL_CL_SUB_TYPE"),
    rpad(col("CLCL_CL_TYPE"), 1, " ").alias("CLCL_CL_TYPE"),
    rpad(col("INPT_DT_SK"), 10, " ").alias("INPT_DT_SK"),
    rpad(col("RCVD_DT_SK"), 10, " ").alias("RCVD_DT_SK"),
    rpad(col("EXTR_DT_SK"), 10, " ").alias("EXTR_DT_SK"),
    rpad(col("STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    col("INVTRY_CT"),
    col("ASG_USER_SK"),
    col("WORK_ITEM_CT")
)

write_files(
    df_IdsClmInvtryExtr,
    f"{adls_path}/key/ImgNwClmInvtryExtr.ClmInvtry.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)