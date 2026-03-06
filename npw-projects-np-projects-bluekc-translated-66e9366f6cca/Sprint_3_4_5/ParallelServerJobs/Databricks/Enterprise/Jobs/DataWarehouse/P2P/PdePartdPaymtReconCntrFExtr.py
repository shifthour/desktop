# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdePartdPaymtReconCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PARTD_RECON* and loads the data into EDW Table PDE_PARTD_PAYMT_RECON_CNTR_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                 Jaideep Mankala         02/24/2022


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# ----------------------------------------------------------------
# Obtain job parameters (including database secret name per spec)
# ----------------------------------------------------------------
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
InFile = get_widget_value('InFile','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# ----------------------------------------------------------------
# 1) Read from EDW DB2ConnectorPX (Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F)
# ----------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = (
    "SELECT FILE_ID, CNTR_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, "
    "PDE_PARTD_PAYMT_RECON_CNTR_SK "
    "FROM " + EDWOwner + ".K_PDE_PARTD_PAYMT_RECON_CNTR_F "
)
df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------
# 2) Read Payable40 (PxSequentialFile) from landing
# ----------------------------------------------------------------
schema_Payable40 = StructType([
    StructField("Field", StringType(), True)
])
file_path_Payable40 = f"{adls_path_raw}/landing/{InFile}"
df_Payable40 = (
    spark.read
    .csv(
        file_path_Payable40,
        header=False,
        sep=",",
        quote='"',
        schema=schema_Payable40,
        nullValue=None
    )
)

# ----------------------------------------------------------------
# 3) xfm_RecId Transformer logic
#    Constraint: Field[1,3] in ('CHD','CTR')
#    Output columns: RECORD_ID, SEQUENCE_NO, RESTOFTHE_FIELDS, InputRowNumber
# ----------------------------------------------------------------
windowSpec_inrownum = Window.orderBy(F.monotonically_increasing_id())
df_xfm_RecId = (
    df_Payable40
    .filter(
        (F.col("Field").substr(1, 3) == F.lit("CHD")) |
        (F.col("Field").substr(1, 3) == F.lit("CTR"))
    )
    .withColumn("RECORD_ID", F.col("Field").substr(1, 3))
    .withColumn("SEQUENCE_NO", F.col("Field").substr(4, 7))
    .withColumn("RESTOFTHE_FIELDS", F.col("Field"))
    .withColumn("InputRowNumber", F.row_number().over(windowSpec_inrownum))
)

# ----------------------------------------------------------------
# 4) Sort_InRowNum (PxSort by InputRowNumber)
# ----------------------------------------------------------------
df_Sort_InRowNum = df_xfm_RecId.orderBy("InputRowNumber")

# ----------------------------------------------------------------
# 5) Xfm_CHD_CTR Transformer
#    We compute a variety of derived columns (stage variables).
#    Then produce two outputs: Lnk_CHD (RECORD_ID='CHD') and Lnk_CTR (RECORD_ID='CTR').
# ----------------------------------------------------------------
# Build a dataframe with all needed "stage variable" expansions in columns:
df_Xfm_CHD_CTR_base = df_Sort_InRowNum

# For repeated decoding, create many columns. We do them one by one:

# ----------------------------------------------------------------
# Helper for last char from a substring [start,len]
# ----------------------------------------------------------------
def last_char(col_str, start_pos, length_val):
    return F.col(col_str).substr(start_pos, length_val).substr(F.length(F.col(col_str).substr(start_pos, length_val)), 1)

# ----------------------------------------------------------------
# Decode function handle ( e.g. If char == '}' then '-0' etc. ), inlined as chained when() statements:
# ----------------------------------------------------------------
def decode_signed_value(col_expr):
    return (
        F.when(col_expr == F.lit('}'), F.lit('-0'))
        .when(col_expr == F.lit('J'), F.lit('-1'))
        .when(col_expr == F.lit('K'), F.lit('-2'))
        .when(col_expr == F.lit('L'), F.lit('-3'))
        .when(col_expr == F.lit('M'), F.lit('-4'))
        .when(col_expr == F.lit('N'), F.lit('-5'))
        .when(col_expr == F.lit('O'), F.lit('-6'))
        .when(col_expr == F.lit('P'), F.lit('-7'))
        .when(col_expr == F.lit('Q'), F.lit('-8'))
        .when(col_expr == F.lit('R'), F.lit('-9'))
        .when(col_expr == F.lit('{'), F.lit('0'))
        .when(col_expr == F.lit('A'), F.lit('1'))
        .when(col_expr == F.lit('B'), F.lit('2'))
        .when(col_expr == F.lit('C'), F.lit('3'))
        .when(col_expr == F.lit('D'), F.lit('4'))
        .when(col_expr == F.lit('E'), F.lit('5'))
        .when(col_expr == F.lit('F'), F.lit('6'))
        .when(col_expr == F.lit('G'), F.lit('7'))
        .when(col_expr == F.lit('H'), F.lit('8'))
        .otherwise('9')
    )

# ----------------------------------------------------------------
# Ereplace logic for the last character (replace with another char).
# We'll simulate with regexp_replace for only the last character.
# We must do numeric cast afterwards. The stage says " * 1 " => cast to numeric.
# If length(...) > 1 => put a '-' sign in front => negative.
# ----------------------------------------------------------------
def signed_amount(col_substring, col_substring_last, col_calc):
    # if length(col_calc) > 1 then negative sign
    # else no negative sign
    # Replacing the last character of col_substring with substring(col_calc, x, 1)
    replace_expr_negative = F.regexp_replace(
        col_substring,
        f"(?s)(.{1})$",
        F.substring(col_calc, 2, 1)
    )
    replace_expr_positive = F.regexp_replace(
        col_substring,
        f"(?s)(.{1})$",
        F.substring(col_calc, 1, 1)
    )
    return F.when(
        F.length(col_calc) > 1,
        F.concat(F.lit("-"), replace_expr_negative).cast("double")
    ).otherwise(
        replace_expr_positive.cast("double")
    )

df_Xfm_CHD_CTR_stagevars = (
    df_Xfm_CHD_CTR_base
    # svNetGrosDrugCstAmtCalc
    .withColumn(
        "col_37_14",
        F.col("RESTOFTHE_FIELDS").substr(37, 14)
    )
    .withColumn("col_37_14_last", last_char("RESTOFTHE_FIELDS", 37, 14))
    .withColumn(
        "svNetGrosDrugCstAmtCalc",
        decode_signed_value(F.col("col_37_14_last"))
    )
    .withColumn(
        "svNetGrosDrugCstAmt",
        signed_amount(F.col("col_37_14"), F.col("col_37_14_last"), F.col("svNetGrosDrugCstAmtCalc"))
    )
    # svNetGrosDrugCstAboveAmtCalc
    .withColumn(
        "col_51_14",
        F.col("RESTOFTHE_FIELDS").substr(51, 14)
    )
    .withColumn("col_51_14_last", last_char("RESTOFTHE_FIELDS", 51, 14))
    .withColumn(
        "svNetGrosDrugCstAboveAmtCalc",
        decode_signed_value(F.col("col_51_14_last"))
    )
    .withColumn(
        "svNetGrosDrugCstAboveAmt",
        signed_amount(F.col("col_51_14"), F.col("col_51_14_last"), F.col("svNetGrosDrugCstAboveAmtCalc"))
    )
    # svNetTotGrosDrugCstAmt1Calc
    .withColumn(
        "col_65_14",
        F.col("RESTOFTHE_FIELDS").substr(65, 14)
    )
    .withColumn("col_65_14_last", last_char("RESTOFTHE_FIELDS", 65, 14))
    .withColumn(
        "svNetTotGrosDrugCstAmt1Calc",
        decode_signed_value(F.col("col_65_14_last"))
    )
    .withColumn(
        "svNetTotGrosDrugCstAmt1",
        signed_amount(F.col("col_65_14"), F.col("col_65_14_last"), F.col("svNetTotGrosDrugCstAmt1Calc"))
    )
    # svNetLowIncmCstSharingAmtCalc
    .withColumn(
        "col_79_14",
        F.col("RESTOFTHE_FIELDS").substr(79, 14)
    )
    .withColumn("col_79_14_last", last_char("RESTOFTHE_FIELDS", 79, 14))
    .withColumn(
        "svNetLowIncmCstSharingAmtCalc",
        decode_signed_value(F.col("col_79_14_last"))
    )
    .withColumn(
        "svNetLowIncmCstSharingAmt",
        signed_amount(F.col("col_79_14"), F.col("col_79_14_last"), F.col("svNetLowIncmCstSharingAmtCalc"))
    )
    # svNetCovPlnPdAmtCalc
    .withColumn(
        "col_93_14",
        F.col("RESTOFTHE_FIELDS").substr(93, 14)
    )
    .withColumn("col_93_14_last", last_char("RESTOFTHE_FIELDS", 93, 14))
    .withColumn(
        "svNetCovPlnPdAmtCalc",
        decode_signed_value(F.col("col_93_14_last"))
    )
    .withColumn(
        "svNetCovPlnPdAmt",
        signed_amount(F.col("col_93_14"), F.col("col_93_14_last"), F.col("svNetCovPlnPdAmtCalc"))
    )
    # svSubmitDueAmtCalc
    .withColumn(
        "col_115_14",
        F.col("RESTOFTHE_FIELDS").substr(115, 14)
    )
    .withColumn("col_115_14_last", last_char("RESTOFTHE_FIELDS", 115, 14))
    .withColumn(
        "svSubmitDueAmtCalc",
        decode_signed_value(F.col("col_115_14_last"))
    )
    .withColumn(
        "svSubmitDueAmt",
        signed_amount(F.col("col_115_14"), F.col("col_115_14_last"), F.col("svSubmitDueAmtCalc"))
    )
    # svFileCrtDt
    .withColumn(
        "svFileCrtDt",
        F.concat(
            F.col("RESTOFTHE_FIELDS").substr(42, 4),
            F.lit("-"),
            F.col("RESTOFTHE_FIELDS").substr(46, 2),
            F.lit("-"),
            F.col("RESTOFTHE_FIELDS").substr(48, 2)
        )
    )
    # svFileCrtTime
    .withColumn(
        "svFileCrtTime",
        F.concat(
            F.col("RESTOFTHE_FIELDS").substr(50, 2),
            F.lit(":"),
            F.col("RESTOFTHE_FIELDS").substr(52, 2),
            F.lit(":"),
            F.col("RESTOFTHE_FIELDS").substr(54, 2)
        )
    )
)

# Now produce df_Lnk_CHD (RECORD_ID='CHD') with the specified columns:
df_Lnk_CHD = (
    df_Xfm_CHD_CTR_stagevars
    .filter(F.col("RECORD_ID") == F.lit("CHD"))
    .select(
        F.lit(0).alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),  # WhereExpression => "0"
        F.trim(F.col("RESTOFTHE_FIELDS").substr(16, 16)).alias("FILE_ID"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(4, 7)).alias("CNTR_SEQ_ID"),
        F.lit("CMS").alias("SRC_SYS_CD"),
        F.col("CurrRunCycleDate").alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
        F.col("CurrRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
        F.trim(F.col("RESTOFTHE_FIELDS").substr(11, 5)).alias("CMS_CNTR_ID"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(36, 4)).alias("AS_OF_YR"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(40, 2)).alias("AS_OF_MO"),
        F.expr('StringToDate(svFileCrtDt, "%yyyy-%mm-%dd")').alias("FILE_CRTN_DT"),
        F.expr('StringToTime(svFileCrtTime, "%hh:%nn:%ss")').alias("FILE_CRTN_TM"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(56, 5)).alias("RPT_ID")
    )
)

# df_Lnk_CTR (RECORD_ID='CTR') with specified columns:
df_Lnk_CTR = (
    df_Xfm_CHD_CTR_stagevars
    .filter(F.col("RECORD_ID") == F.lit("CTR"))
    .select(
        F.trim(F.col("RESTOFTHE_FIELDS").substr(4, 7)).alias("CNTR_SEQ_ID"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(11, 5)).alias("CMS_CNTR_ID"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(16, 1)).alias("DRUG_COV_STTUS_CD_TX"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(17, 11)).alias("BNFCRY_CT"),
        F.col("svNetGrosDrugCstAmt").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("svNetGrosDrugCstAboveAmt").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("svNetTotGrosDrugCstAmt1").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("svNetLowIncmCstSharingAmt").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("svNetCovPlnPdAmt").alias("NET_COV_PLN_PD_AMT"),
        F.trim(F.col("RESTOFTHE_FIELDS").substr(107, 8)).alias("TOT_DTL_RCRD_CT"),
        F.col("svSubmitDueAmt").alias("SUBMT_DUE_AMT"),
        F.col("CurrRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# ----------------------------------------------------------------
# 6) Merge_CntrNoSeqNo (PxMerge)
#    Master link: df_Lnk_CHD, Update link: df_Lnk_CTR
#    Merged output -> df_Merge_CntrNoSeqNo
# ----------------------------------------------------------------
df_Merge_CntrNoSeqNo = (
    df_Lnk_CHD.alias("CHD")
    .join(
        df_Lnk_CTR.alias("CTR"),
        [
            F.col("CHD.CMS_CNTR_ID") == F.col("CTR.CMS_CNTR_ID"),
            F.col("CHD.CNTR_SEQ_ID") == F.col("CTR.CNTR_SEQ_ID")
        ],
        "left"
    )
    .select(
        F.col("CHD.PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
        F.col("CHD.FILE_ID").alias("FILE_ID"),
        F.col("CHD.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("CHD.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CHD.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CHD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CHD.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("CHD.AS_OF_YR").alias("AS_OF_YR"),
        F.col("CHD.AS_OF_MO").alias("AS_OF_MO"),
        F.col("CHD.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("CHD.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("CHD.RPT_ID").alias("RPT_ID"),
        F.col("CTR.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("CTR.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("CTR.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("CTR.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("CTR.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("CTR.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("CTR.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("CTR.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("CTR.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("CTR.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CTR.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# ----------------------------------------------------------------
# 7) Copy stage => two outputs:
#    Lnk_Remove_Dup => FILE_ID, CNTR_SEQ_ID, SRC_SYS_CD
#    Lnk_AllCol_Join => the rest of the columns
# ----------------------------------------------------------------
df_Copy = df_Merge_CntrNoSeqNo

df_Lnk_Remove_Dup = df_Copy.select(
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD")
)

df_Lnk_AllCol_Join = df_Copy.select(
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CMS_CNTR_ID"),
    F.col("AS_OF_YR"),
    F.col("AS_OF_MO"),
    F.col("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM"),
    F.col("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------
# 8) Remove_Duplicates (PxRemDup) => Lnk_RmDup
#    Key: FILE_ID, CNTR_SEQ_ID, SRC_SYS_CD => keep first
# ----------------------------------------------------------------
df_Remove_Duplicates = dedup_sort(
    df_Lnk_Remove_Dup,
    partition_cols=["FILE_ID", "CNTR_SEQ_ID", "SRC_SYS_CD"],
    sort_cols=[("FILE_ID","A"), ("CNTR_SEQ_ID","A"), ("SRC_SYS_CD","A")]
)

df_Lnk_RmDup = df_Remove_Duplicates

# ----------------------------------------------------------------
# 9) Jn1_NKey (PxJoin) => left outer join
#    Input links: Lnk_RmDup and df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F
#    Key: FILE_ID, CNTR_SEQ_ID, SRC_SYS_CD
# ----------------------------------------------------------------
df_Jn1_NKey = (
    df_Lnk_RmDup.alias("A")
    .join(
        df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F.alias("B"),
        [
            F.col("A.FILE_ID") == F.col("B.FILE_ID"),
            F.col("A.CNTR_SEQ_ID") == F.col("B.CNTR_SEQ_ID"),
            F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD")
        ],
        "left"
    )
    .select(
        F.col("A.FILE_ID").alias("FILE_ID"),
        F.col("A.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("A.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("B.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("B.PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK")
    )
)

# ----------------------------------------------------------------
# 10) Transformer => next surrogate key if PDE_PARTD_PAYMT_RECON_CNTR_SK is null or 0
#     Output pins:
#       Lnk_KTableLoad => constraint PDE_PARTD_PAYMT_RECON_CNTR_SK was 0
#       Lnk_Jn         => pass all
# ----------------------------------------------------------------
df_Transformer = df_Jn1_NKey.withColumn(
    "PDE_PARTD_PAYMT_RECON_CNTR_SK",
    F.when(
        (F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK").isNull()) | 
        (F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK") == 0),
        F.lit(0)
    ).otherwise(F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK"))
)

# Mark which are new vs existing
df_Transformer_marked = df_Transformer.withColumn(
    "isNewRecord",
    F.when(F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK") == 0, F.lit(True)).otherwise(F.lit(False))
)

# Now call SurrogateKeyGen to replace 0 with newly generated key
df_enriched = df_Transformer_marked.drop("PDE_PARTD_PAYMT_RECON_CNTR_SK")
df_enriched = df_enriched.withColumnRenamed("FILE_ID","FILE_ID_tmp") \
    .withColumnRenamed("CNTR_SEQ_ID","CNTR_SEQ_ID_tmp") \
    .withColumnRenamed("SRC_SYS_CD","SRC_SYS_CD_tmp") \
    .withColumnRenamed("CRT_RUN_CYC_EXCTN_DT_SK","CRT_RUN_CYC_EXCTN_DT_SK_tmp") \
    .withColumnRenamed("isNewRecord","isNewRecord_tmp")

# Re-add PDE_PARTD_PAYMT_RECON_CNTR_SK as null for the 0 case
df_enriched = df_enriched.withColumn(
    "PDE_PARTD_PAYMT_RECON_CNTR_SK",
    F.when(F.col("isNewRecord_tmp"), None).otherwise(F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK"))
)

# Surrogate Key generation
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PDE_PARTD_PAYMT_RECON_CNTR_SK",<schema>,<secret_name>)

# Rename columns back
df_enriched = (
    df_enriched
    .withColumnRenamed("FILE_ID_tmp",       "FILE_ID")
    .withColumnRenamed("CNTR_SEQ_ID_tmp",   "CNTR_SEQ_ID")
    .withColumnRenamed("SRC_SYS_CD_tmp",    "SRC_SYS_CD")
    .withColumnRenamed("CRT_RUN_CYC_EXCTN_DT_SK_tmp", "CRT_RUN_CYC_EXCTN_DT_SK")
    .withColumnRenamed("isNewRecord_tmp",   "isNewRecord")
)

# Lnk_KTableLoad => rows where isNewRecord == true
df_Lnk_KTableLoad = (
    df_enriched
    .filter(F.col("isNewRecord") == F.lit(True))
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
        F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK")
    )
)

# Lnk_Jn => all rows
df_Lnk_Jn = (
    df_enriched
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.when(F.col("isNewRecord"), F.lit(CurrRunCycleDate))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK")
    )
)

# ----------------------------------------------------------------
# 11) Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F_Load (DB2ConnectorPX) => Upsert logic
# ----------------------------------------------------------------
# Write df_Lnk_KTableLoad to a staging table, then merge into EDWOwner.K_PDE_PARTD_PAYMT_RECON_CNTR_F
df_to_load = df_Lnk_KTableLoad.select(
    "FILE_ID",
    "CNTR_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "PDE_PARTD_PAYMT_RECON_CNTR_SK"
)

temp_table_name = "STAGING.PdePartdPaymtReconCntrFExtr_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_F_Load_temp"
merge_target_table = EDWOwner + ".K_PDE_PARTD_PAYMT_RECON_CNTR_F"

# Create the temp table
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
(
    df_to_load.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("append")
    .save()
)

# Build MERGE statement
# Match on PDE_PARTD_PAYMT_RECON_CNTR_SK
merge_sql = f"""
MERGE INTO {merge_target_table} AS T
USING {temp_table_name} AS S
ON T.PDE_PARTD_PAYMT_RECON_CNTR_SK = S.PDE_PARTD_PAYMT_RECON_CNTR_SK
WHEN MATCHED THEN UPDATE SET 
  T.FILE_ID = S.FILE_ID,
  T.CNTR_SEQ_ID = S.CNTR_SEQ_ID,
  T.SRC_SYS_CD = S.SRC_SYS_CD,
  T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK
WHEN NOT MATCHED THEN
  INSERT (
    PDE_PARTD_PAYMT_RECON_CNTR_SK,
    FILE_ID,
    CNTR_SEQ_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_DT_SK
  )
  VALUES (
    S.PDE_PARTD_PAYMT_RECON_CNTR_SK,
    S.FILE_ID,
    S.CNTR_SEQ_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------
# 12) Jn2Nkey (PxJoin) => inner join of Lnk_AllCol_Join and Lnk_Jn
#    Key: FILE_ID, CNTR_SEQ_ID, SRC_SYS_CD
# ----------------------------------------------------------------
df_Jn2Nkey = (
    df_Lnk_AllCol_Join.alias("A")
    .join(
        df_Lnk_Jn.alias("B"),
        [
            F.col("A.FILE_ID") == F.col("B.FILE_ID"),
            F.col("A.CNTR_SEQ_ID") == F.col("B.CNTR_SEQ_ID"),
            F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD")
        ],
        "inner"
    )
    .select(
        F.col("B.PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
        F.col("A.FILE_ID").alias("FILE_ID"),
        F.col("A.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("A.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("B.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("A.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("A.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("A.AS_OF_YR").alias("AS_OF_YR"),
        F.col("A.AS_OF_MO").alias("AS_OF_MO"),
        F.col("A.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("A.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("A.RPT_ID").alias("RPT_ID"),
        F.col("A.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("A.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("A.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("A.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("A.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("A.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("A.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("A.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("A.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# ----------------------------------------------------------------
# 13) Copy5 => pass columns to Lnk_SeqExtr
# ----------------------------------------------------------------
df_Copy5 = df_Jn2Nkey

df_Lnk_SeqExtr = df_Copy5.select(
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CMS_CNTR_ID"),
    F.col("AS_OF_YR"),
    F.col("AS_OF_MO"),
    F.col("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM"),
    F.col("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------
# 14) Seq_PDE_PARTD_PAYMT_RECON_CNTR_F_Extr => write to verified
#     Path => f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_F_Load.txt"
#     With header=true, quote='"', delimiter=','
# ----------------------------------------------------------------

# Per requirement, rpad columns that are declared char(10):
#   CRT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK
df_final = (
    df_Lnk_SeqExtr
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
)

file_path_SeqExtr = f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_F_Load.txt"
write_files(
    df_final,
    file_path_SeqExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)