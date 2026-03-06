# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdeAcctgRptSubmtCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PDE_ACC_COV* and loads the data into EDW Table PDE_ACCTG_RPT_SUBMT_CNTR_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                    Jaideep Mankala         02/24/2022


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
InFile = get_widget_value('InFile','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read from DB2ConnectorPX: Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F = (
    f"SELECT FILE_ID, SUBMT_CNTR_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, "
    f"PDE_ACCTG_RPT_SUBMT_CNTR_SK FROM {EDWOwner}.K_PDE_ACCTG_RPT_SUBMT_CNTR_F"
)
df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F)
    .load()
)

# Read delimited file: Payable40
schema_Payable40 = StructType([
    StructField("Field", StringType(), True)
])
df_Payable40 = (
    spark.read.format("csv")
    .schema(schema_Payable40)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# xfm_RecId: Filter and create columns
df_Payable40_f = df_Payable40.filter(
    (F.substring("Field",1,3) == "CHD") | (F.substring("Field",1,3) == "CTR")
)
df_xfm_RecId_temp = df_Payable40_f.withColumn("seq_temp", F.monotonically_increasing_id())
windowSpec_xfm_RecId = Window.orderBy("seq_temp")
df_xfm_RecId = (
    df_xfm_RecId_temp
    .withColumn("InputRowNumber", F.row_number().over(windowSpec_xfm_RecId))
    .drop("seq_temp")
    .withColumn("RECORD_ID", F.substring("Field",1,3))
    .withColumn("SEQUENCE_NO", F.substring("Field",4,7))
    .withColumn("RESTOFTHE_FIELDS", F.col("Field"))
    .select("RECORD_ID","SEQUENCE_NO","RESTOFTHE_FIELDS","InputRowNumber")
)

# Sort_InRowNum
df_Sort_InRowNum = df_xfm_RecId.orderBy(F.col("InputRowNumber"))

# Xfm_CHD_CTR: compute stage variables
df_CHD_CTR_sv = df_Sort_InRowNum

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn(
    "svFileCrtDt",
    F.concat(
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",42,8)),1,4),
        F.lit("-"),
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",42,8)),5,2),
        F.lit("-"),
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",42,8)),7,2)
    )
)

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn(
    "svFileCrtTime",
    F.concat(
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",50,6)),1,2),
        F.lit(":"),
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",50,6)),3,2),
        F.lit(":"),
        F.substring(trim(F.substring("RESTOFTHE_FIELDS",50,6)),5,2)
    )
)

def calc_char_to_digit(col_expression):
    return (
        F.when(F.substring(col_expression, F.length(col_expression), 1) == '}', '-0')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'J', '-1')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'K', '-2')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'L', '-3')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'M', '-4')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'N', '-5')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'O', '-6')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'P', '-7')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'Q', '-8')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'R', '-9')
        .when(F.substring(col_expression, F.length(col_expression), 1) == '{', '0')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'A', '1')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'B', '2')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'C', '3')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'D', '4')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'E', '5')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'F', '6')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'G', '7')
        .when(F.substring(col_expression, F.length(col_expression), 1) == 'H', '8')
        .otherwise('9')
    )

def final_amt_calc(df_in, calc_col, original_substr_start, original_substr_len, new_col):
    df_out = df_in.withColumn(
        new_col,
        F.when(F.length(F.col(calc_col)) > 1,
               F.concat(
                   F.lit("-"),
                   (Ereplace(
                       F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len),
                       F.substring(F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len),
                                   F.length(F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len)), 
                                   1),
                       F.substring(F.col(calc_col),2,1)
                   ) * 1)
               )
        ).otherwise(
            Ereplace(
                F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len),
                F.substring(F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len),
                            F.length(F.substring("RESTOFTHE_FIELDS",original_substr_start,original_substr_len)), 1),
                F.substring(F.col(calc_col),1,1)
            ) * 1
        )
    )
    return df_out

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetIngrCstAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",48,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetIngrCstAmtCalc", 48, 14, "svCurNetIngrCstAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetDispnsFeeAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",62,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetDispnsFeeAmtCalc", 62, 14, "svCurNetDispnsFeeAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetSlsTaxAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",76,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetSlsTaxAmtCalc", 76, 14, "svCurNetSlsTaxAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetGrosDrugCstBfrAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",90,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetGrosDrugCstBfrAmtCalc", 90, 14, "svCurNetGrosDrugCstBfrAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetGrosDrugCstAftrAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",104,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetGrosDrugCstAftrAmtCalc", 104, 14, "svCurNetGrosDrugCstAftrAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurTotGrosDrugCstAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",118,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurTotGrosDrugCstAmtCalc", 118, 14, "svCurTotGrosDrugCstAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetPatnPayAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",132,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetPatnPayAmtCalc", 132, 14, "svCurNetPatnPayAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetOthrTrueOopAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",146,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetOthrTrueOopAmtCalc", 146, 14, "svCurNetOthrTrueOopAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetLowIncmCstSharingAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",160,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetLowIncmCstSharingAmtCalc", 160, 14, "svCurNetLowIncmCstSharingAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetTrueOopAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",174,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetTrueOopAmtCalc", 174, 14, "svCurNetTrueOopAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetPatnLiabRedcAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",188,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetPatnLiabRedcAmtCalc", 188, 14, "svCurNetPatnLiabRedcAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetCovPlnPdAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",202,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetCovPlnPdAmtCalc", 202, 14, "svCurNetCovPlnPdAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetNCovPlnPdAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",216,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetNCovPlnPdAmtCalc", 216, 14, "svCurNetNCovPlnPdAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurTotDueAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",346,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurTotDueAmtCalc", 346, 14, "svCurTotDueAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetEstPtOfSaleRbtAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",360,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetEstPtOfSaleRbtAmtCalc", 360, 14, "svCurNetEstPtOfSaleRbtAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetVccnAdmFeeAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",374,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetVccnAdmFeeAmtCalc", 374, 14, "svCurNetVccnAdmFeeAmt")

df_CHD_CTR_sv = df_CHD_CTR_sv.withColumn("svCurNetRptdGapDscntAmtCalc", calc_char_to_digit(F.substring("RESTOFTHE_FIELDS",388,14)))
df_CHD_CTR_sv = final_amt_calc(df_CHD_CTR_sv, "svCurNetRptdGapDscntAmtCalc", 388, 14, "svCurNetRptdGapDscntAmt")

df_Lnk_CHD = (
    df_CHD_CTR_sv
    .filter(F.col("RECORD_ID") == "CHD")
    .select(
        F.lit(None).cast("long").alias("PDE_ACCTG_RPT_SUBMT_CNTR_SK"),
        trim(F.substring("RESTOFTHE_FIELDS",16,16)).alias("FILE_ID"),
        trim(F.substring("RESTOFTHE_FIELDS",4,7)).alias("SUBMT_CNTR_SEQ_ID"),
        F.lit("CMS").alias("SRC_SYS_CD"),
        F.lit(None).cast("string").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        trim(F.substring("RESTOFTHE_FIELDS",11,5)).alias("SUBMT_CMS_CNTR_ID"),
        trim(F.substring("RESTOFTHE_FIELDS",36,4)).alias("AS_OF_YR"),
        trim(F.substring("RESTOFTHE_FIELDS",40,2)).alias("AS_OF_MO"),
        F.expr('StringToDate(svFileCrtDt, "%yyyy-%mm-%dd")').alias("FILE_CRTN_DT"),
        F.expr('StringToTime(svFileCrtTime, "%hh:%nn:%ss")').alias("FILE_CRTN_TM"),
        trim(F.substring("RESTOFTHE_FIELDS",56,5)).alias("RPT_ID"),
        F.lit(None).alias("_dummy_to_ensure_same_schema_CHD1")  # placeholder to keep consistent with CTR's columns if needed
    )
)

df_Lnk_CTR = (
    df_CHD_CTR_sv
    .filter(F.col("RECORD_ID") == "CTR")
    .select(
        trim(F.substring("RESTOFTHE_FIELDS",4,7)).alias("SUBMT_CNTR_SEQ_ID"),
        trim(F.substring("RESTOFTHE_FIELDS",11,5)).alias("SUBMT_CMS_CNTR_ID"),
        trim(F.substring("RESTOFTHE_FIELDS",16,1)).alias("DRUG_COV_STTUS_CD"),
        trim(F.substring("RESTOFTHE_FIELDS",17,11)).alias("BNFCRY_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",37,11)).alias("RX_CT"),
        F.col("svCurNetIngrCstAmt").alias("NET_INGR_CST_AMT"),
        F.col("svCurNetDispnsFeeAmt").alias("NET_DISPNS_FEE_AMT"),
        F.col("svCurNetSlsTaxAmt").alias("NET_SLS_TAX_AMT"),
        F.col("svCurNetGrosDrugCstBfrAmt").alias("NET_GROS_DRUG_CST_BFR_AMT"),
        F.col("svCurNetGrosDrugCstAftrAmt").alias("NET_GROS_DRUG_CST_AFTR_AMT"),
        F.col("svCurTotGrosDrugCstAmt").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("svCurNetPatnPayAmt").alias("NET_PATN_PAY_AMT"),
        F.col("svCurNetOthrTrueOopAmt").alias("NET_OTHR_TRUE_OOP_AMT"),
        F.col("svCurNetLowIncmCstSharingAmt").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("svCurNetTrueOopAmt").alias("NET_TRUE_OOP_AMT"),
        F.col("svCurNetPatnLiabRedcAmt").alias("NET_PATN_LIAB_REDC_AMT"),
        F.col("svCurNetCovPlnPdAmt").alias("NET_COV_PLN_PD_AMT"),
        F.col("svCurNetNCovPlnPdAmt").alias("NET_NCOV_PLN_PD_AMT"),
        trim(F.substring("RESTOFTHE_FIELDS",230,12)).alias("ORIG_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",242,12)).alias("ADJ_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",254,12)).alias("DEL_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",266,12)).alias("CATO_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",278,12)).alias("ATCHMT_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",290,12)).alias("NON_CATO_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",302,12)).alias("NONSTD_FMT_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",314,12)).alias("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
        trim(F.substring("RESTOFTHE_FIELDS",338,8)).alias("TOT_DTL_RCRD_CT"),
        F.col("svCurTotDueAmt").alias("TOT_DUE_AMT"),
        F.col("svCurNetEstPtOfSaleRbtAmt").alias("NET_EST_PT_OF_SALE_RBT_AMT"),
        F.col("svCurNetVccnAdmFeeAmt").alias("NET_VCCN_ADM_FEE_AMT"),
        F.col("svCurNetRptdGapDscntAmt").alias("NET_RPTD_GAP_DSCNT_AMT"),
        trim(F.substring("RESTOFTHE_FIELDS",402,12)).alias("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Merge_CntrNoSeqNo (PxMerge) – first link is CHD, second is CTR
df_Merge_CntrNoSeqNo = (
    df_Lnk_CHD.alias("chd")
    .join(
        df_Lnk_CTR.alias("ctr"),
        (F.col("chd.SUBMT_CMS_CNTR_ID") == F.col("ctr.SUBMT_CMS_CNTR_ID"))
        & (F.col("chd.SUBMT_CNTR_SEQ_ID") == F.col("ctr.SUBMT_CNTR_SEQ_ID")),
        how="left"
    )
    .select(
        F.col("chd.FILE_ID").alias("FILE_ID"),
        F.col("chd.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        F.col("chd.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("chd.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("chd.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
        F.col("chd.AS_OF_YR").alias("AS_OF_YR"),
        F.col("chd.AS_OF_MO").alias("AS_OF_MO"),
        F.col("chd.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("chd.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("chd.RPT_ID").alias("RPT_ID"),
        F.col("ctr.DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
        F.col("ctr.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("ctr.RX_CT").alias("RX_CT"),
        F.col("ctr.NET_INGR_CST_AMT").alias("NET_INGR_CST_AMT"),
        F.col("ctr.NET_DISPNS_FEE_AMT").alias("NET_DISPNS_FEE_AMT"),
        F.col("ctr.NET_SLS_TAX_AMT").alias("NET_SLS_TAX_AMT"),
        F.col("ctr.NET_GROS_DRUG_CST_BFR_AMT").alias("NET_GROS_DRUG_CST_BFR_AMT"),
        F.col("ctr.NET_GROS_DRUG_CST_AFTR_AMT").alias("NET_GROS_DRUG_CST_AFTR_AMT"),
        F.col("ctr.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("ctr.NET_PATN_PAY_AMT").alias("NET_PATN_PAY_AMT"),
        F.col("ctr.NET_OTHR_TRUE_OOP_AMT").alias("NET_OTHR_TRUE_OOP_AMT"),
        F.col("ctr.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("ctr.NET_TRUE_OOP_AMT").alias("NET_TRUE_OOP_AMT"),
        F.col("ctr.NET_PATN_LIAB_REDC_AMT").alias("NET_PATN_LIAB_REDC_AMT"),
        F.col("ctr.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("ctr.NET_NCOV_PLN_PD_AMT").alias("NET_NCOV_PLN_PD_AMT"),
        F.col("ctr.ORIG_RX_DRUG_EVT_CT").alias("ORIG_RX_DRUG_EVT_CT"),
        F.col("ctr.ADJ_RX_DRUG_EVT_CT").alias("ADJ_RX_DRUG_EVT_CT"),
        F.col("ctr.DEL_RX_DRUG_EVT_CT").alias("DEL_RX_DRUG_EVT_CT"),
        F.col("ctr.CATO_RX_DRUG_EVT_CT").alias("CATO_RX_DRUG_EVT_CT"),
        F.col("ctr.ATCHMT_RX_DRUG_EVT_CT").alias("ATCHMT_RX_DRUG_EVT_CT"),
        F.col("ctr.NON_CATO_RX_DRUG_EVT_CT").alias("NON_CATO_RX_DRUG_EVT_CT"),
        F.col("ctr.NONSTD_FMT_RX_DRUG_EVT_CT").alias("NONSTD_FMT_RX_DRUG_EVT_CT"),
        F.col("ctr.OUT_OF_NTWK_RX_DRUG_EVT_CT").alias("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
        F.col("ctr.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("ctr.TOT_DUE_AMT").alias("TOT_DUE_AMT"),
        F.col("ctr.NET_EST_PT_OF_SALE_RBT_AMT").alias("NET_EST_PT_OF_SALE_RBT_AMT"),
        F.col("ctr.NET_VCCN_ADM_FEE_AMT").alias("NET_VCCN_ADM_FEE_AMT"),
        F.col("ctr.NET_RPTD_GAP_DSCNT_AMT").alias("NET_RPTD_GAP_DSCNT_AMT"),
        F.col("ctr.NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT").alias("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
        F.col("ctr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ctr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# RmDupCntrSeq (PxRemDup)
df_RmDupCntrSeq = dedup_sort(
    df_Merge_CntrNoSeqNo,
    partition_cols=["FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD"],
    sort_cols=[("FILE_ID","A"),("SUBMT_CNTR_SEQ_ID","A"),("SRC_SYS_CD","A")]
)

# Copy
df_Copy = df_RmDupCntrSeq
df_Lnk_Remove_Dup = df_Copy.select("FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD")
df_Lnk_AllCol_Join = df_Copy.select(
    "FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","SUBMT_CMS_CNTR_ID",
    "AS_OF_YR","AS_OF_MO","FILE_CRTN_DT","FILE_CRTN_TM","RPT_ID","DRUG_COV_STTUS_CD","BNFCRY_CT","RX_CT",
    "NET_INGR_CST_AMT","NET_DISPNS_FEE_AMT","NET_SLS_TAX_AMT","NET_GROS_DRUG_CST_BFR_AMT","NET_GROS_DRUG_CST_AFTR_AMT",
    "NET_TOT_GROS_DRUG_CST_AMT","NET_PATN_PAY_AMT","NET_OTHR_TRUE_OOP_AMT","NET_LOW_INCM_CST_SHARING_AMT",
    "NET_TRUE_OOP_AMT","NET_PATN_LIAB_REDC_AMT","NET_COV_PLN_PD_AMT","NET_NCOV_PLN_PD_AMT","ORIG_RX_DRUG_EVT_CT",
    "ADJ_RX_DRUG_EVT_CT","DEL_RX_DRUG_EVT_CT","CATO_RX_DRUG_EVT_CT","ATCHMT_RX_DRUG_EVT_CT",
    "NON_CATO_RX_DRUG_EVT_CT","NONSTD_FMT_RX_DRUG_EVT_CT","OUT_OF_NTWK_RX_DRUG_EVT_CT","TOT_DTL_RCRD_CT",
    "TOT_DUE_AMT","NET_EST_PT_OF_SALE_RBT_AMT","NET_VCCN_ADM_FEE_AMT","NET_RPTD_GAP_DSCNT_AMT",
    "NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# Copy1
df_Copy1 = df_Lnk_Remove_Dup
df_Lnk_RmDup = df_Copy1.select("FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD")

# Jn1_NKey (PxJoin) leftouterjoin
df_Jn1_NKey = (
    df_Lnk_RmDup.alias("left")
    .join(
        df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F.alias("right"),
        [
            F.col("left.FILE_ID") == F.col("right.FILE_ID"),
            F.col("left.SUBMT_CNTR_SEQ_ID") == F.col("right.SUBMT_CNTR_SEQ_ID"),
            F.col("left.SRC_SYS_CD") == F.col("right.SRC_SYS_CD")
        ],
        how="left"
    )
    .select(
        F.col("left.FILE_ID").alias("FILE_ID"),
        F.col("left.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        F.col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("right.PDE_ACCTG_RPT_SUBMT_CNTR_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_SK")
    )
)

# Transformer: next stage - define stage variable
df_Transformer = df_Jn1_NKey.withColumn(
    "svPdeAcctgRptSbmCntrSK",
    F.when(
        (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK").isNotNull()) & (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK") != 0),
        F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK")
    ).otherwise(SurrogateKeyGenPlaceholder())  # We call NextSurrogateKey. Actual call to SurrogateKeyGen happens below.
)
# We must fix the SurrogateKeyGen call EXACTLY as per instructions: "df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)"
# So let's rename df_Transformer to df_enriched to do that step directly:
df_enriched = df_Transformer
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svPdeAcctgRptSbmCntrSK",<schema>,<secret_name>)

df_Lnk_KTableLoad = df_enriched.filter(
    (
        (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK").isNull()) | (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK") == 0)
    )
).select(
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svPdeAcctgRptSbmCntrSK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_SK")
)

# Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F_Load => must do a merge
df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F_Load = df_Lnk_KTableLoad

temp_table_load = "STAGING.PdeAcctgRptSubmtCntrFExtr_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_load}", jdbc_url_EDW, jdbc_props_EDW)
(
    df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_F_Load.write.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("dbtable", temp_table_load)
    .mode("append")
    .save()
)

merge_sql_load = (
    f"MERGE {EDWOwner}.K_PDE_ACCTG_RPT_SUBMT_CNTR_F as T "
    f"USING {temp_table_load} as S "
    f"ON (T.FILE_ID=S.FILE_ID AND T.SUBMT_CNTR_SEQ_ID=S.SUBMT_CNTR_SEQ_ID AND T.SRC_SYS_CD=S.SRC_SYS_CD) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.PDE_ACCTG_RPT_SUBMT_CNTR_SK=S.PDE_ACCTG_RPT_SUBMT_CNTR_SK, "
    f"T.CRT_RUN_CYC_EXCTN_DT_SK=S.CRT_RUN_CYC_EXCTN_DT_SK "
    f"WHEN NOT MATCHED THEN INSERT (FILE_ID,SUBMT_CNTR_SEQ_ID,SRC_SYS_CD,PDE_ACCTG_RPT_SUBMT_CNTR_SK,CRT_RUN_CYC_EXCTN_DT_SK) "
    f"VALUES (S.FILE_ID,S.SUBMT_CNTR_SEQ_ID,S.SRC_SYS_CD,S.PDE_ACCTG_RPT_SUBMT_CNTR_SK,S.CRT_RUN_CYC_EXCTN_DT_SK);"
)
execute_dml(merge_sql_load, jdbc_url_EDW, jdbc_props_EDW)

# Lnk_Jn: (Transformer output) => we do another select for the next join
df_Lnk_Jn = df_enriched.select(
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(
        (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK").isNull()) | (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_SK") == 0),
        F.lit(CurrRunCycleDate)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svPdeAcctgRptSbmCntrSK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_SK")
)

# Jn2Nkey (PxJoin) -> inner join
df_Jn2Nkey = (
    df_Lnk_AllCol_Join.alias("allcol")
    .join(
        df_Lnk_Jn.alias("jn"),
        [
            F.col("allcol.FILE_ID") == F.col("jn.FILE_ID"),
            F.col("allcol.SUBMT_CNTR_SEQ_ID") == F.col("jn.SUBMT_CNTR_SEQ_ID"),
            F.col("allcol.SRC_SYS_CD") == F.col("jn.SRC_SYS_CD")
        ],
        how="inner"
    )
    .select(
        F.col("jn.PDE_ACCTG_RPT_SUBMT_CNTR_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_SK"),
        F.col("allcol.FILE_ID").alias("FILE_ID"),
        F.col("allcol.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        F.col("allcol.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("allcol.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("allcol.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
        F.col("allcol.AS_OF_YR").alias("AS_OF_YR"),
        F.col("allcol.AS_OF_MO").alias("AS_OF_MO"),
        F.col("allcol.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("allcol.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("allcol.RPT_ID").alias("RPT_ID"),
        F.col("allcol.DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
        F.col("allcol.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("allcol.RX_CT").alias("RX_CT"),
        F.col("allcol.NET_INGR_CST_AMT").alias("NET_INGR_CST_AMT"),
        F.col("allcol.NET_DISPNS_FEE_AMT").alias("NET_DISPNS_FEE_AMT"),
        F.col("allcol.NET_SLS_TAX_AMT").alias("NET_SLS_TAX_AMT"),
        F.col("allcol.NET_GROS_DRUG_CST_BFR_AMT").alias("NET_GROS_DRUG_CST_BFR_AMT"),
        F.col("allcol.NET_GROS_DRUG_CST_AFTR_AMT").alias("NET_GROS_DRUG_CST_AFTR_AMT"),
        F.col("allcol.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("allcol.NET_PATN_PAY_AMT").alias("NET_PATN_PAY_AMT"),
        F.col("allcol.NET_OTHR_TRUE_OOP_AMT").alias("NET_OTHR_TRUE_OOP_AMT"),
        F.col("allcol.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("allcol.NET_TRUE_OOP_AMT").alias("NET_TRUE_OOP_AMT"),
        F.col("allcol.NET_PATN_LIAB_REDC_AMT").alias("NET_PATN_LIAB_REDC_AMT"),
        F.col("allcol.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("allcol.NET_NCOV_PLN_PD_AMT").alias("NET_NCOV_PLN_PD_AMT"),
        F.col("allcol.ORIG_RX_DRUG_EVT_CT").alias("ORIG_RX_DRUG_EVT_CT"),
        F.col("allcol.ADJ_RX_DRUG_EVT_CT").alias("ADJ_RX_DRUG_EVT_CT"),
        F.col("allcol.DEL_RX_DRUG_EVT_CT").alias("DEL_RX_DRUG_EVT_CT"),
        F.col("allcol.CATO_RX_DRUG_EVT_CT").alias("CATO_RX_DRUG_EVT_CT"),
        F.col("allcol.ATCHMT_RX_DRUG_EVT_CT").alias("ATCHMT_RX_DRUG_EVT_CT"),
        F.col("allcol.NON_CATO_RX_DRUG_EVT_CT").alias("NON_CATO_RX_DRUG_EVT_CT"),
        F.col("allcol.NONSTD_FMT_RX_DRUG_EVT_CT").alias("NONSTD_FMT_RX_DRUG_EVT_CT"),
        F.col("allcol.OUT_OF_NTWK_RX_DRUG_EVT_CT").alias("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
        F.col("allcol.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("allcol.TOT_DUE_AMT").alias("TOT_DUE_AMT"),
        F.col("allcol.NET_EST_PT_OF_SALE_RBT_AMT").alias("NET_EST_PT_OF_SALE_RBT_AMT"),
        F.col("allcol.NET_VCCN_ADM_FEE_AMT").alias("NET_VCCN_ADM_FEE_AMT"),
        F.col("allcol.NET_RPTD_GAP_DSCNT_AMT").alias("NET_RPTD_GAP_DSCNT_AMT"),
        F.col("allcol.NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT").alias("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
        F.col("allcol.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("allcol.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Copy5
df_Copy5 = df_Jn2Nkey

df_Seq_Db2_PDE_ACCTG_RPT_SUBMT_CNTR_F = df_Copy5.select(
    "PDE_ACCTG_RPT_SUBMT_CNTR_SK",
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "SUBMT_CMS_CNTR_ID",
    "AS_OF_YR",
    "AS_OF_MO",
    "FILE_CRTN_DT",
    "FILE_CRTN_TM",
    "RPT_ID",
    "DRUG_COV_STTUS_CD",
    "BNFCRY_CT",
    "RX_CT",
    "NET_INGR_CST_AMT",
    "NET_DISPNS_FEE_AMT",
    "NET_SLS_TAX_AMT",
    "NET_GROS_DRUG_CST_BFR_AMT",
    "NET_GROS_DRUG_CST_AFTR_AMT",
    "NET_TOT_GROS_DRUG_CST_AMT",
    "NET_PATN_PAY_AMT",
    "NET_OTHR_TRUE_OOP_AMT",
    "NET_LOW_INCM_CST_SHARING_AMT",
    "NET_TRUE_OOP_AMT",
    "NET_PATN_LIAB_REDC_AMT",
    "NET_COV_PLN_PD_AMT",
    "NET_NCOV_PLN_PD_AMT",
    "ORIG_RX_DRUG_EVT_CT",
    "ADJ_RX_DRUG_EVT_CT",
    "DEL_RX_DRUG_EVT_CT",
    "CATO_RX_DRUG_EVT_CT",
    "ATCHMT_RX_DRUG_EVT_CT",
    "NON_CATO_RX_DRUG_EVT_CT",
    "NONSTD_FMT_RX_DRUG_EVT_CT",
    "OUT_OF_NTWK_RX_DRUG_EVT_CT",
    "TOT_DTL_RCRD_CT",
    "TOT_DUE_AMT",
    "NET_EST_PT_OF_SALE_RBT_AMT",
    "NET_VCCN_ADM_FEE_AMT",
    "NET_RPTD_GAP_DSCNT_AMT",
    "NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# Apply rpad for char/varchar columns with defined length
df_Seq_Db2_PDE_ACCTG_RPT_SUBMT_CNTR_F = df_Seq_Db2_PDE_ACCTG_RPT_SUBMT_CNTR_F.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

# Write final sequential file
write_files(
    df_Seq_Db2_PDE_ACCTG_RPT_SUBMT_CNTR_F,
    f"{adls_path}/verified/PDE_ACCTG_RPT_SUBMT_CNTR_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)