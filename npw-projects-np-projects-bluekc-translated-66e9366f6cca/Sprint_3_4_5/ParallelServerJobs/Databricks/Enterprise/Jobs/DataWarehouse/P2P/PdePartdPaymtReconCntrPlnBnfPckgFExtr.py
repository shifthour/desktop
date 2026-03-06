# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdePartdPaymtReconCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PARTD_RECON* and loads the data into EDW Table PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                  Jaideep Mankala         02/24/2022                                                
# MAGIC 
# MAGIC Rekha Radhakrishna                     2022-04-19           408843                               Updated the Metadata for column                               EnterpriseDev3                  Jaideep Mankala         04/30/2022
# MAGIC                                                                                                                                SUBMT_CMS_CNTR_ID from Decimal 
# MAGIC                                                                                                                                 to Varchar
# MAGIC                                                                                                                               Added FILE_ID in join to Db2_PDE_PARTD_PAYMT_RECON_CNTR_F
# MAGIC                                                                                                                                to avoid duplicates


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve job parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
InFile = get_widget_value('InFile','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read from EDW: Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_Db2_K = (
    "SELECT FILE_ID, CNTR_SEQ_ID, PLN_BNF_PCKG_SEQ_ID, SRC_SYS_CD, "
    "CRT_RUN_CYC_EXCTN_DT_SK, PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK "
    "FROM " + EDWOwner + ".K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F"
)
df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Db2_K)
    .load()
)

# Read from EDW: Db2_PDE_PARTD_PAYMT_RECON_CNTR_F
extract_query_Db2_PDE = (
    "SELECT CMS_CNTR_ID, FILE_ID, CNTR_SEQ_ID, PDE_PARTD_PAYMT_RECON_CNTR_SK "
    "FROM " + EDWOwner + ".PDE_PARTD_PAYMT_RECON_CNTR_F"
)
df_Db2_PDE_PARTD_PAYMT_RECON_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Db2_PDE)
    .load()
)

# Read Payable40 (PxSequentialFile) with a single column "Field"
schema_Payable40 = StructType([StructField("Field", StringType(), True)])
df_Payable40 = (
    spark.read.format("csv")
    .schema(schema_Payable40)
    .option("delimiter", "\u0001")  # Using a delimiter unlikely to appear, so entire line goes in "Field"
    .option("header", False)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# xfm_RecId: add InputRowNumber, filter RECORD_ID in {PHD,DET,PTR}, parse columns
w_inrow = Window.orderBy(F.monotonically_increasing_id())
df_RecId_withrow = (
    df_Payable40
    .withColumn("InputRowNumber", F.row_number().over(w_inrow))
    .withColumn("RECORD_ID", F.substring(F.col("Field"), 1, 3))
    .withColumn("SEQUENCE_NO", F.substring(F.col("Field"), 4, 7))
    .withColumn("RESTOFTHE_FIELDS", F.col("Field"))
)
df_xfm_RecId = df_RecId_withrow.filter(F.col("RECORD_ID").isin(["PHD","DET","PTR"])) \
    .select(
        "RECORD_ID",
        "SEQUENCE_NO",
        "RESTOFTHE_FIELDS",
        "InputRowNumber"
    )

# Sort_InRowNum: sort by InputRowNumber ascending
df_Sort_InRowNum = df_xfm_RecId.orderBy("InputRowNumber").select(
    "RECORD_ID","SEQUENCE_NO","RESTOFTHE_FIELDS","InputRowNumber"
)

# ------------------------------------------------------------------------------
# Xfm_PHD_PTR Transformer (row-by-row stage-variable logic).
# Because of the complex stage-variable dependencies (previous row, etc.),
# collect to local, run stage-variable logic in Python, then recreate a DataFrame,
# then split into Lnk_PHD, Lnk_PTR, Lnk_XfmDet by constraints.
# ------------------------------------------------------------------------------

pdf_sort = df_Sort_InRowNum.toPandas()
out_rows = []
prevRow = "0"
Key = 1
PrevKey = 1

for i in range(len(pdf_sort)):
    row = pdf_sort.iloc[i]
    REC_ID = row["RECORD_ID"]
    rest = row["RESTOFTHE_FIELDS"] if row["RESTOFTHE_FIELDS"] is not None else ""
    
    # Stage variable CurrRow
    CurrRow = REC_ID if REC_ID is not None else "0"
    
    # Evaluate Key (DataStage logic):
    # If InputRowNumber=1 and CurrRow='PHD' Then Key
    # Else If CurrRow<>PrevRow And CurrRow<>'PHD' Then Key
    # Else If CurrRow<>PrevRow And CurrRow<>'DET' Then Key+1
    # Else If CurrRow=PrevRow Then PrevKey
    # Else Key+1
    # Because the original logic is quite intricate, replicate exactly:
    # Original expression:
    #  If PHD_PTR.InputRowNumber = 1 and CurrRow = 'PHD' Then Key 
    #  Else If CurrRow <> PrevRow And CurrRow <> 'PHD' Then Key 
    #  Else If CurrRow <> PrevRow And CurrRow <> 'DET' Then Key + 1 
    #  Else If CurrRow = PrevRow Then PrevKey 
    #  Else Key + 1
    # This is a chain implying if first row is 'PHD', keep Key as is,
    # if row breaks from 'PHD' or 'DET' in some pattern, etc. 
    # We'll interpret carefully in sequence:
    newKey = Key
    if (row["InputRowNumber"] == 1) and (CurrRow == "PHD"):
        newKey = Key
    elif (CurrRow != prevRow) and (CurrRow != "PHD"):
        newKey = Key
    elif (CurrRow != prevRow) and (CurrRow != "DET"):
        newKey = Key + 1
    elif (CurrRow == prevRow):
        newKey = PrevKey
    else:
        newKey = Key + 1
    
    # Stage var svNetGrosDrugCstAmtCalc etc. We'll define a helper to replicate char funk:
    # Many repeated patterns: check last char for '}' => '-0', 'J' => '-1', ...
    # We'll do a small dictionary:
    map_lastchar = {
        '}':'-0','J':'-1','K':'-2','L':'-3','M':'-4','N':'-5','O':'-6','P':'-7','Q':'-8','R':'-9',
        '{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8'
    }
    def decode_signed_field(full_str):
        # We cannot define a separate function, so nesting inline:
        if not full_str or len(full_str)==0:
            return "0"
        lastc = full_str[-1]
        sign_str = map_lastchar.get(lastc,"9")
        return sign_str
    
    # We'll parse the fields needed by the stage variables:
    # svFileCrtDt
    svFileCrtDt_sub = rest[44:44+8]  # DataStage is 1-based; Python is 0-based => rest[45,8] => rest[44:52]
    svFileCrtDt_trim = trim(svFileCrtDt_sub)
    # Format to yyyy-mm-dd
    # e.g. first 4 => year, next 2 => mm, next 2 => dd
    fileCrtDt_yyyy = svFileCrtDt_trim[:4]
    fileCrtDt_mm = svFileCrtDt_trim[4:6]
    fileCrtDt_dd = svFileCrtDt_trim[6:8]
    stage_svFileCrtDt = fileCrtDt_yyyy + "-" + fileCrtDt_mm + "-" + fileCrtDt_dd

    # svFileCrtTime
    svFileCrtTime_sub = rest[52:52+6]
    svFileCrtTime_trim = trim(svFileCrtTime_sub)
    hh = svFileCrtTime_trim[:2]
    nn = svFileCrtTime_trim[2:4]
    ss = svFileCrtTime_trim[4:6]
    stage_svFileCrtTime = hh + ":" + nn + ":" + ss

    # Example of one numeric decode: "svNetGrosDrugCstAmt" => from offset [31,14]
    # We do python slicing => rest[30:30+14]
    part_31_14 = rest[30:30+14]
    calc_31_14 = decode_signed_field(part_31_14)
    if len(calc_31_14) > 1:
        # negative
        rep_31_14 = part_31_14[:-1] + calc_31_14[1:]
        val_31_14 = "-" + str(float(rep_31_14))
    else:
        rep_31_14 = part_31_14[:-1] + calc_31_14
        val_31_14 = str(float(rep_31_14))

    # Similarly for netGrosDrugCstAbove => offset [45,14] => rest[44:44+14]? Actually 1-based => [45,14] => rest[44:58]
    part_45_14 = rest[44:44+14]
    calc_45_14 = decode_signed_field(part_45_14)
    if len(calc_45_14) > 1:
        rep_45_14 = part_45_14[:-1] + calc_45_14[1:]
        val_45_14 = "-" + str(float(rep_45_14))
    else:
        rep_45_14 = part_45_14[:-1] + calc_45_14
        val_45_14 = str(float(rep_45_14))

    # svNetTotGrosDrugCstAmt1 => offset [59,14] => rest[58:58+14]
    part_59_14 = rest[58:58+14]
    calc_59_14 = decode_signed_field(part_59_14)
    if len(calc_59_14) > 1:
        rep_59_14 = part_59_14[:-1] + calc_59_14[1:]
        val_59_14 = "-" + str(float(rep_59_14))
    else:
        rep_59_14 = part_59_14[:-1] + calc_59_14
        val_59_14 = str(float(rep_59_14))

    # svNetLowIncmCstSharingAmt => offset [73,14] => rest[72:72+14]
    part_73_14 = rest[72:72+14]
    calc_73_14 = decode_signed_field(part_73_14)
    if len(calc_73_14) > 1:
        rep_73_14 = part_73_14[:-1] + calc_73_14[1:]
        val_73_14 = "-" + str(float(rep_73_14))
    else:
        rep_73_14 = part_73_14[:-1] + calc_73_14
        val_73_14 = str(float(rep_73_14))

    # svNetCovPlnPdAmt => offset [87,14] => rest[86:86+14]
    part_87_14 = rest[86:86+14]
    calc_87_14 = decode_signed_field(part_87_14)
    if len(calc_87_14) > 1:
        rep_87_14 = part_87_14[:-1] + calc_87_14[1:]
        val_87_14 = "-" + str(float(rep_87_14))
    else:
        rep_87_14 = part_87_14[:-1] + calc_87_14
        val_87_14 = str(float(rep_87_14))

    # svSubmitDueAmt => offset [109,14] => rest[108:108+14]
    part_109_14 = rest[108:108+14]
    calc_109_14 = decode_signed_field(part_109_14)
    if len(calc_109_14) > 1:
        rep_109_14 = part_109_14[:-1] + calc_109_14[1:]
        val_109_14 = "-" + str(float(rep_109_14))
    else:
        rep_109_14 = part_109_14[:-1] + calc_109_14
        val_109_14 = str(float(rep_109_14))

    # Construct final columns
    # Keep all stage var expansions so we can route them to constraints PHD, PTR, DET
    # DataStage updates Key, PrevKey, PrevRow for next iteration:
    finalKey = newKey
    # update for next iteration:
    prevRow = CurrRow
    PrevKey = finalKey
    Key = finalKey

    # We also parse partial columns for PHD output link, etc.
    # We'll store everything in out_rows and filter later at the next stage, or we can just store them directly.
    out_dict = {
      "RECORD_ID": REC_ID,
      "SEQUENCE_NO": row["SEQUENCE_NO"],
      "RESTOFTHE_FIELDS": rest,
      "InputRowNumber": row["InputRowNumber"],
      "Key": finalKey,
      # For PHD link:
      "FILE_ID_PHD": trim(rest[18:18+16]),
      "PLN_BNF_PCKG_SEQ_ID_PHD": trim(rest[3:3+7]),
      "SRC_SYS_CD_PHD": "CMS",
      "CRT_RUN_CYC_EXCTN_DT_SK_PHD": CurrRunCycleDate,
      "LAST_UPDT_RUN_CYC_EXCTN_DT_SK_PHD": CurrRunCycleDate,
      "CMS_CNTR_ID_PHD": trim(rest[10:10+5]),
      "PLN_BNF_PCKG_ID_PHD": trim(rest[15:15+3]),
      "AS_OF_YR_PHD": trim(rest[38:38+4]),
      "AS_OF_MO_PHD": trim(rest[42:42+2]),
      "FILE_CRTN_DT_PHD": stage_svFileCrtDt,
      "FILE_CRTN_TM_PHD": stage_svFileCrtTime,
      "RPT_ID_PHD": trim(rest[58:58+5]),
      # For PTR link:
      "CMS_CNTR_ID_PTR": trim(rest[10:10+5]),
      "PLN_BNF_PCKG_SEQ_ID_PTR": trim(rest[3:3+7]),
      "PLN_BNF_PCKG_ID_PTR": trim(rest[15:15+3]),
      "DRUG_COV_STTUS_CD_TX_PTR": trim(rest[18:18+1]),
      "BNFCRY_CT_PTR": trim(rest[19:19+11]),
      "NET_GROS_DRUG_CST_BELOW_AMT_PTR": val_31_14,
      "NET_GROS_DRUG_CST_ABOVE_AMT_PTR": val_45_14,
      "NET_TOT_GROS_DRUG_CST_AMT_PTR": val_59_14,
      "NET_LOW_INCM_CST_SHARING_AMT_PTR": val_73_14,
      "NET_COV_PLN_PD_AMT_PTR": val_87_14,
      "TOT_DTL_RCRD_CT_PTR": trim(rest[100:100+8]),
      "SUBMT_DUE_AMT_PTR": val_109_14,
      "CRT_RUN_CYC_EXCTN_SK_PTR": CurrRunCycle,
      "LAST_UPDT_RUN_CYC_EXCTN_SK_PTR": CurrRunCycle,
    }
    out_rows.append(out_dict)

# Create a single big DataFrame from out_rows
schema_cols = list(out_rows[0].keys()) if out_rows else []
df_Xfm_PHD_PTR_full = spark.createDataFrame(out_rows).select(schema_cols)

# Now apply constraints to generate 3 separate outputs (PHD, PTR, DET)
df_Lnk_PHD = df_Xfm_PHD_PTR_full.filter(F.col("RECORD_ID") == "PHD").select(
    F.col("Key").alias("Key"),
    F.col("FILE_ID_PHD").alias("FILE_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID_PHD").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD_PHD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK_PHD").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK_PHD").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CMS_CNTR_ID_PHD").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID_PHD").alias("PLN_BNF_PCKG_ID"),
    F.col("AS_OF_YR_PHD").alias("AS_OF_YR"),
    F.col("AS_OF_MO_PHD").alias("AS_OF_MO"),
    F.col("FILE_CRTN_DT_PHD").alias("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM_PHD").alias("FILE_CRTN_TM"),
    F.col("RPT_ID_PHD").alias("RPT_ID")
)

df_Lnk_PTR = df_Xfm_PHD_PTR_full.filter(F.col("RECORD_ID") == "PTR").select(
    F.col("CMS_CNTR_ID_PTR").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID_PTR").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("PLN_BNF_PCKG_ID_PTR").alias("PLN_BNF_PCKG_ID"),
    F.col("DRUG_COV_STTUS_CD_TX_PTR").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT_PTR").alias("BNFCRY_CT"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT_PTR").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT_PTR").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT_PTR").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT_PTR").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT_PTR").alias("NET_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT_PTR").alias("TOT_DTL_RCRD_CT"),
    F.col("SUBMT_DUE_AMT_PTR").alias("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK_PTR").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK_PTR").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Lnk_DET = df_Xfm_PHD_PTR_full.filter(F.col("RECORD_ID") == "DET").select(
    F.col("Key").alias("Key"),
    F.col("RECORD_ID").alias("RECORD_ID"),
    F.col("SEQUENCE_NO").alias("SEQUENCE_NO"),
    F.col("RESTOFTHE_FIELDS").alias("RESTOFTHE_FIELDS")
)

# ------------------------------------------------------------------------------
# Next stage: Merge_CntrNoSeqNo (PxMerge) => merges df_Lnk_PHD (master) and df_Lnk_PTR (update).
# Parameter says "dropBadMasters" => means only keep master rows that find a match => effectively an inner join.
# Key => CMS_CNTR_ID, PLN_BNF_PCKG_ID, PLN_BNF_PCKG_SEQ_ID
# Output columns are combined from PHD and PTR.
# ------------------------------------------------------------------------------
join_expr_merge = [
   (df_Lnk_PHD["CMS_CNTR_ID"] == df_Lnk_PTR["CMS_CNTR_ID"]) &
   (df_Lnk_PHD["PLN_BNF_PCKG_ID"] == df_Lnk_PTR["PLN_BNF_PCKG_ID"]) &
   (df_Lnk_PHD["PLN_BNF_PCKG_SEQ_ID"] == df_Lnk_PTR["PLN_BNF_PCKG_SEQ_ID"])
]
df_Merge_CntrNoSeqNo = (
    df_Lnk_PHD.alias("Lnk_PHD")
    .join(df_Lnk_PTR.alias("Lnk_PTR"), join_expr_merge, "inner")
    .select(
        F.col("Lnk_PHD.Key").alias("Key"),
        F.col("Lnk_PHD.FILE_ID").alias("FILE_ID"),
        F.col("Lnk_PHD.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("Lnk_PHD.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_PHD.CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_PHD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_PHD.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("Lnk_PHD.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("Lnk_PHD.AS_OF_YR").alias("AS_OF_YR"),
        F.col("Lnk_PHD.AS_OF_MO").alias("AS_OF_MO"),
        F.col("Lnk_PHD.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("Lnk_PHD.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("Lnk_PHD.RPT_ID").alias("RPT_ID"),
        F.col("Lnk_PTR.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("Lnk_PTR.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("Lnk_PTR.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("Lnk_PTR.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("Lnk_PTR.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("Lnk_PTR.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("Lnk_PTR.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("Lnk_PTR.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("Lnk_PTR.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("Lnk_PTR.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_PTR.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# ------------------------------------------------------------------------------
# Lkp_CntrID (PxLookup) => left join with df_Db2_PDE_PARTD_PAYMT_RECON_CNTR_F
# on (CMS_CNTR_ID, FILE_ID).
# ------------------------------------------------------------------------------
df_Lkp_CntrID = (
    df_Merge_CntrNoSeqNo.alias("Lnk_MergeCntr")
    .join(
        df_Db2_PDE_PARTD_PAYMT_RECON_CNTR_F.alias("Lnk_TableIn"),
        (
            (F.col("Lnk_MergeCntr.CMS_CNTR_ID") == F.col("Lnk_TableIn.CMS_CNTR_ID")) &
            (F.col("Lnk_MergeCntr.FILE_ID") == F.col("Lnk_TableIn.FILE_ID"))
        ),
        "left"
    )
    .select(
        F.col("Lnk_MergeCntr.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("Lnk_TableIn.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("Lnk_TableIn.PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
        F.col("Lnk_MergeCntr.Key").alias("Key"),
        F.col("Lnk_MergeCntr.FILE_ID").alias("FILE_ID"),
        F.col("Lnk_MergeCntr.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("Lnk_MergeCntr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_MergeCntr.CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_MergeCntr.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_MergeCntr.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("Lnk_MergeCntr.AS_OF_YR").alias("AS_OF_YR"),
        F.col("Lnk_MergeCntr.AS_OF_MO").alias("AS_OF_MO"),
        F.col("Lnk_MergeCntr.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("Lnk_MergeCntr.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("Lnk_MergeCntr.RPT_ID").alias("RPT_ID"),
        F.col("Lnk_MergeCntr.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("Lnk_MergeCntr.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("Lnk_MergeCntr.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("Lnk_MergeCntr.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("Lnk_MergeCntr.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("Lnk_MergeCntr.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("Lnk_MergeCntr.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("Lnk_MergeCntr.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("Lnk_MergeCntr.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("Lnk_MergeCntr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_MergeCntr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Copy stage => produce two outputs:
df_Lnk_Remove_Dup = df_Lkp_CntrID.select(
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD")
)

df_Lnk_AllCol_Join = df_Lkp_CntrID.select(
    F.col("CMS_CNTR_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
    F.col("Key"),
    F.col("FILE_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PLN_BNF_PCKG_ID"),
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

# Remove_Duplicates (PxRemDup)
df_Remove_Duplicates = dedup_sort(
    df_Lnk_Remove_Dup,
    ["FILE_ID","CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","SRC_SYS_CD"],
    [("FILE_ID","A"),("CNTR_SEQ_ID","A"),("PLN_BNF_PCKG_SEQ_ID","A"),("SRC_SYS_CD","A")]
)

# Jn1_NKey => left outer join with df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F
df_Jn1_NKey = (
    df_Remove_Duplicates.alias("Lnk_RmDup")
    .join(
        df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F.alias("Lnk_KTableIn"),
        ["FILE_ID","CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","SRC_SYS_CD"],
        "left"
    )
    .select(
        F.col("Lnk_RmDup.FILE_ID").alias("FILE_ID"),
        F.col("Lnk_RmDup.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("Lnk_RmDup.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("Lnk_RmDup.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_KTableIn.CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_KTableIn.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK")
    )
)

# Rename for next Transformer input
df_Lnk_Xfm1 = df_Jn1_NKey

# Transformer => stage var: NextSurrogateKey => we call SurrogateKeyGen on rows where PDE_PARTD_...=0 or null
df_Lnk_Xfm1_cached = df_Lnk_Xfm1.cache()

df_for_insert = df_Lnk_Xfm1_cached.filter(
    (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").isNull()) | 
    (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK") == 0)
).select(
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("TEMP_SK")
)

df_for_insert_enriched = df_for_insert.withColumnRenamed("TEMP_SK","PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK")
# Now apply SurrogateKeyGen so that PDE_PARTD_..._SK is replaced by a new surrogate if 0
df_enriched = df_for_insert_enriched
df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK",
    <schema>,
    <secret_name>
)

# This output goes to "Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F_Load" stage => We must do a Merge (upsert).
# The primary key is (FILE_ID,CNTR_SEQ_ID,PLN_BNF_PCKG_SEQ_ID,SRC_SYS_CD).
# We first write df_enriched to a staging table, then we do MERGE.
temp_table_name = "STAGING.PdePartdPaymtReconCntrPlnBnfPckgF_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_enriched.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql_load = f"""
MERGE {EDWOwner}.K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F AS T
USING {temp_table_name} AS S
ON
    T.FILE_ID = S.FILE_ID
    AND T.CNTR_SEQ_ID = S.CNTR_SEQ_ID
    AND T.PLN_BNF_PCKG_SEQ_ID = S.PLN_BNF_PCKG_SEQ_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK = S.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK
WHEN NOT MATCHED THEN
    INSERT (
      FILE_ID,
      CNTR_SEQ_ID,
      PLN_BNF_PCKG_SEQ_ID,
      SRC_SYS_CD,
      CRT_RUN_CYC_EXCTN_DT_SK,
      PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK
    )
    VALUES (
      S.FILE_ID,
      S.CNTR_SEQ_ID,
      S.PLN_BNF_PCKG_SEQ_ID,
      S.SRC_SYS_CD,
      S.CRT_RUN_CYC_EXCTN_DT_SK,
      S.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK
    );
"""
execute_dml(merge_sql_load, jdbc_url, jdbc_props)

# The other output link from that Transformer => "Lnk_Jn" => PDE_PARTD_... != 0 => pass original columns
df_for_update = df_Lnk_Xfm1_cached.filter(
    (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").isNotNull()) &
    (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK") != 0)
).select(
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK")
)

df_Lnk_Jn = df_for_update.withColumnRenamed("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK","svPdeAcctgRptSbmCntrSK")

# Jn2Nkey => innerjoin between df_Lnk_AllCol_Join (link1) and df_Lnk_Jn (link2) on (FILE_ID, CNTR_SEQ_ID, PLN_BNF_PCKG_SEQ_ID, SRC_SYS_CD)
df_Jn2Nkey = (
    df_Lnk_AllCol_Join.alias("Lnk_AllCol_Join")
    .join(
        df_Lnk_Jn.alias("Lnk_Jn"),
        ["FILE_ID","CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","SRC_SYS_CD"],
        "inner"
    )
    .select(
        F.col("Lnk_AllCol_Join.Key").alias("Key"),
        F.col("Lnk_Jn.svPdeAcctgRptSbmCntrSK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
        F.col("Lnk_AllCol_Join.FILE_ID").alias("FILE_ID"),
        F.col("Lnk_AllCol_Join.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("Lnk_AllCol_Join.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("Lnk_AllCol_Join.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_Jn.CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_AllCol_Join.PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
        F.col("Lnk_AllCol_Join.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("Lnk_AllCol_Join.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("Lnk_AllCol_Join.AS_OF_YR").alias("AS_OF_YR"),
        F.col("Lnk_AllCol_Join.AS_OF_MO").alias("AS_OF_MO"),
        F.col("Lnk_AllCol_Join.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        F.col("Lnk_AllCol_Join.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        F.col("Lnk_AllCol_Join.RPT_ID").alias("RPT_ID"),
        F.col("Lnk_AllCol_Join.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("Lnk_AllCol_Join.BNFCRY_CT").alias("BNFCRY_CT"),
        F.col("Lnk_AllCol_Join.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("Lnk_AllCol_Join.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("Lnk_AllCol_Join.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("Lnk_AllCol_Join.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("Lnk_AllCol_Join.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("Lnk_AllCol_Join.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        F.col("Lnk_AllCol_Join.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("Lnk_AllCol_Join.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Xfm_DET_SK_Map => just passing columns to next. We'll rename it so we can feed next "Join_Key" stage
df_Xfm_DET_SK_Map = df_Jn2Nkey.select(
    F.col("Key").alias("Key"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AS_OF_YR").alias("AS_OF_YR"),
    F.col("AS_OF_MO").alias("AS_OF_MO"),
    F.col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("RPT_ID").alias("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Write to Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F_Extr (PxSequentialFile)
# The link: Lnk_SeqExtr => columns from Xfm_DET_SK_Map with the same ordering
df_SeqExtr = df_Xfm_DET_SK_Map.select(
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
    F.col("FILE_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_SK"),
    F.col("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID"),
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

write_files(
    df_SeqExtr,
    f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Finally, Join_Key => we join Xfm_DET (that is df_Xfm_DET) with Xfm_DET_SK_Map again by Key => but from the JSON:
# Actually this stage uses Lnk_JnPlnPkcg from Xfm_DET, Lnk_Jn from Xfm_DET_SK_Map => an inner join on Key
# The output => Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F
# We must go back to df_Xfm_DET => that is in variable df_Lnk_XfmDet from earlier
df_Xfm_DET_cached = df_Lnk_DET.cache()

# Xfm_DET also had transformations. The Transformer is "Xfm_DET", which is similarly complicated. We replicate that row-by-row logic:
# The JSON shows it has stage variables for negative decoding. We'll do a similar pattern as before.

pdf_det = df_Xfm_DET_cached.toPandas()
out_det_rows = []
for i in range(len(pdf_det)):
    r = pdf_det.iloc[i]
    rest = r["RESTOFTHE_FIELDS"] if r["RESTOFTHE_FIELDS"] else ""
    def decode_signed_field2(full_str):
        map_lastchar2 = {
            '}':'-0','J':'-1','K':'-2','L':'-3','M':'-4','N':'-5','O':'-6','P':'-7','Q':'-8','R':'-9',
            '{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8'
        }
        if not full_str:
            return "0"
        lx = full_str[-1]
        return map_lastchar2.get(lx,"9")
    
    # parse fields
    part_52_14 = rest[51:51+14]
    calc_52_14 = decode_signed_field2(part_52_14)
    if len(calc_52_14)>1:
        rep_52_14 = part_52_14[:-1] + calc_52_14[1:]
        val_52_14 = "-" + str(float(rep_52_14))
    else:
        rep_52_14 = part_52_14[:-1] + calc_52_14
        val_52_14 = str(float(rep_52_14))

    part_66_14 = rest[65:65+14]
    c66 = decode_signed_field2(part_66_14)
    if len(c66)>1:
        rr_66 = part_66_14[:-1]+c66[1:]
        vv_66 = "-"+str(float(rr_66))
    else:
        rr_66 = part_66_14[:-1]+c66
        vv_66 = str(float(rr_66))

    part_80_14 = rest[79:79+14]
    c80 = decode_signed_field2(part_80_14)
    if len(c80)>1:
        rr_80 = part_80_14[:-1]+c80[1:]
        vv_80 = "-"+str(float(rr_80))
    else:
        rr_80 = part_80_14[:-1]+c80
        vv_80 = str(float(rr_80))

    part_94_14 = rest[93:93+14]
    c94 = decode_signed_field2(part_94_14)
    if len(c94)>1:
        rr_94 = part_94_14[:-1]+c94[1:]
        vv_94 = "-"+str(float(rr_94))
    else:
        rr_94 = part_94_14[:-1]+c94
        vv_94 = str(float(rr_94))

    part_108_14 = rest[107:107+14]
    c108 = decode_signed_field2(part_108_14)
    if len(c108)>1:
        rr_108 = part_108_14[:-1]+c108[1:]
        vv_108 = "-"+str(float(rr_108))
    else:
        rr_108 = part_108_14[:-1]+c108
        vv_108 = str(float(rr_108))

    part_127_14 = rest[126:126+14]
    c127 = decode_signed_field2(part_127_14)
    if len(c127)>1:
        rr_127 = part_127_14[:-1]+c127[1:]
        vv_127 = "-"+str(float(rr_127))
    else:
        rr_127 = part_127_14[:-1]+c127
        vv_127 = str(float(rr_127))

    out_d = {
       "Key": r["Key"],
       "PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK": 0,  # per stage property
       "DTL_SEQ_ID": trim(rest[3:3+7]),
       "SRC_SYS_CD": "CMS",
       "CRT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
       "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
       "CUR_MCARE_BNFCRY_ID": trim(rest[11:11+20]),
       "LAST_SUBMT_MCARE_BNFCRY_ID": trim(rest[31:31+20]),
       "DRUG_COV_STTUS_CD_TX": trim(rest[10:10+1]),
       "NET_GROS_DRUG_CST_BELOW_AMT": val_52_14,
       "NET_GROS_DRUG_CST_ABOVE_AMT": vv_66,
       "NET_TOT_GROS_DRUG_CST_AMT": vv_80,
       "NET_LOW_INCM_CST_SHARING_AMT": vv_94,
       "NET_COV_PLN_PD_AMT": vv_108,
       "SUBMT_CMS_CNTR_ID": trim(rest[121:121+5]),
       "SUBMT_DUE_AMT": vv_127,
       "CRT_RUN_CYC_EXCTN_SK": CurrRunCycle,
       "LAST_UPDT_RUN_CYC_EXCTN_SK": CurrRunCycle
    }
    out_det_rows.append(out_d)

df_Xfm_DET_out = spark.createDataFrame(out_det_rows)

# That final transformer output correlates to "Lnk_JnPlnPkcg"
df_Lnk_JnPlnPkcg = df_Xfm_DET_out.select(
    F.col("Key"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Join_Key => an innerjoin on Key with df_Xfm_DET_SK_Map => output => PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F
df_Join_Key = (
    df_Lnk_JnPlnPkcg.alias("Lnk_JnPlnPkcg")
    .join(
        df_Xfm_DET_SK_Map.alias("Lnk_Jn"),
        [ "Key" ],
        "inner"
    )
    .select(
        F.col("Lnk_JnPlnPkcg.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK"),
        F.col("Lnk_Jn.FILE_ID").alias("FILE_ID"),
        F.col("Lnk_Jn.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("Lnk_Jn.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("Lnk_JnPlnPkcg.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
        F.col("Lnk_JnPlnPkcg.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_JnPlnPkcg.CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_JnPlnPkcg.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_Jn.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
        F.col("Lnk_Jn.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("Lnk_Jn.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("Lnk_JnPlnPkcg.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
        F.col("Lnk_JnPlnPkcg.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
        F.col("Lnk_JnPlnPkcg.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("Lnk_JnPlnPkcg.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("Lnk_JnPlnPkcg.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("Lnk_JnPlnPkcg.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("Lnk_JnPlnPkcg.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("Lnk_JnPlnPkcg.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("Lnk_JnPlnPkcg.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
        F.col("Lnk_JnPlnPkcg.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("Lnk_JnPlnPkcg.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_JnPlnPkcg.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Write to Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F (PxSequentialFile)
df_Seq_DTL_Extr = df_Join_Key.select(
    "PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK",
    "FILE_ID",
    "CNTR_SEQ_ID",
    "PLN_BNF_PCKG_SEQ_ID",
    "DTL_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK",
    "CMS_CNTR_ID",
    "PLN_BNF_PCKG_ID",
    "CUR_MCARE_BNFCRY_ID",
    "LAST_SUBMT_MCARE_BNFCRY_ID",
    "DRUG_COV_STTUS_CD_TX",
    "NET_GROS_DRUG_CST_BELOW_AMT",
    "NET_GROS_DRUG_CST_ABOVE_AMT",
    "NET_TOT_GROS_DRUG_CST_AMT",
    "NET_LOW_INCM_CST_SHARING_AMT",
    "NET_COV_PLN_PD_AMT",
    "SUBMT_CMS_CNTR_ID",
    "SUBMT_DUE_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_Seq_DTL_Extr,
    f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)