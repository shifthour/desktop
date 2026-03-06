# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdeAcctgRptSubmtCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_RECEIVABLE* and loads the data into EDW Table PDE_REC_RPT_SUBMT_CNTR_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                  Jaideep Mankala             02/24/2022                                   
# MAGIC 
# MAGIC Rekha Radhakrishna                     2022-04-15           408843             Added Key column in Merge Stage inorder to                               EnterpriseDev3                   Jaideep Mankala             04/30/2022
# MAGIC                                                                                                             avoid duplicates.


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
InFile = get_widget_value('InFile','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read from DB2ConnectorPX (Db2_K_PDE_REC_RPT_SUBMT_CNTR_F)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F = (
    "SELECT \n\nFILE_ID,\nSUBMT_CNTR_SEQ_ID,\nSRC_SYS_CD,\nCRT_RUN_CYC_EXCTN_DT_SK,\nPDE_REC_RPT_SUBMT_CNTR_SK\n\n"
    f"FROM {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_F"
)
df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F)
    .load()
)

# Read PxSequentialFile (Payable40) [landing]
schema_Payable40 = StructType([StructField("Field", StringType(), True)])
df_Payable40 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_Payable40)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# xfm_RecId (CTransformerStage)
# @INROWNUM replication with monotonically_increasing_id()+1
df_xfm_RecId = (
    df_Payable40
    .withColumn("RECORD_ID", F.substring(F.col("Field"), 1, 3))
    .withColumn("SEQUENCE_NO", F.substring(F.col("Field"), 4, 7))
    .withColumn("RESTOFTHE_FIELDS", F.col("Field"))
    .withColumn("InputRowNumber", (F.monotonically_increasing_id() + 1))
)

# Sort_InRowNum (PxSort) stable sort on InputRowNumber ascending
df_Sort_InRowNum = df_xfm_RecId.sort("InputRowNumber").select(
    F.col("RECORD_ID"),
    F.col("SEQUENCE_NO"),
    F.col("RESTOFTHE_FIELDS"),
    F.col("InputRowNumber")
)

# Xfm_CHD_CTR (CTransformerStage) with row-by-row stage variable logic
# Replicating row-based logic via pandas to avoid defining any functions or UDFs:
df_Sort_InRowNum_pandas = df_Sort_InRowNum.toPandas()

# We will produce three outputs: Lnk_CHD, Lnk_CTR, and RHD_RTR, each a list of dict rows
rows_chd = []
rows_ctr = []
rows_rhd_rtr = []

svFileCrtDt_list = []
svFileCrtTime_list = []
CurrRow_list = []
Key_list = []
PrevKey_list = []
PrevRow_list = []
svCurMoGrosDrugCstBelowAmt_list = []
svCurMoGrosDrugCstAboveAmt_list = []
svCurMoTotGrosDrugCstAmt_list = []
svCurMoLowIncmCstSharingAmt_list = []
svCurMoCovPlnPdAmt_list = []
svCurMoSubmtDueAmt_list = []

# Initialize stage variable memory
curr_key = 1
prev_key = 1
prev_rowval = "0"

def map_negative_number(text_val):
    # Emulate the large logic used for decoding last char
    # This is straightforward character mapping:
    last_char = text_val[-1:] if len(text_val) > 0 else ""
    mapping = {
        '}': '-0','J': '-1','K': '-2','L': '-3','M': '-4','N': '-5','O': '-6','P': '-7','Q': '-8','R': '-9',
        '{': '0','A': '1','B': '2','C': '3','D': '4','E': '5','F': '6','G': '7','H': '8','I': '9'
    }
    if last_char in mapping:
        sign_piece = mapping[last_char][0]  # either '-' or digit
        digit_piece = mapping[last_char][1:] if len(mapping[last_char])>1 else mapping[last_char]
        if sign_piece == '-':
            # negative scenario
            return "-" + text_val[:-1].replace(last_char, digit_piece)
        else:
            # positive scenario
            return text_val[:-1].replace(last_char, digit_piece)
    return text_val  # fallback if not matched

for i, row in df_Sort_InRowNum_pandas.iterrows():
    record_id = row["RECORD_ID"]
    rest_of_fields = row["RESTOFTHE_FIELDS"] if row["RESTOFTHE_FIELDS"] is not None else ""
    input_row_num = row["InputRowNumber"]
    # Evaluate stage variables in order:

    # svFileCrtDt
    raw_dt = rest_of_fields[41:49] if len(rest_of_fields)>=49 else ""
    trimmed_dt = trim(raw_dt)
    # "YYYY-MM-DD"
    stageval_svFileCrtDt = trim(trimmed_dt[0:4]) + "-" + trim(trimmed_dt[4:6]) + "-" + trim(trimmed_dt[6:8]) if len(trimmed_dt)>=8 else ""

    # svFileCrtTime
    raw_tm = rest_of_fields[49:55] if len(rest_of_fields)>=55 else ""
    trimmed_tm = trim(raw_tm)
    # "HH:MM:SS"
    stageval_svFileCrtTime = trim(trimmed_tm[0:2]) + ":" + trim(trimmed_tm[2:4]) + ":" + trim(trimmed_tm[4:6]) if len(trimmed_tm)>=6 else ""

    # CurrRow
    stageval_CurrRow = record_id

    # Key logic
    old_curr_key = curr_key
    old_prev_key = prev_key
    old_prev_row = prev_rowval
    if input_row_num == 1 and stageval_CurrRow == 'CHD':
        # Key remains curr_key
        pass
    elif stageval_CurrRow != old_prev_row and stageval_CurrRow != 'CHD':
        # then Key remains
        pass
    elif stageval_CurrRow != old_prev_row and stageval_CurrRow != 'DET':
        curr_key = curr_key + 1
    elif stageval_CurrRow == old_prev_row:
        curr_key = prev_key
    else:
        curr_key = curr_key + 1

    # replicate the 'PrevKey' = Key
    stageval_Key = curr_key
    stageval_PrevKey = stageval_Key

    # replicate 'PrevRow' = 'CurrRow'
    stageval_PrevRow = stageval_CurrRow

    # Now do the negative conversions:
    # svCurMoGrosDrugCstBelowAmt
    raw_below_amt = rest_of_fields[36:50] if len(rest_of_fields)>=50 else ""
    mappedChar_below = map_negative_number(raw_below_amt)
    is_negative_below = (len(mappedChar_below)>1 and mappedChar_below[0]=='-')
    if is_negative_below:
        numeric_part = mappedChar_below[1:]
    else:
        numeric_part = mappedChar_below
    # Because the code attempts Ereplace, effectively removing the last char then replacing it with the mapped digit
    # We'll interpret numeric_part as float
    try:
        val_below = float(numeric_part)
        if is_negative_below:
            val_below = -abs(val_below)
    except:
        val_below = 0.0

    # svCurMoGrosDrugCstAboveAmt
    raw_above_amt = rest_of_fields[50:64] if len(rest_of_fields)>=64 else ""
    mappedChar_above = map_negative_number(raw_above_amt)
    is_negative_above = (len(mappedChar_above)>1 and mappedChar_above[0]=='-')
    if is_negative_above:
        numeric_part2 = mappedChar_above[1:]
    else:
        numeric_part2 = mappedChar_above
    try:
        val_above = float(numeric_part2)
        if is_negative_above:
            val_above = -abs(val_above)
    except:
        val_above = 0.0

    # svCurMoTotGrosDrugCstAmt
    raw_tot_amt = rest_of_fields[64:78] if len(rest_of_fields)>=78 else ""
    mappedChar_tot = map_negative_number(raw_tot_amt)
    is_negative_tot = (len(mappedChar_tot)>1 and mappedChar_tot[0]=='-')
    if is_negative_tot:
        numeric_part3 = mappedChar_tot[1:]
    else:
        numeric_part3 = mappedChar_tot
    try:
        val_tot = float(numeric_part3)
        if is_negative_tot:
            val_tot = -abs(val_tot)
    except:
        val_tot = 0.0

    # svCurMoLowIncmCstSharingAmt
    raw_low_amt = rest_of_fields[78:92] if len(rest_of_fields)>=92 else ""
    mappedChar_low = map_negative_number(raw_low_amt)
    is_negative_low = (len(mappedChar_low)>1 and mappedChar_low[0]=='-')
    if is_negative_low:
        numeric_part4 = mappedChar_low[1:]
    else:
        numeric_part4 = mappedChar_low
    try:
        val_low = float(numeric_part4)
        if is_negative_low:
            val_low = -abs(val_low)
    except:
        val_low = 0.0

    # svCurMoCovPlnPdAmt
    raw_cov_amt = rest_of_fields[92:106] if len(rest_of_fields)>=106 else ""
    mappedChar_cov = map_negative_number(raw_cov_amt)
    is_negative_cov = (len(mappedChar_cov)>1 and mappedChar_cov[0]=='-')
    if is_negative_cov:
        numeric_part5 = mappedChar_cov[1:]
    else:
        numeric_part5 = mappedChar_cov
    try:
        val_cov = float(numeric_part5)
        if is_negative_cov:
            val_cov = -abs(val_cov)
    except:
        val_cov = 0.0

    # svCurMoSubmtDueAmt
    raw_due_amt = rest_of_fields[114:128] if len(rest_of_fields)>=128 else ""
    mappedChar_due = map_negative_number(raw_due_amt)
    is_negative_due = (len(mappedChar_due)>1 and mappedChar_due[0]=='-')
    if is_negative_due:
        numeric_part6 = mappedChar_due[1:]
    else:
        numeric_part6 = mappedChar_due
    try:
        val_due = float(numeric_part6)
        if is_negative_due:
            val_due = -abs(val_due)
    except:
        val_due = 0.0

    # Build final stage variable values:
    # update memory
    prev_key = stageval_PrevKey
    prev_rowval = stageval_PrevRow

    # Row-based constraints => produce link outputs
    # Lnk_CHD => "CHD_CTR.RECORD_ID = 'CHD'"
    if record_id == "CHD":
        rows_chd.append({
            "Key": stageval_Key,
            "PDE_REC_RPT_SUBMT_CNTR_SK": 0,  # WhereExpression=0 means pass as is
            "FILE_ID": trim(rest_of_fields[15:31]) if len(rest_of_fields)>=31 else "",
            "SUBMT_CNTR_SEQ_ID": row["SEQUENCE_NO"],
            "SRC_SYS_CD": "CMS",
            "CRT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
            "SUBMT_CMS_CNTR_ID": trim(rest_of_fields[10:15]) if len(rest_of_fields)>=15 else "",
            "AS_OF_YR": trim(rest_of_fields[35:39]) if len(rest_of_fields)>=39 else "",
            "AS_OF_MO": trim(rest_of_fields[39:41]) if len(rest_of_fields)>=41 else "",
            "FILE_CRTN_DT": stageval_svFileCrtDt,
            "FILE_CRTN_TM": stageval_svFileCrtTime,
            "RPT_ID": trim(rest_of_fields[55:60]) if len(rest_of_fields)>=60 else ""
        })

    # Lnk_CTR => "CHD_CTR.RECORD_ID = 'CTR'"
    if record_id == "CTR":
        rows_ctr.append({
            "SUBMT_CMS_CNTR_ID": trim(rest_of_fields[10:15]) if len(rest_of_fields)>=15 else "",
            "SUBMT_CNTR_SEQ_ID": row["SEQUENCE_NO"],
            "DRUG_COV_STTUS_CD_TX": trim(rest_of_fields[15:16]) if len(rest_of_fields)>=16 else "",
            "BNFCRY_CT": trim(rest_of_fields[16:27]) if len(rest_of_fields)>=27 else "",
            "CUR_MO_GROS_DRUG_CST_BELOW_AMT": val_below,
            "CUR_MO_GROS_DRUG_CST_ABOVE_AMT": val_above,
            "CUR_MO_TOT_GROS_DRUG_CST_AMT": val_tot,
            "CUR_MO_LOW_INCM_CST_SHARING_AMT": val_low,
            "CUR_MO_COV_PLN_PD_AMT": val_cov,
            "TOT_DTL_RCRD_CT": trim(rest_of_fields[106:114]) if len(rest_of_fields)>=114 else "",
            "CUR_MO_SUBMT_DUE_AMT": val_due,
            "CRT_RUN_CYC_EXCTN_SK": CurrRunCycle,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": CurrRunCycle
        })

    # RHD_RTR => "RECORD_ID in ('RHD','DET','RTR')"
    if record_id in ["RHD","DET","RTR"]:
        rows_rhd_rtr.append({
            "Key": stageval_Key,
            "RECORD_ID": record_id,
            "SEQUENCE_NO": row["SEQUENCE_NO"],
            "RESTOFTHE_FIELDS": rest_of_fields,
            "InputRowNumber": input_row_num
        })

# Convert to Spark DataFrames
df_Lnk_CHD = spark.createDataFrame(rows_chd)
df_Lnk_CTR = spark.createDataFrame(rows_ctr)
df_RHD_RTR = spark.createDataFrame(rows_rhd_rtr)

# Merge_CntrNoSeqNo (PxMerge) with two inputs => Lnk_CHD as master, Lnk_CTR as update
# DataStage merges the two by "SUBMT_CMS_CNTR_ID" & "SUBMT_CNTR_SEQ_ID"? 
# Actually the stage parameters show key: SUBMT_CMS_CNTR_ID asc, SUBMT_CNTR_SEQ_ID asc
# Implementation: We replicate that logic with an inner/left approach, but the stage says "dropBadMasters", "warnBadMasters", "warnBadUpdates".
# DataStage lumps them, we can do a "full outer" then pick non-null from each side. 
# However, the merge output mapping is straightforward: columns from Lnk_CHD plus from Lnk_CTR. 
# We will emulate it with a join on (SUBMT_CMS_CNTR_ID,SUBMT_CNTR_SEQ_ID).
# But the official "PxMerge" for "dropBadMasters" is effectively an inner join on columns that are primaryKey in both links. 
# Actually the job design merges on "key SUBMT_CMS_CNTR_ID asc key SUBMT_CNTR_SEQ_ID asc" => We do a full outer? The stage default is "dropBadMasters"? Typically that means if there's no master record, that row is dropped. 
# Because we have 2 input pins: "Lnk_CHD" is the "Master link", and "Lnk_CTR" is "Update link". So the job keeps all masters. If an update row doesn't exist, the columns from that link are null. 
# In DataStage "Merge" with "dropBadMasters" means keep only rows that appear on the Master link. That is effectively a left join from Lnk_CHD to Lnk_CTR.
# So let's do a left join from df_Lnk_CHD to df_Lnk_CTR on (SUBMT_CMS_CNTR_ID,SUBMT_CNTR_SEQ_ID).
df_Lnk_CHD_alias = df_Lnk_CHD.alias("Lnk_CHD")
df_Lnk_CTR_alias = df_Lnk_CTR.alias("Lnk_CTR")
df_Merge_CntrNoSeqNo_join = df_Lnk_CHD_alias.join(
    df_Lnk_CTR_alias,
    [
        df_Lnk_CHD_alias.SUBMT_CMS_CNTR_ID == df_Lnk_CTR_alias.SUBMT_CMS_CNTR_ID,
        df_Lnk_CHD_alias.SUBMT_CNTR_SEQ_ID == df_Lnk_CTR_alias.SUBMT_CNTR_SEQ_ID
    ],
    how="left"  # replicate "dropBadMasters"
)

df_Merge_CntrNoSeqNo = df_Merge_CntrNoSeqNo_join.select(
    F.col("Lnk_CHD.Key").alias("Key"),
    F.col("Lnk_CHD.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("Lnk_CHD.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_CHD.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_CHD.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_CHD.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_CHD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_CHD.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_CHD.AS_OF_YR").alias("AS_OF_YR"),
    F.col("Lnk_CHD.AS_OF_MO").alias("AS_OF_MO"),
    F.col("Lnk_CHD.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("Lnk_CHD.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("Lnk_CHD.RPT_ID").alias("RPT_ID"),
    F.col("Lnk_CTR.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_CTR.BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("Lnk_CTR.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_CTR.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_CTR.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_CTR.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_CTR.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_CTR.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("Lnk_CTR.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("Lnk_CTR.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_CTR.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Copy (PxCopy) from df_Merge_CntrNoSeqNo => duplication of streams
df_Copy = df_Merge_CntrNoSeqNo

# 1) Lnk_Remove_Dup => only columns FILE_ID,SUBMT_CNTR_SEQ_ID,SRC_SYS_CD for Remove_Duplicates2
df_Lnk_Remove_Dup = df_Copy.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD")
)

# 2) Lnk_AllCol_Join => the rest of columns
df_Lnk_AllCol_Join = df_Copy.select(
    F.col("Key"),
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("AS_OF_YR"),
    F.col("AS_OF_MO"),
    F.col("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM"),
    F.col("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Remove_Duplicates2 => dedup on FILE_ID,SUBMT_CNTR_SEQ_ID,SRC_SYS_CD retaining first row
df_RemDup_input = df_Lnk_Remove_Dup
df_Remove_Duplicates2 = dedup_sort(
    df_RemDup_input,
    partition_cols=["FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD"],
    sort_cols=[]
)

df_Lnk_RmDup = df_Remove_Duplicates2.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD")
)

# Jn1_NKey => left outer join with Lnk_RmDup as left, Lnk_KTableIn as right
df_Lnk_RmDup_alias = df_Lnk_RmDup.alias("Lnk_RmDup")
df_Lnk_KTableIn_alias = df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F.alias("Lnk_KTableIn")
df_Jn1_NKey_join = df_Lnk_RmDup_alias.join(
    df_Lnk_KTableIn_alias,
    [
        df_Lnk_RmDup_alias.FILE_ID == df_Lnk_KTableIn_alias.FILE_ID,
        df_Lnk_RmDup_alias.SUBMT_CNTR_SEQ_ID == df_Lnk_KTableIn_alias.SUBMT_CNTR_SEQ_ID,
        df_Lnk_RmDup_alias.SRC_SYS_CD == df_Lnk_KTableIn_alias.SRC_SYS_CD
    ],
    how="left"
)

df_Jn1_NKey = df_Jn1_NKey_join.select(
    F.col("Lnk_RmDup.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_RmDup.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_RmDup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_KTableIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_KTableIn.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK")
)

# Transformer => columns: "FILE_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_DT_SK","PDE_REC_RPT_SUBMT_CNTR_SK"
# stage variable "svPdeAcctgRptSbmCntrSK" => "If PDE_REC_RPT_SUBMT_CNTR_SK=0 then NextSurrogateKey() else PDE_REC_RPT_SUBMT_CNTR_SK"
# Also link constraints => row goes to Lnk_KTableLoad if PDE_REC_RPT_SUBMT_CNTR_SK=0 else goes forward
df_Jn1_NKey_pandas = df_Jn1_NKey.toPandas()

rows_Load = []
rows_Jn = []
for i, row in df_Jn1_NKey_pandas.iterrows():
    pde_sk = row["PDE_REC_RPT_SUBMT_CNTR_SK"]
    if pde_sk is None:
        pde_sk = 0
    if pde_sk == 0:
        # calls NextSurrogateKey => SurrogateKeyGen approach
        # We place 0 in PDE_REC_RPT_SUBMT_CNTR_SK then we update after SurrogateKeyGen
        # But instructions say "KeyMgtGetNextValueConcurrent => SurrogateKeyGen" must be called 
        # We can't do partial calls per row. We'll store the row now, the next stage is to call SurrogateKeyGen on the entire DF.
        new_pde_sk = 0  # placeholder
        rows_Load.append({
            "FILE_ID": row["FILE_ID"],
            "SUBMT_CNTR_SEQ_ID": row["SUBMT_CNTR_SEQ_ID"],
            "SRC_SYS_CD": row["SRC_SYS_CD"],
            "CRT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
            "PDE_REC_RPT_SUBMT_CNTR_SK": new_pde_sk
        })
        # We'll handle the SurrogateKeyGen after creating the dataframe.
        # Lnk_Jn is not triggered for these rows.
    else:
        # PDE_REC_RPT_SUBMT_CNTR_SK is not 0 => pass row forward with possible override CRT_RUN_CYC_EXCTN_DT_SK
        # "If pde_sk=0 => CurrRunCycleDate else Lnk_Jn1.CRT_RUN_CYC_EXCTN_DT_SK"
        rows_Jn.append({
            "FILE_ID": row["FILE_ID"],
            "SUBMT_CNTR_SEQ_ID": row["SUBMT_CNTR_SEQ_ID"],
            "SRC_SYS_CD": row["SRC_SYS_CD"],
            "CRT_RUN_CYC_EXCTN_DT_SK": row["CRT_RUN_CYC_EXCTN_DT_SK"] if pde_sk!=0 else CurrRunCycleDate,
            "PDE_REC_RPT_SUBMT_CNTR_SK": pde_sk
        })

df_Lnk_KTableLoad = spark.createDataFrame(rows_Load)
df_Lnk_Jn = spark.createDataFrame(rows_Jn)

# SurrogateKeyGen for df_Lnk_KTableLoad to fill PDE_REC_RPT_SUBMT_CNTR_SK
df_enriched = df_Lnk_KTableLoad
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PDE_REC_RPT_SUBMT_CNTR_SK",<schema>,<secret_name>)

# Now we have to merge the newly assigned PDE_REC_RPT_SUBMT_CNTR_SK back into df_Lnk_KTableLoad. 
# Because SurrogateKeyGen returns an updated DataFrame. We'll rename it to df_Lnk_KTableLoad_enriched
df_Lnk_KTableLoad_enriched = df_enriched

# DB2ConnectorPX (Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load) => Merge into table #$EDWOwner#.K_PDE_REC_RPT_SUBMT_CNTR_F
# Columns: FILE_ID, SUBMT_CNTR_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, PDE_REC_RPT_SUBMT_CNTR_SK
df_Db2_KLoad = df_Lnk_KTableLoad_enriched.select(
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "PDE_REC_RPT_SUBMT_CNTR_SK"
)

temp_table_name_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load = "STAGING.PdeRecRptSubmtCntrFExtr_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load}", jdbc_url, jdbc_props)

(
    df_Db2_KLoad.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load)
    .mode("overwrite")
    .save()
)

merge_sql_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load = f"""
MERGE {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_F as T
USING {temp_table_name_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load} as S
ON 
    T.FILE_ID=S.FILE_ID
    AND T.SUBMT_CNTR_SEQ_ID=S.SUBMT_CNTR_SEQ_ID
    AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_DT_SK=S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.PDE_REC_RPT_SUBMT_CNTR_SK=S.PDE_REC_RPT_SUBMT_CNTR_SK
WHEN NOT MATCHED THEN INSERT
(
    FILE_ID,
    SUBMT_CNTR_SEQ_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_DT_SK,
    PDE_REC_RPT_SUBMT_CNTR_SK
)
VALUES
(
    S.FILE_ID,
    S.SUBMT_CNTR_SEQ_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.PDE_REC_RPT_SUBMT_CNTR_SK
);
"""
execute_dml(merge_sql_Db2_K_PDE_REC_RPT_SUBMT_CNTR_F_Load, jdbc_url, jdbc_props)

# Now gather Lnk_Jn => merges with the all-col table from Copy => we do the join next
df_Lnk_AllCol_Join_alias = df_Lnk_AllCol_Join.alias("Lnk_AllCol_Join")
df_Lnk_Jn_alias = df_Lnk_Jn.alias("Lnk_Jn")
df_Jn2Nkey_join = df_Lnk_AllCol_Join_alias.join(
    df_Lnk_Jn_alias,
    [
        df_Lnk_AllCol_Join_alias.FILE_ID == df_Lnk_Jn_alias.FILE_ID,
        df_Lnk_AllCol_Join_alias.SUBMT_CNTR_SEQ_ID == df_Lnk_Jn_alias.SUBMT_CNTR_SEQ_ID,
        df_Lnk_AllCol_Join_alias.SRC_SYS_CD == df_Lnk_Jn_alias.SRC_SYS_CD
    ],
    how="inner"
)

df_Jn2Nkey = df_Jn2Nkey_join.select(
    F.col("Lnk_AllCol_Join.Key").alias("Key"),
    F.col("Lnk_Jn.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("Lnk_AllCol_Join.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_AllCol_Join.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_AllCol_Join.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_AllCol_Join.AS_OF_YR").alias("AS_OF_YR"),
    F.col("Lnk_AllCol_Join.AS_OF_MO").alias("AS_OF_MO"),
    F.col("Lnk_AllCol_Join.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("Lnk_AllCol_Join.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("Lnk_AllCol_Join.RPT_ID").alias("RPT_ID"),
    F.col("Lnk_AllCol_Join.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_AllCol_Join.BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_AllCol_Join.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("Lnk_AllCol_Join.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("Lnk_AllCol_Join.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Next Transformer_53 => Two outputs: Lnk_Seq_Extr, Lnk_n1
df_Jn2Nkey_pandas = df_Jn2Nkey.toPandas()

rows_Seq_Extr = []
rows_n1 = []
for i, row in df_Jn2Nkey_pandas.iterrows():
    out_dict_Seq = {
        "PDE_REC_RPT_SUBMT_CNTR_SK": row["PDE_REC_RPT_SUBMT_CNTR_SK"],
        "FILE_ID": row["FILE_ID"],
        "SUBMT_CNTR_SEQ_ID": row["SUBMT_CNTR_SEQ_ID"],
        "SRC_SYS_CD": row["SRC_SYS_CD"],
        "CRT_RUN_CYC_EXCTN_DT_SK": row["CRT_RUN_CYC_EXCTN_DT_SK"],
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": row["LAST_UPDT_RUN_CYC_EXCTN_DT_SK"],
        "SUBMT_CMS_CNTR_ID": row["SUBMT_CMS_CNTR_ID"],
        "AS_OF_YR": row["AS_OF_YR"],
        "AS_OF_MO": row["AS_OF_MO"],
        "FILE_CRTN_DT": row["FILE_CRTN_DT"],
        "FILE_CRTN_TM": row["FILE_CRTN_TM"],
        "RPT_ID": row["RPT_ID"],
        "DRUG_COV_STTUS_CD_TX": row["DRUG_COV_STTUS_CD_TX"],
        "BNFCRY_CT": row["BNFCRY_CT"],
        "CUR_MO_GROS_DRUG_CST_BELOW_AMT": row["CUR_MO_GROS_DRUG_CST_BELOW_AMT"],
        "CUR_MO_GROS_DRUG_CST_ABOVE_AMT": row["CUR_MO_GROS_DRUG_CST_ABOVE_AMT"],
        "CUR_MO_TOT_GROS_DRUG_CST_AMT": row["CUR_MO_TOT_GROS_DRUG_CST_AMT"],
        "CUR_MO_LOW_INCM_CST_SHARING_AMT": row["CUR_MO_LOW_INCM_CST_SHARING_AMT"],
        "CUR_MO_COV_PLN_PD_AMT": row["CUR_MO_COV_PLN_PD_AMT"],
        "TOT_DTL_RCRD_CT": row["TOT_DTL_RCRD_CT"],
        "CUR_MO_SUBMT_DUE_AMT": row["CUR_MO_SUBMT_DUE_AMT"],
        "CRT_RUN_CYC_EXCTN_SK": row["CRT_RUN_CYC_EXCTN_SK"],
        "LAST_UPDT_RUN_CYC_EXCTN_SK": row["LAST_UPDT_RUN_CYC_EXCTN_SK"]
    }
    rows_Seq_Extr.append(out_dict_Seq)

    out_dict_n1 = {
        "Key": row["Key"],
        "PDE_REC_RPT_SUBMT_CNTR_SK": row["PDE_REC_RPT_SUBMT_CNTR_SK"],
        "FILE_ID": row["FILE_ID"],
        "SUBMT_CNTR_SEQ_ID": row["SUBMT_CNTR_SEQ_ID"],
        "SUBMT_CMS_CNTR_ID": row["SUBMT_CMS_CNTR_ID"]
    }
    rows_n1.append(out_dict_n1)

df_Lnk_Seq_Extr = spark.createDataFrame(rows_Seq_Extr)
df_Lnk_n1 = spark.createDataFrame(rows_n1)

# Write PxSequentialFile => Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F (the "verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key.txt")
# That is from the stage "Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F" which has input from "Copy3" => that was from "Xfm_DET" with link "Lnk_DtlSeq"
# We have not yet handled "Payable40 -> xfm_RecId -> Sort_InRowNum -> Xfm_CHD_CTR -> Xfm_RHD_RTR -> Xfm_DET -> Copy3 -> Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F" portion for detail records. Let's implement that now.

# Xfm_RHD_RTR: input = df_RHD_RTR from prior. Stage variables with row-by-row approach, producing Lnk_RHD, Lnk_RTR, DET
df_RHD_RTR_pandas = df_RHD_RTR.toPandas()

rows_RHD = []
rows_RTR = []
rows_DET = []

curr_key2 = 1
prev_key2 = 1
prev_rowval2 = "0"

def map_negative_number2(text_val):
    # same as above
    last_char = text_val[-1:] if len(text_val) > 0 else ""
    mapping = {
        '}': '-0','J': '-1','K': '-2','L': '-3','M': '-4','N': '-5','O': '-6','P': '-7','Q': '-8','R': '-9',
        '{': '0','A': '1','B': '2','C': '3','D': '4','E': '5','F': '6','G': '7','H': '8','I': '9'
    }
    if last_char in mapping:
        sign_piece = mapping[last_char][0]
        digit_piece = mapping[last_char][1:] if len(mapping[last_char])>1 else mapping[last_char]
        if sign_piece == '-':
            return "-" + text_val[:-1].replace(last_char, digit_piece)
        else:
            return text_val[:-1].replace(last_char, digit_piece)
    return text_val

for i, row in df_RHD_RTR_pandas.iterrows():
    record_id = row["RECORD_ID"]
    rest_of_fields = row["RESTOFTHE_FIELDS"] if row["RESTOFTHE_FIELDS"] else ""
    input_row_num = row["InputRowNumber"]

    # stage variables
    old_curr_key2 = curr_key2
    old_prev_key2 = prev_key2
    old_prev_row2 = prev_rowval2

    # CurrRow
    stageval_CurrRow2 = record_id

    # Key logic
    if input_row_num == 2 and stageval_CurrRow2 == "RHD":
        pass
    elif stageval_CurrRow2 != old_prev_row2 and stageval_CurrRow2 != "RHD":
        pass
    elif stageval_CurrRow2 != old_prev_row2 and stageval_CurrRow2 != "DET":
        curr_key2 = curr_key2 + 1
    elif stageval_CurrRow2 == old_prev_row2:
        curr_key2 = prev_key2
    else:
        curr_key2 = curr_key2 + 1
    stageval_Key2 = curr_key2
    stageval_PrevKey2 = stageval_Key2
    stageval_PrevRow2 = stageval_CurrRow2

    # parse times, amounts
    raw_dt = rest_of_fields[44:52] if len(rest_of_fields)>=52 else ""
    trimmed_dt = trim(raw_dt)
    stageval_svFileCrtDt2 = trim(trimmed_dt[0:4]) + "-" + trim(trimmed_dt[4:6]) + "-" + trim(trimmed_dt[6:8]) if len(trimmed_dt)>=8 else ""
    raw_tm = rest_of_fields[52:58] if len(rest_of_fields)>=58 else ""
    trimmed_tm = trim(raw_tm)
    stageval_svFileCrtTime2 = trim(trimmed_tm[0:2]) + ":" + trim(trimmed_tm[2:4]) + ":" + trim(trimmed_tm[4:6]) if len(trimmed_tm)>=6 else ""

    raw_above = rest_of_fields[30:44] if len(rest_of_fields)>=44 else ""
    mapped_above = map_negative_number2(raw_above)
    neg_above = (len(mapped_above)>1 and mapped_above[0]=='-')
    try:
        val_above2 = float(mapped_above[1:]) if neg_above else float(mapped_above)
        if neg_above: val_above2 = -abs(val_above2)
    except:
        val_above2 = 0.0

    raw_below = rest_of_fields[44:58] if len(rest_of_fields)>=58 else ""
    mapped_below = map_negative_number2(raw_below)
    neg_below = (len(mapped_below)>1 and mapped_below[0]=='-')
    try:
        val_below2 = float(mapped_below[1:]) if neg_below else float(mapped_below)
        if neg_below: val_below2 = -abs(val_below2)
    except:
        val_below2 = 0.0

    raw_tot = rest_of_fields[58:72] if len(rest_of_fields)>=72 else ""
    mapped_tot = map_negative_number2(raw_tot)
    neg_tot = (len(mapped_tot)>1 and mapped_tot[0]=='-')
    try:
        val_tot2 = float(mapped_tot[1:]) if neg_tot else float(mapped_tot)
        if neg_tot: val_tot2 = -abs(val_tot2)
    except:
        val_tot2 = 0.0

    raw_low = rest_of_fields[72:86] if len(rest_of_fields)>=86 else ""
    mapped_low = map_negative_number2(raw_low)
    neg_low = (len(mapped_low)>1 and mapped_low[0]=='-')
    try:
        val_low2 = float(mapped_low[1:]) if neg_low else float(mapped_low)
        if neg_low: val_low2 = -abs(val_low2)
    except:
        val_low2 = 0.0

    raw_cov = rest_of_fields[86:100] if len(rest_of_fields)>=100 else ""
    mapped_cov = map_negative_number2(raw_cov)
    neg_cov = (len(mapped_cov)>1 and mapped_cov[0]=='-')
    try:
        val_cov2 = float(mapped_cov[1:]) if neg_cov else float(mapped_cov)
        if neg_cov: val_cov2 = -abs(val_cov2)
    except:
        val_cov2 = 0.0

    raw_due = rest_of_fields[108:122] if len(rest_of_fields)>=122 else ""
    mapped_due = map_negative_number2(raw_due)
    neg_due = (len(mapped_due)>1 and mapped_due[0]=='-')
    try:
        val_due2 = float(mapped_due[1:]) if neg_due else float(mapped_due)
        if neg_due: val_due2 = -abs(val_due2)
    except:
        val_due2 = 0.0

    # update memory
    prev_key2 = stageval_PrevKey2
    prev_rowval2 = stageval_PrevRow2

    # Constraints => Lnk_RHD if record_id='RHD', Lnk_RTR if 'RTR', DET if 'DET'
    if record_id == "RHD":
        rows_RHD.append({
            "Key": stageval_Key2,
            "Key1": stageval_Key2,
            "PDE_REC_RPT_SUBMT_CNTR_CNTR_SK": 0,  # WhereExpression=0
            "FILE_ID": trim(rest_of_fields[18:34]) if len(rest_of_fields)>=34 else "",
            "SUBMT_CNTR_SEQ_ID": 0,  # WhereExpression=0
            "CNTR_SEQ_ID": trim(rest_of_fields[3:10]) if len(rest_of_fields)>=10 else "",
            "SRC_SYS_CD": "CMS",
            "CRT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
            "PDE_REC_RPT_SUBMT_CNTR_SK": 0,  # WhereExpression=0
            "SUBMT_CMS_CNTR_ID": 0,  # WhereExpression=0
            "CMS_CNTR_ID": trim(rest_of_fields[10:15]) if len(rest_of_fields)>=15 else "",
            "AS_OF_YR": trim(rest_of_fields[38:42]) if len(rest_of_fields)>=42 else "",
            "AS_OF_MO": trim(rest_of_fields[42:44]) if len(rest_of_fields)>=44 else "",
            "FILE_CRTN_DT": stageval_svFileCrtDt2,
            "FILE_CRTN_TM": stageval_svFileCrtTime2,
            "RPT_ID": trim(rest_of_fields[58:63]) if len(rest_of_fields)>=63 else ""
        })
    elif record_id == "RTR":
        rows_RTR.append({
            "Key": stageval_Key2,
            "CNTR_SEQ_ID": trim(rest_of_fields[3:10]) if len(rest_of_fields)>=10 else "",
            "CMS_CNTR_ID": trim(rest_of_fields[10:15]) if len(rest_of_fields)>=15 else "",
            "DRUG_COV_STTUS_CD_TX": trim(rest_of_fields[18:19]) if len(rest_of_fields)>=19 else "",
            "BNFCRY_CT": trim(rest_of_fields[19:30]) if len(rest_of_fields)>=30 else "",
            "CUR_MO_GROS_DRUG_CST_ABOVE_AMT": val_above2,
            "CUR_MO_GROS_DRUG_CST_BELOW_AMT": val_below2,
            "CUR_MO_TOT_GROS_DRUG_CST_AMT": val_tot2,
            "CUR_MO_LOW_INCM_CST_SHARING_AMT": val_low2,
            "CUR_MO_COV_PLN_PD_AMT": val_cov2,
            "TOT_DTL_RCRD_CT": trim(rest_of_fields[100:108]) if len(rest_of_fields)>=108 else "",
            "CUR_MO_SUBMT_DUE_AMT": val_due2,
            "CRT_RUN_CYC_EXCTN_SK": CurrRunCycle,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": CurrRunCycle
        })
    elif record_id == "DET":
        rows_DET.append({
            "Key": stageval_Key2,
            "Key1": stageval_Key2,
            "InputRowNumber": input_row_num,
            "RECORD_ID": record_id,
            "SEQUENCE_NO": row["SEQUENCE_NO"],
            "RESTOFTHE_FIELDS": rest_of_fields
        })

df_Lnk_RHD = spark.createDataFrame(rows_RHD)
df_Lnk_RTR = spark.createDataFrame(rows_RTR)
df_DET = spark.createDataFrame(rows_DET)

# Merge_CntrNoSeqNo1 => "dropBadMasters" with Lnk_RHD as Master, Lnk_RTR as update. Join on (Key,Key1?? or Key,CMS_CNTR_ID,CNTR_SEQ_ID?). 
# The stage says "key Key asc, key CMS_CNTR_ID asc, key CNTR_SEQ_ID asc"
df_Lnk_RHD_alias = df_Lnk_RHD.alias("Lnk_RHD")
df_Lnk_RTR_alias = df_Lnk_RTR.alias("Lnk_RTR")
df_Merge_CntrNoSeqNo1_join = df_Lnk_RHD_alias.join(
    df_Lnk_RTR_alias,
    [
        df_Lnk_RHD_alias.Key == df_Lnk_RTR_alias.Key,
        df_Lnk_RHD_alias.CMS_CNTR_ID == df_Lnk_RTR_alias.CMS_CNTR_ID,
        df_Lnk_RHD_alias.CNTR_SEQ_ID == df_Lnk_RTR_alias.CNTR_SEQ_ID
    ],
    how="left"
)
df_Merge_CntrNoSeqNo1 = df_Merge_CntrNoSeqNo1_join.select(
    F.col("Lnk_RHD.Key").alias("Key"),
    F.col("Lnk_RHD.Key1").alias("Key1"),
    F.col("Lnk_RHD.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("Lnk_RHD.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_RHD.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_RHD.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("Lnk_RHD.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_RHD.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_RHD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_RHD.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("Lnk_RHD.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_RHD.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("Lnk_RHD.AS_OF_YR").alias("AS_OF_YR"),
    F.col("Lnk_RHD.AS_OF_MO").alias("AS_OF_MO"),
    F.col("Lnk_RHD.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("Lnk_RHD.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("Lnk_RHD.RPT_ID").alias("RPT_ID"),
    F.col("Lnk_RTR.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_RTR.BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("Lnk_RTR.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_RTR.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_RTR.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_RTR.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_RTR.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_RTR.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("Lnk_RTR.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("Lnk_RTR.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_RTR.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Copy1 => same approach as Copy
df_Copy1 = df_Merge_CntrNoSeqNo1

# Lnk_Join1 => subset for join with n1
df_Lnk_Join1 = df_Copy1.select(
    F.col("Key"),
    F.col("Key1"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
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
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# DET => Xfm_DET => produce Lnk_Ropy3 => then Copy3 => then Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F
df_DET_pandas = df_DET.toPandas()

rows_Ropy3 = []
for i, row in df_DET_pandas.iterrows():
    rest_of_fields = row["RESTOFTHE_FIELDS"] if row["RESTOFTHE_FIELDS"] else ""
    # replicate the negative parse again for multiple fields
    def parse_neg(txt):
        last_char = txt[-1:] if len(txt)>0 else ""
        mapping = {
            '}': '-0','J': '-1','K': '-2','L': '-3','M': '-4','N': '-5','O': '-6','P': '-7','Q': '-8','R': '-9',
            '{': '0','A': '1','B': '2','C': '3','D': '4','E': '5','F': '6','G': '7','H': '8','I': '9'
        }
        if last_char in mapping:
            sign_piece = mapping[last_char][0]
            digit_piece = mapping[last_char][1:] if len(mapping[last_char])>1 else mapping[last_char]
            if sign_piece == '-':
                return "-" + txt[:-1].replace(last_char, digit_piece)
            else:
                return txt[:-1].replace(last_char, digit_piece)
        return txt
    raw_above = rest_of_fields[71:85] if len(rest_of_fields)>=85 else ""
    mapped_above = parse_neg(raw_above)
    neg_above = (len(mapped_above)>1 and mapped_above[0]=='-')
    try:
        val_above3 = float(mapped_above[1:]) if neg_above else float(mapped_above)
        if neg_above: val_above3 = -abs(val_above3)
    except:
        val_above3 = 0.0

    raw_below = rest_of_fields[85:99] if len(rest_of_fields)>=99 else ""
    mapped_below = parse_neg(raw_below)
    neg_below = (len(mapped_below)>1 and mapped_below[0]=='-')
    try:
        val_below3 = float(mapped_below[1:]) if neg_below else float(mapped_below)
        if neg_below: val_below3 = -abs(val_below3)
    except:
        val_below3 = 0.0

    raw_tot = rest_of_fields[99:113] if len(rest_of_fields)>=113 else ""
    mapped_tot = parse_neg(raw_tot)
    neg_tot = (len(mapped_tot)>1 and mapped_tot[0]=='-')
    try:
        val_tot3 = float(mapped_tot[1:]) if neg_tot else float(mapped_tot)
        if neg_tot: val_tot3 = -abs(val_tot3)
    except:
        val_tot3 = 0.0

    raw_low = rest_of_fields[113:127] if len(rest_of_fields)>=127 else ""
    mapped_low = parse_neg(raw_low)
    neg_low = (len(mapped_low)>1 and mapped_low[0]=='-')
    try:
        val_low3 = float(mapped_low[1:]) if neg_low else float(mapped_low)
        if neg_low: val_low3 = -abs(val_low3)
    except:
        val_low3 = 0.0

    raw_cov = rest_of_fields[127:141] if len(rest_of_fields)>=141 else ""
    mapped_cov = parse_neg(raw_cov)
    neg_cov = (len(mapped_cov)>1 and mapped_cov[0]=='-')
    try:
        val_cov3 = float(mapped_cov[1:]) if neg_cov else float(mapped_cov)
        if neg_cov: val_cov3 = -abs(val_cov3)
    except:
        val_cov3 = 0.0

    raw_due = rest_of_fields[141:155] if len(rest_of_fields)>=155 else ""
    mapped_due = parse_neg(raw_due)
    neg_due = (len(mapped_due)>1 and mapped_due[0]=='-')
    try:
        val_due3 = float(mapped_due[1:]) if neg_due else float(mapped_due)
        if neg_due: val_due3 = -abs(val_due3)
    except:
        val_due3 = 0.0

    rows_Ropy3.append({
        "Key": row["Key"],
        "Key1": row["Key1"],
        "PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK": 0,  # =0
        "DTL_SEQ_ID": trim(rest_of_fields[3:10]) if len(rest_of_fields)>=10 else "",
        "SRC_SYS_CD": "CMS",
        "CRT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": CurrRunCycleDate,
        "CUR_MCARE_BNFCRY_ID": trim(rest_of_fields[11:31]) if len(rest_of_fields)>=31 else "",
        "LAST_SUBMT_MCARE_BNFCRY_ID": trim(rest_of_fields[31:51]) if len(rest_of_fields)>=51 else "",
        "LAST_SUBMT_CARDHLDR_ID": trim(rest_of_fields[51:71]) if len(rest_of_fields)>=71 else "",
        "DRUG_COV_STTUS_CD_TX": trim(rest_of_fields[10:11]) if len(rest_of_fields)>=11 else "",
        "CUR_MO_GROS_DRUG_CST_ABOVE_AMT": val_above3,
        "CUR_MO_GROS_DRUG_CST_BELOW_AMT": val_below3,
        "CUR_MO_TOT_GROS_DRUG_CST_AMT": val_tot3,
        "CUR_MO_LOW_INCM_CST_SHARING_AMT": val_low3,
        "CUR_MO_COV_PLN_PD_AMT": val_cov3,
        "CUR_SUBMT_DUE_AMT": val_due3,
        "CRT_RUN_CYC_EXCTN_SK": CurrRunCycle,
        "LAST_UPDT_RUN_CYC_EXCTN_SK": CurrRunCycle
    })

df_Lnk_Ropy3 = spark.createDataFrame(rows_Ropy3)

# Copy3 => duplicates the link
df_Copy3 = df_Lnk_Ropy3

# Lnk_DtlSeq => columns for Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F
df_Lnk_DtlSeq = df_Copy3.select(
    F.col("Key"),
    F.col("Key1"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_CARDHLDR_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("CUR_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F => write to verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key.txt
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F = df_Lnk_DtlSeq
write_files(
    df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Seq_PDE_REC_RPT_SUBMT_CNTR_F_Extr => from df_Lnk_Seq_Extr
write_files(
    df_Lnk_Seq_Extr,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Join_1 => input1 = df_Lnk_Join1, input2 = df_Lnk_n1, key=Key
df_Lnk_Join1_alias = df_Lnk_Join1.alias("Lnk_Join1")
df_Lnk_n1_alias = df_Lnk_n1.alias("Lnk_n1")
df_Join_1_join = df_Lnk_Join1_alias.join(
    df_Lnk_n1_alias,
    [df_Lnk_Join1_alias.Key == df_Lnk_n1_alias.Key],
    how="inner"
)
df_Join_1 = df_Join_1_join.select(
    F.col("Lnk_Join1.Key").alias("Key"),
    F.col("Lnk_Join1.Key1").alias("Key1"),
    F.col("Lnk_Join1.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("Lnk_n1.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_n1.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_Join1.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("Lnk_Join1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Join1.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_Join1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_n1.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("Lnk_n1.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_Join1.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("Lnk_Join1.AS_OF_YR").alias("AS_OF_YR"),
    F.col("Lnk_Join1.AS_OF_MO").alias("AS_OF_MO"),
    F.col("Lnk_Join1.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("Lnk_Join1.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("Lnk_Join1.RPT_ID").alias("RPT_ID"),
    F.col("Lnk_Join1.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_Join1.BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("Lnk_Join1.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_Join1.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_Join1.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_Join1.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_Join1.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_Join1.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("Lnk_Join1.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("Lnk_Join1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Join1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Copy4 => from df_Join_1
df_Copy4 = df_Join_1

# Lnk_Seq => columns for Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F
df_Lnk_Seq = df_Copy4.select(
    F.col("Key"),
    F.col("Key1"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID"),
    F.col("AS_OF_YR"),
    F.col("AS_OF_MO"),
    F.col("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM"),
    F.col("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F => "verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_F.txt"
write_files(
    df_Lnk_Seq,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_F.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)