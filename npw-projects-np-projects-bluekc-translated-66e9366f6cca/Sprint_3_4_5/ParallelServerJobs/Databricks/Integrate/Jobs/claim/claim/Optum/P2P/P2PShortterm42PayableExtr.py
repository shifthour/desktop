# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2019  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  P2POptumReconShortTermSeqCntl
# MAGIC 
# MAGIC DESCRIPTION: Create P2P #42 Reconciliation Report.
# MAGIC 
# MAGIC MODIFICATIONS:                                        Project/                                                                                                                                                      Development                                           \(9)\(9)Date 
# MAGIC Developer                            Date                 Altiris #         Change Description                                                                                                               Project                    Code Reviewer         \(9)Reviewed  
# MAGIC ------------------                      --------------------         -----------------   ------------------------------------------------------------------------------------------------------------------------------------------    ---------------------------    -------------------------------   ----------------------------       
# MAGIC Rekha Radhakrishna      2021/03/09           MAP2P            Initial Programming   \(9)                                                                                                       IntegrateDev2         Jaideep Mankala       \(9)04/06/2021


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ExclusionList = get_widget_value('ExclusionList','')
InFile = get_widget_value('InFile','')

# Payable42 (PxSequentialFile) - Read
schema_Payable42 = StructType([
    StructField("Field", StringType(), True)
])
df_Payable42 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\u0001")  # Use a delimiter that will treat the entire line as one column
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_Payable42)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_Payable42 = df_Payable42.withColumn("InputRowNumber", F.monotonically_increasing_id() + F.lit(1))

# xfm_RecId (CTransformerStage) - Filter and map columns
df_xfm_RecId = (
    df_Payable42
    .filter(
        (F.substring("Field", 1, 3) == "CHD")
        | (F.substring("Field", 1, 3) == "DET")
    )
    .select(
        F.substring("Field", 1, 3).alias("RECORD_ID"),
        F.substring("Field", 4, 7).alias("SEQUENCE_NO"),
        F.col("Field").alias("RESTOFTHE_FIELDS"),
        F.col("InputRowNumber").alias("InputRowNumber")
    )
)

# Sort_165 (PxSort) - Sort by InputRowNumber
df_sort_165 = df_xfm_RecId.orderBy("InputRowNumber")

# xfm_CHD_DET (CTransformerStage) - Replicate stage variable logic (row-by-row)
# Collect all rows to driver and process the stateful logic in a single pass.
df_coalesced = df_sort_165.coalesce(1)
all_rows = df_coalesced.collect()

trackKey = 1
trackPrevRow = "0"
trackPrevKey = 1

xfm_CHD_DET_rows = []
for i, row in enumerate(all_rows):
    CurrRow = row["RECORD_ID"]
    newKey = trackKey
    if row["InputRowNumber"] == 1 and CurrRow == "CHD":
        newKey = trackKey
    elif CurrRow != trackPrevRow and CurrRow != "CHD":
        newKey = trackKey
    elif CurrRow != trackPrevRow and CurrRow != "DET":
        newKey = trackKey + 1
    elif CurrRow == trackPrevRow:
        newKey = trackPrevKey
    else:
        newKey = trackKey + 1

    p2pamt_overpunch_val = None
    if row["RECORD_ID"] == "DET":
        val_str = row["RESTOFTHE_FIELDS"]
        if val_str is not None and len(val_str) >= (126 + 14):
            sub_str = val_str[126:126+14]
            last_char = sub_str[-1] if len(sub_str) > 0 else ''
            sign_map = {
                '}': '-0', 'J': '-1', 'K': '-2', 'L': '-3', 'M': '-4', 'N': '-5', 'O': '-6', 'P': '-7',
                'Q': '-8', 'R': '-9', '{': '0', 'A': '1', 'B': '2', 'C': '3', 'D': '4', 'E': '5', 'F': '6',
                'G': '7', 'H': '8', 'I': '9'
            }
            sign_calc = sign_map.get(last_char, '9')
            if len(sign_calc) > 1:
                replaced_str = sub_str[:-1] + sign_calc[1]
                p2pamt_overpunch_val = float("-" + replaced_str) / 100.0
            else:
                replaced_str = sub_str[:-1] + sign_calc
                p2pamt_overpunch_val = float(replaced_str) / 100.0

    xfm_CHD_DET_rows.append((
        newKey,
        row["RECORD_ID"],
        row["SEQUENCE_NO"],
        row["RESTOFTHE_FIELDS"],
        row["InputRowNumber"],
        p2pamt_overpunch_val
    ))

    trackKey = newKey
    trackPrevKey = newKey
    trackPrevRow = CurrRow

schema_xfm_CHD_DET = StructType([
    StructField("Key", IntegerType(), True),
    StructField("RECORD_ID", StringType(), True),
    StructField("SEQUENCE_NO", StringType(), True),
    StructField("RESTOFTHE_FIELDS", StringType(), True),
    StructField("InputRowNumber", LongType(), True),
    StructField("P2PAMT_OVERPUNCH", DoubleType(), True)
])

df_xfm_CHD_DET = spark.createDataFrame(xfm_CHD_DET_rows, schema_xfm_CHD_DET)

# Build out_CHD link
dfCHD = (
    df_xfm_CHD_DET
    .filter("RECORD_ID = 'CHD'")
    .select(
        F.col("Key"),
        F.col("RECORD_ID").alias("CHD_RECORD_ID"),
        F.col("SEQUENCE_NO").alias("CHD_SEQUENCE_NO"),
        F.substring("RESTOFTHE_FIELDS", 11, 5).alias("CONTRACT_NO"),
        F.substring("RESTOFTHE_FIELDS", 16, 16).alias("FILE_ID"),
        F.substring("RESTOFTHE_FIELDS", 32, 4).alias("PROD_TEST_IND"),
        F.substring("RESTOFTHE_FIELDS", 36, 4).alias("AS_OF_YEAR"),
        F.substring("RESTOFTHE_FIELDS", 40, 2).alias("AS_OF_MONTH"),
        F.substring("RESTOFTHE_FIELDS", 42, 8).alias("DDPS_SYSTEM_DATE"),
        F.substring("RESTOFTHE_FIELDS", 50, 6).alias("DDPS_SYSTEM_TIME"),
        F.substring("RESTOFTHE_FIELDS", 56, 5).alias("DDPS_REPORT_ID")
    )
)

# Build out_DET link
dfDET = (
    df_xfm_CHD_DET
    .filter("RECORD_ID = 'DET'")
    .select(
        F.col("Key"),
        F.col("RECORD_ID").alias("DET_RECORD_ID"),
        F.col("SEQUENCE_NO").alias("DET_SEQUENCE_NO"),
        F.substring("RESTOFTHE_FIELDS", 11, 1).alias("DRUG_COVERAGE_STATUS_CODE"),
        F.substring("RESTOFTHE_FIELDS", 12, 20).alias("CURR_CMS_MEDICARE_BEN_ID"),
        F.substring("RESTOFTHE_FIELDS", 32, 20).alias("LST_SUBMTD_CMS_MEDICARE_BEN_ID"),
        F.substring("RESTOFTHE_FIELDS", 52, 14).alias("NET_GDCB_AMOUNT"),
        F.substring("RESTOFTHE_FIELDS", 66, 14).alias("NET_GDCA_AMOUNT"),
        F.substring("RESTOFTHE_FIELDS", 80, 14).alias("NET_TOTAL_GROSS_DRUG_COST"),
        F.substring("RESTOFTHE_FIELDS", 94, 14).alias("NET_LICS_AMOUNT"),
        F.substring("RESTOFTHE_FIELDS", 108, 14).alias("NET_CPP_AMOUNT"),
        F.substring("RESTOFTHE_FIELDS", 122, 5).alias("P2P_CONTRACT"),
        F.substring("RESTOFTHE_FIELDS", 127, 14).alias("P2P_AMOUNT"),
        F.col("P2PAMT_OVERPUNCH").alias("P2PAMT_OVERPUNCH")
    )
)

# Merge_Contract (PxMerge) - Merge on Key (CHD=master, DET=update, keepBadMasters, warnBadUpdates)
df_merged = (
    dfCHD.alias("out_CHD")
    .join(dfDET.alias("out_DET"), F.col("out_CHD.Key") == F.col("out_DET.Key"), "left")
    .select(
        F.col("out_CHD.CONTRACT_NO").alias("BLUEKC_CONTRACT_NO"),
        F.col("out_DET.P2P_CONTRACT").alias("PAYEE_CONTRACT_NO"),
        F.col("out_DET.P2PAMT_OVERPUNCH").alias("P2PAMT_AMOUNT")
    )
)

# seq_42Report (PxSequentialFile) - Write to P2PShortterm42Reconcile.csv
df_42Report = df_merged.select(
    F.rpad(F.col("BLUEKC_CONTRACT_NO"), 5, " ").alias("BLUEKC_CONTRACT_NO"),
    F.rpad(F.col("PAYEE_CONTRACT_NO"), 5, " ").alias("PAYEE_CONTRACT_NO"),
    F.col("P2PAMT_AMOUNT").alias("P2PAMT_AMOUNT")
)

write_files(
    df_42Report,
    f"{adls_path}/verified/P2PShortterm42Reconcile.csv",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=" "
)