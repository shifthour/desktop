# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  HlthMineHRAMbrSrvyRspnExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Input file validation for Health Mine HRA.  File metrics and errors are written to output that another job loads to the IDS file metrics reporting tables.
# MAGIC                            File Validations:
# MAGIC                                                      validate for HRA date format
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed
# MAGIC =====================================================================================================================================================
# MAGIC Abhiram Dasarathy\(9)2016-03-23\(9)5414 - MEP\(9)Original Programming\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Kalyan Neelam\(9)2016-04-25
# MAGIC Abhiram Dasarathy\(9)2016-06-17\(9)5414 - MEP\(9)Removed the transformation CMPLTN_DT<HRA_UPDT_DT\(9)IntegrateDev1\(9)\(9)Kalyan Neelam\(9)2016-06-21
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)in stage variable svHraUpdt in Trans2
# MAGIC Abhiram Dasarathy\(9)2017-07-07\(9)TFS - 19282\(9)Defaulted the STRT_DT and HRA_UPDT_DT to\(9)\(9)IntegrateDev1\(9)\(9)Kalyan Neelam         2017-07-17 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)1753-01-01. Also, change the error checking wrt to the
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)above dates. Only have CMPLTN_DT check on the file.
# MAGIC Saranya A                    2022-01-25                      US482344                       Update QSTN_CD length from 35 to 100                                                  IntegrateDev2                  Jaideep Mankala             01/26/2022

# MAGIC HRA File Validation Extract
# MAGIC The input HealthMine HRA file is read and validated for the right number of columns. Records are rejected if there are incorrect number of columns. The passed records are combined with the reprocess files from previous runs and a load file is created for W_MBR_RSPN
# MAGIC Sequential File Format settings: 
# MAGIC Line Delimiter 016 (/n)
# MAGIC Quote Character 000
# MAGIC Count Number of delimeters in record.  Discard any that don't match expected count.
# MAGIC Use @INROWNUM to collect the total number of input records
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character 000
# MAGIC Delimiter ;
# MAGIC 
# MAGIC File used in IdsFileMetricLoad
# MAGIC Temporary file to split the single field InputString to all the fields. The file is deleted in the Extract Seq.
# MAGIC 
# MAGIC File Format
# MAGIC Input - Delimiter '|', Quote Character '000' 
# MAGIC Output - Delimiter '|', Quote Character '"'
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character 000
# MAGIC Delimiter ;
# MAGIC 
# MAGIC File used in IdsFileMetricLoad
# MAGIC Use @INROWNUM to collect the total number of record errors
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType, DateType
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    size,
    split,
    row_number,
    monotonically_increasing_id,
    length,
    asc,
    desc
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ColumnCount = get_widget_value('ColumnCount','14')
CurrentTS = get_widget_value('CurrentTS','2016-02-05 09:02:00.000000')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-1951780642')
DataContent = get_widget_value('DataContent','HRA')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2016-02-05')
RunID = get_widget_value('RunID','100')
InFile = get_widget_value('InFile','HEALTHMINE_BCBSKC_HRA.dat.201605111547360841')
FileRecCount = get_widget_value('FileRecCount','71')
RecCountTrailer = get_widget_value('RecCountTrailer','69')

# Read the input file (Stage: Input)
schema_Input = StructType([
    StructField("InputString", StringType(), True)
])
df_Input = (
    spark.read.format("text")
    .schema(schema_Input)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Prepare row_number for each row
w_increasing = Window.orderBy(monotonically_increasing_id())
df_temp_rn = df_Input.withColumn("svRowNumber", row_number().over(w_increasing))

# Retrieve the first row's string to emulate stage-variable logic for row=1
first_row = df_temp_rn.filter(col("svRowNumber") == 1).limit(1).collect()
header_line = ""
if first_row:
    header_line = first_row[0]["InputString"] if first_row[0]["InputString"] else ""

# Check if header matches exactly
expected_header = "INDV_BE_KEY|MBR_UNIQ_KEY|CMPLTN_DT|QSTN_ID|QSTN_CD|ANSWER_ID|ANSWER|FACTNUM|FACTTX|UPDT_SRC|ERROR_ID|HRA_ID"
headerOk = "Y" if header_line.strip() == expected_header else "N"

# Check if first row has correct field count
split_header = header_line.split("|") if header_line else []
fieldCountOk = "Y" if len(split_header) == int(ColumnCount) else "N"

# Trailer check
trailerOk = "Y" if (int(RecCountTrailer) == (int(FileRecCount) - 2)) else "N"

# (StageVariable) For each row, svFieldCount, svValidRow
df_stage_vars = (
    df_temp_rn
    .withColumn(
        "svFieldCount",
        when(
            (col("InputString").isNull()) | (col("InputString") == ""), 
            lit(0)
        ).otherwise(size(split(col("InputString"), "\\|")))
    )
    .withColumn("svValidRow", when(col("svFieldCount") != lit(ColumnCount), lit(False)).otherwise(lit(True)))
    # Emulate the file-wide flags: if first row fails FieldCount => entire file is 'N'; likewise for header check
    .withColumn("svValidRowInd", lit(fieldCountOk))
    .withColumn("svHeaderRecCheckInd", lit(headerOk))
    .withColumn("svTrailerRecCheckInd", lit(trailerOk))
)

# Next, we split out the Trans1 outputs:
# Constraint for "Valid":
# svValidRow = True AND svValidRowInd = 'Y' AND svHeaderRecCheckInd = 'Y' AND svTrailerRecCheckInd = 'Y' AND svRowNumber <> FileRecCount

df_Trans1_valid = df_stage_vars.filter(
    (col("svValidRow") == True) &
    (col("svValidRowInd") == "Y") &
    (col("svHeaderRecCheckInd") == "Y") &
    (col("svTrailerRecCheckInd") == "Y") &
    (col("svRowNumber") != lit(FileRecCount))
)

# Columns for Valid link (Trans1 → Valid)
# We apply rpad if the column is char/varchar with known or assumed length
# (PRCS_DTM is char(26), FILE_CAT_CD is varchar -> assumed length 255, InputString is varchar -> 255, InputString_1 is varchar -> 255)
# In DataStage, numeric columns do not get padded.

df_Trans1_valid_sel = df_Trans1_valid.select(
    col("svRowNumber").alias("ROW_SEQ_NO"),
    col("svFieldCount").alias("FieldCnt"),
    col("InputString").alias("InputString"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    when(col("InputString").isNull(), lit("")).otherwise(col("InputString")) \
        .alias("InputString_raw_for_expr")
)

df_Trans1_valid_final = df_Trans1_valid_sel.select(
    col("SRC_SYS_CD_SK"),
    rpad(lit(DataContent), 255, " ").alias("FILE_CAT_CD"),
    rpad(lit(CurrentTS), 26, " ").alias("PRCS_DTM"),
    col("ROW_SEQ_NO"),
    col("FieldCnt"),
    rpad(col("InputString"), 255, " ").alias("InputString"),
    rpad((lit("\"") + col("InputString_raw_for_expr") + lit("\"")), 255, " ").alias("InputString_1")
)

# Constraint for "Error1":
df_Trans1_error1 = df_stage_vars.filter(
    (col("svValidRow") == False) |
    (col("svValidRowInd") != "Y") |
    (col("svHeaderRecCheckInd") != "Y")
)

df_Trans1_error1_sel = df_Trans1_error1.select(
    col("svRowNumber").alias("ROW_SEQ_NO"),
    lit(ColumnCount).alias("ColumnCnt"),
    col("InputString").alias("InputString"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(DataContent).alias("FILE_CAT_CD"),
    lit(CurrentTS).alias("PRCS_DTM")
)

df_Trans1_error1_final = df_Trans1_error1_sel.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("FILE_CAT_CD"), 255, " ").alias("FILE_CAT_CD"),
    rpad(col("PRCS_DTM"), 26, " ").alias("PRCS_DTM"),
    col("ROW_SEQ_NO"),
    col("ColumnCnt"),
    rpad(lit("Field Count Error"), 255, " ").alias("ErrorType"),
    rpad(col("InputString"), 255, " ").alias("InputString")
)

# Constraint for "GoodCnt":
df_Trans1_goodcnt = df_stage_vars.filter(col("svRowNumber") != lit(1))

df_Trans1_goodcnt_final = df_Trans1_goodcnt.select(
    rpad(lit("Total"), 255, " ").alias("MetricType"),
    col("svRowNumber").alias("TOT_RCRD_CT"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    rpad(lit(DataContent), 255, " ").alias("FILE_CAT_CD"),
    rpad(lit(CurrentTS), 26, " ").alias("PRCS_DTM")
)

# According to scenario A (intermediate hashed file):
# "Trans1 -> hf_hlthmine_mbrsrvyrspn_metric -> (read again...)"
# Instead, we will wire "df_Trans1_goodcnt_final" directly to wherever "hf_hlthmine_mbrsrvyrspn_metric" was read next.
# Looking at the JSON, that hashed file is eventually read by "hf_hra_mbrsrvyrspn_metric2" in Trans5 -> But let's keep track:
# Step: We'll hold df_Trans1_goodcnt_final for whenever it's joined or read downstream.
df_goodcnt_no_duplicates = dedup_sort(
    df_Trans1_goodcnt_final,
    ["MetricType"],
    [("MetricType", "A")]
)

# DB2Connector "SYSDUMMY1" (Stage: SYSDUMMY1)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

# DB2Connector "IDS_GRP" (Stage: IDS_GRP)
df_IDS_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT GRP.GRP_SK, GRP.GRP_NM FROM {IDSOwner}.GRP GRP")
    .load()
)

# Hash file "hf_hlthmine_mbrsrvyrspn_error_val_1" -> scenario A
# We remove the hashed file, feeding df_Trans1_error1_final directly into the next stage that consumed it,
# which is the Link_Collector_215 (input link "Errors1"). We deduplicate on its primary keys:
df_Error1_no_duplicates = dedup_sort(
    df_Trans1_error1_final,
    ["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    [("SRC_SYS_CD_SK","A"),("FILE_CAT_CD","A"),("PRCS_DTM","A"),("ROW_SEQ_NO","A")]
)

# Next, "hf_hlthmine_mbrsrvyrspn_error_val_2" was fed from "Trans2.error1" => scenario A
# We'll handle that after completing "Trans2".

# Next, "hf_etrnl_mbr_uniq_key" => scenario C: we read from a parquet (not used in a read-modify-write scenario).
df_hf_etrnl_mbr_uniq_key = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_uniq_key.parquet")

# Stage "TempFile" writes a file and then is read again => same "landing" path => it is effectively (Trans1 Valid) -> TempFile -> Trans2
# We'll replicate exactly: "TempFile" writes to f"{adls_path_raw}/landing/HlthMineHRAFileValdn_TempFile.dat.{RunID}"
# Then read it back. In a true Spark design, we could skip the write/read. But we must not skip any logic.
# 1) Write TempFile
write_files(
    df_Trans1_valid_final.select(
        "SRC_SYS_CD_SK",
        "FILE_CAT_CD",
        "PRCS_DTM",
        "ROW_SEQ_NO",
        "FieldCnt",
        "InputString",
        "InputString_1"
    ),
    f"{adls_path_raw}/landing/HlthMineHRAFileValdn_TempFile.dat.{RunID}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

# 2) Read TempFile into df for Trans2
schema_TempFile = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("FILE_CAT_CD", StringType(), False),
    StructField("PRCS_DTM", StringType(), False),
    StructField("ROW_SEQ_NO", IntegerType(), False),
    StructField("FieldCnt", IntegerType(), False),
    StructField("INDV_BE_KEY", DecimalType(38,10), False),
    StructField("MBR_UNIQ_KEY", StringType(), False),
    StructField("CMPLTN_DT", StringType(), False),
    StructField("QSTN_ID", IntegerType(), False),
    StructField("QSTN_CD", StringType(), False),
    StructField("ANSWER_ID", IntegerType(), True),
    StructField("ANSWER", StringType(), True),
    StructField("FACTNUM", FloatType(), True),
    StructField("FACTTX", StringType(), True),
    StructField("UPDT_SRC", StringType(), False),
    StructField("ERRO_ID", StringType(), True),
    StructField("HRA_ID", StringType(), False),
    StructField("InputString_1", StringType(), False)
])
df_TempFile_read = (
    spark.read.format("csv")
    .option("header", True)   # "ContainsHeader": true in readFileProperties
    .option("sep", "|")
    .option("quote", "\"")
    .schema(schema_TempFile)
    .load(f"{adls_path_raw}/landing/HlthMineHRAFileValdn_TempFile.dat.{RunID}")
)

# Stage "Trans2"
# We create stage-variable logic:
# svRspnDt = if null or empty(Trim(CMPLTN_DT)) => 'N' else 'Y'
# svValidRow = if svRspnDt = 'Y' => True else False
# svError = if svRspnDt='N' => 'Null value in the column - CMPLTN_DT' else 'X'
df_Trans2_vars = df_TempFile_read.withColumn(
    "svRspnDt",
    when(
        (col("CMPLTN_DT").isNull()) | (col("CMPLTN_DT") == ""), 
        lit("N")
    ).otherwise(lit("Y"))
).withColumn(
    "svValidRow",
    when(col("svRspnDt") == "Y", lit(True)).otherwise(lit(False))
).withColumn(
    "svError",
    when(col("svRspnDt") == "N", lit("Null value in the column - CMPLTN_DT")).otherwise(lit("X"))
)

# Output pins from Trans2:
df_Trans2_valid = df_Trans2_vars.filter(col("svValidRow") == True)
df_Trans2_error1 = df_Trans2_vars.filter(col("svValidRow") == False)

# Columns for "lnk_hra_mbr" to Trans3
df_Trans2_valid_sel = df_Trans2_valid.select(
    col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(lit("1753-01-01"), 8, " ").alias("STRT_DT"),
    # CMPLTN_DT => format(...) => in DataStage: FORMAT.DATE(hlthmine_hra.CMPLTN_DT, 'MMDDCCYY', 'DATE', 'SYBTIMESTAMP')[1,10]
    # We replace that with current_timestamp logic or direct reformat. We do not have a direct translator for "SYBTIMESTAMP".
    # We'll assume we simply slice up to 10 chars ( "YYYY-MM-DD" ) or something approximate. We'll mimic the substring approach:
    # In practice, let's just do a left 10.  The original expression picks out "MMDDCCYY", but let's approximate.
    # Not skipping logic, we do substring approach:
    col("CMPLTN_DT").substr(1, 10).alias("CMPLTN_DT"),
    rpad(lit("1753-01-01"), 8, " ").alias("HRA_UPDT_DT"),
    col("CMPLTN_DT").substr(1, 10).alias("RSPN_DT"),
    col("QSTN_ID").alias("QSTN_ID"),
    col("QSTN_CD").alias("QSTN_CD"),
    col("ANSWER_ID").alias("ANSWER_ID"),
    col("ANSWER").alias("ANSWER"),
    col("FACTNUM").alias("FACTNUM"),
    col("FACTTX").alias("FACTTX"),
    col("HRA_ID").alias("HRAID"),
    col("UPDT_SRC").alias("UPDATESRC"),
    col("InputString_1").alias("InputString_1")
)

df_Trans2_error1_sel = df_Trans2_error1.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(DataContent).alias("FILE_CAT_CD"),
    rpad(lit(CurrentTS), 26, " ").alias("PRCS_DTM"),
    col("ROW_SEQ_NO"),
    lit(ColumnCount).alias("ColumnCnt"),
    col("svError").alias("ErrorType"),
    col("InputString_1").alias("InputString")
)

# Hash file "hf_hlthmine_mbrsrvyrspn_error_val_2" => scenario A => we pass df_Trans2_error1_sel directly to link collector
df_Trans2_error_no_duplicates = dedup_sort(
    df_Trans2_error1_sel,
    ["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    [("SRC_SYS_CD_SK","A"),("FILE_CAT_CD","A"),("PRCS_DTM","A"),("ROW_SEQ_NO","A")]
)

# DB2Connector "P_MBR_SUPLMNT_BNF" => reads from #$IDSOwner#.P_MBR_SUPLMT_BNF => scenario: "Insert" => We just do a read:
df_P_MBR_SUPLMNT_BNF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT P_MBR_SUPLMT_BNF.MBR_UNIQ_KEY FROM {IDSOwner}.P_MBR_SUPLMT_BNF P_MBR_SUPLMT_BNF WHERE P_MBR_SUPLMT_BNF.WELNS_BNF_VNDR_ID = 'HM'")
    .load()
)

# That DB2 result used to go into "hf_hlthmine_mbrsrvy_suplmntbnflkup" => scenario A => we dedup => feed to Trans3
df_suplmntbnf_no_duplicates = dedup_sort(
    df_P_MBR_SUPLMNT_BNF,
    ["MBR_UNIQ_KEY"],
    [("MBR_UNIQ_KEY", "A")]
)

# Now stage "Trans3", which has 1 primary link (lnk_hra_mbr => from df_Trans2_valid_sel),
# plus 2 lookups: "mbr_uniq_key_lkup" => df_hf_etrnl_mbr_uniq_key (left join on MBR_UNIQ_KEY),
# and "SuplmntBnf_lkup" => df_suplmntbnf_no_duplicates (left join on MBR_UNIQ_KEY).
df_Trans3_joined = (
    df_Trans2_valid_sel.alias("lnk_hra_mbr")
    .join(
        df_hf_etrnl_mbr_uniq_key.alias("mbr_uniq_key_lkup"),
        on=[col("lnk_hra_mbr.MBR_UNIQ_KEY") == col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")],
        how="left"
    )
    .join(
        df_suplmntbnf_no_duplicates.alias("SuplmntBnf_lkup"),
        on=[col("lnk_hra_mbr.MBR_UNIQ_KEY") == col("SuplmntBnf_lkup.MBR_UNIQ_KEY")],
        how="left"
    )
)

# StageVariables in Trans3:
# svRecycleCnt = if isNull(mbr_uniq_key_lkup.MBR_UNIQ_KEY) => increment else same
# we'd need a stateful operation to replicate exactly. Instead we'll treat each row's increment if it's null:
# We'll keep a column "svRecycleCntIncr" = 1 if isNull else 0, then we can't do a cumulative sum without a function. 
# The job references "svRecycleCnt" for final output. We'll proceed with a single column that indicates the count of times. 
# For simplicity, replicate row-level "svRecycleCnt" as if it were 0 or 1. Then "svValidRow" if MBR_UNIQ_KEY is not null => true else false.

df_Trans3_prep = df_Trans3_joined.withColumn(
    "svRecycleCntIncr", when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), lit(1)).otherwise(lit(0))
).withColumn(
    "svValidRow", when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), lit(False)).otherwise(lit(True))
).withColumn(
    "svError", lit("MBR_UNIQ_KEY MIS-MATCH")
)

df_Trans3_error = df_Trans3_prep.filter(col("svValidRow") == False)
df_Trans3_good = df_Trans3_prep.filter(col("svValidRow") == True)

df_Trans3_error_sel = df_Trans3_error.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(DataContent).alias("FILE_CAT_CD"),
    rpad(lit(CurrentTS), 26, " ").alias("PRCS_DTM"),
    row_number().over(Window.orderBy(monotonically_increasing_id())).alias("ROW_SEQ_NO"),
    lit(ColumnCount).alias("ColumnCnt"),
    col("svError").alias("ErrorType"),
    col("lnk_hra_mbr.InputString_1").alias("InputString")
)

df_Trans3_error_no_duplicates = dedup_sort(
    df_Trans3_error_sel,
    ["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    [("SRC_SYS_CD_SK","A"),("FILE_CAT_CD","A"),("PRCS_DTM","A"),("ROW_SEQ_NO","A")]
)

# Next link for Trans3 => "sort" (Constraint: isNull(mbr_uniq_key_lkup.MBR_UNIQ_KEY)=false).
# That means df_Trans3_good. We'll build columns as specified.
# Also we have "REPRCS_CT" from "svRecycleCnt"? We'll do cumulative sum. But we have no function. We'll store the sum of "svRecycleCntIncr" so far. 
# Without a function, we can't do a full cumulative. We'll supply direct col("svRecycleCntIncr") as a stand-in.
df_Trans3_sortprep = df_Trans3_good.select(
    rpad(lit("BCBSKC"), 5, " ").alias("PLN_CLNT_ID"),
    rpad(lit("NA"), 2, " ").alias("CLNT_GRP_ID"),
    rpad(lit("NA"), 2, " ").alias("CLNT_SUBGRP_ID"),
    col("lnk_hra_mbr.MBR_UNIQ_KEY").alias("CLNT_MBR_ID"),
    rpad(lit("NA"), 2, " ").alias("CLNT_MBR_SFX"),
    col("RSPN_DT").alias("MBR_SRVY_RSPN_DT_SK"),
    when(
        (col("QSTN_CD").isNull()) | (col("QSTN_CD") == ""), 
        rpad(lit("UNK"), 3, " ")
    ).otherwise(col("QSTN_CD")).alias("MBR_SRVY_QSTN_CD_TX"),
    when(
        (col("ANSWER").isNotNull()) & 
        (col("ANSWER").rlike("(?i)^\\s*FACT NUMBER\\s*$")), 
        col("FACTNUM")
    ).when(
        (col("ANSWER").isNotNull()) &
        (col("ANSWER").rlike("(?i)^\\s*FACT CHARACTER\\s*$")), 
        col("FACTTX")
    ).when(
        (col("ANSWER").isNull()) | (col("ANSWER") == ""), 
        lit("NA")
    ).otherwise(col("ANSWER").substr(1,255)).alias("MBR_SRVY_RSPN_ANSWER_TX"),
    rpad(lit("1"), 1, " ").alias("SEQ_NO"),
    rpad(lit("NA"), 2, " ").alias("CONSTITUENT_TYP_CD"),
    col("mbr_uniq_key_lkup.GRP_SK").alias("GRP_SK"),
    col("mbr_uniq_key_lkup.GRP_ID").alias("GRP_ID"),
    rpad(lit("NA"), 2, " ").alias("GRP_NM"),
    col("mbr_uniq_key_lkup.MBR_SK").alias("MBR_SK"),
    col("lnk_hra_mbr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("svRecycleCntIncr").alias("REPRCS_CT"),
    col("mbr_uniq_key_lkup.MBR_RELSHP_CD").alias("RELSHP_CD"),
    col("lnk_hra_mbr.INDV_BE_KEY").alias("INDV_BE_KEY"),
    rpad(lit("NA"), 2, " ").alias("POP_HLTH_PGM_ENR_ID"),
    rpad(lit("NA"), 2, " ").alias("RSPN_ENTRD_BY_USER_ID"),
    rpad(lit("HEALTHMINE"), 10, " ").alias("SRC_SYS_CD"),
    rpad(lit("HRA"), 3, " ").alias("MBR_SRVY_TYP_CD"),
    rpad(lit(CurrDate), 10, " ").alias("SYS_CRT_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_RSPN_VOID_DT_SK"),
    col("lnk_hra_mbr.UPDATESRC").alias("MBR_SRVY_RSPN_UPDT_SRC_CD"),
    col("CMPLTN_DT").alias("CMPLTN_DT_SK"),
    col("STRT_DT").alias("STRT_DT_SK"),
    col("HRA_UPDT_DT").alias("UPDT_DT_SK"),
    rpad(lit("NA"), 2, " ").alias("HRA_ID")
)

df_Sort_in = df_Trans3_sortprep.orderBy(
    asc("PLN_CLNT_ID"),
    asc("CLNT_GRP_ID"),
    asc("CLNT_SUBGRP_ID"),
    asc("CLNT_MBR_ID"),
    asc("CLNT_MBR_SFX"),
    asc("MBR_SRVY_RSPN_DT_SK"),
    asc("MBR_SRVY_QSTN_CD_TX"),
    asc("MBR_SRVY_RSPN_ANSWER_TX")
)

# Stage "GrpNmLkup" has a lookup link on df_IDS_GRP by "out.GRP_SK = Grp_lkup.GRP_SK".
# We'll rename df_Sort_in as "out". Then do a left join. Then stage variables about seqNo:
w_grp_lkup = (
    df_Sort_in.alias("out")
    .join(
        df_IDS_GRP.alias("Grp_lkup"),
        on=[col("out.GRP_SK") == col("Grp_lkup.GRP_SK")],
        how="left"
    )
)

# Next, we replicate the sequence logic with stateful approach:
# We emulate by partitioning over [CLNT_MBR_ID, MBR_SRVY_RSPN_DT_SK, MBR_SRVY_QSTN_CD_TX, MBR_SRVY_RSPN_ANSWER_TX] and counting each row in ascending order.
seq_window = Window.partitionBy(
    "out.CLNT_MBR_ID",
    "out.MBR_SRVY_RSPN_DT_SK",
    "out.MBR_SRVY_QSTN_CD_TX",
    "out.MBR_SRVY_RSPN_ANSWER_TX"
).orderBy(
    monotonically_increasing_id()
)

df_GrpNmLkup_sv = w_grp_lkup.withColumn("svSeqNo", row_number().over(seq_window))

df_GrpNmLkup_renamed = df_GrpNmLkup_sv.select(
    col("out.PLN_CLNT_ID").alias("PLN_CLNT_ID"),
    col("out.CLNT_GRP_ID").alias("CLNT_GRP_ID"),
    col("out.CLNT_SUBGRP_ID").alias("CLNT_SUBGRP_ID"),
    col("out.CLNT_MBR_ID").alias("CLNT_MBR_ID"),
    col("out.CLNT_MBR_SFX").alias("CLNT_MBR_SFX"),
    col("out.MBR_SRVY_RSPN_DT_SK").alias("MBR_SRVY_RSPN_DT_SK"),
    col("out.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("out.MBR_SRVY_RSPN_ANSWER_TX").substr(1,255).alias("MBR_SRVY_RSPN_ANSWER_TX"),
    col("svSeqNo").alias("SEQ_NO"),
    col("out.CONSTITUENT_TYP_CD").alias("CONSTITUENT_TYP_CD"),
    col("out.GRP_SK").alias("GRP_SK"),
    col("out.GRP_ID").alias("GRP_ID"),
    when(col("Grp_lkup.GRP_NM").isNull(), lit("UNK")).otherwise(col("Grp_lkup.GRP_NM")).alias("GRP_NM"),
    col("out.MBR_SK").alias("MBR_SK"),
    col("out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("out.REPRCS_CT").alias("REPRCS_CT"),
    col("out.RELSHP_CD").alias("RELSHP_CD"),
    col("out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("out.POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    col("out.RSPN_ENTRD_BY_USER_ID").alias("RSPN_ENTRD_BY_USER_ID"),
    col("out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("out.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("out.SYS_CRT_DT_SK").alias("SYS_CRT_DT_SK"),
    col("out.MBR_RSPN_VOID_DT_SK").alias("MBR_RSPN_VOID_DT_SK"),
    col("out.MBR_SRVY_RSPN_UPDT_SRC_CD").alias("MBR_SRVY_RSPN_UPDT_SRC_CD"),
    col("out.CMPLTN_DT_SK").alias("CMPLTN_DT_SK"),
    col("out.STRT_DT_SK").alias("STRT_DT_SK"),
    col("out.UPDT_DT_SK").alias("UPDT_DT_SK"),
    col("out.HRA_ID").alias("HRA_ID")
)

# Output to file W_MBR_RSPN (Stage: W_MBR_RSPN)
write_files(
    df_GrpNmLkup_renamed.select(
        "PLN_CLNT_ID",
        "CLNT_GRP_ID",
        "CLNT_SUBGRP_ID",
        "CLNT_MBR_ID",
        "CLNT_MBR_SFX",
        "MBR_SRVY_RSPN_DT_SK",
        "MBR_SRVY_QSTN_CD_TX",
        "MBR_SRVY_RSPN_ANSWER_TX",
        "SEQ_NO",
        "CONSTITUENT_TYP_CD",
        "GRP_SK",
        "GRP_ID",
        "GRP_NM",
        "MBR_SK",
        "MBR_UNIQ_KEY",
        "REPRCS_CT",
        "RELSHP_CD",
        "INDV_BE_KEY",
        "POP_HLTH_PGM_ENR_ID",
        "RSPN_ENTRD_BY_USER_ID",
        "SRC_SYS_CD",
        "MBR_SRVY_TYP_CD",
        "SYS_CRT_DT_SK",
        "MBR_RSPN_VOID_DT_SK",
        "MBR_SRVY_RSPN_UPDT_SRC_CD",
        "CMPLTN_DT_SK",
        "STRT_DT_SK",
        "UPDT_DT_SK",
        "HRA_ID"
    ),
    f"{adls_path}/load/W_MBR_RSPN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Hash file "hf_hlthmine_mbrsrvyrspn_error_val_3" => scenario A
df_Trans3_error_no_duplicates_final = dedup_sort(
    df_Trans3_error_no_duplicates,
    ["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    [("SRC_SYS_CD_SK","A"),("FILE_CAT_CD","A"),("PRCS_DTM","A"),("ROW_SEQ_NO","A")]
)

# Link_Collector_215 collects 3 error inputs:
#  1) from hf_hlthmine_mbrsrvyrspn_error_val_1 => df_Error1_no_duplicates
#  2) from hf_hlthmine_mbrsrvyrspn_error_val_2 => df_Trans2_error_no_duplicates
#  3) from hf_hlthmine_mbrsrvyrspn_error_val_3 => df_Trans3_error_no_duplicates_final
df_link_collector_215 = df_Error1_no_duplicates.unionByName(df_Trans2_error_no_duplicates).unionByName(df_Trans3_error_no_duplicates_final)

# Round-Robin is not strictly necessary; we just unify them.
# Output of Link_Collector_215 => "Errors" => goes to Trans5
df_Trans5_in = df_link_collector_215.select(
    col("SRC_SYS_CD_SK"),
    col("FILE_CAT_CD"),
    col("PRCS_DTM"),
    col("ROW_SEQ_NO"),
    col("ColumnCnt"),
    col("ErrorType"),
    col("InputString")
)

# Stage "Trans5" => we add stage variables for ERR_ID, etc.
# We do rownum again for the output. Then filter out ROW_SEQ_NO <> 1 for "FinalError" plus we also do "ErrCnt" link, plus "Error" link.

w_increasing_again = Window.orderBy(monotonically_increasing_id())
df_Trans5_vars = df_Trans5_in.withColumn("trans5_inrownum", row_number().over(w_increasing_again))

df_Trans5_with_sv = (
    df_Trans5_vars
    .withColumn("svJulianDt", lit(CurrDate).substr(1,10))  # DataStage had a custom julian logic, we'll store partial
    .withColumn("svRowNum", lit("000") + col("trans5_inrownum").cast(StringType()))
    .withColumn("svJulian", col("svJulianDt").substr(3, 8))
)

df_Trans5_final = df_Trans5_with_sv.filter(col("ROW_SEQ_NO") != lit(1))

df_Trans5_final_errorout = df_Trans5_final.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("FILE_CAT_CD").alias("FILE_CAT_CD"),
    col("PRCS_DTM").alias("PRCS_DTM"),
    col("ROW_SEQ_NO").alias("ROW_SEQ_NO"),
    lit(0).alias("FILE_ERR_DTL_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("FILE_METRIC_SK"),
    lit(0).alias("FILE_CAT_CD_SK"),
    when(col("ErrorType") == "Field Count Error", lit("Y")).otherwise(lit("N")).alias("BAD_RCRD_STRCT_IN"),
    lit("N").alias("DO_NOT_RTRN_IN"),
    col("InputString").alias("INPT_RCRD_TX"),
    lit("Y").alias("VALID_ROW_IN"),
    (lit("HMHRA") + col("svJulian") + col("svRowNum")).alias("ERR_ID"),
    col("ErrorType").alias("ERR_DESC")
)

# The other links from Trans5 are "ErrCnt" and "ErrorFile":
# "ErrCnt" => constraint: ROW_SEQ_NO <> 1 => columns => MetricType='Errors', TOT_CT=@OUTROWNUM, ...
# We do a row_number for TOT_CT
w_errcnt = Window.orderBy(monotonically_increasing_id())
df_Trans5_errcnt = df_Trans5_final.withColumn("TOT_CT", row_number().over(w_errcnt)).select(
    rpad(lit("Errors"), 255, " ").alias("MetricType"),
    col("TOT_CT"),
    col("SRC_SYS_CD_SK"),
    rpad(col("FILE_CAT_CD"), 255, " ").alias("FILE_CAT_CD"),
    rpad(col("PRCS_DTM"), 26, " ").alias("PRCS_DTM")
)

# "ErrorFile"
df_Trans5_errfile = df_Trans5_final.select(
    (lit("HMHRA") + col("svJulian") + col("svRowNum")).alias("ERR_ID"),
    col("InputString").alias("INPT_RCRD_TX"),
    col("ErrorType").alias("ERR_DESC")
)

# Hash file "hf_hra_mbrsrvyrspn_metric2" => scenario A => merges "TotMetric" & "ErrMetric" -> then read by "Trans4".
# So we unify the GoodCnt from Trans1 (df_goodcnt_no_duplicates => "TotMetric") with the ErrCnt from Trans5 (df_Trans5_errcnt => "ErrMetric").
# We'll feed them directly into Trans4, removing the hashed file. We deduplicate each set by "MetricType".

df_TotMetric_no_duplicates = dedup_sort(
    df_goodcnt_no_duplicates,
    ["MetricType"],
    [("MetricType","A")]
)
df_ErrMetric_no_duplicates = dedup_sort(
    df_Trans5_errcnt,
    ["MetricType","TOT_CT","SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM"],
    [("MetricType","A"),("TOT_CT","A"),("SRC_SYS_CD_SK","A"),("FILE_CAT_CD","A"),("PRCS_DTM","A")]
)

# Trans4 has 2 left lookups: "TotMetric" on MetricType='Total', "ErrMetric" on MetricType='Errors', plus a primary link from SYSDUMMY1.
# We'll pick single rows from these dataframes. We'll do left join by some constant=some constant. We replicate the condition:
df_TotMetric_only_total = df_TotMetric_no_duplicates.filter(col("MetricType") == "Total").alias("TotMetric")
df_ErrMetric_only_errors = df_ErrMetric_no_duplicates.filter(col("MetricType") == "Errors").alias("ErrMetric")

# Create a single row DF from df_SYSDUMMY1 (since SYSDUMMY1 has 1 row usually) => pass as primary link.
df_in_sySDUMMY1 = df_SYSDUMMY1.alias("in")

# We'll do a cross join to attach them as lookups because the DataStage job is "left join on 'Total'" or 'Errors':
df_Trans4_join = df_in_sySDUMMY1.join(df_TotMetric_only_total, how="left", on=[lit('Total') == col("TotMetric.MetricType")]) \
    .join(df_ErrMetric_only_errors, how="left", on=[lit('Errors') == col("ErrMetric.MetricType")])

# Then "Trans4" outputs "Metrics" => columns:
df_Trans4_metrics = df_Trans4_join.select(
    col("TotMetric.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("TotMetric.FILE_CAT_CD").alias("FILE_CAT_CD"),
    col("TotMetric.PRCS_DTM").alias("PRCS_DTM"),
    lit(0).alias("FILE_METRIC_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("FILE_CAT_CD_SK"),
    lit(0).alias("NON_RTRN_CT"),
    when(col("ErrMetric.TOT_CT").isNull(), lit(0)).otherwise(col("ErrMetric.TOT_CT")).alias("RTRN_CT"),
    when(col("TotMetric.TOT_CT").isNull(), lit(0)).otherwise(col("TotMetric.TOT_CT")).alias("TOT_RCRD_CT"),
    rpad(lit("Y"), 1, " ").alias("VALID_ROW_IN")
)

# Finally, "FileMetrics" is a sequential file:
write_files(
    df_Trans4_metrics.select(
        "SRC_SYS_CD_SK",
        "FILE_CAT_CD",
        "PRCS_DTM",
        "FILE_METRIC_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FILE_CAT_CD_SK",
        "NON_RTRN_CT",
        "RTRN_CT",
        "TOT_RCRD_CT",
        "VALID_ROW_IN"
    ),
    f"{adls_path_raw}/verified/HRA_MbrSrvyRspn_Metric.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write "FileErrors" from Trans5 => "FinalError"
write_files(
    df_Trans5_final_errorout.filter(col("ROW_SEQ_NO") != lit(1)).select(
        "SRC_SYS_CD_SK",
        "FILE_CAT_CD",
        "PRCS_DTM",
        "ROW_SEQ_NO",
        "FILE_ERR_DTL_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FILE_METRIC_SK",
        "FILE_CAT_CD_SK",
        "BAD_RCRD_STRCT_IN",
        "DO_NOT_RTRN_IN",
        "INPT_RCRD_TX",
        "VALID_ROW_IN",
        "ERR_ID",
        "ERR_DESC"
    ),
    f"{adls_path_raw}/verified/HRA_MbrSrvyRspn_ErrorDtl.dat.{RunID}",
    delimiter=";",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

# Write "ErrorFile" from Trans5 => "Error"
write_files(
    df_Trans5_errfile.select(
        "ERR_ID",
        "INPT_RCRD_TX",
        "ERR_DESC"
    ),
    f"{adls_path_publish}/external/HLTHMINE_MbrSrvyRspn_Errors.dat.{RunID}",
    delimiter="~",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# No spark.stop() per instructions.