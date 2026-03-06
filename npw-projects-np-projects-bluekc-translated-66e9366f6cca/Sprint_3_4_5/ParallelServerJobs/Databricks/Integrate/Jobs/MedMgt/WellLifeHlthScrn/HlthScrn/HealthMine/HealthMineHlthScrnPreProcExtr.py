# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:  HlthMineHlthScrnExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Input file validation for Health Mine Biometric File.  File metrics and errors are written to output that another job loads to the IDS file metrics reporting tables.
# MAGIC                            File Validations:
# MAGIC                                                      * Column count matches expected number
# MAGIC                                                      * Validate Source System Code                                                                   
# MAGIC                                                              SrcSysCd from Zena                                                      
# MAGIC                                                       * Validate Screening Date
# MAGIC                                                       * Validate for existing member
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================
# MAGIC Developer	Date		Project/Altiris #	Change Description					Development Project	Code Reviewer	Date Reviewed
# MAGIC =====================================================================================================================================================
# MAGIC Abhiram Dasarathy	2016-08-10	5414 - MEP	Original Programming				IntegrateDev1		 Kalyan Neelam        2016-08-16

# MAGIC Sequential File Format settings: 
# MAGIC Line Delimiter 016 (/n)    
# MAGIC Quote Character 000
# MAGIC Count Number of delimeters in record.  Discard any that don't match expected count.
# MAGIC Validate 'screen date' and 'source system code'.  Discard any rows that fail validation. Input file 'source system code' must match input parm SrcSysCd from ZENA.
# MAGIC Combine errors and write to output.
# MAGIC Use @INROWNUM to collect the total number of input records
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character "
# MAGIC Delimiter ,
# MAGIC File used in IdsFileMetricLoad
# MAGIC #$FilePath#/verified/HLTHMINE_HLTHSCRN_ #SrcSysCd#_FileMetrics. dat.#RunID#
# MAGIC Temp to split InputString into Health Screening fields
# MAGIC Input - Delimiter ',', Quote Character '000' 
# MAGIC Output - Delimiter ',', Quote Character '"'
# MAGIC #$FilePath#/landing/HLTHMINE_HLTHSCRN_ #SrcSysCd#_TempFile.dat
# MAGIC VENDOR REJECT- Error File is sent back to VENDOR through Sterling Server; Non-Member errors are NOT returned to vendor.
# MAGIC #$FilePath#/external/HLTHMINE_HLTHSCRN_ #SrcSysCd#_ErrorRecords. dat.#RunID#
# MAGIC Health Mine Health Screen Pre Process & Transaction File Validation
# MAGIC Error in input file format
# MAGIC Error in field value
# MAGIC Invalid MBR ID, not returned to vendor, separate count
# MAGIC Invalid MBR ID - will FTP to LAN for BlueKC
# MAGIC #$FilePath#/external/ HLTHMINE_HLTHSCRN_ #SrcSysCd#_ MbrErr. dat.#RunID#
# MAGIC (to WellLifeBioHlthScrnExtr)
# MAGIC #$FilePath#/verified/ 
# MAGIC HLTHMINE_HLTHSCRN_ #SrcSysCd#_LandingFile. dat.#RunID#
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character 000
# MAGIC Delimiter ;
# MAGIC File used in IdsFileMetricLoad
# MAGIC #$FilePath#/verified/HLTHMINE_HLTHSCRN_ #SrcSysCd#_FileErrors. dat.#RunID#
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    CharType
)
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    split,
    size,
    length,
    trim,
    row_number,
    monotonically_increasing_id,
    last,
    concat
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ColumnCount = get_widget_value('ColumnCount','')
CurrentTS = get_widget_value('CurrentTS','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
DataContent = get_widget_value('DataContent','')
FileName = get_widget_value('FileName','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
RunID = get_widget_value('RunID','')
RecCountTrailer = get_widget_value('RecCountTrailer','')
FileRecCount = get_widget_value('FileRecCount','')

# Stage 1: Input (CSeqFileStage) - read entire line into one column
schema_Input = StructType([
    StructField("InputString", StringType(), False)
])
df_Input = (
    spark.read
    .option("header", False)
    .option("delimiter", "~")  # Use a delimiter unlikely to appear, keeping entire line in one column
    .schema(schema_Input)
    .csv(f"{adls_path_raw}/landing/{FileName}")
)

# Stage 2: Trans1 (CTransformerStage)

# Add columns needed for expressions
df_Trans1a = (
    df_Input
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk).cast("int"))
    .withColumn("CurrentTS", lit(CurrentTS))
    .withColumn("FileRecCount", lit(FileRecCount).cast("int"))
    .withColumn("RecCountTrailer", lit(RecCountTrailer).cast("int"))
    .withColumn("ColumnCount", lit(ColumnCount).cast("int"))
)

# Generate rowNumber (simulating @INROWNUM)
w_row = Window.orderBy(monotonically_increasing_id())
df_Trans1a = df_Trans1a.withColumn("rowNumber", row_number().over(w_row))

# svFieldCount: If (IsNull(svString) or Len(Trim(svString))=0) then 0 else number_of_fields
df_Trans1a = df_Trans1a.withColumn(
    "svFieldCount",
    when(
        col("InputString").isNull() | (length(trim(col("InputString"))) == 0),
        lit(0)
    ).otherwise(size(split(col("InputString"), "\\|")))
)

# We fill down the first-row-based logic for svValidRowInd
df_Trans1a = df_Trans1a.withColumn(
    "firstRowValidRowInd",
    when(
        col("rowNumber") == 1,
        when(col("svFieldCount") != col("ColumnCount"), lit("N")).otherwise(lit("Y"))
    ).otherwise(lit(None).cast("string"))
)

w_fill = Window.orderBy("rowNumber").rowsBetween(Window.unboundedPreceding, 0)
df_Trans1a = df_Trans1a.withColumn(
    "svValidRowInd",
    last("firstRowValidRowInd", True).over(w_fill)
)

# Check for header record in first row, then fill down
expected_header = "INDV_BE_KEY|MBR_UNIQ_KEY|TESTDATE|HEIGHT_IN|WEIGHT|RESTSYS|RESTDIAS|TOTCHOL|HDL|LDL|chlstrl_ratio_no|TRIG|GLUCOSE|Fasting|WAISTCIRCUMFERENCE|BMI|SOURCE_CODE|Error_ID|Facility_Name|Facility_Address|Facility_City|Facility_State_Code|Facility_Zip"
df_Trans1a = df_Trans1a.withColumn(
    "firstRowHeaderRecCheckInd",
    when(
        col("rowNumber") == 1,
        when(trim(col("InputString")) == lit(expected_header), lit("Y")).otherwise(lit("N"))
    ).otherwise(lit(None).cast("string"))
)

df_Trans1a = df_Trans1a.withColumn(
    "svHeaderRecCheckInd",
    last("firstRowHeaderRecCheckInd", True).over(w_fill)
)

# svTrailerRecCheckInd: If RecCountTrailer = (FileRecCount - 2) Then 'Y' Else 'N'
df_Trans1a = df_Trans1a.withColumn(
    "svTrailerRecCheckInd",
    when(col("RecCountTrailer") == (col("FileRecCount") - lit(2)), lit("Y")).otherwise(lit("N"))
)

# svValidRow: If svFieldCount = ColumnCount => True else False
df_Trans1a = df_Trans1a.withColumn(
    "svValidRow",
    (col("svFieldCount") == col("ColumnCount"))
)

# Output links from Trans1

# Error1 constraint: svValidRow = false OR svValidRowInd!='Y' OR svHeaderRecCheckInd!='Y' OR svTrailerRecCheckInd!='Y'
df_Error1 = (
    df_Trans1a
    .filter(
        (~col("svValidRow")) |
        (col("svValidRowInd") != "Y") |
        (col("svHeaderRecCheckInd") != "Y") |
        (col("svTrailerRecCheckInd") != "Y")
    )
    .select(
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        lit("SCRNG").alias("FILE_CAT_CD"),
        col("CurrentTS").alias("PRCS_DTM"),
        col("rowNumber").alias("ROW_SEQ_NO"),
        col("svFieldCount").alias("ColumnCnt"),
        lit("Field Count Error").alias("ErrorType"),
        col("InputString")
    )
)

# GoodCnt constraint: (rowNumber <> 1)
df_GoodCnt = (
    df_Trans1a
    .filter(col("rowNumber") != lit(1))
    .select(
        lit("Total").alias("MetricType"),
        (col("rowNumber") - lit(1)).alias("TOT_RCRD_CT"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        lit("SCRNG").alias("FILE_CAT_CD"),
        col("CurrentTS").alias("PRCS_DTM")
    )
)

# Valid1 constraint: svValidRow= True AND svValidRowInd='Y' AND svHeaderRecCheckInd='Y' AND svTrailerRecCheckInd='Y' AND rowNumber != FileRecCount
df_Valid1 = (
    df_Trans1a
    .filter(
        (col("svValidRow")) &
        (col("svValidRowInd") == "Y") &
        (col("svHeaderRecCheckInd") == "Y") &
        (col("svTrailerRecCheckInd") == "Y") &
        (col("rowNumber") != col("FileRecCount"))
    )
    .select(
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        lit("SCRNG").alias("FILE_CAT_CD"),
        col("CurrentTS").alias("PRCS_DTM"),
        col("rowNumber").alias("ROW_SEQ_NO"),
        col("svFieldCount").alias("FieldCnt"),
        col("InputString"),
        concat(lit("\""), col("InputString"), lit("\"")).alias("InputString_1")
    )
)

# Stage 3: hf_hlthmine_hlthscrn_dtl_total_metric_1 (CHashedFileStage)
# Scenario A: "Trans1 -> CHashedFileStage -> next stage" with no same-file rewrite => remove duplicates across key columns if any
# The job marks "MetricType" as primary key. We deduplicate on that.

df_hf_hlthmine_hlthscrn_dtl_total_metric_1 = dedup_sort(
    df_GoodCnt,
    partition_cols=["MetricType"],
    sort_cols=[]
)

# Stage 4: SYSDUMMY1 (DB2Connector) reading from IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

# Stage 5: hf_hlthmine_hlthscrn_dtl_error_val_1 (CHashedFileStage)
# Also scenario A: "Trans1 -> hf -> next" => deduplicate on PK = (SRC_SYS_CD_SK, FILE_CAT_CD, PRCS_DTM, ROW_SEQ_NO)

df_hf_hlthmine_hlthscrn_dtl_error_val_1 = dedup_sort(
    df_Error1,
    partition_cols=["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    sort_cols=[]
)

# Stage 6: hf_etrnl_mbr_uniq_key (CHashedFileStage) => used as a lookup source
# This is scenario C (a hashed file used as source). We read from parquet, no dedup needed here unless the job had that logic.
df_hf_etrnl_mbr_uniq_key = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_uniq_key.parquet")

# Stage 7: HlthMine_Hlthscrn_TempFile (CSeqFileStage) - Write then read the same file
# We first produce the final data that matches the 29 columns the job defines on output.

# The job’s stage mapping: 
#  1..5 direct from df_Valid1 columns
#  6..28 come from splitting "InputString" into 23 fields
#  29th is "InputString_1"

df_HlthMine_Hlthscrn_TempFile = (
    df_Valid1
    .withColumn("arr", split(col("InputString"), "\\|"))
    .select(
        col("SRC_SYS_CD_SK"),
        col("FILE_CAT_CD"),
        col("PRCS_DTM"),
        col("ROW_SEQ_NO"),
        col("FieldCnt"),
        col("arr")[0].alias("INDV_BE_KEY"),
        col("arr")[1].alias("MBR_UNIQ_KEY"),
        col("arr")[2].alias("TESTDATE"),
        col("arr")[3].alias("HEIGHT_IN"),
        col("arr")[4].alias("WEIGHT"),
        col("arr")[5].alias("RESTSYS"),
        col("arr")[6].alias("RESTDIAS"),
        col("arr")[7].alias("TOTCHOL"),
        col("arr")[8].alias("HDL"),
        col("arr")[9].alias("LDL"),
        col("arr")[10].alias("CHLSTRL_HDL_RATIO"),
        col("arr")[11].alias("TRIG"),
        col("arr")[12].alias("GLUCOSE"),
        col("arr")[13].alias("FASTING"),
        col("arr")[14].alias("WAISTCIRCOMFERENCE"),
        col("arr")[15].alias("BMI"),
        col("arr")[16].alias("SOURCE_CODE"),
        col("arr")[17].alias("ERR_ID"),
        col("arr")[18].alias("FCLTY_NM"),
        col("arr")[19].alias("FCLTY_ADDR"),
        col("arr")[20].alias("FCLTY_CITY"),
        col("arr")[21].alias("FCLTY_ST_CD"),
        col("arr")[22].alias("FCLTY_ZIP"),
        col("InputString_1")
    )
)

write_files(
    df_HlthMine_Hlthscrn_TempFile,
    f"{adls_path_raw}/landing/HLTHMINE_HLTHSCRN_{SrcSysCd}.TempFile.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="000",
    nullValue=None
)

# Now the same stage reads that file. We define a schema for the 29 columns:
schema_HlthMine_Hlthscrn_TempFile = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("FILE_CAT_CD", StringType(), False),
    StructField("PRCS_DTM", StringType(), False),
    StructField("ROW_SEQ_NO", IntegerType(), False),
    StructField("FieldCnt", IntegerType(), False),
    StructField("INDV_BE_KEY", StringType(), False),
    StructField("MBR_UNIQ_KEY", StringType(), False),
    StructField("TESTDATE", CharType(10), False),
    StructField("HEIGHT_IN", StringType(), False),
    StructField("WEIGHT", StringType(), False),
    StructField("RESTSYS", StringType(), False),
    StructField("RESTDIAS", StringType(), False),
    StructField("TOTCHOL", StringType(), False),
    StructField("HDL", StringType(), False),
    StructField("LDL", StringType(), False),
    StructField("CHLSTRL_HDL_RATIO", StringType(), False),
    StructField("TRIG", StringType(), False),
    StructField("GLUCOSE", StringType(), False),
    StructField("FASTING", StringType(), True),
    StructField("WAISTCIRCOMFERENCE", StringType(), False),
    StructField("BMI", StringType(), False),
    StructField("SOURCE_CODE", StringType(), False),
    StructField("ERR_ID", StringType(), True),
    StructField("FCLTY_NM", StringType(), True),
    StructField("FCLTY_ADDR", StringType(), True),
    StructField("FCLTY_CITY", CharType(20), True),
    StructField("FCLTY_ST_CD", CharType(2), True),
    StructField("FCLTY_ZIP", IntegerType(), True),
    StructField("InputString_1", StringType(), True)
])

df_Trans2_input = (
    spark.read
    .option("header", True)             # The job says "ContainsHeader"=true
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_HlthMine_Hlthscrn_TempFile)
    .csv(f"{adls_path_raw}/landing/HLTHMINE_HLTHSCRN_{SrcSysCd}.TempFile.dat")
)

# Stage 8: Trans2 (CTransformerStage)
# Implement stage variables and constraints
df_Trans2_vars = (
    df_Trans2_input
    .withColumn("svTestDtFrmt", FORMAT_DATE(col("TESTDATE"), "MMDDCCYY", "DATE", "SYBTIMESTAMP"))  # Assume user-defined function
    .withColumn("svCurrDt", when(
        col("svTestDtFrmt").substr(1,10) <= lit(CurrDate),
        lit("YES")
    ).otherwise(lit("NO")))
    .withColumn("svScrnDtFrmt", FORMAT_DATE(col("svTestDtFrmt"), "SYBASE", "DATE", "MM/DD/CCYY"))  # Another user-defined call
    .withColumn("svScrnDtMsg", when(
        (col("svScrnDtFrmt").isNotNull()) & (length(col("svScrnDtFrmt")) == 10) & (col("svCurrDt") == "YES"),
        lit("GOOD")
    ).otherwise(lit("BAD DATE")))
    .withColumn("svValidDate", (col("svScrnDtMsg") == "GOOD"))
    .withColumn("svErrorTypeDt", when(
        col("svScrnDtMsg") == "GOOD", lit("YES")
    ).otherwise(concat(lit("Screen Date "), col("TESTDATE"))))
    .withColumn("svValidRow", (col("svErrorTypeDt") == "YES"))
    .withColumn("svErrorType",
        when(col("svValidRow"), lit(None).cast("string"))
        .otherwise(concat(lit("Screen Date "), col("TESTDATE")))
    )
)

df_Error2 = (
    df_Trans2_vars
    .filter(col("svErrorTypeDt") != "YES")
    .select(
        col("SRC_SYS_CD_SK"),
        col("FILE_CAT_CD"),
        col("PRCS_DTM"),
        col("ROW_SEQ_NO"),
        col("FieldCnt").alias("ColumnCnt"),
        lit("START DATE GREATER THAN FILE SENT DATE").alias("ErrorType"),
        col("InputString_1").alias("InputString")
    )
)

df_Valid2 = (
    df_Trans2_vars
    .filter(col("svValidRow"))
    .select(
        col("SRC_SYS_CD_SK"),
        col("FILE_CAT_CD"),
        col("PRCS_DTM"),
        col("ROW_SEQ_NO"),
        col("FieldCnt"),
        col("INDV_BE_KEY"),
        col("MBR_UNIQ_KEY"),
        col("svScrnDtFrmt").alias("TESTDATE"),
        col("HEIGHT_IN"),
        col("WEIGHT"),
        col("RESTSYS"),
        col("RESTDIAS"),
        col("TOTCHOL"),
        col("HDL"),
        col("LDL"),
        col("CHLSTRL_HDL_RATIO"),
        col("TRIG"),
        col("GLUCOSE"),
        col("FASTING"),
        col("WAISTCIRCOMFERENCE"),
        col("BMI"),
        col("SOURCE_CODE"),
        col("ERR_ID"),
        col("FCLTY_NM"),
        col("FCLTY_ADDR"),
        col("FCLTY_CITY"),
        col("FCLTY_ST_CD"),
        col("FCLTY_ZIP"),
        col("InputString_1")
    )
)

# Stage 9: hf_hlthmine_hlthscrn_dtl_error_val_2 (CHashedFileStage)
df_hf_hlthmine_hlthscrn_dtl_error_val_2 = dedup_sort(
    df_Error2,
    partition_cols=["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    sort_cols=[]
)

# Stage 10: Trans3 (CTransformerStage) - has 2 inputs: a left lookup on df_hf_etrnl_mbr_uniq_key, and primary df_Valid2
# Join conditions: Valid.MBR_UNIQ_KEY == mbr_uniq_key_lkup.MBR_UNIQ_KEY (left join)

df_Trans3_joined = (
    df_Valid2.alias("Valid")
    .join(
        df_hf_etrnl_mbr_uniq_key.alias("mbr_uniq_key_lkup"),
        on=[col("Valid.MBR_UNIQ_KEY") == col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")],
        how="left"
    )
    .withColumn("MbrLookup", when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNotNull(), lit("GOOD")).otherwise(lit("MBR_UNIQ_KEY MIS-MATCH")))
    .withColumn("ValidRow", when(col("MbrLookup") == "GOOD", lit(True)).otherwise(lit(False)))
    .withColumn("SourceCode", when(
        (col("Valid.SOURCE_CODE") == "ONSITE") | 
        (col("Valid.SOURCE_CODE") == "CLINICAL") | 
        (col("Valid.SOURCE_CODE") == "OFFICE"),
        lit("GOOD")
    ).otherwise(lit("SOURCE CODE NOT IN THE LIST PROVIDED")))
)

df_Error3 = (
    df_Trans3_joined
    .filter((~col("ValidRow")) | (col("SourceCode") != "GOOD"))
    .select(
        col("Valid.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("Valid.FILE_CAT_CD").alias("FILE_CAT_CD"),
        col("Valid.PRCS_DTM").alias("PRCS_DTM"),
        col("Valid.ROW_SEQ_NO").alias("ROW_SEQ_NO"),
        col("Valid.FieldCnt").alias("ColumnCnt"),
        when(
            (~col("ValidRow")) & (col("SourceCode")=="GOOD"),
            col("MbrLookup")
        ).otherwise(col("SourceCode")).alias("ErrorType"),
        col("Valid.InputString_1").alias("InputString")
    )
)

df_Valid3 = (
    df_Trans3_joined
    .filter((col("ValidRow")) & (col("SourceCode") == "GOOD"))
    .select(
        (row_number().over(Window.orderBy(monotonically_increasing_id()))).alias("RowNum"),
        col("Valid.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("Valid.FILE_CAT_CD").alias("FILE_CAT_CD"),
        col("Valid.INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("Valid.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("Valid.TESTDATE").alias("TESTDATE"),
        col("Valid.HEIGHT_IN").alias("HEIGHT_IN"),
        col("Valid.WEIGHT").alias("WEIGHT"),
        col("Valid.RESTSYS").alias("RESTSYS"),
        col("Valid.RESTDIAS").alias("RESTDIAS"),
        col("Valid.TOTCHOL").alias("TOTCHOL"),
        col("Valid.HDL").alias("HDL"),
        col("Valid.LDL").alias("LDL"),
        col("Valid.CHLSTRL_HDL_RATIO").alias("CHLSTRL_HDL_RATIO"),
        col("Valid.TRIG").alias("TRIG"),
        col("Valid.GLUCOSE").alias("GLUCOSE"),
        col("Valid.FASTING").alias("FASTING"),
        col("Valid.WAISTCIRCOMFERENCE").alias("WAISTCIRCOMFERENCE"),
        col("Valid.BMI").alias("BMI"),
        col("Valid.SOURCE_CODE").alias("SOURCE_CODE"),
        col("Valid.ERR_ID").alias("ERR_ID"),
        col("Valid.FCLTY_NM").alias("FCLTY_NM"),
        col("Valid.FCLTY_ADDR").alias("FCLTY_ADDR"),
        col("Valid.FCLTY_CITY").alias("FCLTY_CITY"),
        col("Valid.FCLTY_ST_CD").alias("FCLTY_ST_CD"),
        col("Valid.FCLTY_ZIP").alias("FCLTY_ZIP")
    )
)

# Stage 11: hf_hlthmine_hlthscrn_dtl_nonmbr_val_1 (CHashedFileStage)
df_hf_hlthmine_hlthscrn_dtl_nonmbr_val_1 = dedup_sort(
    df_Error3,
    partition_cols=["SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM","ROW_SEQ_NO"],
    sort_cols=[]
)

# Stage 12: Trans6 (CTransformerStage) - input is df_Valid3
df_Trans6 = (
    df_Valid3
    .withColumn("svMbrUniqKey", when((col("MBR_UNIQ_KEY").isNull()) | (length(col("MBR_UNIQ_KEY")) == 0), lit("N")).otherwise(lit("Y")))
    .withColumn("svScrnDtFrmt", FORMAT_DATE(col("TESTDATE"), "DATE", "MM/DD/CCYY", "CCYY-MM-DD"))
    .withColumn("svScrnDt", when((col("svScrnDtFrmt").isNull()) | (length(col("svScrnDtFrmt")) == 0), lit("1753-01-01")).otherwise(col("svScrnDtFrmt")))
)

df_WriteToFile = df_Trans6.select(
    lit("0").alias("HLTH_SCRN_SK"),
    lit("HEALTHMINE").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("svScrnDt").alias("SCRN_DT"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("GRP_SK"),
    lit("0").alias("MBR_SK"),
    lit("0").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
    lit("N").alias("CUR_SMOKER_RISK_IN"),
    lit("N").alias("DBTC_RISK_IN"),
    lit("N").alias("FRMR_SMOKER_RISK_IN"),
    lit("N").alias("HEART_DSS_RISK_IN"),
    lit("N").alias("HI_CHLSTRL_RISK_IN"),
    lit("N").alias("HI_BP_RISK_IN"),
    lit("N").alias("EXRCS_LACK_RISK_IN"),
    lit("N").alias("OVERWT_RISK_IN"),
    lit("N").alias("STRESS_RISK_IN"),
    lit("N").alias("STROKE_RISK_IN"),
    when((col("BMI").isNull())|(length(col("BMI"))==0), lit("0")).otherwise(col("BMI")).alias("BMI_NO"),
    when((col("CHLSTRL_HDL_RATIO").isNull())|(length(col("CHLSTRL_HDL_RATIO"))==0), lit("0")).otherwise(col("CHLSTRL_HDL_RATIO")).alias("CHLSTRL_RATIO_NO"),
    when((col("RESTDIAS").isNull())|(length(col("RESTDIAS"))==0), lit("0")).otherwise(col("RESTDIAS")).alias("DIASTOLIC_BP_NO"),
    when((col("GLUCOSE").isNull())|(length(col("GLUCOSE"))==0), lit("0")).otherwise(col("GLUCOSE")).alias("GLUCOSE_NO"),
    when((col("HDL").isNull())|(length(col("HDL"))==0), lit("0")).otherwise(col("HDL")).alias("HDL_NO"),
    when((col("HEIGHT_IN").isNull())|(length(col("HEIGHT_IN"))==0), lit("0")).otherwise(col("HEIGHT_IN")).alias("HT_INCH_NO"),
    when((col("LDL").isNull())|(length(col("LDL"))==0), lit("0")).otherwise(col("LDL")).alias("LDL_NO"),
    lit("0").alias("MBR_AGE_NO"),
    lit("0").alias("PSA_NO"),
    when((col("RESTSYS").isNull())|(length(col("RESTSYS"))==0), lit("0")).otherwise(col("RESTSYS")).alias("SYSTOLIC_BP_NO"),
    when((col("TOTCHOL").isNull())|(length(col("TOTCHOL"))==0), lit("0")).otherwise(col("TOTCHOL")).alias("TOT_CHLSTRL_NO"),
    when((col("TRIG").isNull())|(length(col("TRIG"))==0), lit("0")).otherwise(col("TRIG")).alias("TGL_NO"),
    when((col("WAISTCIRCOMFERENCE").isNull())|(length(col("WAISTCIRCOMFERENCE"))==0), lit("0")).otherwise(col("WAISTCIRCOMFERENCE")).alias("WAIST_CRCMFR_NO"),
    when((col("WEIGHT").isNull())|(length(col("WEIGHT"))==0), lit("0")).otherwise(col("WEIGHT")).alias("WT_NO"),
    lit(None).cast("string").alias("BODY_FAT_PCT"),
    when((col("FASTING").isNull()) | (length(col("FASTING"))==0), lit("N")).otherwise(
        when(col("FASTING")==lit("1"), lit("Y")).otherwise(lit("N"))
    ).alias("FSTNG_IN"),
    lit("0").alias("HA1C_NO"),
    lit("N").alias("PRGNCY_IN"),
    lit("N").alias("RFRL_TO_DM_IN"),
    lit("N").alias("RFRL_TO_PHYS_IN"),
    lit("N").alias("RESCRN_IN"),
    lit("0").alias("TSH_NO"),
    lit("0").alias("BONE_DENSITY_NO"),
    when((col("SOURCE_CODE").isNull())|(length(col("SOURCE_CODE"))==0), lit(None).cast("string")).otherwise(trim(col("SOURCE_CODE")).alias("dummy_source")).alias("HLTH_SCRN_SRC_SUBTYP"),
    lit("0").alias("NCTN_TST_RSLT_CD"),
    lit(None).cast("string").alias("HLTH_SCRN_VNDR_CD")
)

# Stage 13: ValidOutput (CSeqFileStage)
write_files(
    df_WriteToFile,
    f"{adls_path}/verified/HLTHMINE_HLTHSCRN_{SrcSysCd}_LandingFile.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage 14: hf_hlthmine_hlthscrn_dtl_error_val_2 handled above

# Stage 15: hf_hlthmine_hlthscrn_dtl_nonmbr_val_1 handled above

# Stage 16: Link_Collector (CCollector)
# Union of the three deduped error DataFrames: hf_hlthmine_hlthscrn_dtl_error_val_1, hf_hlthmine_hlthscrn_dtl_error_val_2, hf_hlthmine_hlthscrn_dtl_nonmbr_val_1
# All share columns (SRC_SYS_CD_SK, FILE_CAT_CD, PRCS_DTM, ROW_SEQ_NO, ColumnCnt, ErrorType, InputString)
df_LinkCols_1 = df_hf_hlthmine_hlthscrn_dtl_error_val_1.select(
    col("SRC_SYS_CD_SK"), col("FILE_CAT_CD"), col("PRCS_DTM"), col("ROW_SEQ_NO"), col("ColumnCnt"), col("ErrorType"), col("InputString")
)
df_LinkCols_2 = df_hf_hlthmine_hlthscrn_dtl_error_val_2.select(
    col("SRC_SYS_CD_SK"), col("FILE_CAT_CD"), col("PRCS_DTM"), col("ROW_SEQ_NO"), col("ColumnCnt"), col("ErrorType"), col("InputString")
)
df_LinkCols_3 = df_hlthmine_hlthscrn_dtl_nonmbr_val_1.select(
    col("SRC_SYS_CD_SK"), col("FILE_CAT_CD"), col("PRCS_DTM"), col("ROW_SEQ_NO"), col("ColumnCnt"), col("ErrorType"), col("InputString")
)
df_Link_Collector = df_LinkCols_1.union(df_LinkCols_2).union(df_LinkCols_3)

# Stage 17: Trans5 (CTransformerStage)
df_Trans5_vars = (
    df_Link_Collector
    .withColumn("svJulianDt", FORMAT_DATE(lit(CurrDate), "DATE", "DATE", "JULIAN"))
    .withColumn("svRowNum", concat(lit("000"), row_number().over(Window.orderBy(monotonically_increasing_id()))))
    .withColumn("svJulian", trim(col("svJulianDt")).substr(3, length(col("svJulianDt"))))
)

# Two outputs: FinalError -> FileErrors, ErrCnt -> hf_hlthmine_hlthscrn_metric_1, ErrorString -> ErrorFile
df_FinalError = (
    df_Trans5_vars
    .filter(col("ROW_SEQ_NO") != 1)
    .select(
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("FILE_CAT_CD").alias("FILE_CAT_CD"),
        col("PRCS_DTM").alias("PRCS_DTM"),
        (row_number().over(Window.orderBy(monotonically_increasing_id()))).alias("ROW_SEQ_NO"),
        lit("0").alias("FILE_ERR_DTL_SK"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("0").alias("FILE_METRIC_SK"),
        lit("0").alias("FILE_CAT_CD_SK"),
        when(col("ErrorType")=="Field Count Error", lit("Y")).otherwise(lit("N")).alias("BAD_RCRD_STRCT_IN"),
        when(col("ErrorType")=="NonMbr", lit("Y")).otherwise(lit("N")).alias("DO_NOT_RTRN_IN"),
        col("InputString").alias("INPT_RCRD_TX"),
        lit("Y").alias("VALID_ROW_IN"),
        concat(lit("HMBIO"), col("svJulian"), col("svRowNum")).alias("ERR_ID"),
        col("ErrorType").alias("ERR_DESC")
    )
)

df_ErrCnt = (
    df_Trans5_vars
    .filter(col("ROW_SEQ_NO") != 1)
    .select(
        lit("Errors").alias("MetricType"),
        (row_number().over(Window.orderBy(monotonically_increasing_id()))).alias("TOT_CT"),
        col("SRC_SYS_CD_SK"),
        col("FILE_CAT_CD"),
        col("PRCS_DTM")
    )
)

df_ErrorString = (
    df_Trans5_vars
    .filter(col("ROW_SEQ_NO") != 1)
    .select(
        concat(lit("HMBIO"), col("svJulian"), col("svRowNum")).alias("ERR_ID"),
        col("InputString").alias("INPT_RCRD_TX"),
        col("ErrorType").alias("ERR_DESC")
    )
)

# Stage 18: FileErrors (CSeqFileStage)
write_files(
    df_FinalError,
    f"{adls_path}/verified/HLTHMINE_HLTHSCRN_{SrcSysCd}_FileErrors.dat.{RunID}",
    delimiter=";",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="000",
    nullValue=None
)

# Stage 19: ErrorFile (CSeqFileStage)
write_files(
    df_ErrorString,
    f"{adls_path_publish}/external/HLTHMINE_HLTHSCRN_{SrcSysCd}_ErrorRecords.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage 20: hf_hlthmine_hlthscrn_metric_1 (CHashedFileStage)
# The stage merges "ErrCnt" with "hf_hlthmine_hlthscrn_dtl_total_metric_1" under readFileProperties => scenario A again for writing, but it also reads this file along with the "hf_hlthmine_hlthscrn_dtl_err_metric_1"? 
# We deduplicate the incoming ErrCnt first
df_hf_hlthmine_hlthscrn_dtl_err_metric_1 = dedup_sort(
    df_ErrCnt,
    partition_cols=["MetricType","SRC_SYS_CD_SK","FILE_CAT_CD","PRCS_DTM"],
    sort_cols=[]
)
# We already deduplicated df_hlthmine_hlthscrn_dtl_total_metric_1 above as df_hf_hlthmine_hlthscrn_dtl_total_metric_1.

# Stage 21: Trans4 (CTransformerStage) has an input "df_SYSDUMMY1" (primary), left join df_hlthmine_hlthscrn_dtl_total_metric_1 => alias TotMetric, left join df_hlthmine_hlthscrn_dtl_err_metric_1 => alias ErrMetric
# We'll do a cross-like approach: we can't truly "join on" a constant in Spark easily except using an expression. We'll do a single row from SYSDUMMY1, then left joins on [ 'Total' = TotMetric.MetricType ] and [ 'Errors' = ErrMetric.MetricType ].

df_SYSDUMMY1_alias = df_SYSDUMMY1.alias("in")
df_TotMetric_alias = df_hlthmine_hlthscrn_dtl_total_metric_1.alias("TotMetric")
df_ErrMetric_alias = df_hf_hlthmine_hlthscrn_dtl_err_metric_1.alias("ErrMetric")

# First, tie them together. We'll replicate the logic of lookups with how="left" and the condition `'Total'=TotMetric.MetricType` ...
df_Trans4_1 = df_SYSDUMMY1_alias.join(
    df_TotMetric_alias,
    on=[lit("Total") == col("TotMetric.MetricType")],
    how="left"
)
df_Trans4_2 = df_Trans4_1.join(
    df_ErrMetric_alias,
    on=[lit("Errors") == col("ErrMetric.MetricType")],
    how="left"
)

df_Metrics = df_Trans4_2.select(
    col("TotMetric.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("TotMetric.FILE_CAT_CD").alias("FILE_CAT_CD"),
    col("TotMetric.PRCS_DTM").alias("PRCS_DTM"),
    lit("0").alias("FILE_METRIC_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("FILE_CAT_CD_SK"),
    lit("0").alias("NON_RTRN_CT"),
    when(col("ErrMetric.TOT_CT").isNull(), lit("0")).otherwise(col("ErrMetric.TOT_CT")).alias("RTRN_CT"),
    when(col("TotMetric.TOT_CT").isNull(), lit("0")).otherwise(col("TotMetric.TOT_CT")).alias("TOT_RCRD_CT"),
    lit("Y").alias("VALID_ROW_IN")
)

# Stage 22: FileMetrics (CSeqFileStage)
write_files(
    df_Metrics,
    f"{adls_path}/verified/HLTHMINE_HLTHSCRN_{SrcSysCd}_FileMetrics.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# The job has "AfterJobRoutine": "1", which we do not implement.

# End of script.