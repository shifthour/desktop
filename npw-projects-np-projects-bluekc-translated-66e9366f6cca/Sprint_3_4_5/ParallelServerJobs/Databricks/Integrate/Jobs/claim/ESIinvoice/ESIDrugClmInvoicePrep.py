# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:  Takes drug file from ESI and strips out applicable records (rec4).  converts punch sign 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                Date                 Prjoect / TTR        Change Description                                                                                        Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------              --------------------    -----------------------       -----------------------------------------------------------------------------------------------------                   --------------------------------                   -------------------------------     ----------------------------       
# MAGIC SANdrew               2008-09-19       3784(PBM)            Originally Programmed                                                                                       devlIDSnew                                 Steph Goddard           11/07/2008
# MAGIC                                                                                      Added invoice header to drug log
# MAGIC                                                                                      Added writing of head count to LOAD_DATES table
# MAGIC Dan Long              2013-11-12        TFS-1292             Added Stage Variables svSignClacpdAmt and svpdamt to the                         Integrate NewDevl                        Kalyan Neelam           2013-11-13
# MAGIC                                                                                     Trans5 transformer to be used to populate the Pd-amount column
# MAGIC                                                                                     in the Log sequential file 
# MAGIC 
# MAGIC SAndrew              2014-04-01          5082 ESI F14   ESIDrugInvoice - Changed the length of the input file from 800 to 1300             Integrate NewDevl                         Bhoomi Dasari             4/16/2014 
# MAGIC 
# MAGIC                                                                                   ESI_InvoiceRec4 - Will now begin capturing the first 1200.   Before was only capturing the first 800 characters of the Rec4.
# MAGIC 
# MAGIC                                                                                   Also changed length from 800 to 1200 for ESI_Invoice_Header and ESI_Invoice_Trailer

# MAGIC Used in next job ESIDrugClmInvoiceLand
# MAGIC ESI Invoice Transform
# MAGIC Log file information
# MAGIC Header record count compared with \"Rec4\" link count above in sequencer
# MAGIC Append header record to audit file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, substring, length, regexp_replace, when, trim, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value("RunID", "100")
CurrentDate = get_widget_value("CurrentDate", "")

# ----------------------------------------------------------------------------
# Stage: ESIDrugInvoice (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_ESIDrugInvoice = StructType([
    StructField("ESIInvoiceAllRec", StringType(), nullable=False)
])
df_ESIDrugInvoice = (
    spark.read.csv(
        f"{adls_path_raw}/landing/ESI_DrugInvoice.dat",
        schema=schema_ESIDrugInvoice,
        sep=",",
        quote='"',
        header=False
    )
)

# ----------------------------------------------------------------------------
# Stage: StripOffRec4 (CTransformerStage)
# ----------------------------------------------------------------------------
df_StripOffRec4 = df_ESIDrugInvoice.filter(
    substring(col("ESIInvoiceAllRec"), 1, 1) == "4"
).withColumn(
    "tempRemove", regexp_replace(col("ESIInvoiceAllRec"), "[\r\n\t]", "")
).withColumn(
    "ESIDrugRec4", substring(col("tempRemove"), 1, 1200)
).drop(
    "tempRemove", "ESIInvoiceAllRec"
)

# The output column is considered char(1200), so rpad to preserve length.
df_StripOffRec4 = df_StripOffRec4.withColumn(
    "ESIDrugRec4", rpad(col("ESIDrugRec4"), 1200, " ")
)

# ----------------------------------------------------------------------------
# Stage: ESI_InvoiceRec4 (CSeqFileStage) - Write
# ----------------------------------------------------------------------------
write_files(
    df_StripOffRec4.select("ESIDrugRec4"),
    f"{adls_path}/verified/ESI_InvoiceRec4.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: ESI_Invoice_Trailer (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_ESI_Invoice_Trailer = StructType([
    StructField("RCRD_ID", IntegerType(), nullable=False),
    StructField("PRCSR_NO", IntegerType(), nullable=False),
    StructField("BTCH_NO", IntegerType(), nullable=False),
    StructField("PHARMACY_CNT", IntegerType(), nullable=False),
    StructField("COMMENT2", StringType(), nullable=False),
    StructField("TOTAL_CNT", IntegerType(), nullable=False),
    StructField("TOTAL_BILL", StringType(), nullable=False),
    StructField("TOTAL_ADMIN_FEE", StringType(), nullable=False),
    StructField("EXPAN_AREA1", StringType(), nullable=False),
    StructField("EXPAN_AREA2", StringType(), nullable=False)
])
df_ESI_Invoice_Trailer = (
    spark.read.csv(
        f"{adls_path_raw}/landing/ESI_DrugInvoice.dat",
        schema=schema_ESI_Invoice_Trailer,
        sep=",",
        quote='"',
        header=False
    )
)

# ----------------------------------------------------------------------------
# Stage: Transformer_123 (CTransformerStage)
#   Constraint: All_in.RCRD_ID = 8
# ----------------------------------------------------------------------------
df_Transformer_123 = df_ESI_Invoice_Trailer.filter(col("RCRD_ID") == 8)

# Apply rpad to char columns:
df_Transformer_123 = df_Transformer_123.withColumn("COMMENT2", rpad(col("COMMENT2"), 298, " ")) \
    .withColumn("TOTAL_BILL", rpad(col("TOTAL_BILL"), 11, " ")) \
    .withColumn("TOTAL_ADMIN_FEE", rpad(col("TOTAL_ADMIN_FEE"), 10, " ")) \
    .withColumn("EXPAN_AREA1", rpad(col("EXPAN_AREA1"), 653, " ")) \
    .withColumn("EXPAN_AREA2", rpad(col("EXPAN_AREA2"), 300, " "))

# ----------------------------------------------------------------------------
# Stage: hf_esi_invoice_trailer (CHashedFileStage) - Scenario A
#   Key column = RCRD_ID
#   We replace the hashed file with a deduplicate step on RCRD_ID
# ----------------------------------------------------------------------------
# Deduplicate on RCRD_ID to emulate intermediate hashed file usage
df_Trail_In = dedup_sort(
    df_Transformer_123,
    partition_cols=["RCRD_ID"],
    sort_cols=[("RCRD_ID", "A")]
)

# ----------------------------------------------------------------------------
# Stage: ESI_Invoice_Header (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_ESI_Invoice_Header = StructType([
    StructField("RCRD_ID", IntegerType(), nullable=False),
    StructField("PRCSR_NO", IntegerType(), nullable=False),
    StructField("BTCH_NO", IntegerType(), nullable=False),
    StructField("PROC_NAME", StringType(), nullable=False),
    StructField("PROC_ADDR", StringType(), nullable=False),
    StructField("PROC_CITY", StringType(), nullable=False),
    StructField("PROC_STATE", StringType(), nullable=False),
    StructField("PROC_ZIP", StringType(), nullable=False),
    StructField("PROC_PHONE", StringType(), nullable=False),
    StructField("RUN_DATE", StringType(), nullable=False),
    StructField("THIRD_PARTY_TYP", StringType(), nullable=False),
    StructField("VERSION", StringType(), nullable=False),
    StructField("EXPAN_AREA1", StringType(), nullable=False),
    StructField("MASTER_CARRIER", StringType(), nullable=False),
    StructField("SUB_CARRIER", StringType(), nullable=False),
    StructField("EXPAN_AREA2", StringType(), nullable=False),
    StructField("EXPAN_AREA3", StringType(), nullable=False)
])
df_ESI_Invoice_Header = (
    spark.read.csv(
        f"{adls_path_raw}/landing/ESI_DrugInvoice.dat",
        schema=schema_ESI_Invoice_Header,
        sep=",",
        quote='"',
        header=False
    )
)

# ----------------------------------------------------------------------------
# Stage: Trans4 (CTransformerStage)
#   Constraint: Hdr_In.RCRD_ID = 0
# ----------------------------------------------------------------------------
df_Trans4 = df_ESI_Invoice_Header.filter(col("RCRD_ID") == 0)

# Apply rpad to char columns
df_Trans4 = df_Trans4.withColumn("PROC_NAME", rpad(col("PROC_NAME"), 20, " ")) \
    .withColumn("PROC_ADDR", rpad(col("PROC_ADDR"), 20, " ")) \
    .withColumn("PROC_CITY", rpad(col("PROC_CITY"), 18, " ")) \
    .withColumn("PROC_STATE", rpad(col("PROC_STATE"), 2, " ")) \
    .withColumn("PROC_ZIP", rpad(col("PROC_ZIP"), 9, " ")) \
    .withColumn("PROC_PHONE", rpad(col("PROC_PHONE"), 9, " ")) \
    .withColumn("RUN_DATE", rpad(col("RUN_DATE"), 8, " ")) \
    .withColumn("THIRD_PARTY_TYP", rpad(col("THIRD_PARTY_TYP"), 1, " ")) \
    .withColumn("VERSION", rpad(col("VERSION"), 2, " ")) \
    .withColumn("EXPAN_AREA1", rpad(col("EXPAN_AREA1"), 187, " ")) \
    .withColumn("MASTER_CARRIER", rpad(col("MASTER_CARRIER"), 4, " ")) \
    .withColumn("SUB_CARRIER", rpad(col("SUB_CARRIER"), 4, " ")) \
    .withColumn("EXPAN_AREA2", rpad(col("EXPAN_AREA2"), 699, " ")) \
    .withColumn("EXPAN_AREA3", rpad(col("EXPAN_AREA3"), 301, " "))

# ----------------------------------------------------------------------------
# Stage: hf_esi_invoice_header (CHashedFileStage) - Scenario A
#   Key column = RCRD_ID
# ----------------------------------------------------------------------------
df_HdrLkup_noDups = dedup_sort(
    df_Trans4,
    partition_cols=["RCRD_ID"],
    sort_cols=[("RCRD_ID", "A")]
)

# We only need to look up the row where RCRD_ID=0 later, so isolate it here
df_HdrLkup = df_HdrLkup_noDups.filter(col("RCRD_ID") == 0)

# ----------------------------------------------------------------------------
# Stage: Trans5 (CTransformerStage)
#   Primary link: Trail_In (RCRD_ID=8)
#   Lookup link: HdrLkup (join type = left, on constant 0 = RCRD_ID)
#   Stage vars: svSignCalcpdAmt, svpdamt
# ----------------------------------------------------------------------------

# We'll prepare the single-row from df_HdrLkup (RCRD_ID=0) so we can bring it in:
df_HdrLkupRow = df_HdrLkup.limit(1).collect()
lk_RUN_DATE = None
if len(df_HdrLkupRow) > 0:
    lk_RUN_DATE = df_HdrLkupRow[0]["RUN_DATE"]

df_Trail_In_StgVars = df_Trail_In.withColumn(
    "svSignCalcpdAmt",
    GetRvrsPunchSign( substring(col("TOTAL_BILL"), length(col("TOTAL_BILL")), 1) )
).withColumn(
    "svpdamt",
    when(
        length(col("svSignCalcpdAmt")) > 1,
        lit("-") + (
            Ereplace(
                col("TOTAL_BILL"),
                substring(col("TOTAL_BILL"), length(col("TOTAL_BILL")), 1),
                substring(col("svSignCalcpdAmt"), 2, 1)
            ) * 1
        )
    ).otherwise(
        Ereplace(
            col("TOTAL_BILL"),
            substring(col("TOTAL_BILL"), length(col("TOTAL_BILL")), 1),
            substring(col("svSignCalcpdAmt"), 1, 1)
        ) * 1
    )
)

# Output link: "Log" (Drug_Header_Record) -> constraint: RCRD_ID=8 (already filtered in df_Trail_In)
df_Log = df_Trail_In_StgVars.withColumn(
    "Run_Date",
    lit(CurrentDate)
).withColumn(
    "Filler1",
    lit("  ")
).withColumn(
    "Frequency",
    lit("WEEKLY")
).withColumn(
    "Filler2",
    lit("*  ")
).withColumn(
    "Program",
    lit("ESI")
).withColumn(
    "File_date",
    lit(lk_RUN_DATE) if lk_RUN_DATE is not None else lit(None)
).withColumn(
    "Row_Count",
    substring(col("TOTAL_CNT").cast(StringType()), -7, 7)
).withColumn(
    "Pd_amount",
    when(
        length(trim(col("TOTAL_CNT").cast(StringType()))) != 0,
        col("svpdamt") / 100
    ).otherwise(lit("0.00"))
).select(
    rpad(col("Run_Date"), 10, " ").alias("Run_Date"),
    rpad(col("Filler1"), 2, " ").alias("Filler1"),
    rpad(col("Frequency"), 9, " ").alias("Frequency"),
    rpad(col("Filler2"), 3, " ").alias("Filler2"),
    rpad(col("Program"), 9, " ").alias("Program"),
    rpad(col("File_date"), 9, " ").alias("File_date"),
    rpad(col("Row_Count"), 8, " ").alias("Row_Count"),
    rpad(col("Pd_amount").cast(StringType()), 12, " ").alias("Pd_amount")
)

# Output link: "RecCnt" -> constraint: RCRD_ID=8
# Columns: JOBNAME='IdsESIHdrCnt', BEGINDATE=Right(TOTAL_CNT,7)*1, PREVBEGINDATE=same
df_RecCnt = df_Trail_In_StgVars.select(
    rpad(lit("IdsESIHdrCnt"), 25, " ").alias("JOBNAME"),
    substring(col("TOTAL_CNT").cast(StringType()), -7, 7).cast(IntegerType()).alias("BEGINDATE"),
    substring(col("TOTAL_CNT").cast(StringType()), -7, 7).cast(IntegerType()).alias("PREVBEGINDATE")
)

# ----------------------------------------------------------------------------
# Stage: LOAD_DATES (CUvStage)
#   We translate to a merge into LOAD_DATES table using a staging table
# ----------------------------------------------------------------------------
db_secret_name = get_widget_value("load_dates_secret_name", "")
jdbc_url, jdbc_props = get_db_config(db_secret_name)

# 1) Drop the temp table if exists
execute_dml("DROP TABLE IF EXISTS STAGING.ESIDrugClmInvoicePrep_LOAD_DATES_temp", jdbc_url, jdbc_props)

# 2) Create temp table by writing df_RecCnt
(
    df_RecCnt.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.ESIDrugClmInvoicePrep_LOAD_DATES_temp")
    .mode("overwrite")
    .save()
)

# 3) Merge logic
merge_sql = """
MERGE INTO LOAD_DATES AS T
USING STAGING.ESIDrugClmInvoicePrep_LOAD_DATES_temp AS S
ON (T.JOBNAME = S.JOBNAME)
WHEN MATCHED THEN
  UPDATE SET
    T.BEGINDATE = S.BEGINDATE,
    T.PREVBEGINDATE = S.PREVBEGINDATE
WHEN NOT MATCHED THEN
  INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE)
  VALUES (S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------------------
# Stage: Drug_Header_Record (CSeqFileStage) - Write
# ----------------------------------------------------------------------------
write_files(
    df_Log,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)