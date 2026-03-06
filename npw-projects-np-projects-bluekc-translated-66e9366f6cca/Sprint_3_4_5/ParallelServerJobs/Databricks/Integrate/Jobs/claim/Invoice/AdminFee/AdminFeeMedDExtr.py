# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2021 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by the Sequence: AdmnFeeSeq
# MAGIC JOB NAME:  AdminFeeAcaExtr
# MAGIC 
# MAGIC Modifications:                        
# MAGIC 
# MAGIC Developer                         Date                  Project/User Story                                                               Change Description                                                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------             ------------------           ----------------------------------------                                                   -----------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -
# MAGIC Peter Gichiri                  2021--06-13     6264 - US 348759 -  PBM PHASE II - Government Programs      Initial Development                                                                 IntegrateDev2\(9)Abhiram Dasarathy\(9)2021-07-06
# MAGIC 
# MAGIC Ashok kumar                2024--05-09     615238-                                                                                         Invoice comapre email                                                           IntegrateDev2\(9)Jeyaprasanna                     2024-05-09
# MAGIC 
# MAGIC Ashok kumar                2025--02-09     640887-Admin Fee Recon EDW Amount Format fix             Invoice decimal values missing                                                        IntegrateDev2\(9)      Jeyaprasanna               2025-02-12

# MAGIC Extract Sum and Count from EDW
# MAGIC Reads MEDD Input File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, regexp_replace, trim, to_date, sum as _sum
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSFilePath = get_widget_value('IDSFilePath','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
BILL_START_DT = get_widget_value('BILL_START_DT','')
BILL_END_DT = get_widget_value('BILL_END_DT','')
FileName = get_widget_value('FileName','')
LOB = get_widget_value('LOB','')

# --------------------------------------------------------------------------------
# db2_CLM_F (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_CLM_F = f"""
SELECT
  SUM(CLM_F.DRUG_CLM_ADM_FEE_AMT) AS DRUG_CLM_ADM_FEE_AMT,
  COUNT(CLM_F.CLM_ID) AS CLM_F_CLM_IDS
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.GRP_D GRP_D ON GRP_D.GRP_SK = CLM_F.GRP_SK
INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
WHERE CLM_F.SRC_SYS_CD='OPTUMRX'
  AND GRP_D.GRP_CLNT_ID='MA'
  AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
"""
df_db2_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_F)
    .load()
)

# --------------------------------------------------------------------------------
# Xfm_CLM_F_Sum (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfm_CLM_F_Sum_pre = (
    df_db2_CLM_F
    .withColumn(
        "svAdmFeeAmnt",
        when(
            (col("DRUG_CLM_ADM_FEE_AMT").isNull()) | (col("DRUG_CLM_ADM_FEE_AMT") == " "),
            lit(0)
        ).otherwise(col("DRUG_CLM_ADM_FEE_AMT"))
    )
    .withColumn("TRIM_CLM_F_CLM_IDS", trim(col("CLM_F_CLM_IDS")))
    .withColumn("KEY", lit(1))
)

# Applying LastRow() constraint is effectively moot here because the query itself is aggregated to a single row.
df_Xfm_CLM_F_Sum = df_Xfm_CLM_F_Sum_pre.select(
    col("svAdmFeeAmnt").alias("DRUG_CLM_ADM_FEE_AMT"),
    col("TRIM_CLM_F_CLM_IDS").alias("CLM_F_CLM_IDS"),
    col("KEY").alias("KEY")
)

# --------------------------------------------------------------------------------
# MedDAdminFeeFile (PxSequentialFile) - Read a comma-delimited file with header
# --------------------------------------------------------------------------------
schema_MedDAdminFeeFile = StructType([
    StructField("Invoice_Number", StringType(), True),
    StructField("Invoice_Date", StringType(), True),
    StructField("Billing_Entity_ID", StringType(), True),
    StructField("Billing_Entity_Name", StringType(), True),
    StructField("Activity_Level", StringType(), True),
    StructField("Carrier_ID", StringType(), True),
    StructField("Carrier_Name", StringType(), True),
    StructField("Account_ID", StringType(), True),
    StructField("Account_Name", StringType(), True),
    StructField("Group_ID", StringType(), True),
    StructField("Group_Name", StringType(), True),
    StructField("Service_Date_Period", StringType(), True),
    StructField("Classifier_Code", StringType(), True),
    StructField("Classifier_Description", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Unit_Cost", StringType(), True),
    StructField("Total_Fees", StringType(), True),
    StructField("Additional_Information", StringType(), True)
])

read_file_path_MedDAdminFeeFile = f"{adls_path_raw}/landing/{FileName}"
df_MedDAdminFeeFile = (
    spark.read.format("csv")
    .schema(schema_MedDAdminFeeFile)
    .option("header", True)
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(read_file_path_MedDAdminFeeFile)
)

# --------------------------------------------------------------------------------
# Trans1 (CTransformerStage)
# --------------------------------------------------------------------------------
df_Trans1_pre = df_MedDAdminFeeFile.withColumn(
    "LOB",
    when(trim(col("Billing_Entity_ID")) == "BKCACA", lit("ACA"))
    .when(trim(col("Billing_Entity_ID")) == "BKCMEDD", lit("MEDD"))
    .otherwise(lit("COMM"))
)

df_Trans1_pre = df_Trans1_pre.withColumn(
    "Service_Date_Period_parsed",
    to_date(col("Service_Date_Period"), "MM-dd-yyyy")
).withColumn(
    "Total_Fees_cleaned",
    regexp_replace(col("Total_Fees"), "\"", "")
).withColumn(
    "ClaimFeeCount_cleaned",
    regexp_replace(col("Quantity"), "\"", "")
)

df_Trans1 = df_Trans1_pre.select(
    col("Service_Date_Period_parsed").alias("Service_Date_Period"),
    col("Total_Fees_cleaned").alias("Total_Fees"),
    col("LOB").alias("LOB"),
    col("ClaimFeeCount_cleaned").alias("ClaimFeeCount")
)

# --------------------------------------------------------------------------------
# Aggregate (PxAggregator)
# Group by Service_Date_Period, LOB -> sum(Total_Fees), sum(ClaimFeeCount)
# --------------------------------------------------------------------------------
df_Aggregate_pre = (
    df_Trans1
    .withColumn("Total_Fees", col("Total_Fees").cast("double"))
    .withColumn("ClaimFeeCount", col("ClaimFeeCount").cast("double"))
)

df_Aggregate = (
    df_Aggregate_pre
    .groupBy("Service_Date_Period", "LOB")
    .agg(
        _sum("Total_Fees").alias("AdminFee"),
        _sum("ClaimFeeCount").alias("ClaimFeeCount")
    )
)

# --------------------------------------------------------------------------------
# Trans2 (CTransformerStage)
# (The stagevar to clean AdminFee is not used by the output columns, so we pass them directly.)
# --------------------------------------------------------------------------------
df_Trans2 = df_Aggregate.withColumn("Key", lit(1)).select(
    col("Service_Date_Period").alias("Service_Date_Period"),
    col("AdminFee").alias("AdminFee"),
    col("LOB").alias("LOB"),
    col("ClaimFeeCount").alias("ClaimFeeCount"),
    col("Key").alias("Key")
)

# --------------------------------------------------------------------------------
# Lkp_Key (PxLookup)
#   Primary link: Lnk_DrugClmPriceF_Counts_Out (df_Xfm_CLM_F_Sum)
#   Lookup link:  toLkup (df_Trans2)
#   Join: Lnk_DrugClmPriceF_Counts_Out.KEY = toLkup.Key (inner)
# --------------------------------------------------------------------------------
df_Lkp_Key = df_Xfm_CLM_F_Sum.alias("Lnk_DrugClmPriceF_Counts_Out").join(
    df_Trans2.alias("toLkup"),
    col("Lnk_DrugClmPriceF_Counts_Out.KEY") == col("toLkup.Key"),
    "inner"
)

df_Lkp_Key_out = df_Lkp_Key.select(
    col("toLkup.Service_Date_Period").alias("Service_Date_Period"),
    col("toLkup.LOB").alias("LOB"),
    col("Lnk_DrugClmPriceF_Counts_Out.DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    col("toLkup.AdminFee").alias("AdminFee"),
    col("toLkup.ClaimFeeCount").alias("ClaimFeeCount"),
    col("Lnk_DrugClmPriceF_Counts_Out.CLM_F_CLM_IDS").alias("CLM_F_CLM_IDS")
)

# --------------------------------------------------------------------------------
# xfm_Format (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_Format_pre = df_Lkp_Key_out

# svSumClmF
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svSumClmF",
    when(
        (trim(regexp_replace(trim(col("DRUG_CLM_ADM_FEE_AMT")), "0", " "))) == ".00",
        lit("0.00")
    ).otherwise(
        regexp_replace(
            when(
                trim(col("DRUG_CLM_ADM_FEE_AMT")) == "", lit("0")
            ).otherwise(trim(col("DRUG_CLM_ADM_FEE_AMT"))),
            " ",
            "0"
        )
    )
)

# svSumAdmnFee
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svSumAdmnFee",
    when(
        (trim(regexp_replace(trim(col("AdminFee")), "0", " "))) == ".00",
        lit("0.00")
    ).otherwise(
        regexp_replace(
            when(
                trim(col("AdminFee")) == "", lit("0")
            ).otherwise(trim(col("AdminFee"))),
            " ",
            "0"
        )
    )
)

# svCountClmF
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svCountClmF",
    trim(col("CLM_F_CLM_IDS"))
)

# svCount1
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svCount1",
    regexp_replace(
        when(
            trim(col("ClaimFeeCount")) == "", lit("0")
        ).otherwise(trim(col("ClaimFeeCount"))),
        " ",
        "0"
    )
)

# svCountAdmFee (DecimalToString(svCount1,"fix_zero,suppress_zero")) => user-defined function
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svCountAdmFee",
    DecimalToString(col("svCount1"), "fix_zero,suppress_zero")
)

# svCountCompare
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svCountCompare",
    when(
        trim(col("svCountClmF")) == trim(col("svCountAdmFee")),
        lit("YES")
    ).otherwise(lit("NO"))
)

# svSumCompare
df_xfm_Format_pre = df_xfm_Format_pre.withColumn(
    "svSumCompare",
    when(
        col("svSumAdmnFee") == col("svSumClmF"),
        lit("YES")
    ).otherwise(lit("NO"))
)

df_xfm_Format = df_xfm_Format_pre.select(
    col("Service_Date_Period").alias("INVOICE_DATE"),
    col("LOB").alias("LOB"),
    col("svCountAdmFee").alias("CLAIM_FEE_COUNT"),
    col("CLM_F_CLM_IDS").alias("CLM_F_COUNT"),
    col("svCountCompare").alias("COUNT_MATCH"),
    col("svSumClmF").alias("CLM_F_TOT_ADM_FEE_AMT"),
    col("svSumAdmnFee").alias("INVOICED_ADMIN_FEE_AMT"),
    col("svSumCompare").alias("SUM_MATCH")
)

# --------------------------------------------------------------------------------
# Sf_AdminFeeReport (PxSequentialFile) - Write CSV
# --------------------------------------------------------------------------------
write_output_path = f"{adls_path}/verified/{LOB}_Summary_AdminFee.csv"
write_files(
    df_xfm_Format,
    write_output_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote=None,
    nullValue=None
)