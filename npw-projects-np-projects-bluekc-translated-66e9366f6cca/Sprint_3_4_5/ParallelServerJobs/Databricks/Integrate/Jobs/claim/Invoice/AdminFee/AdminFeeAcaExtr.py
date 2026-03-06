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
# MAGIC Peter Gichiri                  2021--06-13     6264 - US 348759 -  PBM PHASE II - Government Programs      Initial Development                                                                    .  IntegrateDev2\(9)Abhiram Dasarathy\(9)2021-07-06
# MAGIC 
# MAGIC Ashok kumar                2024--05-09     615238-                                                                                         Invoice comapre email                                                                IntegrateDev2\(9)   Jeyaprasanna              2024-05-09

# MAGIC Extract Sum and Count from EDW
# MAGIC Reads ACA Input File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, sum as sum_, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSFilePath = get_widget_value('IDSFilePath','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
BILL_START_DT = get_widget_value('BILL_START_DT','')
BILL_END_DT = get_widget_value('BILL_END_DT','')
FileName = get_widget_value('FileName','')
LOB = get_widget_value('LOB','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""
SELECT
  SUM(CLM_F.DRUG_CLM_ADM_FEE_AMT) AS DRUG_CLM_ADM_FEE_AMT,
  COUNT(CLM_F.CLM_ID) as CLM_F_CLM_IDS
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F
  ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
WHERE DRUG_CLM_PRICE_F.CLM_SK IN (
  SELECT DISTINCT DRUG_CLM_PRICE_F.CLM_SK
  FROM {EDWOwner}.CLM_F CLM_F
  INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
  INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
  INNER JOIN {EDWOwner}.SUBGRP_D SUBGRP_D ON SUBGRP_D.GRP_SK = CLM_F.GRP_SK
  INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
  WHERE GRP_D.GRP_ID = '10001000'
    AND SUBGRP_D.SUBGRP_ID IN ('0001', '0002')
    AND CLM_F.SRC_SYS_CD = 'OPTUMRX'
    AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
    AND CLM_F.PROD_SH_NM IN ('BLUE-SELECT','BLUESELECT+','UNK')
  UNION
  SELECT DISTINCT DRUG_CLM_PRICE_F.CLM_SK
  FROM {EDWOwner}.CLM_F CLM_F
  INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
  INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
  INNER JOIN {EDWOwner}.SUBGRP_D SUBGRP_D ON SUBGRP_D.GRP_SK = CLM_F.GRP_SK
  INNER JOIN {EDWOwner}.FNCL_LOB_D FNCL_LOB_D ON CLM_F.FNCL_LOB_SK = FNCL_LOB_D.FNCL_LOB_SK
  INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
  WHERE GRP_D.GRP_ID = '10001000'
    AND SUBGRP_D.SUBGRP_ID IN ('0001', '0002')
    AND CLM_F.SRC_SYS_CD = 'OPTUMRX'
    AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
    AND CLM_F.PROD_SH_NM IN ('PCB')
    AND FNCL_LOB_D.FNCL_LOB_DESC LIKE 'ACA%'
)
"""
df_db2_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_lnk_CLM_F_In = df_db2_CLM_F

temp_clm_f = df_lnk_CLM_F_In.collect()
if temp_clm_f:
    last_row = temp_clm_f[-1]
    df_Lnk_DrugClmPriceF_Counts_Out = spark.createDataFrame(
        [
            (
                trim(last_row["DRUG_CLM_ADM_FEE_AMT"]),
                trim(last_row["CLM_F_CLM_IDS"]),
                1
            )
        ],
        ["DRUG_CLM_ADM_FEE_AMT", "CLM_F_CLM_IDS", "KEY"]
    )
else:
    df_Lnk_DrugClmPriceF_Counts_Out = spark.createDataFrame(
        [],
        StructType([
            StructField("DRUG_CLM_ADM_FEE_AMT", StringType(), True),
            StructField("CLM_F_CLM_IDS", StringType(), True),
            StructField("KEY", IntegerType(), True)
        ])
    )

schema_AcaAdminFeeFile = StructType([
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
df_AcaAdminFeeFile = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_AcaAdminFeeFile)
    .load(f"{adls_path_raw}/landing/{FileName}")
)
df_toTrans1 = df_AcaAdminFeeFile

df_Trans1 = (
    df_toTrans1
    .withColumn("Service_Date_Period", StringToDate(col("Service_Date_Period"), "%mm-%dd-%yyyy"))
    .withColumn("Total_Fees", Ereplace(col("Total_Fees"), "\"", ""))
    .withColumn("LOB", lit(LOB))
    .withColumn("ClaimFeeCount", Ereplace(col("Quantity"), "\"", ""))
)
df_toAggre = df_Trans1

df_Aggregate = (
    df_toAggre
    .groupBy("Service_Date_Period", "LOB")
    .agg(
        sum_(col("Total_Fees")).alias("AdminFee"),
        sum_(col("ClaimFeeCount")).alias("ClaimFeeCount")
    )
)
df_toTrans2 = df_Aggregate

df_Trans2_stgVar = (
    df_toTrans2
    .withColumn("_tmp1", convert(lit('0'), lit(' '), col("AdminFee")))
    .withColumn("_tmp2", TrimF(col("_tmp1")))
    .withColumn("_tmp3", convert(lit(' '), lit('0'), col("_tmp2")))
    .withColumn("svAdminFee", when(col("_tmp3") == '.00', '0.00').otherwise(col("_tmp3")))
)
df_Trans2_out = df_Trans2_stgVar.select(
    col("Service_Date_Period"),
    col("AdminFee"),
    col("LOB"),
    col("ClaimFeeCount"),
    lit(1).alias("Key")
)
df_toLkup = df_Trans2_out

df_lkp_key = (
    df_Lnk_DrugClmPriceF_Counts_Out.alias("Lnk_DrugClmPriceF_Counts_Out")
    .join(
        df_toLkup.alias("toLkup"),
        col("Lnk_DrugClmPriceF_Counts_Out.KEY") == col("toLkup.Key"),
        "left"
    )
)
df_Lnk_Reportn_Out = df_lkp_key.select(
    col("toLkup.Service_Date_Period").alias("Service_Date_Period"),
    col("toLkup.LOB").alias("LOB"),
    col("Lnk_DrugClmPriceF_Counts_Out.DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    col("toLkup.AdminFee").alias("AdminFee"),
    col("toLkup.ClaimFeeCount").alias("ClaimFeeCount"),
    col("Lnk_DrugClmPriceF_Counts_Out.CLM_F_CLM_IDS").alias("CLM_F_CLM_IDS")
)

df_format_stgVar = (
    df_Lnk_Reportn_Out
    .withColumn("_tmp0", convert(lit('0'), lit(' '), col("DRUG_CLM_ADM_FEE_AMT")))
    .withColumn("_tmp1", TrimF(col("_tmp0")))
    .withColumn("_tmp2", convert(lit(' '), lit('0'), col("_tmp1")))
    .withColumn("svSumClmF", when(col("_tmp2") == '.00', '0.00').otherwise(col("_tmp2")))
    .withColumn("_tmp3", convert(lit('0'), lit(' '), col("AdminFee")))
    .withColumn("_tmp4", TrimF(col("_tmp3")))
    .withColumn("_tmp5", convert(lit(' '), lit('0'), col("_tmp4")))
    .withColumn("svSumAdmnFee", when(col("_tmp5") == '.00', '0.00').otherwise(col("_tmp5")))
    .withColumn("svCountClmF", trim(col("CLM_F_CLM_IDS")))
    .withColumn("_tmp6", convert(lit('0'), lit(' '), col("ClaimFeeCount")))
    .withColumn("_tmp7", TrimF(col("_tmp6")))
    .withColumn("_tmp8", convert(lit(' '), lit('0'), col("_tmp7")))
    .withColumn("svCount1", col("_tmp8"))
    .withColumn("svCountAdmFee", DecimalToString(col("svCount1"), "fix_zero,suppress_zero"))
    .withColumn("svCountCompare", when(col("svCountClmF") == col("svCountAdmFee"), 'YES').otherwise('NO'))
    .withColumn("svSumCompare", when(col("svSumAdmnFee") == col("svSumClmF"), 'YES').otherwise('NO'))
)
df_xfm_Admin = df_format_stgVar.select(
    col("Service_Date_Period").alias("INVOICE_DATE"),
    col("LOB"),
    col("svCountAdmFee").alias("CLAIM_FEE_COUNT"),
    col("CLM_F_CLM_IDS").alias("CLM_F_COUNT"),
    col("svCountCompare").alias("COUNT_MATCH"),
    col("svSumClmF").alias("CLM_F_TOT_ADM_FEE_AMT"),
    col("svSumAdmnFee").alias("INVOICED_ADMIN_FEE_AMT"),
    col("svSumCompare").alias("SUM_MATCH")
)

df_final = df_xfm_Admin.select(
    rpad(col("INVOICE_DATE"), <...>, " ").alias("INVOICE_DATE"),
    rpad(col("LOB"), <...>, " ").alias("LOB"),
    rpad(col("CLAIM_FEE_COUNT"), <...>, " ").alias("CLAIM_FEE_COUNT"),
    rpad(col("CLM_F_COUNT"), <...>, " ").alias("CLM_F_COUNT"),
    rpad(col("COUNT_MATCH"), <...>, " ").alias("COUNT_MATCH"),
    rpad(col("CLM_F_TOT_ADM_FEE_AMT"), <...>, " ").alias("CLM_F_TOT_ADM_FEE_AMT"),
    rpad(col("INVOICED_ADMIN_FEE_AMT"), <...>, " ").alias("INVOICED_ADMIN_FEE_AMT"),
    rpad(col("SUM_MATCH"), <...>, " ").alias("SUM_MATCH")
)

write_files(
    df_final,
    f"{adls_path}/verified/{LOB}_Summary_AdminFee.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)