# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2021 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by the Sequence: AdmnFeeSeq
# MAGIC JOB NAME:  AdminFeeCommExtr
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC 
# MAGIC Developer                         Date                  Project/User Story                                                               Change Description                                                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------             ------------------           ----------------------------------------                                                   -----------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -
# MAGIC Peter Gichiri                  2021--06-13     6264 - US 348759 -  PBM PHASE II - Government Programs      Initial Development                                                                    .  IntegrateDev2\(9)Abhiram Dasarathy\(9)2021-07-06
# MAGIC 
# MAGIC 
# MAGIC Ashok kumar Baskaran    2024--04-05               615238                                                                updated Invoice compare email for Finance                                               IntegrateDev2\(9)        Jeyaprasanna             2024-04-22
# MAGIC 
# MAGIC Ashok kumar Baskaran    2024--04-05               615238                                                                fix Invoice compare email for Finance                                                         IntegrateDev2                        Jeyaprasanna             2024-05-09
# MAGIC 
# MAGIC Ashok kumar Baskaran    2024--06-05               615238                                                                fix penc claims count calculations                                                                IntegrateDev2        Jeyaprasanna           2024-06-07

# MAGIC Extract Sum and Count from EDW
# MAGIC Reads St Jose Input File
# MAGIC Reads COMM Input File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
IDSFilePath = get_widget_value('IDSFilePath','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
BILL_START_DT = get_widget_value('BILL_START_DT','')
BILL_END_DT = get_widget_value('BILL_END_DT','')
FileName = get_widget_value('FileName','')
LOB = get_widget_value('LOB','')
BKC1STJOSFEE_File = get_widget_value('BKC1STJOSFEE_File','')

# Database connection props for EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# --------------------------------------------------------------------------------
# db2_CLM_F (DB2ConnectorPX)
# --------------------------------------------------------------------------------
sql_db2_CLM_F = f"""
SELECT 
    SUM(CLM_F.DRUG_CLM_ADM_FEE_AMT) AS DRUG_CLM_ADM_FEE_AMT, 
    COUNT(CLM_F.CLM_ID) as CLM_F_CLM_IDS
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F AS DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
WHERE 
    CLM_F.SRC_SYS_CD='OPTUMRX' 
    AND DRUG_CLM_PRICE_F.DRUG_PLN_TYP_ID<>'PENC'
    AND GRP_D.GRP_CLNT_ID <> 'MA'
    AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
    AND DRUG_CLM_PRICE_F.CLM_SK NOT IN
    (
      SELECT A.CLM_SK
      FROM
      (
        SELECT DISTINCT DRUG_CLM_PRICE_F.CLM_SK
        FROM {EDWOwner}.CLM_F CLM_F
        INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
        INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
        INNER JOIN {EDWOwner}.SUBGRP_D SUBGRP_D ON SUBGRP_D.GRP_SK = CLM_F.GRP_SK
        INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
        WHERE 
          GRP_D.GRP_ID = '10001000'
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
        WHERE 
          GRP_D.GRP_ID = '10001000'
          AND SUBGRP_D.SUBGRP_ID IN ('0001', '0002')
          AND CLM_F.SRC_SYS_CD = 'OPTUMRX'
          AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
          AND CLM_F.PROD_SH_NM IN ('PCB')
          AND FNCL_LOB_D.FNCL_LOB_DESC LIKE 'ACA%'
      ) A
    )
"""
df_db2_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CLM_F)
    .load()
)

# --------------------------------------------------------------------------------
# Xfm_CLM_F_Sum (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfm_CLM_F_Sum = df_db2_CLM_F.select(
    trim(F.col("DRUG_CLM_ADM_FEE_AMT")).alias("DRUG_CLM_ADM_FEE_AMT"),
    trim(F.col("CLM_F_CLM_IDS")).alias("COUNT_CLM_F_CLM_IDS"),
    F.lit("Non-PENC Claims").alias("CLAIM_TYPE"),
    F.lit("1").alias("KEY")
)

# --------------------------------------------------------------------------------
# db2_CLM_F_Penc (DB2ConnectorPX)
# --------------------------------------------------------------------------------
sql_db2_CLM_F_Penc = f"""
SELECT 
    SUM(CLM_F.DRUG_CLM_ADM_FEE_AMT) AS DRUG_CLM_ADM_FEE_AMT,
    COUNT(CLM_F.CLM_ID) as CLM_F_CLM_IDS
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F AS DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
WHERE 
    CLM_F.SRC_SYS_CD='OPTUMRX' 
    AND DRUG_CLM_PRICE_F.DRUG_PLN_TYP_ID='PENC'
    AND GRP_D.GRP_CLNT_ID <> 'MA'
    AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
    AND DRUG_CLM_PRICE_F.CLM_SK NOT IN
    (
      SELECT A.CLM_SK
      FROM
      (
        SELECT DISTINCT DRUG_CLM_PRICE_F.CLM_SK
        FROM {EDWOwner}.CLM_F CLM_F
        INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
        INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
        INNER JOIN {EDWOwner}.SUBGRP_D SUBGRP_D ON SUBGRP_D.GRP_SK = CLM_F.GRP_SK
        INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
        WHERE 
          GRP_D.GRP_ID = '10001000'
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
        WHERE 
          GRP_D.GRP_ID = '10001000'
          AND SUBGRP_D.SUBGRP_ID IN ('0001', '0002')
          AND CLM_F.SRC_SYS_CD = 'OPTUMRX'
          AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
          AND CLM_F.PROD_SH_NM IN ('PCB')
          AND FNCL_LOB_D.FNCL_LOB_DESC LIKE 'ACA%'
      ) A
    )
"""
df_db2_CLM_F_Penc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CLM_F_Penc)
    .load()
)

# --------------------------------------------------------------------------------
# Xfm_CLM_F_Sum_Penc (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfm_CLM_F_Sum_Penc = df_db2_CLM_F_Penc.select(
    trim(F.col("DRUG_CLM_ADM_FEE_AMT")).alias("DRUG_CLM_ADM_FEE_AMT"),
    trim(F.col("CLM_F_CLM_IDS")).alias("COUNT_CLM_F_CLM_IDS"),
    F.lit("PENC Claims").alias("CLAIM_TYPE"),
    F.lit("1").alias("KEY")
)

# --------------------------------------------------------------------------------
# db2_CLM_F_Total (DB2ConnectorPX)
# --------------------------------------------------------------------------------
sql_db2_CLM_F_Total = f"""
SELECT 
    SUM(CLM_F.DRUG_CLM_ADM_FEE_AMT) AS DRUG_CLM_ADM_FEE_AMT,
    COUNT(CLM_F.CLM_ID) as CLM_F_CLM_IDS
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F AS DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
WHERE 
    CLM_F.SRC_SYS_CD='OPTUMRX'
    AND GRP_D.GRP_CLNT_ID <> 'MA'
    AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
    AND DRUG_CLM_PRICE_F.CLM_SK NOT IN
    (
      SELECT A.CLM_SK
      FROM
      (
        SELECT DISTINCT DRUG_CLM_PRICE_F.CLM_SK
        FROM {EDWOwner}.CLM_F CLM_F
        INNER JOIN {EDWOwner}.CLM_F2 CLM_F2 ON CLM_F.CLM_SK = CLM_F2.CLM_SK
        INNER JOIN {EDWOwner}.DRUG_CLM_PRICE_F DRUG_CLM_PRICE_F ON CLM_F.CLM_SK = DRUG_CLM_PRICE_F.CLM_SK
        INNER JOIN {EDWOwner}.SUBGRP_D SUBGRP_D ON SUBGRP_D.GRP_SK = CLM_F.GRP_SK
        INNER JOIN {EDWOwner}.GRP_D GRP_D on GRP_D.GRP_SK = CLM_F.GRP_SK
        WHERE 
          GRP_D.GRP_ID = '10001000'
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
        WHERE 
          GRP_D.GRP_ID = '10001000'
          AND SUBGRP_D.SUBGRP_ID IN ('0001', '0002')
          AND CLM_F.SRC_SYS_CD = 'OPTUMRX'
          AND CLM_F2.DRUG_CLM_ADJ_DT_SK BETWEEN '{BILL_START_DT}' AND '{BILL_END_DT}'
          AND CLM_F.PROD_SH_NM IN ('PCB')
          AND FNCL_LOB_D.FNCL_LOB_DESC LIKE 'ACA%'
      ) A
    )
"""
df_db2_CLM_F_Total = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CLM_F_Total)
    .load()
)

# --------------------------------------------------------------------------------
# Xfm_CLM_F_Sum_Total (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfm_CLM_F_Sum_Total = df_db2_CLM_F_Total.select(
    trim(F.col("DRUG_CLM_ADM_FEE_AMT")).alias("DRUG_CLM_ADM_FEE_AMT"),
    trim(F.col("CLM_F_CLM_IDS")).alias("COUNT_CLM_F_CLM_IDS"),
    F.lit("Total Claims").alias("CLAIM_TYPE"),
    F.lit("1").alias("KEY")
)

# --------------------------------------------------------------------------------
# CLM_F_Funnel (PxFunnel)
# --------------------------------------------------------------------------------
df_CLM_F_Funnel = (
    df_Xfm_CLM_F_Sum.select(
        "DRUG_CLM_ADM_FEE_AMT",
        "COUNT_CLM_F_CLM_IDS",
        "CLAIM_TYPE",
        "KEY"
    )
    .unionByName(
        df_Xfm_CLM_F_Sum_Penc.select(
            "DRUG_CLM_ADM_FEE_AMT",
            "COUNT_CLM_F_CLM_IDS",
            "CLAIM_TYPE",
            "KEY"
        )
    )
    .unionByName(
        df_Xfm_CLM_F_Sum_Total.select(
            "DRUG_CLM_ADM_FEE_AMT",
            "COUNT_CLM_F_CLM_IDS",
            "CLAIM_TYPE",
            "KEY"
        )
    )
)

# --------------------------------------------------------------------------------
# CommAdminFeeFile (PxSequentialFile) - Reading from landing
# --------------------------------------------------------------------------------
schema_CommAdminFeeFile = StructType([
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

df_CommAdminFeeFile = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_CommAdminFeeFile)
    .csv(f"{adls_path_raw}/landing/{FileName}")
)

# --------------------------------------------------------------------------------
# Trans1 (CTransformerStage)
# --------------------------------------------------------------------------------
df_Trans1_sv = df_CommAdminFeeFile.withColumn(
    "svLOB",
    F.when(trim(F.col("Billing_Entity_ID")) == "BKCACA", F.lit("ACA"))
     .when(trim(F.col("Billing_Entity_ID")) == "BKCMEDD", F.lit("MEDD"))
     .otherwise(F.lit("COMM"))
)

# Output columns
df_Trans1 = df_Trans1_sv.select(
    F.to_date(F.col("Service_Date_Period"), "MM-dd-yyyy").alias("Service_Date_Period"),
    Ereplace(F.col("Total_Fees"), '\"', '').alias("Total_Fees"),
    F.col("svLOB").alias("LOB"),
    Ereplace(F.col("Quantity"), '\"', '').alias("ClaimFeeCount")
)

# --------------------------------------------------------------------------------
# Aggregate (PxAggregator)
# key: Service_Date_Period, LOB
# method: hash
# reduce: sum of Total_Fees as AdminFee, sum of ClaimFeeCount as ClaimFeeCount
# --------------------------------------------------------------------------------
df_Aggregate = (
    df_Trans1
    .groupBy("Service_Date_Period", "LOB")
    .agg(
        F.sum(F.col("Total_Fees")).alias("AdminFee"),
        F.sum(F.col("ClaimFeeCount")).alias("ClaimFeeCount")
    )
)

# --------------------------------------------------------------------------------
# Trans2 (CTransformerStage)
# Has stage variable svAdminFee that is not used in the final output
# --------------------------------------------------------------------------------
df_Trans2_sv = df_Aggregate.withColumn(
    "svAdminFee",
    F.when(
        convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee")))) == F.lit(".00"),
        F.lit("0.00")
    ).otherwise(
        convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee"))))
    )
)

df_Trans2 = df_Trans2_sv.select(
    F.col("Service_Date_Period").alias("Service_Date_Period"),
    F.col("AdminFee").alias("AdminFee"),
    F.col("ClaimFeeCount").alias("ClaimFeeCount"),
    F.lit("1").alias("Key"),
    F.col("LOB").alias("LOB")
)

# --------------------------------------------------------------------------------
# BKC1STJOSFEEFile (PxSequentialFile) - Reading from verified (no 'landing' or 'external', so use adls_path)
# --------------------------------------------------------------------------------
schema_BKC1STJOSFEEFile = StructType([
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

df_BKC1STJOSFEEFile = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_BKC1STJOSFEEFile)
    .csv(f"{adls_path}/verified/{BKC1STJOSFEE_File}")
)

# --------------------------------------------------------------------------------
# TransSj (CTransformerStage)
# --------------------------------------------------------------------------------
df_TransSj_sv = df_BKC1STJOSFEEFile.withColumn(
    "svLOB",
    F.when(trim(F.col("Billing_Entity_ID")) == "BKCACA", F.lit("ACA"))
     .when(trim(F.col("Billing_Entity_ID")) == "BKCMEDD", F.lit("MEDD"))
     .otherwise(F.lit("COMM"))
)

df_TransSj = df_TransSj_sv.select(
    F.to_date(F.col("Service_Date_Period"), "MM-dd-yyyy").alias("Service_Date_Period"),
    Ereplace(F.col("Total_Fees"), '\"', '').alias("Total_Fees"),
    F.col("svLOB").alias("LOB"),
    Ereplace(F.col("Quantity"), '\"', '').alias("ClaimFeeCount")
)

# --------------------------------------------------------------------------------
# AggregateSJ (PxAggregator)
# key: Service_Date_Period, LOB
# reduce sums
# --------------------------------------------------------------------------------
df_AggregateSJ = (
    df_TransSj
    .groupBy("Service_Date_Period", "LOB")
    .agg(
        F.sum(F.col("Total_Fees")).alias("AdminFee"),
        F.sum(F.col("ClaimFeeCount")).alias("ClaimFeeCount")
    )
)

# --------------------------------------------------------------------------------
# Trans2SJ (CTransformerStage)
# Has stage variable svAdminFee that is not used in the final output
# --------------------------------------------------------------------------------
df_Trans2SJ_sv = df_AggregateSJ.withColumn(
    "svAdminFee",
    F.when(
        convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee")))) == F.lit(".00"),
        F.lit("0.00")
    ).otherwise(
        convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee"))))
    )
)

df_Trans2SJ = df_Trans2SJ_sv.select(
    F.col("Service_Date_Period").alias("Service_Date_Period"),
    F.col("ClaimFeeCount").alias("ClaimFeeCountSj"),
    F.lit("1").alias("Key")
)

# --------------------------------------------------------------------------------
# Lkp_Key (PxLookup)
# Primary link = df_CLM_F_Funnel as Lnk_DrugClmPriceF_Counts_Out
# Lookup link #1 = df_Trans2SJ as toLkupSJ (left join on KEY)
# Lookup link #2 = df_Trans2 as toLkupAdmFile (left join on KEY)
# --------------------------------------------------------------------------------
df_Lkp_Key_joined = (
    df_CLM_F_Funnel.alias("Lnk_DrugClmPriceF_Counts_Out")
    .join(
        df_Trans2SJ.alias("toLkupSJ"),
        F.col("Lnk_DrugClmPriceF_Counts_Out.KEY") == F.col("toLkupSJ.Key"),
        "left"
    )
    .join(
        df_Trans2.alias("toLkupAdmFile"),
        F.col("Lnk_DrugClmPriceF_Counts_Out.KEY") == F.col("toLkupAdmFile.Key"),
        "left"
    )
)

df_Lkp_Key = df_Lkp_Key_joined.select(
    F.col("Lnk_DrugClmPriceF_Counts_Out.DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    F.col("Lnk_DrugClmPriceF_Counts_Out.COUNT_CLM_F_CLM_IDS").alias("COUNT_CLM_F_CLM_IDS"),
    F.col("Lnk_DrugClmPriceF_Counts_Out.CLAIM_TYPE").alias("CLAIM_TYPE"),
    F.col("Lnk_DrugClmPriceF_Counts_Out.KEY").alias("KEY"),
    F.col("toLkupAdmFile.Service_Date_Period").alias("Service_Date_Period"),
    F.col("toLkupAdmFile.AdminFee").alias("AdminFee"),
    F.col("toLkupAdmFile.ClaimFeeCount").alias("ClaimFeeCount"),
    F.col("toLkupAdmFile.LOB").alias("LOB"),
    F.col("toLkupSJ.ClaimFeeCountSj").alias("ClaimFeeCountSj")
)

# --------------------------------------------------------------------------------
# Filter (PxFilter) - No conditions => pass all rows to three output links
# --------------------------------------------------------------------------------
df_Filter = df_Lkp_Key

df_filter_non_pen = df_Filter.select(
    F.col("DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    F.col("COUNT_CLM_F_CLM_IDS").alias("COUNT_CLM_F_CLM_IDS"),
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE"),
    F.col("KEY").alias("KEY"),
    F.col("Service_Date_Period").alias("Service_Date_Period"),
    F.col("AdminFee").alias("AdminFee"),
    F.col("ClaimFeeCount").alias("ClaimFeeCount"),
    F.col("LOB").alias("LOB"),
    F.col("ClaimFeeCountSj").alias("ClaimFeeCountSj")
)

df_filter_penc = df_Filter.select(
    F.col("Service_Date_Period").alias("Service_Date_Period"),
    F.col("LOB").alias("LOB"),
    F.col("DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    F.col("AdminFee").alias("AdminFee"),
    F.col("ClaimFeeCount").alias("ClaimFeeCount"),
    F.col("COUNT_CLM_F_CLM_IDS").alias("COUNT_CLM_F_CLM_IDS"),
    F.col("ClaimFeeCountSj").alias("ClaimFeeCountSj"),
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE")
)

df_filter_total = df_Filter.select(
    F.col("Service_Date_Period").alias("Service_Date_Period"),
    F.col("LOB").alias("LOB"),
    F.col("DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT"),
    F.col("AdminFee").alias("AdminFee"),
    F.col("ClaimFeeCount").alias("ClaimFeeCount"),
    F.col("COUNT_CLM_F_CLM_IDS").alias("COUNT_CLM_F_CLM_IDS"),
    F.col("ClaimFeeCountSj").alias("ClaimFeeCountSj"),
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE")
)

# --------------------------------------------------------------------------------
# xfm_Format_Total (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_Format_Total_sv = (
    df_filter_total
    .withColumn(
        "svSumClmF",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT"))))
        )
    )
    .withColumn(
        "svSumAdmnFee",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee"))))
        )
    )
    .withColumn(
        "svCounSj",
        trim(F.col("ClaimFeeCountSj"))
    )
    .withColumn(
        "svCountClmF",
        trim(F.col("COUNT_CLM_F_CLM_IDS")) - F.col("svCounSj")
    )
    .withColumn(
        "svCount1",
        convert(' ', '0', TrimF(convert('0',' ',F.col("ClaimFeeCount"))))
    )
    .withColumn(
        "svCountAdmFee",
        DecimalToString(F.col("svCount1"), "fix_zero,suppress_zero")
    )
    .withColumn(
        "svCountCompare",
        F.when(
            F.trim(F.col("svCountClmF")) == F.trim(F.col("svCountAdmFee")),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
    .withColumn(
        "svSumCompare",
        F.when(
            F.col("svSumClmF") == F.col("svSumAdmnFee"),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
)

df_xfm_Format_Total = df_xfm_Format_Total_sv.select(
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE"),
    F.col("Service_Date_Period").alias("INVOICE_DATE"),
    F.col("LOB").alias("LOB"),
    F.col("svCountAdmFee").alias("CLAIM_FEE_COUNT"),
    F.col("svCountClmF").alias("CLM_F_COUNT"),
    F.col("svCountCompare").alias("COUNT_MATCH"),
    F.col("svSumClmF").alias("CLM_F_TOT_ADM_FEE_AMT"),
    F.col("svSumAdmnFee").alias("INVOICED_ADMIN_FEE_AMT"),
    F.col("svSumCompare").alias("SUM_MATCH"),
    F.col("svCounSj").alias("STJOSFEECOUNT")  # needed for the Peek
)

# Split the xfm_Format_Total outputs:
df_xfm_Format_Total_forFnl = df_xfm_Format_Total.select(
    "CLAIM_TYPE",
    "INVOICE_DATE",
    "LOB",
    "CLAIM_FEE_COUNT",
    "CLM_F_COUNT",
    "COUNT_MATCH",
    "CLM_F_TOT_ADM_FEE_AMT",
    "INVOICED_ADMIN_FEE_AMT",
    "SUM_MATCH"
)

df_xfm_Format_Total_forPeek = df_xfm_Format_Total.select(
    F.col("INVOICE_DATE").alias("INVOICE_DT"),
    F.col("STJOSFEECOUNT").alias("STJOSFEECOUNT")
)

# --------------------------------------------------------------------------------
# Peek (PxPeek)
# --------------------------------------------------------------------------------
# No transformation, just limiting rows in DataStage. We do nothing extra in PySpark

# --------------------------------------------------------------------------------
# xfm_Format (CTransformerStage) for Non-PENC
# --------------------------------------------------------------------------------
df_xfm_Format_sv = (
    df_filter_non_pen
    .withColumn(
        "svSumClmF",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT"))))
        )
    )
    .withColumn(
        "svSumAdmnFee",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee"))))
        )
    )
    .withColumn(
        "svCounSj",
        trim(F.col("ClaimFeeCountSj"))
    )
    .withColumn(
        "svCountClmF",
        trim(F.col("COUNT_CLM_F_CLM_IDS")) - F.col("svCounSj")
    )
    .withColumn(
        "svCount1",
        convert(' ', '0', TrimF(convert('0',' ',F.col("ClaimFeeCount"))))
    )
    .withColumn(
        "svCountAdmFee",
        DecimalToString(F.col("svCount1"), "fix_zero,suppress_zero")
    )
    .withColumn(
        "svCountCompare",
        F.when(
            F.trim(F.col("svCountClmF")) == F.trim(F.col("svCountAdmFee")),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
    .withColumn(
        "svSumCompare",
        F.when(
            F.col("svSumClmF") == F.col("svSumAdmnFee"),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
)

df_xfm_Format = df_xfm_Format_sv.select(
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE"),
    F.col("Service_Date_Period").alias("INVOICE_DATE"),
    F.col("LOB").alias("LOB"),
    F.col("svCountAdmFee").alias("CLAIM_FEE_COUNT"),
    F.col("svCountClmF").alias("CLM_F_COUNT"),
    F.col("svCountCompare").alias("COUNT_MATCH"),
    F.col("svSumClmF").alias("CLM_F_TOT_ADM_FEE_AMT"),
    F.col("svSumAdmnFee").alias("INVOICED_ADMIN_FEE_AMT"),
    F.col("svSumCompare").alias("SUM_MATCH")
)

# --------------------------------------------------------------------------------
# xfm_Format_Penc (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_Format_Penc_sv = (
    df_filter_penc
    .withColumn(
        "svSumClmF",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("DRUG_CLM_ADM_FEE_AMT"))))
        )
    )
    .withColumn(
        "svSumAdmnFee",
        F.when(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee")))) == F.lit(".00"),
            F.lit("0.00")
        ).otherwise(
            convert(' ', '0', TrimF(convert('0',' ',F.col("AdminFee"))))
        )
    )
    .withColumn(
        "svCounSj",
        trim(F.col("ClaimFeeCountSj"))
    )
    .withColumn(
        "svCountClmF",
        trim(F.col("COUNT_CLM_F_CLM_IDS"))
    )
    .withColumn(
        "svCount1",
        convert(' ', '0', TrimF(convert('0',' ',F.col("ClaimFeeCount"))))
    )
    .withColumn(
        "svCountAdmFee",
        DecimalToString(F.col("svCount1"), "fix_zero,suppress_zero")
    )
    .withColumn(
        "svCountCompare",
        F.when(
            F.trim(F.col("svCountClmF")) == F.trim(F.col("svCountAdmFee")),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
    .withColumn(
        "svSumCompare",
        F.when(
            F.col("svSumClmF") == F.col("svSumAdmnFee"),
            F.lit("YES")
        ).otherwise(F.lit("NO"))
    )
)

df_xfm_Format_Penc = df_xfm_Format_Penc_sv.select(
    F.col("CLAIM_TYPE").alias("CLAIM_TYPE"),
    F.col("Service_Date_Period").alias("INVOICE_DATE"),
    F.col("LOB").alias("LOB"),
    F.lit("0").alias("CLAIM_FEE_COUNT"),
    F.col("svCountClmF").alias("CLM_F_COUNT"),
    F.lit("NO").alias("COUNT_MATCH"),
    F.col("svSumClmF").alias("CLM_F_TOT_ADM_FEE_AMT"),
    F.lit("0").alias("INVOICED_ADMIN_FEE_AMT"),
    F.lit("NO").alias("SUM_MATCH")
)

# --------------------------------------------------------------------------------
# Fnl (PxFunnel)
# Funnel the three xfm outputs
# --------------------------------------------------------------------------------
df_Fnl = (
    df_xfm_Format.select(
        "CLAIM_TYPE",
        "INVOICE_DATE",
        "LOB",
        "CLAIM_FEE_COUNT",
        "CLM_F_COUNT",
        "COUNT_MATCH",
        "CLM_F_TOT_ADM_FEE_AMT",
        "INVOICED_ADMIN_FEE_AMT",
        "SUM_MATCH"
    )
    .unionByName(
        df_xfm_Format_Penc.select(
            "CLAIM_TYPE",
            "INVOICE_DATE",
            "LOB",
            "CLAIM_FEE_COUNT",
            "CLM_F_COUNT",
            "COUNT_MATCH",
            "CLM_F_TOT_ADM_FEE_AMT",
            "INVOICED_ADMIN_FEE_AMT",
            "SUM_MATCH"
        )
    )
    .unionByName(
        df_xfm_Format_Total_forFnl.select(
            "CLAIM_TYPE",
            "INVOICE_DATE",
            "LOB",
            "CLAIM_FEE_COUNT",
            "CLM_F_COUNT",
            "COUNT_MATCH",
            "CLM_F_TOT_ADM_FEE_AMT",
            "INVOICED_ADMIN_FEE_AMT",
            "SUM_MATCH"
        )
    )
)

# --------------------------------------------------------------------------------
# Sf_AdminFeeReport (PxSequentialFile) - Writing to verified
# --------------------------------------------------------------------------------
# Final select re-order (already in correct order) + rpad for each varchar column
df_Sf_AdminFeeReport = (
    df_Fnl
    .select(
        F.rpad(F.col("CLAIM_TYPE"), <...>, " ").alias("CLAIM_TYPE"),
        F.rpad(F.col("INVOICE_DATE"), <...>, " ").alias("INVOICE_DATE"),
        F.rpad(F.col("LOB"), <...>, " ").alias("LOB"),
        F.rpad(F.col("CLAIM_FEE_COUNT"), <...>, " ").alias("CLAIM_FEE_COUNT"),
        F.rpad(F.col("CLM_F_COUNT"), <...>, " ").alias("CLM_F_COUNT"),
        F.rpad(F.col("COUNT_MATCH"), <...>, " ").alias("COUNT_MATCH"),
        F.rpad(F.col("CLM_F_TOT_ADM_FEE_AMT"), <...>, " ").alias("CLM_F_TOT_ADM_FEE_AMT"),
        F.rpad(F.col("INVOICED_ADMIN_FEE_AMT"), <...>, " ").alias("INVOICED_ADMIN_FEE_AMT"),
        F.rpad(F.col("SUM_MATCH"), <...>, " ").alias("SUM_MATCH")
    )
)

write_files(
    df_Sf_AdminFeeReport,
    f"{adls_path}/verified/{LOB}_Summary_AdminFee.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)