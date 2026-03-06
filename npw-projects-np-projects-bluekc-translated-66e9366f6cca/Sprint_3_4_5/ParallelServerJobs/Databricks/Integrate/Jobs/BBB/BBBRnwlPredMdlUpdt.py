# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/01/07 14:40:01 Batch  14519_52860 PROMOTE bckcetl ids20 dsadm rc for nate 
# MAGIC ^1_1 10/01/07 14:35:34 Batch  14519_52546 INIT bckcett devlIDS30 dsadm rc for nate
# MAGIC ^1_2 06/29/04 10:46:17 Batch  13330_38782 INIT bckccdt ids u08717 Brent
# MAGIC ^1_1 06/29/04 10:08:55 Batch  13330_36549 INIT bckccdt ids u10157 S.Andrew 
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  BBBRnwlPredMdl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Load BBB table BCBSKC_PredictiveModel via SQL inserts from text file in landing directory.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   BBBRnwlPredMdlCntl (job sequencer)
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC 
# MAGIC               file input: /ids/devl-stage-prod/landing/BCBSKC_PredictiveModel.txt
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  None
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Load BBB table BCBSKC_PredictiveModel via SQL inserts.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED: none
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Database                    Table Name
# MAGIC                  ----------------------------       ---------------------------------------------------------------------
# MAGIC \(9) BlueBusinessBuilder    BCBSKC_PredictiveModel
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED:    None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  data loaded to BCBSKC_PredictiveModel
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset just rerun. 
# MAGIC              PREVIOUS RUN ABORTED:         Normally nothing has to be done before restarting. Seems problems seem to require job to be recompiled.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Nate Dow                09/26/2007                                   Orginial programming                                                                    devlIDS30                       Steph Goddard          09/28/2007
# MAGIC 
# MAGIC Rahul David            05/03/2024        US617044           Added  (2) new fields, RiskScore and AgeGenderScore             IntegrateDev1                  Jeyaprasanna           2024-06-03
# MAGIC                                                                                         to table BBB_PredictiveModel in UNIX stage , so that the 
# MAGIC                                                                                        new fields  are added to the upload.

# MAGIC Import Actuarial provided predictive model data
# MAGIC BBBRnwlPredMdl
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


BlueBusOwner = get_widget_value('BlueBusOwner','')
bluebus_secret_name = get_widget_value('bluebus_secret_name','')

schema_UNIX = StructType([
    StructField("blockID", IntegerType(), nullable=False),
    StructField("groupID", StringType(), nullable=False),
    StructField("subscriberID", StringType(), nullable=False),
    StructField("suffix", StringType(), nullable=False),
    StructField("pmKnownValue", FloatType(), nullable=False),
    StructField("pmUnknownValue", FloatType(), nullable=False),
    StructField("memberHighLevelClaims", FloatType(), nullable=False),
    StructField("WeightedVariance", FloatType(), nullable=False),
    StructField("pmDiagnosisPrognosis", StringType(), nullable=False),
    StructField("pmFlag", IntegerType(), nullable=False),
    StructField("RiskScore", FloatType(), nullable=True),
    StructField("AgeGenderScore", FloatType(), nullable=True)
])

df_UNIX = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_UNIX)
    .load(f"{adls_path_raw}/landing/BCBSKC_PredictiveModel.txt")
)

df_TransForInsert = df_UNIX

df_insert_BCBSKC_PredictiveModel = df_TransForInsert.select(
    "blockID",
    "groupID",
    "subscriberID",
    "suffix",
    "pmKnownValue",
    "pmUnknownValue",
    "memberHighLevelClaims",
    "WeightedVariance",
    "pmDiagnosisPrognosis",
    "pmFlag",
    "RiskScore",
    "AgeGenderScore"
)

df_insert_BCBSKC_PredictiveModel = df_insert_BCBSKC_PredictiveModel.withColumn(
    "groupID", F.rpad(F.col("groupID"), <...>, " ")
).withColumn(
    "subscriberID", F.rpad(F.col("subscriberID"), <...>, " ")
).withColumn(
    "suffix", F.rpad(F.col("suffix"), <...>, " ")
).withColumn(
    "pmDiagnosisPrognosis", F.rpad(F.col("pmDiagnosisPrognosis"), <...>, " ")
)

jdbc_url, jdbc_props = get_db_config(bluebus_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.BBBRnwlPredMdlUpdt_BlueBusinessBuilder_temp", jdbc_url, jdbc_props)

df_insert_BCBSKC_PredictiveModel.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.BBBRnwlPredMdlUpdt_BlueBusinessBuilder_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {BlueBusOwner}.BCBSKC_PredictiveModel AS T
USING STAGING.BBBRnwlPredMdlUpdt_BlueBusinessBuilder_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
  INSERT 
  (
    blockID, groupID, subscriberID, suffix, pmKnownValue, pmUnknownValue, memberHighLevelClaims, WeightedVariance, pmDiagnosisPrognosis, pmFlag, RiskScore, AgeGenderScore
  )
  VALUES
  (
    S.blockID, S.groupID, S.subscriberID, S.suffix, S.pmKnownValue, S.pmUnknownValue, S.memberHighLevelClaims, S.WeightedVariance, S.pmDiagnosisPrognosis, S.pmFlag, S.RiskScore, S.AgeGenderScore
  )
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)