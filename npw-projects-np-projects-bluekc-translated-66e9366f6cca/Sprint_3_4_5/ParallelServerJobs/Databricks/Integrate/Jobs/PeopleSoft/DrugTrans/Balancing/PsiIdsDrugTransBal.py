# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/10/07 14:54:28 Batch  14528_53717 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 14:22:50 Batch  14528_51775 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:14:50 Batch  14526_62095 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 16:58:04 Batch  14526_61088 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      PsiIdsDrugTransBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/07/2007          3264                              Originally Programmed                                    devlIDS30   
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                       devlIDS30                   Steph Goddard            09/15/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
DRUG_TRANS.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
DRUG_TRANS.DRUG_TRANS_CK AS SRC_DRUG_TRANS_CK,
DRUG_TRANS.ACCTG_DT_SK AS SRC_ACCTG_DT_SK,
B_DRUG_TRANS.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_DRUG_TRANS.DRUG_TRANS_CK AS TRGT_DRUG_TRANS_CK,
B_DRUG_TRANS.ACCTG_DT_SK AS TRGT_ACCTG_DT_SK
FROM {IDSOwner}.DRUG_TRANS DRUG_TRANS
FULL OUTER JOIN {IDSOwner}.B_DRUG_TRANS B_DRUG_TRANS
ON DRUG_TRANS.SRC_SYS_CD_SK = B_DRUG_TRANS.SRC_SYS_CD_SK
AND DRUG_TRANS.DRUG_TRANS_CK = B_DRUG_TRANS.DRUG_TRANS_CK
AND DRUG_TRANS.ACCTG_DT_SK = B_DRUG_TRANS.ACCTG_DT_SK
WHERE DRUG_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    col("SRC_ACCTG_DT_SK").isNull()
    | col("SRC_DRUG_TRANS_CK").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_ACCTG_DT_SK").isNull()
    | col("TRGT_DRUG_TRANS_CK").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
)
df_research = df_research.withColumn("TRGT_ACCTG_DT_SK", rpad(col("TRGT_ACCTG_DT_SK"), 10, " "))
df_research = df_research.withColumn("SRC_ACCTG_DT_SK", rpad(col("SRC_ACCTG_DT_SK"), 10, " "))
df_research = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_DRUG_TRANS_CK",
    "TRGT_ACCTG_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_DRUG_TRANS_CK",
    "SRC_ACCTG_DT_SK"
)
write_files(
    df_research,
    f"{adls_path}/balancing/research/PsiIdsDrugTransResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_notify = df_SrcTrgtComp.withColumn("rownum", row_number().over(Window.orderBy(lit(None))))
df_notify = df_notify.filter((col("rownum") == 1) & (ToleranceCd == "OUT"))
df_notify = df_notify.withColumn("NOTIFICATION", lit("ROW COUNT BALANCING PSI - IDS DRUG TRANS OUT OF TOLERANCE"))
df_notify = df_notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))
df_notify = df_notify.select("NOTIFICATION")
write_files(
    df_notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)