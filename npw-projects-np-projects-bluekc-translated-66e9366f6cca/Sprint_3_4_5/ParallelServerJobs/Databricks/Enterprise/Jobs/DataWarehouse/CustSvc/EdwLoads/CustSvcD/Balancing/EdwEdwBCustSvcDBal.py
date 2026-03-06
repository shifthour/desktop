# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/27/2007          3264                              Originally Programmed                           devlEDW10               Steph Goddard            10/21/2007             
# MAGIC 
# MAGIC Bhupinder Kaur                12/05/2013          5114                               Create Load File for                          EnterpriseWhseDevl       Jag Yelavarthi              2014-02-25
# MAGIC                                                                                                              EDW Table B_CUST_SVC_D

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC Job: EdwEdwBCustSvcDBal
# MAGIC File checked later for rows and email to on-call
# MAGIC Pull all the Matching Records from 
# MAGIC CUST_SVC_D 
# MAGIC B_CUST_SVC_D
# MAGIC Write all the Matching Records from EDWtables:
# MAGIC CUST_SVC_D and B_CUST_SVC_D
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC Pull all the Missing Records from
# MAGIC CUST_SVC_D 
# MAGIC B_CUST_SVC_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------

# COMMAND ----------
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
# COMMAND ----------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_B_CUST_SVC_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CustSvcD.SRC_SYS_CD, CustSvcD.CUST_SVC_ID, CustSvcD.CUST_SVC_DISCLMR_IN "
        f"FROM {EDWOwner}.CUST_SVC_D CustSvcD "
        f"FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_D BCustSvcD "
        f"ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD "
        f"AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID "
        f"WHERE CustSvcD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle} "
        f"AND CustSvcD.CUST_SVC_DISCLMR_IN <> BCustSvcD.CUST_SVC_DISCLMR_IN"
    )
    .load()
)
# COMMAND ----------
df_xfrm_BusinessLogic_out1 = df_db2_B_CUST_SVC_D_Missing_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("CUST_SVC_DISCLMR_IN").alias("CUST_SVC_DISCLMR_IN")
)
df_xfrm_BusinessLogic_out1 = df_xfrm_BusinessLogic_out1.withColumn(
    "CUST_SVC_DISCLMR_IN",
    F.rpad(F.col("CUST_SVC_DISCLMR_IN"), 1, " ")
)
w = Window.orderBy(F.lit(1))
df_temp_out2 = df_db2_B_CUST_SVC_D_Missing_in.withColumn("rn", F.row_number().over(w))
df_xfrm_BusinessLogic_out2 = df_temp_out2.filter("rn=1").select(
    F.lit("ROW TO ROW BALANCING IDS - EDW CUST SVC D OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_xfrm_BusinessLogic_out2 = df_xfrm_BusinessLogic_out2.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
)
# COMMAND ----------
write_files(
    df_xfrm_BusinessLogic_out1.select("SRC_SYS_CD", "CUST_SVC_ID", "CUST_SVC_DISCLMR_IN"),
    f"{adls_path}/balancing/research/IdsEdwCustSvcDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
# COMMAND ----------
df_dedup = dedup_sort(
    df_xfrm_BusinessLogic_out2,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)
df_dedup = df_dedup.select(
    F.col("NOTIFICATION")
)
# COMMAND ----------
write_files(
    df_dedup,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
# COMMAND ----------
df_db2_B_CUST_SVC_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CustSvcD.CUST_SVC_DISCLMR_IN "
        f"FROM {EDWOwner}.CUST_SVC_D CustSvcD "
        f"INNER JOIN {EDWOwner}.B_CUST_SVC_D BCustSvcD "
        f"ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD "
        f"AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID "
        f"WHERE CustSvcD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle} "
        f"AND CustSvcD.CUST_SVC_DISCLMR_IN = BCustSvcD.CUST_SVC_DISCLMR_IN"
    )
    .load()
)
df_db2_B_CUST_SVC_D_Matching_in = df_db2_B_CUST_SVC_D_Matching_in.withColumn(
    "CUST_SVC_DISCLMR_IN",
    F.rpad(F.col("CUST_SVC_DISCLMR_IN"), 1, " ")
)
# COMMAND ----------
write_files(
    df_db2_B_CUST_SVC_D_Matching_in.select("CUST_SVC_DISCLMR_IN"),
    f"{adls_path}/balancing/sync/CustSvcDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)