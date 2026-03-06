# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/11/07 15:00:54 Batch  14529_54061 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/11/07 14:39:56 Batch  14529_52800 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/10/07 07:56:07 Batch  14528_28571 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/10/07 07:39:54 Batch  14528_27598 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsWebdmClmnSumUmIpBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/31/2007          3264                              Originally Programmed                           devlIDS30                              
# MAGIC 
# MAGIC Manasa Andru                  11/02/2011        TTR- 1234                  Changed the Output file extension from   IntegrateNewDevl        SAndrew                      2011-11-17
# MAGIC                                                                                                                     .TXT to .DAT

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

query_SrcTrgtRowComp = f"""SELECT
UM_IP.SRC_SYS_CD,
UM_IP.UM_REF_ID,
UM_IP.UM_IP_ACTL_LOS_DAYS_QTY
FROM {ClmMartOwner}.MED_MGT_DM_UM_IP UM_IP
FULL OUTER JOIN {ClmMartOwner}.B_MED_MGT_DM_UM_IP BUmIp
   ON UM_IP.SRC_SYS_CD = BUmIp.SRC_SYS_CD
   AND UM_IP.UM_REF_ID = BUmIp.UM_REF_ID
WHERE
   UM_IP.LAST_UPDT_RUN_CYC_NO >= {ExtrRunCycle}
   AND UM_IP.UM_IP_ACTL_LOS_DAYS_QTY <> BUmIp.UM_IP_ACTL_LOS_DAYS_QTY
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp)
    .load()
)

df_SrcTrgtRowComp = df_SrcTrgtRowComp.withColumn(
    "_row_number_",
    F.row_number().over(Window.orderBy(F.lit(1)))
)

df_Research = df_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_IP_ACTL_LOS_DAYS_QTY"
)

df_Notify = df_SrcTrgtRowComp.filter(F.col("_row_number_") == 1).select(
    F.lit("COLUMN SUM BALANCING IDS - DATAMART MED MGT DM UM IP OUT OF TOLERANCE").alias("NOTIFICATION")
)

write_files(
    df_Research.select("SRC_SYS_CD","UM_REF_ID","UM_IP_ACTL_LOS_DAYS_QTY"),
    f"{adls_path}/balancing/research/IdsWebdmUmIpColumnSumResearch.dat.#RunID#",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)

df_NotifyFinal = df_Notify.select(
    F.rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_NotifyFinal.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    '"',
    None
)