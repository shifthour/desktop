# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/05/07 14:33:17 Batch  14554_52458 PROMOTE bckcetl edw10 dsadm rc for brent
# MAGIC ^1_1 11/05/07 13:30:09 Batch  14554_48642 INIT bckcett testEDW10 dsadm rc for brent
# MAGIC ^1_3 11/02/07 15:11:26 Batch  14551_54690 PROMOTE bckcett testEDW10 u08717 Brent
# MAGIC ^1_3 11/02/07 15:00:18 Batch  14551_54021 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_2 11/02/07 07:43:09 Batch  14551_27794 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 11/01/07 11:00:01 Batch  14550_39611 INIT bckcett devlEDW10 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwPcaClmFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/06/2007          3264                              Originally Programmed                           devlEDW10               Steph Goddard            10/23/2007

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_SrcTrgtRowComp = f"""
SELECT
PCA_CLM_F.SRC_SYS_CD,
PCA_CLM_F.CLM_ID,
PCA_CLM_F.CLM_PCA_TOT_CNSD_AMT,
PCA_CLM_F.CLM_PCA_TOT_PD_AMT
FROM 
{EDWOwner}.PCA_CLM_F PCA_CLM_F FULL OUTER JOIN {EDWOwner}.B_PCA_CLM_F B_PCA_CLM_F
ON PCA_CLM_F.SRC_SYS_CD = B_PCA_CLM_F.SRC_SYS_CD 
AND PCA_CLM_F.CLM_ID = B_PCA_CLM_F.CLM_ID
WHERE 
PCA_CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND
(PCA_CLM_F.CLM_PCA_TOT_CNSD_AMT <> B_PCA_CLM_F.CLM_PCA_TOT_CNSD_AMT 
OR PCA_CLM_F.CLM_PCA_TOT_PD_AMT <> B_PCA_CLM_F.CLM_PCA_TOT_PD_AMT)

{EDWOwner}.CUST_SVC_D CustSvcD FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_D BCustSvcD ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SrcTrgtRowComp)
    .load()
)

df_research = df_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_PCA_TOT_CNSD_AMT",
    "CLM_PCA_TOT_PD_AMT"
)

has_rows = df_SrcTrgtRowComp.limit(1).count()
if has_rows > 0:
    df_notify_temp = spark.createDataFrame(
        [("COLUMN SUM BALANCING IDS - EDW PCA CLM F OUT OF TOLERANCE",)],
        ["NOTIFICATION"]
    )
else:
    df_notify_temp = spark.createDataFrame(
        [],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )

df_notify = df_notify_temp.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

write_files(
    df_notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_research.select("SRC_SYS_CD", "CLM_ID", "CLM_PCA_TOT_CNSD_AMT", "CLM_PCA_TOT_PD_AMT"),
    f"{adls_path}/balancing/research/IdsEdwPcaClmFColumnSumResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)