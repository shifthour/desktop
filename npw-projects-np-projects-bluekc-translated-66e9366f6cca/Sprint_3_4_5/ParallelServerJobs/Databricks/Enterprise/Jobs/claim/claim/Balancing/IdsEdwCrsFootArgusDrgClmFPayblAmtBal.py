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
# MAGIC CALLED BY:          IdsEdwCrsFootArgusDrgClmFPayblAmtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/02/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/22/2007

# MAGIC Grouped By Procedure Code Sk For Drug Claims to get Total Sum
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
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = f"""
SELECT
CLM_F.PROC_CD_1_SK,
CLM_F.CLM_PAYE_CD,
CLM_F.CLM_ECRP_IN,
CLM_F.CLM_HOST_IN,
CLM_F.CLM_LN_TOT_MBR_OBLGT_DSALW_AMT,
CLM_F.CLM_LN_TOT_PAYBL_AMT,
CLM_F.DRUG_CLM_DISPNS_FEE_AMT,
CLM_F.DRUG_CLM_INGR_CST_ALW_AMT,
CLM_F.DRUG_CLM_SLS_TAX_AMT

FROM 
{EDWOwner}.CLM_F CLM_F

WHERE 
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A08','A09','A02') AND CLM_F.CLM_FINL_DISP_CD = 'ACPTD'
AND CLM_F.GRP_ID <> 'NASCOPAR' AND CLM_F.SRC_SYS_CD = 'ARGUS' AND CLM_F.CLM_TYP_CD = 'MED'

{EDWOwner}.CUST_SVC_D CustSvcD FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_D BCustSvcD ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_TransformLogic = (
    df_SrcTrgtRowComp
    .withColumn("svTotalCost", col("DRUG_CLM_INGR_CST_ALW_AMT") + col("DRUG_CLM_DISPNS_FEE_AMT") + col("DRUG_CLM_SLS_TAX_AMT"))
    .withColumn("svMbrRspnsblty", col("CLM_LN_TOT_MBR_OBLGT_DSALW_AMT") + col("DRUG_CLM_SLS_TAX_AMT"))
    .withColumn("svBcbsPaybl", col("CLM_LN_TOT_PAYBL_AMT"))
    .withColumn(
        "svCalcPaybl",
        when((col("svMbrRspnsblty") == 0) & (col("svBcbsPaybl") == 0), 0)
        .when(col("svMbrRspnsblty") > col("svTotalCost"), 0)
        .otherwise(col("svTotalCost") - col("svMbrRspnsblty"))
    )
)

# Constraint for "Missing" link
df_Missing = df_TransformLogic.filter(
    (
        ((col("CLM_HOST_IN") != 'Y') | (col("CLM_ECRP_IN") != 'Y'))
        & ((col("CLM_HOST_IN") != 'Y') | (col("CLM_PAYE_CD") != 'SUB') | (col("CLM_LN_TOT_PAYBL_AMT") != 0))
        & (col("svBcbsPaybl") != col("svCalcPaybl"))
    )
).select(
    col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
    col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
)

# Constraint for "AggregatedSum" link
df_AggregatedSum = df_TransformLogic.filter(
    (
        ((col("CLM_HOST_IN") != 'Y') | (col("CLM_ECRP_IN") != 'Y'))
        & ((col("CLM_HOST_IN") != 'Y') | (col("CLM_PAYE_CD") != 'SUB') | (col("CLM_LN_TOT_PAYBL_AMT") != 0))
    )
).select(
    col("PROC_CD_1_SK").alias("PROC_CD_1_SK"),
    col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
    col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
)

# Trans1: separate the single row for "Notify" from the rest, based on @INROWNUM=1
w = Window.orderBy(lit(1))
df_Missing_rn = df_Missing.withColumn("__rownum", row_number().over(w))

df_Notify = df_Missing_rn.filter(col("__rownum") == 1).selectExpr(
    "'CROSS FOOT BALANCING IDS - EDW CLM F OUT OF TOLERANCE FOR CLM LN TOT PAYBL AMT FIELD ON ARGUS DRUG ONLY' as NOTIFICATION"
)
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " ")).select("NOTIFICATION")

df_Miss = df_Missing.select("CLM_BCBS_PAYBL_AMT", "CLM_CALC_PAYBL_AMT")

# ErrorNotificationFile
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ResearchFile
write_files(
    df_Miss,
    f"{adls_path}/balancing/research/IdsEdwClmFArgusDrugCrossFootResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Aggregator_17
df_Aggregator_17 = (
    df_AggregatedSum
    .groupBy("PROC_CD_1_SK")
    .agg(
        F.sum("CLM_BCBS_PAYBL_AMT").alias("CLM_BCBS_PAYBL_AMT"),
        F.sum("CLM_CALC_PAYBL_AMT").alias("CLM_CALC_PAYBL_AMT")
    )
)

# Transform
df_Transform = (
    df_Aggregator_17
    .withColumn("svCalcArgusDrugPayblAmtSum", col("CLM_CALC_PAYBL_AMT"))
    .withColumn("svWriteSumJobInfo1", WriteRunInfo("CalcArgusDrugPayblAmtSum", col("svCalcArgusDrugPayblAmtSum"), lit(0)))
    .withColumn("svOrigArgusDrugPayblAmtSum", col("CLM_BCBS_PAYBL_AMT"))
    .withColumn("svWriteSumJobInfo2", WriteRunInfo("OrigArgusDrugPayblAmtSum", col("svOrigArgusDrugPayblAmtSum"), lit(0)))
    .withColumn("SUM_NOTIFY", F.lit("SUM VALUES FOR AMOUNT FIELDS USED BY SEQUENCER LATER"))
)

df_ValueNote = df_Transform.select(
    rpad(col("SUM_NOTIFY"), 70, " ").alias("SUM_NOTIFY")
)

# CrsFtSumFile
write_files(
    df_ValueNote,
    f"{adls_path}/balancing/sync/ClmFArgusDrugPayblAmt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)