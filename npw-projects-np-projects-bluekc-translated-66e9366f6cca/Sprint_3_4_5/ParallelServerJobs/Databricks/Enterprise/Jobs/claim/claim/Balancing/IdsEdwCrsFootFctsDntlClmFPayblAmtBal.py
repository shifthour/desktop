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
# MAGIC CALLED BY:          IdsEdwCrsFootFctsDntlClmFPayblAmtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/02/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/22/2007

# MAGIC Grouped By NDC Sk For Non Drug Claims to get Total Sum
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
from pyspark.sql.functions import col, lit, when, rpad, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
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
CLM_F.NDC_SK,
CLM_F.CLM_COB_CD,
CLM_F.CLM_PAYE_CD,
CLM_F.CLM_ECRP_IN,
CLM_F.CLM_HOST_IN,
CLM_F.CLM_IN_NTWK_IN,
CLM_F.CLM_LN_TOT_COINS_AMT,
CLM_F.CLM_LN_TOT_CNSD_CHRG_AMT,
CLM_F.CLM_LN_TOT_COPAY_AMT,
CLM_F.CLM_LN_TOT_DEDCT_AMT,
CLM_F.CLM_LN_TOT_DSALW_AMT,
CLM_F.CLM_LN_TOT_PAYBL_AMT,
CLM_F.CLM_LN_TOT_RISK_WTHLD_AMT,
CLM_F.CLM_LN_TOT_SUPLMT_DSCNT_AMT,
CLM_F.GRP_ID,
CLM_LN_COB_F.CLM_LN_COB_ADJ_AMT,
PROD_SH_NM_D.PROD_SH_NM_CAT_CD
FROM {EDWOwner}.CLM_F CLM_F,
     {EDWOwner}.CLM_LN_F CLM_LN_F,
     {EDWOwner}.CLM_LN_COB_F CLM_LN_COB_F,
     {EDWOwner}.PROD_SH_NM_D PROD_SH_NM_D
WHERE CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND CLM_F.SRC_SYS_CD = 'FACETS'
  AND CLM_F.CLM_TYP_CD = 'DNTL'
  AND CLM_F.CLM_STTUS_CD IN ('A08','A09','A02')
  AND CLM_F.CLM_FINL_DISP_CD = 'ACPTD'
  AND CLM_F.GRP_ID <> 'NASCOPAR'
  AND CLM_F.PROD_SH_NM_SK = PROD_SH_NM_D.PROD_SH_NM_SK
  AND CLM_F.CLM_SK = CLM_LN_F.CLM_SK
  AND CLM_LN_F.CLM_LN_SK = CLM_LN_COB_F.CLM_LN_SK
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
    .withColumn("svMbrRspnsblty",
                col("CLM_LN_TOT_COPAY_AMT") + col("CLM_LN_TOT_COINS_AMT") + col("CLM_LN_TOT_DEDCT_AMT"))
    .withColumn("svOthrBcbsRspnsblty",
                col("CLM_LN_TOT_RISK_WTHLD_AMT") + col("CLM_LN_TOT_SUPLMT_DSCNT_AMT"))
    .withColumn(
        "svCobAdj",
        when(col("PROD_SH_NM_CAT_CD") == 'MCARESUPLMT', lit(0))
        .otherwise(
            when(col("CLM_HOST_IN") == 'N', col("CLM_LN_COB_ADJ_AMT"))
            .otherwise(
                when(
                    (col("CLM_HOST_IN") == 'Y') &
                    ((col("CLM_COB_CD") == 'COMMED') | (col("CLM_COB_CD") == 'MCAREAB')),
                    col("CLM_LN_COB_ADJ_AMT")
                ).otherwise(lit(0))
            )
        )
    )
    .withColumn("svBcbsPaybl", col("CLM_LN_TOT_PAYBL_AMT"))
    .withColumn(
        "svCalcPaybl",
        col("CLM_LN_TOT_CNSD_CHRG_AMT")
        - col("svMbrRspnsblty")
        - col("svOthrBcbsRspnsblty")
        - col("CLM_LN_TOT_DSALW_AMT")
        - col("svCobAdj")
    )
)

df_Missing = df_TransformLogic.filter(
    ~((col("CLM_HOST_IN") == 'Y') & (col("CLM_ECRP_IN") == 'Y')) &
    ~((col("CLM_HOST_IN") == 'Y') & (col("CLM_PAYE_CD") == 'SUB') & (col("CLM_LN_TOT_PAYBL_AMT") == 0)) &
    ~((col("GRP_ID") == '10023000') & (col("CLM_IN_NTWK_IN") == 'N')) &
    (col("svBcbsPaybl") != col("svCalcPaybl"))
)

df_AggregatedSum = df_TransformLogic.filter(
    ~((col("CLM_HOST_IN") == 'Y') & (col("CLM_ECRP_IN") == 'Y')) &
    ~((col("CLM_HOST_IN") == 'Y') & (col("CLM_PAYE_CD") == 'SUB') & (col("CLM_LN_TOT_PAYBL_AMT") == 0)) &
    ~((col("GRP_ID") == '10023000') & (col("CLM_IN_NTWK_IN") == 'N'))
)

df_Aggregated_17 = (
    df_AggregatedSum
    .select(
        col("NDC_SK").alias("NDC_SK"),
        col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
        col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
    )
    .groupBy("NDC_SK")
    .agg(
        sum_("CLM_BCBS_PAYBL_AMT").alias("CLM_BCBS_PAYBL_AMT"),
        sum_("CLM_CALC_PAYBL_AMT").alias("CLM_CALC_PAYBL_AMT")
    )
)

df_Transform = (
    df_Aggregated_17
    .withColumn("svCalcFctsDntlPayblAmtSum", col("CLM_CALC_PAYBL_AMT"))
    .withColumn("svWriteSumJobInfo1", WriteRunInfo(lit("CalcFctsDntlPayblAmtSum"), col("svCalcFctsDntlPayblAmtSum"), lit(0)))
    .withColumn("svOrigFctsDntlPayblAmtSum", col("CLM_BCBS_PAYBL_AMT"))
    .withColumn("svWriteSumJobInfo2", WriteRunInfo(lit("OrigFctsDntlPayblAmtSum"), col("svOrigFctsDntlPayblAmtSum"), lit(0)))
    .withColumn("SUM_NOTIFY", lit("SUM VALUES FOR AMOUNT FIELDS USED BY SEQUENCER LATER"))
)

df_ValueNote = df_Transform.select("SUM_NOTIFY")
df_ValueNote = df_ValueNote.withColumn("SUM_NOTIFY", rpad(col("SUM_NOTIFY"), 70, " "))

write_files(
    df_ValueNote,
    f"{adls_path}/balancing/sync/ClmFFctsDntlPayblAmt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer_25_input = df_Missing

df_Notify = df_Transformer_25_input.limit(1).select(
    lit("CROSS FOOT BALANCING IDS - EDW CLM F OUT OF TOLERANCE FOR CLM LN TOT PAYBL AMT FIELD ON FCTS DNTL ONLY")
    .alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))
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

df_Miss = df_Transformer_25_input.select(
    col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
    col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
)

write_files(
    df_Miss,
    f"{adls_path}/balancing/research/IdsEdwClmFFctsDntlCrossFootResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)