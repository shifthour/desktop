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
# MAGIC CALLED BY:          IdsEdwCrsFootFctsMedClmFPayblAmtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/02/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/22/2007

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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = """SELECT
CLM_F.NDC_SK,
CLM_F.CLM_COB_CD,
CLM_F.CLM_PAYE_CD,
CLM_F.CLM_ECRP_IN,
CLM_F.CLM_HOST_IN,
CLM_F.CLM_LN_TOT_COINS_AMT,
CLM_F.CLM_LN_TOT_CNSD_CHRG_AMT,
CLM_F.CLM_LN_TOT_COPAY_AMT,
CLM_F.CLM_LN_TOT_DEDCT_AMT,
CLM_F.CLM_LN_TOT_DSALW_AMT,
CLM_F.CLM_LN_TOT_PAYBL_AMT,
CLM_F.CLM_LN_TOT_RISK_WTHLD_AMT,
CLM_F.CLM_LN_TOT_SUPLMT_DSCNT_AMT,
CLM_LN_COB_F.CLM_LN_COB_ADJ_AMT,
PROD_SH_NM_D.PROD_SH_NM_CAT_CD
FROM """ + EDWOwner + """.CLM_F CLM_F,""" + EDWOwner + """.CLM_LN_F CLM_LN_F,""" + EDWOwner + """.CLM_LN_COB_F CLM_LN_COB_F,""" + EDWOwner + """.PROD_SH_NM_D PROD_SH_NM_D
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = """ + ExtrRunCycle + """
AND CLM_F.SRC_SYS_CD = 'FACETS'
AND CLM_F.CLM_TYP_CD = 'MED'
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
    .withColumn(
        "svMbrRspnsblty",
        F.col("CLM_LN_TOT_COPAY_AMT") + F.col("CLM_LN_TOT_COINS_AMT") + F.col("CLM_LN_TOT_DEDCT_AMT")
    )
    .withColumn(
        "svOthrBcbsRspnsblty",
        F.col("CLM_LN_TOT_RISK_WTHLD_AMT") + F.col("CLM_LN_TOT_SUPLMT_DSCNT_AMT")
    )
    .withColumn(
        "svCobAdj",
        F.when(F.col("PROD_SH_NM_CAT_CD") == "MCARESUPLMT", F.lit(0))
        .otherwise(
            F.when(F.col("CLM_HOST_IN") == "N", F.col("CLM_LN_COB_ADJ_AMT"))
            .otherwise(
                F.when(
                    (F.col("CLM_HOST_IN") == "Y")
                    & ((F.col("CLM_COB_CD") == "COMMED") | (F.col("CLM_COB_CD") == "MCAREAB")),
                    F.col("CLM_LN_COB_ADJ_AMT")
                )
                .otherwise(F.lit(0))
            )
        )
    )
    .withColumn("svBcbsPaybl", F.col("CLM_LN_TOT_PAYBL_AMT"))
    .withColumn(
        "svCalcPaybl",
        F.col("CLM_LN_TOT_CNSD_CHRG_AMT")
        - F.col("svMbrRspnsblty")
        - F.col("svOthrBcbsRspnsblty")
        - F.col("CLM_LN_TOT_DSALW_AMT")
        - F.col("svCobAdj")
    )
)

df_Missing = (
    df_TransformLogic
    .filter(
        ((F.col("CLM_HOST_IN") != 'Y') | (F.col("CLM_ECRP_IN") != 'Y'))
        & ((F.col("CLM_HOST_IN") != 'Y') | (F.col("CLM_PAYE_CD") != 'SUB') | (F.col("CLM_LN_TOT_PAYBL_AMT") != 0))
        & (F.col("svBcbsPaybl") != F.col("svCalcPaybl"))
    )
    .select(
        F.col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
        F.col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
    )
)

df_AggregatedSum = (
    df_TransformLogic
    .filter(
        ((F.col("CLM_HOST_IN") != 'Y') | (F.col("CLM_ECRP_IN") != 'Y'))
        & ((F.col("CLM_HOST_IN") != 'Y') | (F.col("CLM_PAYE_CD") != 'SUB') | (F.col("CLM_LN_TOT_PAYBL_AMT") != 0))
    )
    .select(
        F.col("NDC_SK"),
        F.col("svBcbsPaybl").alias("CLM_BCBS_PAYBL_AMT"),
        F.col("svCalcPaybl").alias("CLM_CALC_PAYBL_AMT")
    )
)

df_Transformer_20 = df_Missing

df_Miss = df_Transformer_20.select(
    F.col("CLM_BCBS_PAYBL_AMT"),
    F.col("CLM_CALC_PAYBL_AMT")
)

df_Notify_temp = df_Transformer_20.limit(1).select(
    F.lit("CROSS FOOT BALANCING IDS - EDW CLM F OUT OF TOLERANCE FOR CLM LN TOT PAYBL AMT FIELD ON FCTS MED ONLY").alias("NOTIFICATION")
)

df_Notify = (
    df_Notify_temp
    .withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
    .select("NOTIFICATION")
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

df_ResearchFile = df_Miss

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsEdwClmFFctsMedCrossFootResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Aggregator_17 = (
    df_AggregatedSum
    .groupBy("NDC_SK")
    .agg(
        F.sum("CLM_BCBS_PAYBL_AMT").alias("CLM_BCBS_PAYBL_AMT"),
        F.sum("CLM_CALC_PAYBL_AMT").alias("CLM_CALC_PAYBL_AMT")
    )
)

df_Transform = (
    df_Aggregator_17
    .withColumn("svCalcFctsMedPayblAmtSum", F.col("CLM_CALC_PAYBL_AMT"))
    .withColumn(
        "svWriteSumJobInfo1",
        WriteRunInfo(
            F.lit("CalcFctsMedPayblAmtSum"),
            F.col("svCalcFctsMedPayblAmtSum"),
            F.lit(0)
        )
    )
    .withColumn("svOrigFctsMedPayblAmtSum", F.col("CLM_BCBS_PAYBL_AMT"))
    .withColumn(
        "svWriteSumJobInfo2",
        WriteRunInfo(
            F.lit("OrigFctsMedPayblAmtSum"),
            F.col("svOrigFctsMedPayblAmtSum"),
            F.lit(0)
        )
    )
)

df_ValueNote_temp = df_Transform.select(
    F.lit("SUM VALUES FOR AMOUNT FIELDS USED BY SEQUENCER LATER").alias("SUM_NOTIFY")
)

df_ValueNote = (
    df_ValueNote_temp
    .withColumn("SUM_NOTIFY", F.rpad(F.col("SUM_NOTIFY"), 70, " "))
    .select("SUM_NOTIFY")
)

write_files(
    df_ValueNote,
    f"{adls_path}/balancing/sync/ClmFFctsMedPayblAmt.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)