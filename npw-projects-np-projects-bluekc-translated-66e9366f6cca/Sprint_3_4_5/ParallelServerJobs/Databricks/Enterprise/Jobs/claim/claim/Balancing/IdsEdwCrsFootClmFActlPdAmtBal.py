# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwCrsFootClmFActlPdAmtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/28/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/22/2007                  
# MAGIC 
# MAGIC Shanmugam A.                  03/08/2017         5321                         Updated Aliase for all columns for link        EnterpriseDev2          Jag Yelavarthi              2017-03-08
# MAGIC                                                                                                            SumAmts in SrcTrgtRowComp stage

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
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_extr = f"""
SELECT
CLM_F.CLM_ACTL_PD_AMT,
CLM_F.CLM_LN_TOT_PAYBL_TO_PROV_AMT,
CLM_F.CLM_LN_TOT_PAYBL_TO_SUB_AMT
FROM
{EDWOwner}.CLM_F CLM_F
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_CAP_CD NOT IN ('N','P') AND CLM_F.CLM_TYP_CD IN ('MED','DNTL')
AND CLM_F.CLM_CAT_CD = 'STD' AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
AND CLM_F.SRC_SYS_CD = 'FACETS' AND CLM_F.CLM_FINL_DISP_CD IN ('ACPTD','DENIEDREJ','SUSP')
"""
df_SrcTrgtRowComp_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_extr)
    .load()
)

extract_query_sumamts = f"""
SELECT
Sum(CLM_F.CLM_ACTL_PD_AMT) AS CLM_ACTL_PD_AMT_SUM,
Sum(CLM_F.CLM_LN_TOT_PAYBL_TO_PROV_AMT) AS CLM_LN_TOT_PAYBL_TO_PROV_AMT_SUM,
Sum(CLM_F.CLM_LN_TOT_PAYBL_TO_SUB_AMT) AS CLM_LN_TOT_PAYBL_TO_SUB_AMT_SUM
FROM
{EDWOwner}.CLM_F CLM_F
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_CAP_CD NOT IN ('N','P') AND CLM_F.CLM_TYP_CD IN ('MED','DNTL')
AND CLM_F.CLM_CAT_CD = 'STD' AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
AND CLM_F.SRC_SYS_CD = 'FACETS' AND CLM_F.CLM_FINL_DISP_CD IN ('ACPTD','DENIEDREJ','SUSP')
"""
df_SrcTrgtRowComp_SumAmts = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_sumamts)
    .load()
)

df_Transform = (
    df_SrcTrgtRowComp_SumAmts
    .withColumn(
        "svCalcPayblProvAmtSum",
        when(
            col("CLM_LN_TOT_PAYBL_TO_PROV_AMT_SUM").isNull()
            | (length(trim(col("CLM_LN_TOT_PAYBL_TO_PROV_AMT_SUM"))) == 0),
            lit(0),
        ).otherwise(col("CLM_LN_TOT_PAYBL_TO_PROV_AMT_SUM"))
    )
    .withColumn(
        "svCalcPayblSubAmtSum",
        when(
            col("CLM_LN_TOT_PAYBL_TO_SUB_AMT_SUM").isNull()
            | (length(trim(col("CLM_LN_TOT_PAYBL_TO_SUB_AMT_SUM"))) == 0),
            lit(0),
        ).otherwise(col("CLM_LN_TOT_PAYBL_TO_SUB_AMT_SUM"))
    )
    .withColumn("svCalcActlPdAmtSum", col("svCalcPayblProvAmtSum") + col("svCalcPayblSubAmtSum"))
    .withColumn("svWriteSumJobInfo1", WriteRunInfo("CalcActlPdAmtSum", col("svCalcActlPdAmtSum"), lit(0)))
    .withColumn(
        "svOrigActlPdAmtSum",
        when(
            col("CLM_ACTL_PD_AMT_SUM").isNull()
            | (length(trim(col("CLM_ACTL_PD_AMT_SUM"))) == 0),
            lit(0),
        ).otherwise(col("CLM_ACTL_PD_AMT_SUM"))
    )
    .withColumn("svWriteSumJobInfo2", WriteRunInfo("OrigActlPdAmtSum", col("svOrigActlPdAmtSum"), lit(0)))
)

df_Transform_ValueNote = df_Transform.select(
    rpad(lit("SUM VALUES FOR AMOUNT FIELDS USED BY SEQUENCER LATER"), 70, " ").alias("SUM_NOTIFY")
)

write_files(
    df_Transform_ValueNote,
    f"{adls_path}/balancing/sync/ClmFActlPdAmtSum.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_hf_chk_crs_ft_clm_f_Load = dedup_sort(
    df_SrcTrgtRowComp_Extr,
    ["CLM_ACTL_PD_AMT", "CLM_LN_TOT_PAYBL_TO_PROV_AMT", "CLM_LN_TOT_PAYBL_TO_SUB_AMT"],
    []
)

df_TransformLogic_Missing = (
    df_hf_chk_crs_ft_clm_f_Load.filter(
        col("CLM_ACTL_PD_AMT")
        != (col("CLM_LN_TOT_PAYBL_TO_PROV_AMT") + col("CLM_LN_TOT_PAYBL_TO_SUB_AMT"))
    )
    .select(
        col("CLM_ACTL_PD_AMT"),
        col("CLM_LN_TOT_PAYBL_TO_PROV_AMT"),
        col("CLM_LN_TOT_PAYBL_TO_SUB_AMT"),
    )
)

df_Transformer_19_Miss = df_TransformLogic_Missing.select(
    col("CLM_ACTL_PD_AMT"),
    col("CLM_LN_TOT_PAYBL_TO_PROV_AMT"),
    col("CLM_LN_TOT_PAYBL_TO_SUB_AMT"),
)

df_Transformer_19_Notify_temp = df_TransformLogic_Missing.limit(1)
df_Transformer_19_Notify = df_Transformer_19_Notify_temp.select(
    rpad(
        lit("CROSS FOOT BALANCING IDS - EDW CLM F OUT OF TOLERANCE FOR CLM ACTL PD AMT FIELD"),
        70,
        " ",
    ).alias("NOTIFICATION")
)

write_files(
    df_Transformer_19_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Transformer_19_Miss,
    f"{adls_path}/balancing/research/IdsEdwClmFPdAmtCrossFootResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)