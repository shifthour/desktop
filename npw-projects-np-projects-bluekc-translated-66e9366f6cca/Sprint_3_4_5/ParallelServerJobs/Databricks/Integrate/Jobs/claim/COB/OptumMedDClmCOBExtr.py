# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                      Date                 Project/Altiris #                                                          Change Description                                                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                       --------------------     ------------------------                                                                -----------------------------------------------------------------------                                  --------------------------------             -------------------------------   ----------------------------       
# MAGIC Ramu Avula                      2020-11-05       6264 - PBM Phase II - Government Programs          Initial Programming                                                                                                 IntegrateDev2                      Jaideep Mankala         12/10/2020
# MAGIC Geetanjali Rajendran        2020-12-18       6264 - PBM Phase II - Government Programs          GROUP BY: RXCLAIM_NO + RXCLAIM_SEQ_NO + CLM_STTUS                     IntegrateDev2     Jaideep Mankala         12/19/2020                       
# MAGIC                                                                                                                                                    Combine matching records into a single record with COPAY_AMT, 
# MAGIC                                                                                                                                                    DEDCT_AMT and MED_COINS_AMT field(s) populated according to OTHR_PAYER_PATN_RESP_QLFR
# MAGIC                                                                                                                                                    Other amount fields will be summed across matching record.

# MAGIC Read OptumRx File  Daily
# MAGIC Writing Sequential File to /key
# MAGIC GROUP BY: RXCLAIM_NO + RXCLAIM_SEQ_NO + CLM_STTUS + CLM_COB_COV_TYP_CD
# MAGIC Combining matching records into a single record
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmCobPK
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile = get_widget_value('InFile','')

schema_OptumRx = StructType([
    StructField("RXCLM_NO", StringType(), False),
    StructField("RXCLAIM_SEQ_NBR", StringType(), False),
    StructField("CLAIM_STATUS", StringType(), False),
    StructField("COB_SEQUENCE", StringType(), False),
    StructField("OTHER_PAYMENTS_COUNT", StringType(), False),
    StructField("OTHR_PAYER_COV_TYP", StringType(), False),
    StructField("OTHER_PAYER_ID_QUALIFIER", StringType(), False),
    StructField("OTHER_PAYER_ID", StringType(), False),
    StructField("OTHR_PAYER_DT", StringType(), False),
    StructField("OTHER_PAYER_AMT_PAID_COUNT", StringType(), False),
    StructField("OTHER_PAYER_AMOUNT_PAID", StringType(), False),
    StructField("OTHER_PAYER_REJECT_COUNT", StringType(), False),
    StructField("OTHER_PAYER_REJECT_CODE_1", StringType(), False),
    StructField("OTHER_PAYER_REJECT_CODE_2", StringType(), False),
    StructField("OTHER_PAYER_REJECT_CODE_3", StringType(), False),
    StructField("OPR", StringType(), False),
    StructField("OTHR_PAYER_PATN_RESP_AMT", StringType(), False),
    StructField("OTHPYRPTRSPBAMTCNT", StringType(), False),
    StructField("OTHPYRBNSTGAMTCNT", StringType(), False),
    StructField("OTHRPYRBENSTGQUAL", StringType(), False),
    StructField("OTHRPYRBENSTGAMT", StringType(), False),
    StructField("OTHR_PAYER_PATN_RESP_QLFR", StringType(), False),
    StructField("OTHR_PAYER_AMT_PD_QLFR", StringType(), False),
    StructField("OTHR_PAYER_AMT_PD_AMT", StringType(), False),
    StructField("COB_CAT", StringType(), False),
    StructField("FILLER", StringType(), False)
])

df_OptumRx = (
    spark.read.csv(
        path=f"{adls_path_raw}/landing/{InFile}",
        schema=schema_OptumRx,
        sep=",",
        quote="\"",
        header=False
    )
)

df_BusinessRulesVar = df_OptumRx.withColumn(
    "clmid",
    F.when(trim(F.col("CLAIM_STATUS")) == "X",
           trim(F.col("RXCLM_NO")).concat(trim(F.col("RXCLAIM_SEQ_NBR"))).concat(F.lit("R")))
     .otherwise(trim(F.col("RXCLM_NO")).concat(trim(F.col("RXCLAIM_SEQ_NBR"))))
)

df_BusinessRulesFilter = df_BusinessRulesVar.filter(
    (F.upper(trim(F.col("CLAIM_STATUS"))) == "P") | (F.upper(trim(F.col("CLAIM_STATUS"))) == "X")
)

df_BusinessRules = (
    df_BusinessRulesFilter
    .withColumn("CLM_ID", F.col("clmid"))
    .withColumn(
        "CLM_COB_TYP_CD",
        F.when(
            F.substring(trim(F.col("OTHR_PAYER_COV_TYP")), 1, 1) == "0",
            F.substring(trim(F.col("OTHR_PAYER_COV_TYP")), 2, 1)
        ).otherwise(trim(F.col("OTHR_PAYER_COV_TYP")))
    )
    .withColumn("CLM_COB_LIAB_TYP_CD", trim(F.col("COB_CAT")))
    .withColumn(
        "COPAY_AMT",
        F.when(
            trim(F.col("OTHR_PAYER_PATN_RESP_QLFR")) == "05",
            F.when(F.col("CLAIM_STATUS") == "X",
                   (F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()) * -1))
            .otherwise(F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()))
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "DEDCT_AMT",
        F.when(
            trim(F.col("OTHR_PAYER_PATN_RESP_QLFR")) == "06",
            F.when(F.col("CLAIM_STATUS") == "X",
                   (F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()) * -1))
            .otherwise(F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()))
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "MED_COINS_AMT",
        F.when(
            trim(F.col("OTHR_PAYER_PATN_RESP_QLFR")) == "07",
            F.when(F.col("CLAIM_STATUS") == "X",
                   (F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()) * -1))
            .otherwise(F.col("OTHR_PAYER_PATN_RESP_AMT").cast(DoubleType()))
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "PD_AMT",
        F.when(
            (trim(F.col("OTHR_PAYER_AMT_PD_AMT")) == "") | (F.col("OTHR_PAYER_AMT_PD_AMT").isNull()),
            F.lit(0.0)
        ).otherwise(
            F.when(
                F.col("CLAIM_STATUS") == "X",
                (F.col("OTHR_PAYER_AMT_PD_AMT").cast(DoubleType()) * -1)
            ).otherwise(F.col("OTHR_PAYER_AMT_PD_AMT").cast(DoubleType()))
        )
    )
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CLM_COB_LIAB_TYP_CD",
        "COPAY_AMT",
        "DEDCT_AMT",
        "MED_COINS_AMT",
        "PD_AMT"
    )
)

df_AggClm = (
    df_BusinessRules
    .groupBy("SRC_SYS_CD_SK", "CLM_ID", "CLM_COB_TYP_CD")
    .agg(
        F.max("CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD"),
        F.sum("COPAY_AMT").alias("COPAY_AMT"),
        F.sum("DEDCT_AMT").alias("DEDCT_AMT"),
        F.sum("MED_COINS_AMT").alias("MED_COINS_AMT"),
        F.sum("PD_AMT").alias("PD_AMT")
    )
    .select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CLM_COB_LIAB_TYP_CD",
        "COPAY_AMT",
        "DEDCT_AMT",
        "MED_COINS_AMT",
        "PD_AMT"
    )
)

df_SnapshotRowCount = (
    df_AggClm
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
        F.lit(0.0).alias("ALW_AMT"),
        F.col("PD_AMT").alias("PD_AMT")
    )
)

df_SnapshotAllCol = (
    df_AggClm
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_COB_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_SK"),
        F.col("CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD"),
        F.lit(0.0).alias("ALW_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("DEDCT_AMT").alias("DEDCT_AMT"),
        F.lit(0.0).alias("DSALW_AMT"),
        F.col("MED_COINS_AMT").alias("MED_COINS_AMT"),
        F.lit(0.0).alias("MNTL_HLTH_COINS_AMT"),
        F.col("PD_AMT").alias("PD_AMT"),
        F.lit(0.0).alias("SANC_AMT"),
        F.lit(0.0).alias("COB_CAR_RSN_CD_TX"),
        F.lit(" ").alias("COB_CAR_RSN_TX")
    )
)

df_SnapshotTransform = (
    df_AggClm
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD")
    )
)

df_TransformerVars = (
    df_SnapshotRowCount
    .withColumn(
        "ITSHost",
        F.when(F.substring(F.col("CLM_COB_TYP_CD"), 1, 1) == "*", F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "ITSCOBType",
        F.when(F.col("ITSHost") == "Y", F.substring(F.col("CLM_COB_TYP_CD"), 2, 1)).otherwise(F.lit("NA"))
    )
    .withColumn(
        "svClmCobTypCdSk",
        F.when(
            F.col("ITSHost") == "N",
            GetFkeyCodes("FACETS", F.lit(0), "CLAIM COB", F.col("CLM_COB_TYP_CD"), "X")
        ).otherwise(
            GetFkeyCodes("FIT", F.lit(0), "CLAIM COB", F.col("ITSCOBType"), "X")
        )
    )
)

df_B_CLM_COB = (
    df_TransformerVars
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("svClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
        F.col("ALW_AMT").alias("ALW_AMT"),
        F.col("PD_AMT").alias("PD_AMT")
    )
)

write_files(
    df_B_CLM_COB,
    f"{adls_path}/load/B_CLM_COB.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmCobPK = {
    "CurrRunCycle": RunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "CurrentDate": CurrentDate,
    "IDSOwner": IDSOwner
}

df_ClmCobPK = ClmCobPK(df_SnapshotAllCol, df_SnapshotTransform, params_ClmCobPK)

df_ClmCobPK_padded = (
    df_ClmCobPK
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_COB_LIAB_TYP_CD", rpad(F.col("CLM_COB_LIAB_TYP_CD"), 20, " "))
)

write_files(
    df_ClmCobPK_padded.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_COB_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "ALW_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "MED_COINS_AMT",
        "MNTL_HLTH_COINS_AMT",
        "PD_AMT",
        "SANC_AMT",
        "COB_CAR_RSN_CD_TX",
        "COB_CAR_RSN_TX"
    ),
    f"{adls_path}/key/OptumMedDClmCOBExtr.DrugClmCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)