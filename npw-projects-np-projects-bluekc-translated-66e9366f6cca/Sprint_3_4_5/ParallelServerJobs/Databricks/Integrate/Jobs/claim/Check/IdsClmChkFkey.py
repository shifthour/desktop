# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmChkFkey
# MAGIC 
# MAGIC DESCRIPTION:     Foreign Key Building for load file
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  
# MAGIC   
# MAGIC HASH FILES:     
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Massive quantities of Stage Variables with CDMA lookups for surrogate key assignement.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             2006-10-11   Hugh Sisson      Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                         Steph Goddard           07/30/2008
# MAGIC Sharon Andrew        2013-01-07     TTR-1517                Changed FK lookup for  CLM_CHK_PAYE_TYP_CD_SK                                       IntegrateNewDevl         Bhoomi Dasari            1/9/2013
# MAGIC                                                                                         from   GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_CHK_SK, "CLAIM PAYEE", Key.CLM_CHK_PAYE_TYP_CD, Logging)  
# MAGIC                                                                                        to    GetFkeySrcTrgtDomainCodes( "FACETS", Key.CLM_CHK_SK ,  Key.CLM_CHK_PAYE_TYP_CD,  "CLAIM PAYEE",   "CLAIM PAYEE", Logging)  
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi           2021-01-14     US-318408                   LUMERIS (FACETS) Codes lookup                                                              IntegrateDev2                Kalyan Neelam        2021-01-14
# MAGIC 
# MAGIC 
# MAGIC Kshema H K                   2024-07-23      US 621773      Updated Logic  for DOMINION in Stage Variables PayeTypCdSk,LobCdSk,           IntegrateDev1                Reddy Sanam         2024-07-28
# MAGIC                                                                                        PaymtMethCdSk  and ClmRemitHistPaymtovrdCdSk  in Trans1 stage

# MAGIC Read common record format file created in primary key job.
# MAGIC Writing Sequential File to /load
# MAGIC Create default rows for UNK and NA
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmChkCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_CHK_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_CHK_PAYE_TYP_CD", StringType(), False),
    StructField("CLM_CHK_LOB_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_ID_1", StringType(), False),
    StructField("CLM_CHK_PAYMT_METH_CD", StringType(), False),
    StructField("CLM_REMIT_HIST_PAYMTOVRD_CD", StringType(), False),
    StructField("EXTRNL_CHK_IN", StringType(), False),
    StructField("PCA_CHK_IN", StringType(), False),
    StructField("CHK_PD_DT", DateType(), False),
    StructField("CHK_NET_PAYMT_AMT", DecimalType(38,10), False),
    StructField("CHK_ORIG_AMT", DecimalType(38,10), False),
    StructField("CHK_NO", IntegerType(), False),
    StructField("CHK_SEQ_NO", IntegerType(), False),
    StructField("CHK_PAYE_NM", StringType(), True),
    StructField("CHK_PAYMT_REF_ID", StringType(), False)
])

df_ClmChkCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmChkCrf)
    .load(f"{adls_path}/key/{InFile}")
)

dfTrns1 = df_ClmChkCrf \
    .withColumn(
        "svSrcSysCd",
        F.when(F.col("SRC_SYS_CD") == "LUMERIS", F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD"))
    ) \
    .withColumn(
        "ClmSk",
        GetFkeyClm(Source, F.col("CLM_CHK_SK"), F.col("CLM_ID"), Logging)
    ) \
    .withColumn(
        "PayeTypCdSk",
        F.when(
            F.col("SRC_SYS_CD") == "DOMINION",
            GetFkeySrcTrgtDomainCodes("DOMINION", F.col("CLM_CHK_SK"), F.lit("CLAIM PAYEE"), F.lit("P"), F.lit("CLAIM PAYEE"), Logging)
        ).otherwise(
            GetFkeySrcTrgtDomainCodes("FACETS", F.col("CLM_CHK_SK"), F.lit("CLAIM PAYEE"), F.col("CLM_CHK_PAYE_TYP_CD"), F.lit("CLAIM PAYEE"), Logging)
        )
    ) \
    .withColumn(
        "LobCdSk",
        F.when(
            F.col("SRC_SYS_CD") == "DOMINION",
            GetFkeySrcTrgtDomainCodes("FACETS", F.col("CLM_CHK_SK"), F.lit("CLAIM LINE LOB"), F.lit("BCBS"), F.lit("LOB"), Logging)
        ).otherwise(
            GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_CHK_SK"), F.lit("CLAIM LINE LOB"), F.col("CLM_CHK_LOB_CD"), Logging)
        )
    ) \
    .withColumn(
        "PaymtMethCdSk",
        F.when(
            F.col("SRC_SYS_CD") == "DOMINION",
            F.lit(1)
        ).otherwise(
            GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_CHK_SK"), F.lit("PAYMENT METHOD"), F.col("CLM_CHK_PAYMT_METH_CD"), Logging)
        )
    ) \
    .withColumn(
        "ClmRemitHistPaymtovrdCdSk",
        F.when(
            F.col("SRC_SYS_CD") == "DOMINION",
            F.lit(1)
        ).otherwise(
            GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_CHK_SK"), F.lit("CLAIM REMITTANCE HISTORY PAYMENT OVERRIDE"), F.col("CLM_REMIT_HIST_PAYMTOVRD_CD"), Logging)
        )
    ) \
    .withColumn(
        "ChkPdDtSk",
        GetFkeyDate("IDS", F.col("CLM_CHK_SK"), F.col("CHK_PD_DT"), Logging)
    ) \
    .withColumn(
        "PassThru",
        F.col("PASS_THRU_IN")
    ) \
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("CLM_CHK_SK"))
    )

dfTrns1_ClmChkFkeyOut1 = dfTrns1.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")).select(
    F.col("CLM_CHK_SK").alias("CLM_CHK_SK"),
    F.lit(SrcSysCdSk).cast("int").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("PayeTypCdSk").alias("CLM_CHK_PAYE_TYP_CD_SK"),
    F.col("LobCdSk").alias("CLM_CHK_LOB_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("PaymtMethCdSk").alias("CLM_CHK_PAYMT_METH_CD_SK"),
    F.col("ClmRemitHistPaymtovrdCdSk").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    F.col("EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.when(F.col("SRC_SYS_CD")=="DOMINION", F.col("CHK_PD_DT")).otherwise(F.col("ChkPdDtSk")).alias("CHK_PD_DT_SK"),
    F.col("CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
    F.col("CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("CHK_NO"),
    F.col("CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
)

wDefault = Window.orderBy(F.lit(1))

dfTrns1_DefaultUNK = dfTrns1 \
    .withColumn("__row_num", F.row_number().over(wDefault)) \
    .filter(F.col("__row_num") == 1) \
    .select(
        F.lit(0).alias("CLM_CHK_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_CHK_PAYE_TYP_CD_SK"),
        F.lit(0).alias("CLM_CHK_LOB_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.lit(0).alias("CLM_CHK_PAYMT_METH_CD_SK"),
        F.lit(0).alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
        F.lit("U").alias("EXTRNL_CHK_IN"),
        F.lit("U").alias("PCA_CHK_IN"),
        F.lit("NA").alias("CHK_PD_DT_SK"),
        F.lit(0).alias("CHK_NET_PAYMT_AMT"),
        F.lit(0).alias("CHK_ORIG_AMT"),
        F.lit(0).alias("CHK_NO"),
        F.lit(0).alias("CHK_SEQ_NO"),
        F.lit("UNK").alias("CHK_PAYE_NM"),
        F.lit("UNK").alias("CHK_PAYMT_REF_ID")
    )

dfTrns1_DefaultNA = dfTrns1 \
    .withColumn("__row_num", F.row_number().over(wDefault)) \
    .filter(F.col("__row_num") == 1) \
    .select(
        F.lit(1).alias("CLM_CHK_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_CHK_PAYE_TYP_CD_SK"),
        F.lit(1).alias("CLM_CHK_LOB_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("CLM_CHK_PAYMT_METH_CD_SK"),
        F.lit(1).alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
        F.lit("X").alias("EXTRNL_CHK_IN"),
        F.lit("X").alias("PCA_CHK_IN"),
        F.lit("NA").alias("CHK_PD_DT_SK"),
        F.lit(0).alias("CHK_NET_PAYMT_AMT"),
        F.lit(0).alias("CHK_ORIG_AMT"),
        F.lit(1).alias("CHK_NO"),
        F.lit(1).alias("CHK_SEQ_NO"),
        F.lit("NA").alias("CHK_PAYE_NM"),
        F.lit("NA").alias("CHK_PAYMT_REF_ID")
    )

dfTrns1_Recycle1 = dfTrns1.filter(F.col("ErrCount") > 0).select(
    F.when(F.col("ErrCount") > 0, GetRecycleKey(F.col("CLM_CHK_SK"))).otherwise(F.lit(0)).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_CHK_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
    F.col("CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_ID_1").alias("CLM_ID_1"),
    F.col("CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
    F.col("CLM_REMIT_HIST_PAYMTOVRD_CD").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
    F.col("EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
    F.col("PCA_CHK_IN").alias("PCA_CHK_IN"),
    F.col("CHK_PD_DT").alias("CHK_PD_DT"),
    F.col("CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
    F.col("CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("CHK_NO"),
    F.col("CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
)

dfTrns1_RecycleClms = dfTrns1.filter(F.col("ErrCount") > 0).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    dfTrns1_Recycle1.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_SK",
        "CLM_ID",
        "CLM_CHK_PAYE_TYP_CD",
        "CLM_CHK_LOB_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_ID_1",
        "CLM_CHK_PAYMT_METH_CD",
        "CLM_REMIT_HIST_PAYMTOVRD_CD",
        "EXTRNL_CHK_IN",
        "PCA_CHK_IN",
        "CHK_PD_DT",
        "CHK_NET_PAYMT_AMT",
        "CHK_ORIG_AMT",
        "CHK_NO",
        "CHK_SEQ_NO",
        "CHK_PAYE_NM",
        "CHK_PAYMT_REF_ID"
    ),
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    dfTrns1_RecycleClms.select(
        "SRC_SYS_CD",
        "CLM_ID"
    ),
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

common_cols_merge = [
    "CLM_CHK_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_CHK_PAYE_TYP_CD_SK",
    "CLM_CHK_LOB_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CLM_CHK_PAYMT_METH_CD_SK",
    "CLM_REMIT_HIST_PAYMTOVRD_CD_SK",
    "EXTRNL_CHK_IN",
    "PCA_CHK_IN",
    "CHK_PD_DT_SK",
    "CHK_NET_PAYMT_AMT",
    "CHK_ORIG_AMT",
    "CHK_NO",
    "CHK_SEQ_NO",
    "CHK_PAYE_NM",
    "CHK_PAYMT_REF_ID"
]

df_merge_load = (
    dfTrns1_ClmChkFkeyOut1.select(common_cols_merge)
    .unionByName(dfTrns1_DefaultUNK.select(common_cols_merge))
    .unionByName(dfTrns1_DefaultNA.select(common_cols_merge))
)

df_merge_load_rpad = (
    df_merge_load
    .withColumn("EXTRNL_CHK_IN", F.rpad(F.col("EXTRNL_CHK_IN"), 1, " "))
    .withColumn("PCA_CHK_IN", F.rpad(F.col("PCA_CHK_IN"), 1, " "))
    .withColumn("CHK_PD_DT_SK", F.rpad(F.col("CHK_PD_DT_SK"), 10, " "))
)

final_cols = [
    "CLM_CHK_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_CHK_PAYE_TYP_CD_SK",
    "CLM_CHK_LOB_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CLM_CHK_PAYMT_METH_CD_SK",
    "CLM_REMIT_HIST_PAYMTOVRD_CD_SK",
    "EXTRNL_CHK_IN",
    "PCA_CHK_IN",
    "CHK_PD_DT_SK",
    "CHK_NET_PAYMT_AMT",
    "CHK_ORIG_AMT",
    "CHK_NO",
    "CHK_SEQ_NO",
    "CHK_PAYE_NM",
    "CHK_PAYMT_REF_ID"
]

df_final = df_merge_load_rpad.select(final_cols)

write_files(
    df_final,
    f"CLM_CHK.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)