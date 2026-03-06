# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwCapFundExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids based upon BeginCycle for loading into edw.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  CAP_FUND
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd and cd_nm from cd_sk
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Ralph Tucker - 08/02/2005 -   Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC Modifications:                               
# MAGIC                                                       Project/                                                                                                                                                                                         Code                  Date
# MAGIC Developer               Date              Altiris #                                     Change Description                                                                                                                      Project                     Reviewer            Reviewed
# MAGIC -------------------------     -------------------    -------------                                   --------------------------------------------------------------------------------------------------------------------------------------------  ---------------------------         -------------------------  -------------------
# MAGIC Ralph Tucker       2011-04-13      TTR-1058                               Bring up to current standards                                                                                                  IntegrateCurDevl           SAndrew           2011-04-18
# MAGIC Leandrew Moore  2013-07-26      5114                                       Rewrite in parallel                                                                                                                   EnterpriseWrhsDevl       Bhoomi Dasari   9/6/2013
# MAGIC 
# MAGIC Nagesh Bandi      2018-07-30      5832: SF Reporting                Added  CAP_FUND_SPIRA_ID  colums for Spira Care project 
# MAGIC                                                                                                    from  xfrm_BusinessLogic stage                                                                                              EnterpriseDev2             Kalyan Neelam    2018-07-30
# MAGIC 
# MAGIC 
# MAGIC Sharon Andrew     2021-02-02     MA Insourcing                         Changed main extract where clause from using one run cycle to run cycles specific to the IDS Source system - just like EDW Capitation  Kalyan Neelam   2021-02-25
# MAGIC WHERE
# MAGIC ((CD.TRGT_CD = 'FACETS' and CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= #IDSRunCycle#) Or (CD.TRGT_CD = 'BCBSKC' and CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= #IDSBCBSKCRunCycle#) Or (CD.TRGT_CD = 'BCBSA' and CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= #IDSBCBSARunCycle#) 
# MAGIC Or (CD.TRGT_CD = 'LUMERIS' and CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= #IDSLumerisRunCycle#))

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK   refSrcSys  refRateCd 
# MAGIC refAcctgCatCd  refPrortRuleCd  refPymtMethCd
# MAGIC Write CAP_FUND_D Data into a Sequential file for Load Job IdsEdwCapFundDLoad.
# MAGIC Read all the Data from IDS CAP_FUND Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCapFundDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, when, upper, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter widgets
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSLumerisRunCycle = get_widget_value('IDSLumerisRunCycle','')

# Read from db2_CAP_FUND_in (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CAP_FUND_in = (
    f"SELECT CAP.CAP_FUND_SK, CAP.SRC_SYS_CD_SK, CAP.CAP_FUND_ID, CAP.CRT_RUN_CYC_EXCTN_SK, "
    f"CAP.CAP_FUND_ACCTG_CAT_CD_SK, CAP.CAP_FUND_PAYMT_METH_CD_SK, CAP.CAP_FUND_PRORT_RULE_CD_SK, "
    f"CAP.CAP_FUND_RATE_CD_SK, CAP.GRP_CAP_MOD_APLD_IN, CAP.MAX_AMT, CAP.MIN_AMT, CAP.FUND_DESC "
    f"FROM {IDSOwner}.CAP_FUND CAP, {IDSOwner}.CD_MPPNG CD "
    f"WHERE CAP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"AND ("
    f"(CD.TRGT_CD = 'FACETS' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}) "
    f"OR "
    f"(CD.TRGT_CD = 'LUMERIS' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSLumerisRunCycle})"
    f")"
)
df_db2_CAP_FUND_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CAP_FUND_in)
    .load()
)

# Read from db2_CD_MPPNG_in (DB2ConnectorPX)
extract_query_db2_CD_MPPNG_in = (
    f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') AS TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# cpy_cd_mppng (PxCopy)
df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_CAP_FUND_in.alias("lnk_IdsEdwCapFundDExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("lnk_refAcctgCatCd"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_ACCTG_CAT_CD_SK") == col("lnk_refAcctgCatCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_refRateCd"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_RATE_CD_SK") == col("lnk_refRateCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_refSrcSys"),
        col("lnk_IdsEdwCapFundDExtr_InABC.SRC_SYS_CD_SK") == col("lnk_refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_refPrortRuleCd"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_PRORT_RULE_CD_SK") == col("lnk_refPrortRuleCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_refPymtMethCd"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_PAYMT_METH_CD_SK") == col("lnk_refPymtMethCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_SK").alias("CAP_FUND_SK"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_ID").alias("CAP_FUND_ID"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_refAcctgCatCd.TRGT_CD").alias("ACCTG_CD"),
        col("lnk_refAcctgCatCd.TRGT_CD_NM").alias("ACCTG_CD_NM"),
        col("lnk_refRateCd.TRGT_CD").alias("RATE_CD"),
        col("lnk_refRateCd.TRGT_CD_NM").alias("RATE_CD_NM"),
        col("lnk_refSrcSys.TRGT_CD").alias("SRC_SYS_CD"),
        col("lnk_refPrortRuleCd.TRGT_CD").alias("RULE_CD"),
        col("lnk_refPrortRuleCd.TRGT_CD_NM").alias("RULE_CD_NM"),
        col("lnk_refPymtMethCd.TRGT_CD").alias("PAY_CD"),
        col("lnk_refPymtMethCd.TRGT_CD_NM").alias("PAY_CD_NM"),
        col("lnk_IdsEdwCapFundDExtr_InABC.GRP_CAP_MOD_APLD_IN").alias("GRP_CAP_MOD_APLD_IN"),
        col("lnk_IdsEdwCapFundDExtr_InABC.MAX_AMT").alias("MAX_AMT"),
        col("lnk_IdsEdwCapFundDExtr_InABC.MIN_AMT").alias("MIN_AMT"),
        col("lnk_IdsEdwCapFundDExtr_InABC.FUND_DESC").alias("FUND_DESC"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_PAYMT_METH_CD_SK").alias("CAP_FUND_PAYMT_METH_CD_SK"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_PRORT_RULE_CD_SK").alias("CAP_FUND_PRORT_RULE_CD_SK"),
        col("lnk_IdsEdwCapFundDExtr_InABC.CAP_FUND_RATE_CD_SK").alias("CAP_FUND_RATE_CD_SK")
    )
)

# xfrm_BusinessLogic (CTransformerStage)
# First, we will attach a global row number to handle the constraints for lnk_Na and lnk_Unk
windowSpec = Window.orderBy(lit(1))
df_lkp_Codes_with_rn = df_lkp_Codes.withColumn("global_rn", row_number().over(windowSpec))

# lnk_Detail
df_lnk_Detail = df_lkp_Codes_with_rn.filter(
    (col("CAP_FUND_SK") != 0) & (col("CAP_FUND_SK") != 1)
).select(
    col("CAP_FUND_SK"),
    when(col("SRC_SYS_CD").isNull(), lit(" ")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("CAP_FUND_ID").alias("CAP_FUND_ID"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    when(col("ACCTG_CD").isNull(), lit(" ")).otherwise(col("ACCTG_CD")).alias("CAP_FUND_ACCTG_CAT_CD"),
    when(col("ACCTG_CD_NM").isNull(), lit(" ")).otherwise(col("ACCTG_CD_NM")).alias("CAP_FUND_ACCTG_CAT_NM"),
    col("FUND_DESC").alias("CAP_FUND_DESC"),
    col("GRP_CAP_MOD_APLD_IN").alias("CAP_FUND_GRP_CAP_MOD_APLD_IN"),  # char(1)
    col("MAX_AMT").alias("CAP_FUND_MAX_AMT"),
    col("MIN_AMT").alias("CAP_FUND_MIN_AMT"),
    when(col("PAY_CD").isNull(), lit(" ")).otherwise(col("PAY_CD")).alias("CAP_FUND_PAYMT_METH_CD"),
    when(col("PAY_CD_NM").isNull(), lit(" ")).otherwise(col("PAY_CD_NM")).alias("CAP_FUND_PAYMT_METH_NM"),
    when(col("RULE_CD").isNull(), lit(" ")).otherwise(col("RULE_CD")).alias("CAP_FUND_PRORT_RULE_CD"),
    when(col("RULE_CD_NM").isNull(), lit(" ")).otherwise(col("RULE_CD_NM")).alias("CAP_FUND_PRORT_RULE_NM"),
    when(col("RATE_CD").isNull(), lit(" ")).otherwise(col("RATE_CD")).alias("CAP_FUND_RATE_CD"),
    when(col("RATE_CD_NM").isNull(), lit(" ")).otherwise(col("RATE_CD_NM")).alias("CAP_FUND_RATE_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
    col("CAP_FUND_PAYMT_METH_CD_SK").alias("CAP_FUND_PAYMT_METH_CD_SK"),
    col("CAP_FUND_PRORT_RULE_CD_SK").alias("CAP_FUND_PRORT_RULE_CD_SK"),
    col("CAP_FUND_RATE_CD_SK").alias("CAP_FUND_RATE_CD_SK"),
    when( 
        upper(trim(col("FUND_DESC"))).eqNullSafe(lit("NA")), lit("NA")
    ).when(
        upper(trim(col("FUND_DESC"))).contains("SPIRA"), lit("CARECNTR")
    ).otherwise(lit("NA")).alias("CAP_FUND_SPIRA_ID")
)

# lnk_Na (constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)
df_lnk_Na = df_lkp_Codes_with_rn.filter(col("global_rn") == 1).select(
    lit(1).alias("CAP_FUND_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("CAP_FUND_ID"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    lit("NA").alias("CAP_FUND_ACCTG_CAT_CD"),
    lit("NA").alias("CAP_FUND_ACCTG_CAT_NM"),
    lit("NA").alias("CAP_FUND_DESC"),
    lit("N").alias("CAP_FUND_GRP_CAP_MOD_APLD_IN"),  # char(1)
    lit(0).alias("CAP_FUND_MAX_AMT"),
    lit(0).alias("CAP_FUND_MIN_AMT"),
    lit("NA").alias("CAP_FUND_PAYMT_METH_CD"),
    lit("NA").alias("CAP_FUND_PAYMT_METH_NM"),
    lit("NA").alias("CAP_FUND_PRORT_RULE_CD"),
    lit("NA").alias("CAP_FUND_PRORT_RULE_NM"),
    lit("NA").alias("CAP_FUND_RATE_CD"),
    lit("NA").alias("CAP_FUND_RATE_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CAP_FUND_ACCTG_CAT_CD_SK"),
    lit(1).alias("CAP_FUND_PAYMT_METH_CD_SK"),
    lit(1).alias("CAP_FUND_PRORT_RULE_CD_SK"),
    lit(1).alias("CAP_FUND_RATE_CD_SK"),
    lit("NA").alias("CAP_FUND_SPIRA_ID")
)

# lnk_Unk (constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)
df_lnk_Unk = df_lkp_Codes_with_rn.filter(col("global_rn") == 1).select(
    lit(0).alias("CAP_FUND_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("CAP_FUND_ID"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    lit("UNK").alias("CAP_FUND_ACCTG_CAT_CD"),
    lit("UNK").alias("CAP_FUND_ACCTG_CAT_NM"),
    lit("UNK").alias("CAP_FUND_DESC"),
    lit("N").alias("CAP_FUND_GRP_CAP_MOD_APLD_IN"),  # char(1)
    lit(0).alias("CAP_FUND_MAX_AMT"),
    lit(0).alias("CAP_FUND_MIN_AMT"),
    lit("UNK").alias("CAP_FUND_PAYMT_METH_CD"),
    lit("UNK").alias("CAP_FUND_PAYMT_METH_NM"),
    lit("UNK").alias("CAP_FUND_PRORT_RULE_CD"),
    lit("UNK").alias("CAP_FUND_PRORT_RULE_NM"),
    lit("UNK").alias("CAP_FUND_RATE_CD"),
    lit("UNK").alias("CAP_FUND_RATE_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CAP_FUND_ACCTG_CAT_CD_SK"),
    lit(0).alias("CAP_FUND_PAYMT_METH_CD_SK"),
    lit(0).alias("CAP_FUND_PRORT_RULE_CD_SK"),
    lit(0).alias("CAP_FUND_RATE_CD_SK"),
    lit("UNK").alias("CAP_FUND_SPIRA_ID")
)

# Fnl_Cap_Fund_D (PxFunnel)
common_cols = [
    "CAP_FUND_SK",
    "SRC_SYS_CD",
    "CAP_FUND_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CAP_FUND_ACCTG_CAT_CD",
    "CAP_FUND_ACCTG_CAT_NM",
    "CAP_FUND_DESC",
    "CAP_FUND_GRP_CAP_MOD_APLD_IN",
    "CAP_FUND_MAX_AMT",
    "CAP_FUND_MIN_AMT",
    "CAP_FUND_PAYMT_METH_CD",
    "CAP_FUND_PAYMT_METH_NM",
    "CAP_FUND_PRORT_RULE_CD",
    "CAP_FUND_PRORT_RULE_NM",
    "CAP_FUND_RATE_CD",
    "CAP_FUND_RATE_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CAP_FUND_ACCTG_CAT_CD_SK",
    "CAP_FUND_PAYMT_METH_CD_SK",
    "CAP_FUND_PRORT_RULE_CD_SK",
    "CAP_FUND_RATE_CD_SK",
    "CAP_FUND_SPIRA_ID"
]

df_Fnl_Cap_Fund_D = (
    df_lnk_Detail.select(common_cols)
    .unionByName(df_lnk_Na.select(common_cols))
    .unionByName(df_lnk_Unk.select(common_cols))
)

# Apply rpad to char columns in the final DataFrame
df_Fnl_Cap_Fund_D = (
    df_Fnl_Cap_Fund_D
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CAP_FUND_GRP_CAP_MOD_APLD_IN", rpad(col("CAP_FUND_GRP_CAP_MOD_APLD_IN"), 1, " "))
)

# eq_CAP_FUND_D_load_csv (PxSequentialFile)
write_files(
    df_Fnl_Cap_Fund_D,
    f"{adls_path}/load/CAP_FUND_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)