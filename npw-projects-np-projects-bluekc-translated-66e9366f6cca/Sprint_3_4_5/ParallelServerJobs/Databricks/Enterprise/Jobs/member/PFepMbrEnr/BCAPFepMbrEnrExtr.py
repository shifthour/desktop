# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwPFepMbrEnrCntl
# MAGIC 
# MAGIC PROCESSING:  Extracts data from BCA file and loads it into P_FEP_MBR_ENR table.
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                  Project/Altiris #      \(9)    Change Description                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------      ------------------------      \(9)    ------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Santosh Bokka       2013-09-23        5056\(9)                    Original Programming\(9)\(9)               EnterpriseNewDevl          Kalyan Neelam          2013-10-09   
# MAGIC Santosh Bokka       2013-10-21        5056\(9)                   Added new column SSN     \(9)\(9)               EnterpriseNewDevl          SAndrew                    2013-10-23
# MAGIC Santosh Bokka       2013-12-31        5056\(9)                   Changed code to handle multiple files                 EnterpriseNewDevl          Bhoomi Dasari           1/2/2014

# MAGIC P_FEP_MBR_ENR Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunDate = get_widget_value('RunDate','2013-06-10')
InFile = get_widget_value('InFile','')

# Read from EDW (DB2Connector) - Stage: MBR
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_MBR = f"""
SELECT
MBR.MBR_SK,
MBR.SUB_ID,
MBR.MBR_BRTH_DT_SK,
MBR.MBR_SSN,
MBR.MBR_LAST_NM,
MBR.MBR_FIRST_NM,
MBR.MBR_GNDR_CD,
MBR.MBR_FEP_LOCAL_CNTR_IN
FROM
{EDWOwner}.MBR_D As MBR,
{EDWOwner}.SUB_D As SUB
WHERE
SUB.SUB_SK = MBR.SUB_SK
and MBR.MBR_FEP_LOCAL_CNTR_IN =  'Y'
"""
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_MBR)
    .load()
)

# Transformer Stage: Mbr_Info (CTransformerStage)
# Output link "fep_mbr" becomes df_fep_mbr
# The expressions: 
#   SUB_ID => fep_mbr.SUB_ID[2,10]  (1-based index in DataStage => substring in Python: str[1:1+10] = str[1:11])
#   MBR_BRTH_DT_SK => same
#   MBR_LAST_NM => same
#   MBR_FIRST_NM => same
#   MBR_GNDR_CD => same
#   MBR_SK => same
#   MBR_SSN => same
df_fep_mbr = df_MBR.select(
    F.expr("substring(SUB_ID, 2, 10)").alias("SUB_ID"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_SSN").alias("MBR_SSN")
)

# We now split df_fep_mbr into 4 separate DataFrames for the 4 output pins from Mbr_Info:
# step1_lkup, step2_lkup, step3_lkup, step4_lkup

# step1_lkup
df_step1_lkup = df_fep_mbr.select(
    F.col("SUB_ID"),
    F.col("MBR_BRTH_DT_SK"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_GNDR_CD"),
    F.col("MBR_SK"),
    F.col("MBR_SSN")
)

# step2_lkup
df_step2_lkup = df_fep_mbr.select(
    F.col("SUB_ID"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_BRTH_DT_SK"),
    F.col("MBR_SK"),
    F.col("MBR_GNDR_CD")
)

# step3_lkup
df_step3_lkup = df_fep_mbr.select(
    F.col("SUB_ID"),
    F.col("MBR_BRTH_DT_SK"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_GNDR_CD"),
    F.col("MBR_SK"),
    F.col("MBR_SSN")
)

# step4_lkup
df_step4_lkup = df_fep_mbr.select(
    F.col("SUB_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_GNDR_CD"),
    F.col("MBR_SK")
)

# Each of these was going into a CHashedFileStage (scenario A).
# We apply dedup according to the primary key columns before using them as reference lookups.

# For step1_lkup: PK are SUB_ID, MBR_BRTH_DT_SK, MBR_LAST_NM, MBR_FIRST_NM
df_step1_lkup = dedup_sort(
    df_step1_lkup,
    partition_cols=["SUB_ID","MBR_BRTH_DT_SK","MBR_LAST_NM","MBR_FIRST_NM"],
    sort_cols=[]
)

# For step2_lkup: PK are SUB_ID, MBR_LAST_NM, MBR_FIRST_NM
# plus MBR_BRTH_DT_SK also indicated
df_step2_lkup = dedup_sort(
    df_step2_lkup,
    partition_cols=["SUB_ID","MBR_LAST_NM","MBR_FIRST_NM","MBR_BRTH_DT_SK"],
    sort_cols=[]
)

# For step3_lkup: PK are SUB_ID, MBR_BRTH_DT_SK, MBR_LAST_NM
df_step3_lkup = dedup_sort(
    df_step3_lkup,
    partition_cols=["SUB_ID","MBR_BRTH_DT_SK","MBR_LAST_NM"],
    sort_cols=[]
)

# For step4_lkup: PK are SUB_ID, MBR_FIRST_NM
# (the JSON also shows MBR_LAST_NM not flagged as PrimaryKey, but let's include it? 
#  Actually it's flagged "MBR_LAST_NM" with no PrimaryKey in the JSON. We'll only use SUB_ID, MBR_FIRST_NM as indicated.)
df_step4_lkup = dedup_sort(
    df_step4_lkup,
    partition_cols=["SUB_ID","MBR_FIRST_NM"],
    sort_cols=[]
)

# Read from BCA_File (CSeqFileStage)
schema_BCA_File = StructType([
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(38,10), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("CUR_PATN_CD", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("ENR_OPT_CD", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("ST_ADDR_1", StringType(), True),
    StructField("ST_ADDR_2", StringType(), True),
    StructField("ADDR_CITY", StringType(), True),
    StructField("ADDR_ST", StringType(), True),
    StructField("ADDR_ZIP", StringType(), True),
    StructField("SUB_PHN_NO", StringType(), True),
    StructField("MBR_MO_BASE_YR", IntegerType(), True),
    StructField("PROSP_RISK_SCORE", DecimalType(38,10), True),
    StructField("ACTL_AMT_AGGD_DCG_CAT", StringType(), True),
    StructField("ACTL_LOW_AMT", DecimalType(38,10), True),
    StructField("ACTL_HI_AMT", DecimalType(38,10), True),
    StructField("AGGD_DCG_CAT_YR_2", StringType(), True),
    StructField("XPCT_LOW_AMT_YR_2", DecimalType(38,10), True),
    StructField("XPCT_HI_AMT_YR_2", DecimalType(38,10), True),
    StructField("MCARE_FLAG", StringType(), True),
    StructField("HSPC_FLAG", StringType(), True),
    StructField("DROP_DT", StringType(), True),
    StructField("EXTR_STTUS_CD", StringType(), True),
    StructField("ACTV_ANNUITANT_GRP_CD", StringType(), True),
    StructField("MCARE_COV_CD", StringType(), True),
    StructField("MCARE_STTUS_CD", StringType(), True),
    StructField("OTHR_PRI_ENR_COV", StringType(), True)
])

filePath_BCA = f"{adls_path_raw}/landing/{InFile}"
df_BCA_File = spark.read.format("csv") \
    .option("header", "false") \
    .option("sep", ",") \
    .option("quote", "\"") \
    .schema(schema_BCA_File) \
    .load(filePath_BCA)

# Trim_Data (CTransformerStage)
# The expressions are: Trim(STRIP.FIELD(...))
df_Trim_Data = df_BCA_File.select(
    trim(strip_field(F.col("ALLOCD_CRS_PLN_CD"))).alias("ALLOCD_CRS_PLN_CD"),
    trim(strip_field(F.col("ALLOCD_SHIELD_PLN_CD"))).alias("ALLOCD_SHIELD_PLN_CD"),
    trim(strip_field(F.col("CONSIS_MBR_ID"))).alias("CONSIS_MBR_ID"),
    trim(strip_field(F.col("FEP_CNTR_ID"))).alias("FEP_CNTR_ID"),
    trim(strip_field(F.col("LGCY_MBR_ID"))).alias("LGCY_MBR_ID"),
    trim(strip_field(F.col("CUR_PATN_CD"))).alias("CUR_PATN_CD"),
    trim(strip_field(F.col("RELSHP_CD"))).alias("RELSHP_CD"),
    trim(strip_field(F.col("ENR_OPT_CD"))).alias("ENR_OPT_CD"),
    trim(strip_field(F.col("LAST_NM"))).alias("LAST_NM"),
    trim(strip_field(F.col("FIRST_NM"))).alias("FIRST_NM"),
    trim(strip_field(F.col("MIDINIT"))).alias("MIDINIT"),
    trim(strip_field(F.col("GNDR_CD"))).alias("GNDR_CD"),
    trim(strip_field(F.col("DOB"))).alias("DOB"),
    trim(strip_field(F.col("ST_ADDR_1"))).alias("ST_ADDR_1"),
    trim(strip_field(F.col("ST_ADDR_2"))).alias("ST_ADDR_2"),
    trim(strip_field(F.col("ADDR_CITY"))).alias("ADDR_CITY"),
    trim(strip_field(F.col("ADDR_ST"))).alias("ADDR_ST"),
    trim(strip_field(F.col("ADDR_ZIP"))).alias("ADDR_ZIP"),
    trim(strip_field(F.col("SUB_PHN_NO"))).alias("SUB_PHN_NO"),
    trim(strip_field(F.col("MBR_MO_BASE_YR"))).alias("MBR_MO_BASE_YR"),
    trim(strip_field(F.col("PROSP_RISK_SCORE"))).alias("PROSP_RISK_SCORE"),
    trim(strip_field(F.col("ACTL_AMT_AGGD_DCG_CAT"))).alias("ACTL_AMT_AGGD_DCG_CAT"),
    trim(strip_field(F.col("ACTL_LOW_AMT"))).alias("ACTL_LOW_AMT"),
    trim(strip_field(F.col("ACTL_HI_AMT"))).alias("ACTL_HI_AMT"),
    trim(strip_field(F.col("AGGD_DCG_CAT_YR_2"))).alias("AGGD_DCG_CAT_YR_2"),
    trim(strip_field(F.col("XPCT_LOW_AMT_YR_2"))).alias("XPCT_LOW_AMT_YR_2"),
    trim(strip_field(F.col("XPCT_HI_AMT_YR_2"))).alias("XPCT_HI_AMT_YR_2"),
    trim(strip_field(F.col("MCARE_FLAG"))).alias("MCARE_FLAG"),
    trim(strip_field(F.col("HSPC_FLAG"))).alias("HSPC_FLAG"),
    trim(strip_field(F.col("DROP_DT"))).alias("DROP_DT"),
    trim(strip_field(F.col("EXTR_STTUS_CD"))).alias("EXTR_STTUS_CD"),
    trim(strip_field(F.col("ACTV_ANNUITANT_GRP_CD"))).alias("ACTV_ANNUITANT_GRP_CD"),
    trim(strip_field(F.col("MCARE_COV_CD"))).alias("MCARE_COV_CD"),
    trim(strip_field(F.col("MCARE_STTUS_CD"))).alias("MCARE_STTUS_CD"),
    trim(strip_field(F.col("OTHR_PRI_ENR_COV"))).alias("OTHR_PRI_ENR_COV")
)

# Pre_Process (CTransformerStage)
df_Pre_Process = df_Trim_Data.select(
    F.col("ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    trim(F.col("FEP_CNTR_ID"), "0", "L").alias("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD").alias("CUR_PATN_CD"),
    F.col("RELSHP_CD").alias("RELSHP_CD"),
    F.col("ENR_OPT_CD").alias("ENR_OPT_CD"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("MIDINIT").alias("MIDINIT"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("DOB").alias("DOB"),
    F.col("ST_ADDR_1").alias("ST_ADDR_1"),
    F.col("ST_ADDR_2").alias("ST_ADDR_2"),
    F.col("ADDR_CITY").alias("ADDR_CITY"),
    F.col("ADDR_ST").alias("ADDR_ST"),
    F.col("ADDR_ZIP").alias("ADDR_ZIP"),
    F.col("SUB_PHN_NO").alias("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG").alias("MCARE_FLAG"),
    F.col("HSPC_FLAG").alias("HSPC_FLAG"),
    F.col("DROP_DT").alias("DROP_DT"),
    F.col("EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD").alias("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV"),
    F.col("FEP_CNTR_ID").alias("FEP_CNTR_ID_1")
)

# Now, Step_1 (CTransformerStage) does a left join:
# Primary link: df_Pre_Process (alias bca_out)
# Lookup link: df_step1_lkup_in => we already replaced hashed file with df_step1_lkup. 
# Join on: 
#   bca_out.FEP_CNTR_ID == step1_lkup.SUB_ID
#   bca_out.DOB == step1_lkup.MBR_BRTH_DT_SK
#   bca_out.LAST_NM == step1_lkup.MBR_LAST_NM
#   bca_out.FIRST_NM == step1_lkup.MBR_FIRST_NM
#   bca_out.GNDR_CD == step1_lkup.MBR_GNDR_CD
#   bca_out.MBR_SSN == step1_lkup.MBR_SSN
df_step1_join = df_Pre_Process.alias("bca_out").join(
    df_step1_lkup.alias("lkp"),
    (
        (F.col("bca_out.FEP_CNTR_ID") == F.col("lkp.SUB_ID")) &
        (F.col("bca_out.DOB") == F.col("lkp.MBR_BRTH_DT_SK")) &
        (F.col("bca_out.LAST_NM") == F.col("lkp.MBR_LAST_NM")) &
        (F.col("bca_out.FIRST_NM") == F.col("lkp.MBR_FIRST_NM")) &
        (F.col("bca_out.GNDR_CD") == F.col("lkp.MBR_GNDR_CD")) &
        (F.col("bca_out.MBR_SSN") == F.col("lkp.MBR_SSN"))
    ),
    how="left"
)

# We now create all columns that Step_1 output might need to pass onward
df_step1_all = df_step1_join.select(
    F.col("bca_out.*"),
    F.col("lkp.MBR_SK").alias("step1_mbr_sk"),
    F.col("lkp.SUB_ID").alias("lkp_sub_id"),
    F.col("lkp.MBR_BRTH_DT_SK").alias("lkp_mbr_brth_dt_sk"),
    F.col("lkp.MBR_FIRST_NM").alias("lkp_mbr_first_nm"),
    F.col("lkp.MBR_LAST_NM").alias("lkp_mbr_last_nm"),
    F.col("lkp.MBR_GNDR_CD").alias("lkp_mbr_gndr_cd"),
    F.col("lkp.MBR_SSN").alias("lkp_mbr_ssn")
)

# match1 constraint: lkp_sub_id, lkp_mbr_brth_dt_sk, lkp_mbr_first_nm, lkp_mbr_last_nm all not null
df_match1 = df_step1_all.filter(
    (F.col("lkp_sub_id").isNotNull()) &
    (F.col("lkp_mbr_brth_dt_sk").isNotNull()) &
    (F.col("lkp_mbr_first_nm").isNotNull()) &
    (F.col("lkp_mbr_last_nm").isNotNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("step1_mbr_sk").alias("MBR_SK"),
    F.col("FEP_CNTR_ID_1")
)

# nomatch1 constraint: sub_id etc are all null
df_nomatch1 = df_step1_all.filter(
    (F.col("lkp_sub_id").isNull()) &
    (F.col("lkp_mbr_brth_dt_sk").isNull()) &
    (F.col("lkp_mbr_first_nm").isNull()) &
    (F.col("lkp_mbr_last_nm").isNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("FEP_CNTR_ID_1")
)

# Step_2 (CTransformerStage)
# Primary link: df_nomatch1
# Lookup link: df_step2_lkup
# Join keys:
#   nomatch1.FEP_CNTR_ID == step2_lkup.SUB_ID
#   nomatch1.LAST_NM == step2_lkup.MBR_LAST_NM
#   nomatch1.FIRST_NM == step2_lkup.MBR_FIRST_NM
#   nomatch1.DOB == step2_lkup.MBR_BRTH_DT_SK
df_step2_join = df_nomatch1.alias("nomatch1").join(
    df_step2_lkup.alias("lkp"),
    (
        (F.col("nomatch1.FEP_CNTR_ID") == F.col("lkp.SUB_ID")) &
        (F.col("nomatch1.LAST_NM") == F.col("lkp.MBR_LAST_NM")) &
        (F.col("nomatch1.FIRST_NM") == F.col("lkp.MBR_FIRST_NM")) &
        (F.col("nomatch1.DOB") == F.col("lkp.MBR_BRTH_DT_SK"))
    ),
    how="left"
)

df_step2_all = df_step2_join.select(
    F.col("nomatch1.*"),
    F.col("lkp.MBR_SK").alias("step2_mbr_sk"),
    F.col("lkp.SUB_ID").alias("lkp_sub_id"),
    F.col("lkp.MBR_LAST_NM").alias("lkp_mbr_last_nm"),
    F.col("lkp.MBR_FIRST_NM").alias("lkp_mbr_first_nm"),
    F.col("lkp.MBR_BRTH_DT_SK").alias("lkp_mbr_brth_dt_sk")
)

# match2
df_match2 = df_step2_all.filter(
    (F.col("lkp_sub_id").isNotNull()) &
    (F.col("lkp_mbr_first_nm").isNotNull()) &
    (F.col("lkp_mbr_last_nm").isNotNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("step2_mbr_sk").alias("MBR_SK"),
    F.col("FEP_CNTR_ID_1")
)

# nomatch2
df_nomatch2 = df_step2_all.filter(
    (F.col("lkp_sub_id").isNull()) &
    (F.col("lkp_mbr_first_nm").isNull()) &
    (F.col("lkp_mbr_last_nm").isNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("FEP_CNTR_ID_1")
)

# Step_3
df_step3_join = df_nomatch2.alias("nomatch2").join(
    df_step3_lkup.alias("lkp"),
    (
        (F.col("nomatch2.FEP_CNTR_ID") == F.col("lkp.SUB_ID")) &
        (F.col("nomatch2.DOB") == F.col("lkp.MBR_BRTH_DT_SK")) &
        (F.col("nomatch2.LAST_NM") == F.col("lkp.MBR_LAST_NM")) &
        (F.col("nomatch2.GNDR_CD") == F.col("lkp.MBR_GNDR_CD")) &
        (F.col("nomatch2.MBR_SSN") == F.col("lkp.MBR_SSN"))
    ),
    how="left"
)

df_step3_all = df_step3_join.select(
    F.col("nomatch2.*"),
    F.col("lkp.MBR_SK").alias("step3_mbr_sk"),
    F.col("lkp.SUB_ID").alias("lkp_sub_id"),
    F.col("lkp.MBR_BRTH_DT_SK").alias("lkp_mbr_brth_dt_sk"),
    F.col("lkp.MBR_LAST_NM").alias("lkp_mbr_last_nm"),
    F.col("lkp.MBR_GNDR_CD").alias("lkp_mbr_gndr_cd"),
    F.col("lkp.MBR_SSN").alias("lkp_mbr_ssn")
)

# match3
df_match3 = df_step3_all.filter(
    (F.col("lkp_sub_id").isNotNull()) &
    (F.col("lkp_mbr_brth_dt_sk").isNotNull()) &
    (F.col("lkp_mbr_last_nm").isNotNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("step3_mbr_sk").alias("MBR_SK"),
    F.col("FEP_CNTR_ID_1")
)

# nomatch3
df_nomatch3 = df_step3_all.filter(
    (F.col("lkp_sub_id").isNull()) &
    (F.col("lkp_mbr_brth_dt_sk").isNull()) &
    (F.col("lkp_mbr_last_nm").isNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("FEP_CNTR_ID_1")
)

# Step_4
df_step4_join = df_nomatch3.alias("nomatch3").join(
    df_step4_lkup.alias("lkp"),
    (
        (F.col("nomatch3.FEP_CNTR_ID") == F.col("lkp.SUB_ID")) &
        (F.col("nomatch3.FIRST_NM") == F.col("lkp.MBR_FIRST_NM")) &
        (F.col("nomatch3.LAST_NM") == F.col("lkp.MBR_LAST_NM")) &
        (F.col("nomatch3.GNDR_CD") == F.col("lkp.MBR_GNDR_CD"))
    ),
    how="left"
)

df_step4_all = df_step4_join.select(
    F.col("nomatch3.*"),
    F.col("lkp.MBR_SK").alias("step4_mbr_sk"),
    F.col("lkp.SUB_ID").alias("lkp_sub_id"),
    F.col("lkp.MBR_FIRST_NM").alias("lkp_mbr_first_nm"),
    F.col("lkp.MBR_LAST_NM").alias("lkp_mbr_last_nm"),
    F.col("lkp.MBR_GNDR_CD").alias("lkp_mbr_gndr_cd")
)

# match4
df_match4 = df_step4_all.filter(
    (F.col("lkp_sub_id").isNotNull()) &
    (F.col("lkp_mbr_first_nm").isNotNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("step4_mbr_sk").alias("MBR_SK"),
    F.col("FEP_CNTR_ID_1")
)

# NoMatch
df_NoMatch = df_step4_all.filter(
    (F.col("lkp_sub_id").isNull()) &
    (F.col("lkp_mbr_first_nm").isNull())
).select(
    F.col("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD"),
    F.col("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID"),
    F.col("CUR_PATN_CD"),
    F.col("RELSHP_CD"),
    F.col("ENR_OPT_CD"),
    F.col("LAST_NM"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("GNDR_CD"),
    F.col("DOB"),
    F.col("ST_ADDR_1"),
    F.col("ST_ADDR_2"),
    F.col("ADDR_CITY"),
    F.col("ADDR_ST"),
    F.col("ADDR_ZIP"),
    F.col("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE"),
    F.col("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT"),
    F.col("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2"),
    F.col("MCARE_FLAG"),
    F.col("HSPC_FLAG"),
    F.col("DROP_DT"),
    F.col("EXTR_STTUS_CD"),
    F.col("ACTV_ANNUITANT_GRP_CD"),
    F.col("MCARE_COV_CD"),
    F.col("MCARE_STTUS_CD"),
    F.col("OTHR_PRI_ENR_COV"),
    F.col("MBR_SK"),
    F.col("FEP_CNTR_ID_1")
)

# Write PFepMbrEnr_NoMatch (CSeqFileStage) - external
df_NoMatch_final = df_NoMatch.select(
    rpad(F.col("ALLOCD_CRS_PLN_CD"), 15, " ").alias("ALLOCD_CRS_PLN_CD"),
    rpad(F.col("ALLOCD_SHIELD_PLN_CD"), 0, " ").alias("ALLOCD_SHIELD_PLN_CD"),  # no length in JSON => keep rpad(...,0, ) as default
    F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    rpad(F.col("FEP_CNTR_ID"), 25, " ").alias("FEP_CNTR_ID"),
    rpad(F.col("LGCY_MBR_ID"), 45, " ").alias("LGCY_MBR_ID"),
    rpad(F.col("CUR_PATN_CD"), 3, " ").alias("CUR_PATN_CD"),
    rpad(F.col("RELSHP_CD"), 2, " ").alias("RELSHP_CD"),
    rpad(F.col("ENR_OPT_CD"), 15, " ").alias("ENR_OPT_CD"),
    rpad(F.col("LAST_NM"), 30, " ").alias("LAST_NM"),
    rpad(F.col("FIRST_NM"), 30, " ").alias("FIRST_NM"),
    rpad(F.col("MIDINIT"), 1, " ").alias("MIDINIT"),
    rpad(F.col("GNDR_CD"), 1, " ").alias("GNDR_CD"),
    rpad(F.col("DOB"), 10, " ").alias("DOB"),
    rpad(F.col("ST_ADDR_1"), 30, " ").alias("ST_ADDR_1"),
    rpad(F.col("ST_ADDR_2"), 30, " ").alias("ST_ADDR_2"),
    rpad(F.col("ADDR_CITY"), 30, " ").alias("ADDR_CITY"),
    rpad(F.col("ADDR_ST"), 2, " ").alias("ADDR_ST"),
    rpad(F.col("ADDR_ZIP"), 9, " ").alias("ADDR_ZIP"),
    rpad(F.col("SUB_PHN_NO"), 10, " ").alias("SUB_PHN_NO"),
    F.col("MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    F.col("PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    rpad(F.col("ACTL_AMT_AGGD_DCG_CAT"), 40, " ").alias("ACTL_AMT_AGGD_DCG_CAT"),
    F.col("ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    F.col("ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    rpad(F.col("AGGD_DCG_CAT_YR_2"), 40, " ").alias("AGGD_DCG_CAT_YR_2"),
    F.col("XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    F.col("XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    rpad(F.col("MCARE_FLAG"), 1, " ").alias("MCARE_FLAG"),
    rpad(F.col("HSPC_FLAG"), 1, " ").alias("HSPC_FLAG"),
    rpad(F.col("DROP_DT"), 10, " ").alias("DROP_DT"),
    rpad(F.col("EXTR_STTUS_CD"), 2, " ").alias("EXTR_STTUS_CD"),
    rpad(F.col("ACTV_ANNUITANT_GRP_CD"), 15, " ").alias("ACTV_ANNUITANT_GRP_CD"),
    rpad(F.col("MCARE_COV_CD"), 3, " ").alias("MCARE_COV_CD"),
    rpad(F.col("MCARE_STTUS_CD"), 2, " ").alias("MCARE_STTUS_CD"),
    rpad(F.col("OTHR_PRI_ENR_COV"), 3, " ").alias("OTHR_PRI_ENR_COV"),
    F.col("MBR_SK").alias("MBR_SK"),
    rpad(F.col("FEP_CNTR_ID_1"), 25, " ").alias("FEP_CNTR_ID_1")
)

write_files(
    df_NoMatch_final,
    f"{adls_path_publish}/external/PFepMbrEnr_NoMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Link_Collector (CCollector)
# Union of match1, match2, match3, match4 => single df
df_link_collector = (
    df_match1.unionByName(df_match2)
    .unionByName(df_match3)
    .unionByName(df_match4)
)

# RULES (CTransformerStage)
# Input: df_link_collector => we produce "dupe_in"
df_rules = df_link_collector.select(
    F.col("LGCY_MBR_ID").alias("FEP_MBR_ID"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.when(F.col("MCARE_COV_CD").isNull(), F.lit("NA")).otherwise(F.col("MCARE_COV_CD")).alias("MCARE_COV_TX"),
    F.when(F.col("MCARE_STTUS_CD").isNull(), F.lit("NA")).otherwise(F.col("MCARE_STTUS_CD")).alias("MCARE_STTUS_DESC"),
    F.col("FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    F.col("FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("LAST_NM").alias("MBR_LAST_NM"),
    F.col("DOB").alias("MBR_DOB"),
    F.col("GNDR_CD").alias("MBR_GNDR_CD"),
    F.lit(0).alias("MBR_SSN"),   # "WhereExpression": "0" => direct 0
    F.expr("right(LGCY_MBR_ID, 9)").alias("LGCY_FEP_MBR_ID"),
    F.lit(RunDate).alias("DT_LAST_UPDT")
)

# Next: hf_p_fep_mbr_enr_dupes => scenario A => dedup on PK=FEP_MBR_ID
df_rules_dedup = dedup_sort(
    df_rules,
    partition_cols=["FEP_MBR_ID"],
    sort_cols=[]
)

# Then P_FEP_MBR_ENR (CSeqFileStage)
# Create final output with rpad if needed
df_P_FEP_MBR_ENR = df_rules_dedup.select(
    rpad(F.col("FEP_MBR_ID"), 0, " ").alias("FEP_MBR_ID"),   # not given length => 0
    F.col("MBR_SK").cast("int").alias("MBR_SK"),
    rpad(F.col("MCARE_COV_TX"), 0, " ").alias("MCARE_COV_TX"),  # no length => 0
    rpad(F.col("MCARE_STTUS_DESC"), 0, " ").alias("MCARE_STTUS_DESC"),
    F.col("FEP_CNTR_ID").cast("int").alias("FEP_CNTR_ID"),
    rpad(F.col("MBR_FIRST_NM"), 0, " ").alias("MBR_FIRST_NM"),
    rpad(F.col("MBR_LAST_NM"), 0, " ").alias("MBR_LAST_NM"),
    rpad(F.col("MBR_DOB"), 10, " ").alias("MBR_DOB"),
    rpad(F.col("MBR_GNDR_CD"), 0, " ").alias("MBR_GNDR_CD"),
    F.col("MBR_SSN").cast("int").alias("MBR_SSN"),
    F.col("LGCY_FEP_MBR_ID").cast("decimal(38,10)").alias("LGCY_FEP_MBR_ID"), 
    rpad(F.col("DT_LAST_UPDT"), 10, " ").alias("DT_LAST_UPDT")
)

write_files(
    df_P_FEP_MBR_ENR,
    f"{adls_path}/load/P_FEP_MBR_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)