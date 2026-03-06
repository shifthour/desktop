# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from excel spreadsheet that has been saved as .csv file
# MAGIC   Creates rows if there is something in the exclusion field 
# MAGIC   Since this is not in IDS, primary key is assigned here.  
# MAGIC   Table is recreated each time
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  GRP
# MAGIC                 EDW:  GRP_MAIL_EXCL_D
# MAGIC                        
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard   08/16/2005   Originally programmed
# MAGIC              Suzanne Saylor  04/12/2006 - Added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC                                                           Renamed from EdwGrpMailExclExtr to IdsGrpMailExclExtr.  
# MAGIC                                                          Changed hf_group_ref to hf_grpmail_extr_group_ref
# MAGIC 
# MAGIC 
# MAGIC                                                                                                                                                                                                                                            DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                          -------------------------------    ------------------------------     ---------------------------
# MAGIC Bhupinder Kaur            07/19/2013      5114                               Create Load File for EDW Table GRP_MAIL_EXCL_D                                          EnterpriseWhseDevl
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela        01/22/2014        5114                             Changed the lookup column for GRP_ID and added a reject link                             EnterpriseWhseDevl      Jag Yelavarthi               2014-01-27

# MAGIC Group Mailing Exclusions from lan file into EDW.
# MAGIC Null handling ,Business logic and transformations
# MAGIC Trim the length of GRP_NBR
# MAGIC Read from IDS source table:
# MAGIC GRP
# MAGIC Job: IdsEdwGrpMailExclDExtr
# MAGIC Add Defaults and Null Handling and record split.
# MAGIC If a GRP_ID from incoming CSV file is not present in GRP_D table, then that record will be rejected into a reject file. A notification email will be triggered if there are any rows in Reject file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query = f"SELECT GRP_SK, GRP_ID FROM {IDSOwner}.GRP"
df_db2_GRP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

schema_seq_GRP_MAIL_EXCL_DAT = StructType([
    StructField("GroupName", StringType(), False),
    StructField("Rep", StringType(), True),
    StructField("GroupNumber", StringType(), True),
    StructField("Funding", StringType(), True),
    StructField("ProductType", StringType(), True),
    StructField("WellAwareExclusion", StringType(), True),
    StructField("HWComExclusion", StringType(), True),
    StructField("SurveysExclusion", StringType(), True),
    StructField("AppEventLettersExclusion", StringType(), True),
    StructField("COBLettersforASO", StringType(), True),
    StructField("MarketingLettersExclusion", StringType(), True),
    StructField("ChoiceCoverageExclusion", StringType(), True)
])
df_seq_GRP_MAIL_EXCL_DAT = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_seq_GRP_MAIL_EXCL_DAT)
    .load(f"{adls_path_raw}/landing/grp_mail_excl.dat")
)

df_xfrm_Trim = df_seq_GRP_MAIL_EXCL_DAT.select(
    col("GroupName").alias("GroupName"),
    col("Rep").alias("Rep"),
    trim(col("GroupNumber")).alias("GroupNumber"),
    col("Funding").alias("Funding"),
    col("ProductType").alias("ProductType"),
    col("WellAwareExclusion").alias("WellAwareExclusion"),
    col("HWComExclusion").alias("HWComExclusion"),
    col("SurveysExclusion").alias("SurveysExclusion"),
    col("AppEventLettersExclusion").alias("AppEventLettersExclusion"),
    col("COBLettersforASO").alias("COBLettersforASO"),
    col("MarketingLettersExclusion").alias("MarketingLettersExclusion"),
    col("ChoiceCoverageExclusion").alias("ChoiceCoverageExclusion")
)

df_lkp_Codes_joined = (
    df_xfrm_Trim.alias("lnk_xfrm_lkp")
    .join(
        df_db2_GRP_Extr.alias("Ref_GrpId"),
        col("lnk_xfrm_lkp.GroupNumber") == col("Ref_GrpId.GRP_ID"),
        "left"
    )
)
df_lkp_Codes_out = df_lkp_Codes_joined.select(
    col("lnk_xfrm_lkp.WellAwareExclusion").alias("WellAwareExclusion"),
    col("lnk_xfrm_lkp.HWComExclusion").alias("HWComExclusion"),
    col("lnk_xfrm_lkp.SurveysExclusion").alias("SurveysExclusion"),
    col("lnk_xfrm_lkp.AppEventLettersExclusion").alias("AppEventLettersExclusion"),
    col("lnk_xfrm_lkp.COBLettersforASO").alias("COBLettersforASO"),
    col("lnk_xfrm_lkp.MarketingLettersExclusion").alias("MarketingLettersExclusion"),
    col("lnk_xfrm_lkp.ChoiceCoverageExclusion").alias("ChoiceCoverageExclusion"),
    col("lnk_xfrm_lkp.GroupNumber").alias("GRP_ID"),
    col("Ref_GrpId.GRP_SK").alias("GRP_SK")
)

df_lkp_Codes_rej = df_lkp_Codes_joined.filter(
    col("Ref_GrpId.GRP_ID").isNull()
).select(
    "lnk_xfrm_lkp.GroupName",
    "lnk_xfrm_lkp.Rep",
    "lnk_xfrm_lkp.GroupNumber",
    "lnk_xfrm_lkp.Funding",
    "lnk_xfrm_lkp.ProductType",
    "lnk_xfrm_lkp.WellAwareExclusion",
    "lnk_xfrm_lkp.HWComExclusion",
    "lnk_xfrm_lkp.SurveysExclusion",
    "lnk_xfrm_lkp.AppEventLettersExclusion",
    "lnk_xfrm_lkp.COBLettersforASO",
    "lnk_xfrm_lkp.MarketingLettersExclusion",
    "lnk_xfrm_lkp.ChoiceCoverageExclusion"
)

write_files(
    df_lkp_Codes_rej,
    f"{adls_path}/load/GRP_MAIL_EXCL_D_Lkp_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_xfrm_BusinessLogic_Surveys = df_lkp_Codes_out.filter(
    col("SurveysExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("SRVY").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("SurveysExclusion").alias("EXCL_DESC"),
    lit("SURVEY").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_WellAware = df_lkp_Codes_out.filter(
    col("WellAwareExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("WELLAWARE").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("WellAwareExclusion").alias("EXCL_DESC"),
    lit("WELL AWARE").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_ChoiceCov = df_lkp_Codes_out.filter(
    col("ChoiceCoverageExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("CHOICECOV").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("ChoiceCoverageExclusion").alias("EXCL_DESC"),
    lit("CHOICE COVERAGE").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_MarkLetters = df_lkp_Codes_out.filter(
    col("MarketingLettersExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("MKTNGLTR").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("MarketingLettersExclusion").alias("EXCL_DESC"),
    lit("MARKETING LETTER").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_COBLetters = df_lkp_Codes_out.filter(
    col("COBLettersforASO").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("COBLTRASO").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("COBLettersforASO").alias("EXCL_DESC"),
    lit("COB LETTER FOR ASO GROUPS ONLY").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_ApproachEvent = df_lkp_Codes_out.filter(
    col("AppEventLettersExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("APPRCHINGEVTL").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("AppEventLettersExclusion").alias("EXCL_DESC"),
    lit("APPROACHING EVENT LETTER").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_HealthWellness = df_lkp_Codes_out.filter(
    col("HWComExclusion").isNotNull()
).select(
    lit("EDW").alias("SRC_SYS_CD"),
    when(col("GRP_ID").isNull(), lit(0)).otherwise(col("GRP_ID")).alias("GRP_ID"),
    lit("HLTHWELLNS").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    when(col("GRP_SK").isNull(), lit(0)).otherwise(col("GRP_SK")).alias("GRP_SK"),
    col("HWComExclusion").alias("EXCL_DESC"),
    lit("HEALTH AND WELLNESS").alias("MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_funl_GrpMailExcl = (
    df_xfrm_BusinessLogic_WellAware
    .unionByName(df_xfrm_BusinessLogic_HealthWellness)
    .unionByName(df_xfrm_BusinessLogic_MarkLetters)
    .unionByName(df_xfrm_BusinessLogic_ChoiceCov)
    .unionByName(df_xfrm_BusinessLogic_Surveys)
    .unionByName(df_xfrm_BusinessLogic_ApproachEvent)
    .unionByName(df_xfrm_BusinessLogic_COBLetters)
)

df_funl_GrpMailExcl_renamed = df_funl_GrpMailExcl.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("MAIL_TYP_CD").alias("MAIL_TYP_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("EXCL_DESC").alias("GRP_MAIL_EXCL_DESC"),
    col("MAIL_TYP_NM").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_UNK_NA_Logic_full_data = df_funl_GrpMailExcl_renamed.filter(
    (col("SRC_SYS_CD") != "UNK")
    & (col("GRP_ID") != "UNK")
    & (col("MAIL_TYP_CD") != "UNK")
    & (col("SRC_SYS_CD") != "NA")
    & (col("GRP_ID") != "NA")
    & (col("MAIL_TYP_CD") != "NA")
).select(
    lit("0").alias("GRP_MAIL_EXCL_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("MAIL_TYP_CD").alias("MAIL_TYP_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("GRP_MAIL_EXCL_DESC").alias("GRP_MAIL_EXCL_DESC"),
    col("GRP_MAIL_EXCL_MAIL_TYP_NM").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_UNK_NA_Logic_NA = df_funl_GrpMailExcl_renamed.limit(1).select(
    lit("1").alias("GRP_MAIL_EXCL_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("GRP_ID"),
    lit("NA").alias("MAIL_TYP_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("1").alias("GRP_SK"),
    lit("NA").alias("GRP_MAIL_EXCL_DESC"),
    lit("NA").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_UNK_NA_Logic_UNK = df_funl_GrpMailExcl_renamed.limit(1).select(
    lit("0").alias("GRP_MAIL_EXCL_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("GRP_ID"),
    lit("UNK").alias("MAIL_TYP_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("0").alias("GRP_SK"),
    lit("UNK").alias("GRP_MAIL_EXCL_DESC"),
    lit("UNK").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Fnl_UNK_NA_data = (
    df_xfrm_UNK_NA_Logic_NA
    .unionByName(df_xfrm_UNK_NA_Logic_UNK)
    .unionByName(df_xfrm_UNK_NA_Logic_full_data)
)

df_final = df_Fnl_UNK_NA_data.select(
    rpad(col("GRP_MAIL_EXCL_SK"), <...>, " ").alias("GRP_MAIL_EXCL_SK"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("MAIL_TYP_CD"), <...>, " ").alias("MAIL_TYP_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("GRP_SK"), <...>, " ").alias("GRP_SK"),
    rpad(col("GRP_MAIL_EXCL_DESC"), <...>, " ").alias("GRP_MAIL_EXCL_DESC"),
    rpad(col("GRP_MAIL_EXCL_MAIL_TYP_NM"), <...>, " ").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK"), <...>, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/ds/GRP_MAIL_EXCL_D.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)