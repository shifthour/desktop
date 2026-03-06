# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  EdwScreeningMemberEligibilityCntl
# MAGIC  
# MAGIC PROCESSING: Creates the Screening Member Eligibility file from EDW which is sent to the associated Vendors.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2011-07-08        4673                      Initial Programming                                                                       EnterpriseWrhsDevl       Brent Leland              7-27-2011
# MAGIC 
# MAGIC Bhoomi Dasari         2011-09-26        4673                      Changed 'SUBSCRIBER' to 'SUB' in the WHERE                       EnterpriseWrhsDevl       Brent Leland              9-26-2011
# MAGIC                                                                                         clause

# MAGIC Member Eligibility Extract for Screening Data
# MAGIC The Constraint in the Transformer will only extract the current source's and current Group's data for lookup and creates the Member Eligibility file for that source and Group only.
# MAGIC Group/SubGroup/Source combination file created in EdwPIhmEligGrpListExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
GrpID = get_widget_value('GrpID','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""
SELECT DISTINCT
       MBR.MBR_UNIQ_KEY,
       MBR.GRP_ID,
       MBR.GRP_NM,
       MBR.SUBGRP_ID,
       MBR.SUBGRP_NM,
       MBR.MBR_BRTH_DT_SK,
       MBR.MBR_FIRST_NM,
       MBR.MBR_LAST_NM,
       MBR.MBR_GNDR_CD,
       MBR.MBR_SSN,
       MBR.MBR_SFX_NO,
       MBR.MBR_HOME_ADDR_LN_1,
       MBR.MBR_HOME_ADDR_LN_2,
       MBR.MBR_HOME_ADDR_CITY_NM,
       MBR.MBR_HOME_ADDR_ST_CD,
       MBR.MBR_HOME_ADDR_ZIP_CD_5,
       MBR.SUB_ALPHA_PFX_CD,
       MBR.SUB_ID,
       ELIG.SRC_SYS_CD
FROM
       {EDWOwner}.P_IHM_ELIG_GRP_LIST ELIG,
       {EDWOwner}.MBR_D MBR,
       {EDWOwner}.MBR_ENR_D ENR
WHERE
       ELIG.GRP_SK = MBR.GRP_SK
       AND ELIG.SUBGRP_SK = MBR.SUBGRP_SK
       AND MBR.MBR_SK = ENR.MBR_SK
       AND MBR.MBR_RELSHP_CD IN ('SPOUSE', 'SUB')
       AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD IN ('AHY', 'MED')
       AND ENR.MBR_ENR_ELIG_IN = 'Y'
       AND ENR.MBR_ENR_EFF_DT_SK <= '{CurrDate}'
       AND ENR.MBR_ENR_TERM_DT_SK >= '{CurrDate}'
"""

df_EDW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_MbrElig_GrpSubgrpSrcCombinations = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("RecInd", IntegerType(), False)
])

df_MbrElig_GrpSubgrpSrcCombinations = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_MbrElig_GrpSubgrpSrcCombinations)
    .csv(f"{adls_path_publish}/external/ScrnMbrElig_GrpSubgrpSrcCombinations.dat")
)

df_sourceConstraint = df_MbrElig_GrpSubgrpSrcCombinations.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd) &
    (F.col("GRP_ID") == GrpID)
).select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_hf_mbrelig_grpsubgrpsrc_combs = dedup_sort(
    df_sourceConstraint,
    ["GRP_ID", "SUBGRP_ID", "SRC_SYS_CD"],
    []
)

df_srcLkup = df_hf_mbrelig_grpsubgrpsrc_combs

df_BusinessRules_Joined = (
    df_EDW.alias("Extract")
    .join(
        df_srcLkup.alias("SrcLkup"),
        (
            (F.col("Extract.GRP_ID") == F.col("SrcLkup.GRP_ID")) &
            (F.col("Extract.SUBGRP_ID") == F.col("SrcLkup.SUBGRP_ID")) &
            (F.col("Extract.SRC_SYS_CD") == F.col("SrcLkup.SRC_SYS_CD"))
        ),
        how="left"
    )
)

df_BusinessRules_Filtered = df_BusinessRules_Joined.filter(
    F.col("SrcLkup.SRC_SYS_CD").isNotNull()
)

df_BusinessRules_Out = (
    df_BusinessRules_Filtered
    .withColumn("Screening_Date", F.lit(None).cast(StringType()))
    .withColumn("Group_Identifier", F.col("Extract.GRP_ID"))
    .withColumn("Group_Name", F.col("Extract.GRP_NM"))
    .withColumn("Subgroup_Identifier", F.col("Extract.SUBGRP_ID"))
    .withColumn("Member_Social_Security_Number", F.col("Extract.MBR_SSN"))
    .withColumn("Subgroup_name", F.col("Extract.SUBGRP_NM"))
    .withColumn("First_Name", F.col("Extract.MBR_FIRST_NM"))
    .withColumn("Last_Name", F.col("Extract.MBR_LAST_NM"))
    .withColumn("Date__of_Birth", F.col("Extract.MBR_BRTH_DT_SK").cast(StringType()))
    .withColumn("Member_Unique_Key", F.col("Extract.MBR_UNIQ_KEY"))
    .withColumn("Prefix", F.col("Extract.SUB_ALPHA_PFX_CD"))
    .withColumn("Sub_Identifier", F.col("Extract.SUB_ID"))
    .withColumn("Suffix_Number", F.col("Extract.MBR_SFX_NO"))
    .withColumn("Member_Home_Address_Line_1", F.col("Extract.MBR_HOME_ADDR_LN_1"))
    .withColumn("Member_Home_Address_Line_2", F.col("Extract.MBR_HOME_ADDR_LN_2"))
    .withColumn("Member_Home_Address_City_Name", F.col("Extract.MBR_HOME_ADDR_CITY_NM"))
    .withColumn("Member_Home_Address_State", F.col("Extract.MBR_HOME_ADDR_ST_CD"))
    .withColumn("Member_Home_Address_Zip", F.col("Extract.MBR_HOME_ADDR_ZIP_CD_5"))
    .withColumn("Member_Home_Address_Phone", F.lit(None).cast(StringType()))
    .withColumn("Email_Address", F.lit(None).cast(StringType()))
    .withColumn("HMO", F.lit(None).cast(StringType()))
    .withColumn("PPO", F.lit(None).cast(StringType()))
    .withColumn("Not_Employer_Plan", F.lit(None).cast(StringType()))
    .withColumn("No_Response", F.lit(None).cast(StringType()))
    .withColumn("Height_Inches", F.lit(None).cast(StringType()))
    .withColumn("Weight_pounds", F.lit(None).cast(StringType()))
    .withColumn("Body_Mass_Index", F.lit(None).cast(StringType()))
    .withColumn("Age", AGE(F.col("Extract.MBR_BRTH_DT_SK"), F.lit(CurrDate)))
    .withColumn("Gender", F.col("Extract.MBR_GNDR_CD"))
    .withColumn("Systolic_Pressure", F.lit(None).cast(StringType()))
    .withColumn("Diastolic_Pressure", F.lit(None).cast(StringType()))
    .withColumn("Total_Cholesterol", F.lit(None).cast(StringType()))
    .withColumn("Triglycerides", F.lit(None).cast(StringType()))
    .withColumn("HDL", F.lit(None).cast(StringType()))
    .withColumn("LDL", F.lit(None).cast(StringType()))
    .withColumn("Ratio", F.lit(None).cast(StringType()))
    .withColumn("Glucose", F.lit(None).cast(StringType()))
    .withColumn("Current_Smoker", F.lit(None).cast(StringType()))
    .withColumn("Former_Smoker", F.lit(None).cast(StringType()))
    .withColumn("High_BP", F.lit(None).cast(StringType()))
    .withColumn("High_Cholesterol", F.lit(None).cast(StringType()))
    .withColumn("Lack_Regular_Exercise", F.lit(None).cast(StringType()))
    .withColumn("Over_Weight", F.lit(None).cast(StringType()))
    .withColumn("Diabetic", F.lit(None).cast(StringType()))
    .withColumn("Stress", F.lit(None).cast(StringType()))
    .withColumn("Heart_Disease", F.lit(None).cast(StringType()))
    .withColumn("Stroke", F.lit(None).cast(StringType()))
    .withColumn("PSA", F.lit(None).cast(StringType()))
    .withColumn("Waist_Circumference", F.lit(None).cast(StringType()))
    .withColumn("Body_Fat_Percent", F.lit(None).cast(StringType()))
    .withColumn("TSH", F.lit(None).cast(StringType()))
    .withColumn("HA1C", F.lit(None).cast(StringType()))
    .withColumn("Rescreen_Indicator", F.lit(None).cast(StringType()))
    .withColumn("Referral_To_DM", F.lit(None).cast(StringType()))
    .withColumn("Referral_to_Physician", F.lit(None).cast(StringType()))
    .withColumn("Pregnant", F.lit(None).cast(StringType()))
    .withColumn("Fasting", F.lit(None).cast(StringType()))
    .withColumn("Source_System_Code", F.col("Extract.SRC_SYS_CD"))
)

df_final = df_BusinessRules_Out.select(
    rpad(F.col("Screening_Date"), 10, " ").alias("Screening_Date"),
    F.col("Group_Identifier").alias("Group_Identifier"),
    F.col("Group_Name").alias("Group_Name"),
    F.col("Subgroup_Identifier").alias("Subgroup_Identifier"),
    F.col("Member_Social_Security_Number").alias("Member_Social_Security_Number"),
    F.col("Subgroup_name").alias("Subgroup_name"),
    F.col("First_Name").alias("First_Name"),
    F.col("Last_Name").alias("Last_Name"),
    rpad(F.col("Date__of_Birth"), 10, " ").alias("Date__of_Birth"),
    F.col("Member_Unique_Key").alias("Member_Unique_Key"),
    F.col("Prefix").alias("Prefix"),
    F.col("Sub_Identifier").alias("Sub_Identifier"),
    F.col("Suffix_Number").alias("Suffix_Number"),
    F.col("Member_Home_Address_Line_1").alias("Member_Home_Address_Line_1"),
    F.col("Member_Home_Address_Line_2").alias("Member_Home_Address_Line_2"),
    F.col("Member_Home_Address_City_Name").alias("Member_Home_Address_City_Name"),
    F.col("Member_Home_Address_State").alias("Member_Home_Address_State"),
    F.col("Member_Home_Address_Zip").alias("Member_Home_Address_Zip"),
    F.col("Member_Home_Address_Phone").alias("Member_Home_Address_Phone"),
    F.col("Email_Address").alias("Email_Address"),
    F.col("HMO").alias("HMO"),
    F.col("PPO").alias("PPO"),
    F.col("Not_Employer_Plan").alias("Not_Employer_Plan"),
    F.col("No_Response").alias("No_Response"),
    F.col("Height_Inches").alias("Height_Inches"),
    F.col("Weight_pounds").alias("Weight_pounds"),
    F.col("Body_Mass_Index").alias("Body_Mass_Index"),
    F.col("Age").alias("Age"),
    F.col("Gender").alias("Gender"),
    F.col("Systolic_Pressure").alias("Systolic_Pressure"),
    F.col("Diastolic_Pressure").alias("Diastolic_Pressure"),
    F.col("Total_Cholesterol").alias("Total_Cholesterol"),
    F.col("Triglycerides").alias("Triglycerides"),
    F.col("HDL").alias("HDL"),
    F.col("LDL").alias("LDL"),
    F.col("Ratio").alias("Ratio"),
    F.col("Glucose").alias("Glucose"),
    rpad(F.col("Current_Smoker"), 18, " ").alias("Current_Smoker"),
    F.col("Former_Smoker").alias("Former_Smoker"),
    F.col("High_BP").alias("High_BP"),
    F.col("High_Cholesterol").alias("High_Cholesterol"),
    F.col("Lack_Regular_Exercise").alias("Lack_Regular_Exercise"),
    F.col("Over_Weight").alias("Over_Weight"),
    F.col("Diabetic").alias("Diabetic"),
    F.col("Stress").alias("Stress"),
    F.col("Heart_Disease").alias("Heart_Disease"),
    F.col("Stroke").alias("Stroke"),
    F.col("PSA").alias("PSA"),
    F.col("Waist_Circumference").alias("Waist_Circumference"),
    F.col("Body_Fat_Percent").alias("Body_Fat_Percent"),
    F.col("TSH").alias("TSH"),
    F.col("HA1C").alias("HA1C"),
    rpad(F.col("Rescreen_Indicator"), 1, " ").alias("Rescreen_Indicator"),
    rpad(F.col("Referral_To_DM"), 1, " ").alias("Referral_To_DM"),
    rpad(F.col("Referral_to_Physician"), 1, " ").alias("Referral_to_Physician"),
    rpad(F.col("Pregnant"), 1, " ").alias("Pregnant"),
    rpad(F.col("Fasting"), 1, " ").alias("Fasting"),
    F.col("Source_System_Code").alias("Source_System_Code")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/ScrnMbrEligFile_{SrcSysCd}_{GrpID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)