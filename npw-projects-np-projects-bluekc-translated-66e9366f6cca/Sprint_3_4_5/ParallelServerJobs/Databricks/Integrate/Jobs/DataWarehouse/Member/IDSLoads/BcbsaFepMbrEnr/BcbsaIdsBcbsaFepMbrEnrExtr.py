# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extracts data from FEP Member and Member Enroll files from BCBSA and loads into a DataSets
# MAGIC 
# MAGIC Modifications:                          
# MAGIC                                                  Project/                                                                                                       Development                            Code                  Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                     Environment                            Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------       ----------------------------------------------   -------------------------  -------------------
# MAGIC Kalyan Neelam             2015-11-23     5403        Initial Programming                                                                    IntegrateDev1                        Bhoomi Dasari   12/2/2015
# MAGIC 
# MAGIC Kailash Jadhav          2017-06-27     5781        Added new attribute FEP_COV in the source file.                     IntegrateDev1                        Kalyan Neelam    2017-06-29 
# MAGIC 
# MAGIC Karthik Chintalapani  2020-11-12  us296747    Modified the jobs as per the new file layout from Association.    IntegrateDev3                      Jaideep Mankala  11/12/2020


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
FEP_MbrFile = get_widget_value('FEP_MbrFile','BCBSA.FEP.MemberID.2020-10-02.dat')
FEP_MbrEnrFile = get_widget_value('FEP_MbrEnrFile','BCBSA.FEP.Enrollment.2020-10-02.dat')

schema_FEP_MBR = StructType([
    StructField("MBR_ID", StringType(), True),
    StructField("R_NO", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("MBR_DOB", StringType(), True),
    StructField("MBR_GNDR", StringType(), True),
    StructField("MMI", StringType(), True)
])

df_FEP_MBR = (
    spark.read.format("csv")
    .schema(schema_FEP_MBR)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("header", "false")
    .option("nullValue", None)
    .load(f"{adls_path_raw}/landing/{FEP_MbrFile}")
)

df_transform_mbr = df_FEP_MBR.select(
    col("MBR_ID").alias("MBR_ID"),
    col("R_NO").alias("R_NO"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("MBR_DOB").alias("MBR_DOB"),
    col("MBR_GNDR").alias("MBR_GNDR")
)

df_final_mbr = df_transform_mbr.select(
    rpad(col("MBR_ID"), <...>, " ").alias("MBR_ID"),
    rpad(col("R_NO"), <...>, " ").alias("R_NO"),
    rpad(col("MBR_FIRST_NM"), <...>, " ").alias("MBR_FIRST_NM"),
    rpad(col("MBR_LAST_NM"), <...>, " ").alias("MBR_LAST_NM"),
    rpad(col("MBR_DOB"), 10, " ").alias("MBR_DOB"),
    rpad(col("MBR_GNDR"), <...>, " ").alias("MBR_GNDR")
)

write_files(
    df_final_mbr,
    f"{adls_path}/ds/BCBSA_FEP_MBR_ENR_mbr.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

schema_FEP_ENR = StructType([
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("ENR_DT", StringType(), True),
    StructField("TERM_DT", StringType(), True),
    StructField("PLN_BNF_ID", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PLN_PROD_ID", StringType(), True),
    StructField("EMPL_STTUS", StringType(), True),
    StructField("CONF_RSTRCT_ACES_FLAG", StringType(), True),
    StructField("CSTM_FLD_1", StringType(), True),
    StructField("MKT_ID", StringType(), True),
    StructField("RATE_CAT", StringType(), True),
    StructField("HLTH_COND_CD", StringType(), True),
    StructField("PDX_BNF_FLAG", StringType(), True),
    StructField("OP_BNF_FLAG", StringType(), True),
    StructField("MNTL_HLTH_BNF_FLAG", StringType(), True),
    StructField("MNTL_HLTH_IP_BNF_FLAG", StringType(), True),
    StructField("MNTL_HLTH_AMBLTRY_BNF_FLAG", StringType(), True),
    StructField("MNTL_HLTH_DAY_NIGHT_BNF_FLAG", StringType(), True),
    StructField("CHMCL_DPNDC_BNF_FLAG", StringType(), True),
    StructField("CHMCL_DPNDC_IP_BNF_FLAG", StringType(), True),
    StructField("CHMCL_DPNDC_AMBLTRY_BNF_FLAG", StringType(), True),
    StructField("CHMCL_DPNDC_DAY_NIGHT_BNF_FLAG", StringType(), True),
    StructField("DNTL_BNF_FLAG", StringType(), True),
    StructField("HSPC_BNF_FLAG", StringType(), True),
    StructField("PLN_RGN", StringType(), True),
    StructField("CH_MBR_ID", StringType(), True),
    StructField("BCBS_PLN_CD", StringType(), True),
    StructField("FEP_PROD_DESC", StringType(), True),
    StructField("OTHR_PARTY_LIAB_COV", StringType(), True),
    StructField("MCARE_BNF_CD", StringType(), True),
    StructField("MCARE_SNP_BNF_ID", IntegerType(), True),
    StructField("ALTCSTM_FLD_1", StringType(), True),
    StructField("ALTCSTM_FLD_2", StringType(), True),
    StructField("ALTCSTM_FLD_3", StringType(), True),
    StructField("ALTCSTM_FLD_4", StringType(), True),
    StructField("PROV_ORG_ID", StringType(), True),
    StructField("PA_MBR_FEP_PROD", StringType(), True),
    StructField("NY_MBR_FEP_PROD", StringType(), True),
    StructField("HLTH_BNF_EXCH_ID", StringType(), True),
    StructField("FEP_COV", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("SRC_SYS", StringType(), True),
    StructField("MMI", StringType(), True)
])

df_FEP_ENR = (
    spark.read.format("csv")
    .schema(schema_FEP_ENR)
    .option("delimiter", "|")
    .option("header", "false")
    .option("nullValue", None)
    .load(f"{adls_path_raw}/landing/{FEP_MbrEnrFile}")
)

df_transform_enr = df_FEP_ENR.select(
    col("FEP_PROD").alias("FEP_PROD"),
    col("MBR_ID").alias("MBR_ID"),
    col("ENR_DT").alias("ENR_DT"),
    col("TERM_DT").alias("TERM_DT"),
    col("PLN_BNF_ID").alias("PLN_BNF_ID"),
    col("PROV_ID").alias("PROV_ID"),
    col("PLN_PROD_ID").alias("PLN_PROD_ID"),
    col("EMPL_STTUS").alias("EMPL_STTUS"),
    col("CONF_RSTRCT_ACES_FLAG").alias("CONF_RSTRCT_ACES_FLAG"),
    col("CSTM_FLD_1").alias("CSTM_FLD_1"),
    col("MKT_ID").alias("MKT_ID"),
    col("RATE_CAT").alias("RATE_CAT"),
    col("HLTH_COND_CD").alias("HLTH_COND_CD"),
    col("PDX_BNF_FLAG").alias("PDX_BNF_FLAG"),
    col("OP_BNF_FLAG").alias("OP_BNF_FLAG"),
    col("MNTL_HLTH_BNF_FLAG").alias("MNTL_HLTH_BNF_FLAG"),
    col("MNTL_HLTH_IP_BNF_FLAG").alias("MNTL_HLTH_IP_BNF_FLAG"),
    col("MNTL_HLTH_AMBLTRY_BNF_FLAG").alias("MNTL_HLTH_AMBLTRY_BNF_FLAG"),
    col("MNTL_HLTH_DAY_NIGHT_BNF_FLAG").alias("MNTL_HLTH_DAY_NIGHT_BNF_FLAG"),
    col("CHMCL_DPNDC_BNF_FLAG").alias("CHMCL_DPNDC_BNF_FLAG"),
    col("CHMCL_DPNDC_IP_BNF_FLAG").alias("CHMCL_DPNDC_IP_BNF_FLAG"),
    col("CHMCL_DPNDC_AMBLTRY_BNF_FLAG").alias("CHMCL_DPNDC_AMBLTRY_BNF_FLAG"),
    col("CHMCL_DPNDC_DAY_NIGHT_BNF_FLAG").alias("CHMCL_DPNDC_DAY_NIGHT_BNF_FLAG"),
    col("DNTL_BNF_FLAG").alias("DNTL_BNF_FLAG"),
    col("HSPC_BNF_FLAG").alias("HSPC_BNF_FLAG"),
    col("PLN_RGN").alias("PLN_RGN"),
    col("CH_MBR_ID").alias("CH_MBR_ID"),
    col("BCBS_PLN_CD").alias("BCBS_PLN_CD"),
    col("FEP_PROD_DESC").alias("FEP_PROD_DESC"),
    col("OTHR_PARTY_LIAB_COV").alias("OTHR_PARTY_LIAB_COV"),
    col("MCARE_BNF_CD").alias("MCARE_BNF_CD"),
    col("MCARE_SNP_BNF_ID").alias("MCARE_SNP_BNF_ID"),
    col("ALTCSTM_FLD_1").alias("ALTCSTM_FLD_1"),
    col("ALTCSTM_FLD_2").alias("ALTCSTM_FLD_2"),
    col("ALTCSTM_FLD_3").alias("ALTCSTM_FLD_3"),
    col("ALTCSTM_FLD_4").alias("ALTCSTM_FLD_4"),
    col("PROV_ORG_ID").alias("PROV_ORG_ID"),
    col("PA_MBR_FEP_PROD").alias("PA_MBR_FEP_PROD"),
    col("NY_MBR_FEP_PROD").alias("NY_MBR_FEP_PROD"),
    col("HLTH_BNF_EXCH_ID").alias("HLTH_BNF_EXCH_ID"),
    col("FEP_COV").alias("FEP_COV"),
    col("RUN_DT").alias("RUN_DT"),
    col("SRC_SYS").alias("SRC_SYS")
)

df_final_enr = df_transform_enr.select(
    rpad(col("FEP_PROD"), <...>, " ").alias("FEP_PROD"),
    rpad(col("MBR_ID"), <...>, " ").alias("MBR_ID"),
    rpad(col("ENR_DT"), 10, " ").alias("ENR_DT"),
    rpad(col("TERM_DT"), 10, " ").alias("TERM_DT"),
    rpad(col("PLN_BNF_ID"), <...>, " ").alias("PLN_BNF_ID"),
    rpad(col("PROV_ID"), <...>, " ").alias("PROV_ID"),
    rpad(col("PLN_PROD_ID"), <...>, " ").alias("PLN_PROD_ID"),
    rpad(col("EMPL_STTUS"), <...>, " ").alias("EMPL_STTUS"),
    rpad(col("CONF_RSTRCT_ACES_FLAG"), 1, " ").alias("CONF_RSTRCT_ACES_FLAG"),
    rpad(col("CSTM_FLD_1"), <...>, " ").alias("CSTM_FLD_1"),
    rpad(col("MKT_ID"), 2, " ").alias("MKT_ID"),
    rpad(col("RATE_CAT"), <...>, " ").alias("RATE_CAT"),
    rpad(col("HLTH_COND_CD"), 2, " ").alias("HLTH_COND_CD"),
    rpad(col("PDX_BNF_FLAG"), 1, " ").alias("PDX_BNF_FLAG"),
    rpad(col("OP_BNF_FLAG"), 1, " ").alias("OP_BNF_FLAG"),
    rpad(col("MNTL_HLTH_BNF_FLAG"), 1, " ").alias("MNTL_HLTH_BNF_FLAG"),
    rpad(col("MNTL_HLTH_IP_BNF_FLAG"), 1, " ").alias("MNTL_HLTH_IP_BNF_FLAG"),
    rpad(col("MNTL_HLTH_AMBLTRY_BNF_FLAG"), 1, " ").alias("MNTL_HLTH_AMBLTRY_BNF_FLAG"),
    rpad(col("MNTL_HLTH_DAY_NIGHT_BNF_FLAG"), 1, " ").alias("MNTL_HLTH_DAY_NIGHT_BNF_FLAG"),
    rpad(col("CHMCL_DPNDC_BNF_FLAG"), 1, " ").alias("CHMCL_DPNDC_BNF_FLAG"),
    rpad(col("CHMCL_DPNDC_IP_BNF_FLAG"), 1, " ").alias("CHMCL_DPNDC_IP_BNF_FLAG"),
    rpad(col("CHMCL_DPNDC_AMBLTRY_BNF_FLAG"), 1, " ").alias("CHMCL_DPNDC_AMBLTRY_BNF_FLAG"),
    rpad(col("CHMCL_DPNDC_DAY_NIGHT_BNF_FLAG"), 1, " ").alias("CHMCL_DPNDC_DAY_NIGHT_BNF_FLAG"),
    rpad(col("DNTL_BNF_FLAG"), 1, " ").alias("DNTL_BNF_FLAG"),
    rpad(col("HSPC_BNF_FLAG"), 1, " ").alias("HSPC_BNF_FLAG"),
    rpad(col("PLN_RGN"), <...>, " ").alias("PLN_RGN"),
    rpad(col("CH_MBR_ID"), <...>, " ").alias("CH_MBR_ID"),
    rpad(col("BCBS_PLN_CD"), <...>, " ").alias("BCBS_PLN_CD"),
    rpad(col("FEP_PROD_DESC"), <...>, " ").alias("FEP_PROD_DESC"),
    rpad(col("OTHR_PARTY_LIAB_COV"), <...>, " ").alias("OTHR_PARTY_LIAB_COV"),
    rpad(col("MCARE_BNF_CD"), 3, " ").alias("MCARE_BNF_CD"),
    col("MCARE_SNP_BNF_ID").alias("MCARE_SNP_BNF_ID"),
    rpad(col("ALTCSTM_FLD_1"), <...>, " ").alias("ALTCSTM_FLD_1"),
    rpad(col("ALTCSTM_FLD_2"), <...>, " ").alias("ALTCSTM_FLD_2"),
    rpad(col("ALTCSTM_FLD_3"), <...>, " ").alias("ALTCSTM_FLD_3"),
    rpad(col("ALTCSTM_FLD_4"), <...>, " ").alias("ALTCSTM_FLD_4"),
    rpad(col("PROV_ORG_ID"), <...>, " ").alias("PROV_ORG_ID"),
    rpad(col("PA_MBR_FEP_PROD"), 1, " ").alias("PA_MBR_FEP_PROD"),
    rpad(col("NY_MBR_FEP_PROD"), 1, " ").alias("NY_MBR_FEP_PROD"),
    rpad(col("HLTH_BNF_EXCH_ID"), <...>, " ").alias("HLTH_BNF_EXCH_ID"),
    rpad(col("FEP_COV"), 1, " ").alias("FEP_COV"),
    rpad(col("RUN_DT"), 10, " ").alias("RUN_DT"),
    rpad(col("SRC_SYS"), <...>, " ").alias("SRC_SYS")
)

write_files(
    df_final_enr,
    f"{adls_path}/ds/BCBSA_FEP_MBR_ENR_enr.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)