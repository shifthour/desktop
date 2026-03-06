# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsProvDEAFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the Foreign key.  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  File from IdsProvDEAPkey
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - contains records that had errors on them for recycle.  Recycle is not used in this program - file is completely updated from Facets each time.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes 
# MAGIC                                  
# MAGIC                            Unknown and Not Applicable rows are created - all rows are sent through collector to combine into output file
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  This job would be run whenever there is an update to the NDC table/file.  Until now, we got the NDC (National Drug Code) file from CRMS, but now are looking for a 
# MAGIC   different source.  When this source is located and we get new files, these jobs will probably need to be re-written.
# MAGIC 
# MAGIC OUTPUTS:  File for PROV_DEA table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             BJ Luce        08/2004         -   Originally Programmed
# MAGIC             Brent Leland 09/20/2004    -  Added link partioner.
# MAGIC             Steph Goddard  06/01/2005  changed for sequencer
# MAGIC 
# MAGIC Developer                  Date                 Project/Altiris #      Change Description                                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                       --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2009-10-28            TTR-525         Added new fields CLM_TRANS_ADD_IN, ADDR_LN2,             devlIDS                      Steph Goddard        10/29/2009
# MAGIC                                                                                       NTNL_PROV_ID, NTNL_PROV_ID_SPEC_DESC                                                           
# MAGIC 
# MAGIC Manasa Andru         2014-03-14           TFS - 4011      Updated the fields EFF_DT_SK and TERM_DT_SK               IntegrateCurDevl              Kalyan Neelam            2014-03-20
# MAGIC                                                                                    as per the mapping rules in the ForeignKey1 transformer.
# MAGIC Kalyan Neelam        2015-10-30      5403/TFS1048     Added 2 new columns on end -                                                 IntegrateDev1              Bhoomi Dasari           11/9/2015
# MAGIC                                                                                  NTNL_PROV_ID_PROV_TYP_DESC and NTNL_PROV_ID_PROV_TYP_DESC
# MAGIC 
# MAGIC Ravi Abburi             2018-01-29    5781 - HEDIS            Added the NTNL_PROV_SK                                               IntegrateDev2                   Kalyan Neelam            2018-04-19

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Error count forced to 0 - table is updated each time.
# MAGIC Create default rows for UNK and NA
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
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    row_number,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "N")

schema_dea_pkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PROV_DEA_SK", IntegerType(), False),
    StructField("DEA_NO", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CMN_PRCT_CD", StringType(), False),
    StructField("CLM_TRANS_ADD_IN", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("PROV_NM", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("CITY_NM", StringType(), True),
    StructField("PROV_DEA_ST_CD", StringType(), False),
    StructField("POSTAL_CD", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), False),
    StructField("NTNL_PROV_ID_SPEC_DESC", StringType(), True),
    StructField("NTNL_PROV_ID_PROV_TYP_DESC", StringType(), True),
    StructField("NTNL_PROV_ID_PROV_CLS_DESC", StringType(), True)
])

df_DEA_PKey = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_dea_pkey)
    .csv(f"{adls_path}/key/IdsProvDEAExtr.ProvDEA.uniq")
)

df_foreignkey1_base = (
    df_DEA_PKey
    .withColumn("svCmnPrctFacetsSK", GetFkeyCmnPrct(lit("FACETS"), col("PROV_DEA_SK"), col("CMN_PRCT_CD"), lit("X")))
    .withColumn("svCmnPrctVcacSK", GetFkeyCmnPrct(lit("VCAC"), col("PROV_DEA_SK"), col("CMN_PRCT_CD"), lit("X")))
    .withColumn(
        "CmnPrctSk",
        when(col("svCmnPrctFacetsSK") == 0, col("svCmnPrctVcacSK")).otherwise(col("svCmnPrctFacetsSK"))
    )
    .withColumn("svStCdSK", GetFkeyCodes(lit("IDS"), col("PROV_DEA_SK"), lit("STATE"), col("PROV_DEA_ST_CD"), lit(Logging)))
    .withColumn("svEffDtSK", GetFkeyDate(lit("IDS"), col("PROV_DEA_SK"), col("EFF_DT_SK"), lit(Logging)))
    .withColumn("svTermDtSK", GetFkeyDate(lit("IDS"), col("PROV_DEA_SK"), col("TERM_DT_SK"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PROV_DEA_SK")))
    .withColumn("svNtnlProvSK", GetFkeyNtnlProvId(lit("CMS"), col("PROV_DEA_SK"), col("NTNL_PROV_ID"), lit(Logging)))
)

df_fkey = (
    df_foreignkey1_base
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .withColumn("EFF_DT_SK", when(col("svEffDtSK").isin("UNK", "NA"), lit("1753-01-01")).otherwise(col("svEffDtSK")))
    .withColumn("TERM_DT_SK", when(col("svTermDtSK").isin("UNK", "NA"), lit("2199-12-31")).otherwise(col("svTermDtSK")))
    .withColumn(
        "NTNL_PROV_SK",
        when(col("NTNL_PROV_ID") == lit("NA"), lit(1))
        .otherwise(when(col("NTNL_PROV_ID").isNull(), lit(0)).otherwise(col("svNtnlProvSK")))
    )
    .select(
        col("PROV_DEA_SK").alias("PROV_DEA_SK"),
        col("DEA_NO").alias("DEA_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CmnPrctSk").alias("CMN_PRCT_SK"),
        col("CLM_TRANS_ADD_IN").alias("CLM_TRANS_ADD_IN"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("PROV_NM").alias("PROV_NM"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("CITY_NM").alias("CITY_NM"),
        col("svStCdSK").alias("PROV_DEA_ST_CD_SK"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("NTNL_PROV_ID_SPEC_DESC").alias("NTNL_PROV_ID_SPEC_DESC"),
        col("NTNL_PROV_ID_PROV_TYP_DESC").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
        col("NTNL_PROV_ID_PROV_CLS_DESC").alias("NTNL_PROV_ID_PROV_CLS_DESC"),
        col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
    )
)

df_recycle = (
    df_foreignkey1_base
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("PROV_DEA_SK")))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("PROV_DEA_SK"),
        col("DEA_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CMN_PRCT_CD"),
        col("CLM_TRANS_ADD_IN"),
        col("EFF_DT_SK"),
        col("TERM_DT_SK"),
        col("PROV_NM"),
        col("ADDR_LN_1"),
        col("ADDR_LN_2"),
        col("CITY_NM"),
        col("PROV_DEA_ST_CD"),
        col("POSTAL_CD"),
        col("NTNL_PROV_ID"),
        col("NTNL_PROV_ID_SPEC_DESC"),
        col("NTNL_PROV_ID_PROV_TYP_DESC"),
        col("NTNL_PROV_ID_PROV_CLS_DESC")
    )
)

window_all = Window.orderBy(lit(1))
df_foreignkey1_base_with_rn = df_foreignkey1_base.withColumn("row_num", row_number().over(window_all))

df_defaultUNK = (
    df_foreignkey1_base_with_rn
    .filter(col("row_num") == 1)
    .select(
        lit(0).alias("PROV_DEA_SK"),
        lit("UNK").alias("DEA_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CMN_PRCT_SK"),
        lit("X").alias("CLM_TRANS_ADD_IN"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("UNK").alias("PROV_NM"),
        lit("UNK").alias("ADDR_LN_1"),
        lit("UNK").alias("ADDR_LN_2"),
        lit("UNK").alias("CITY_NM"),
        lit(0).alias("PROV_DEA_ST_CD_SK"),
        lit("UNK").alias("POSTAL_CD"),
        lit("UNK").alias("NTNL_PROV_ID"),
        lit("UNK").alias("NTNL_PROV_ID_SPEC_DESC"),
        lit("UNK").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
        lit("UNK").alias("NTNL_PROV_ID_PROV_CLS_DESC"),
        lit(0).alias("NTNL_PROV_SK")
    )
)

df_defaultNA = (
    df_foreignkey1_base_with_rn
    .filter(col("row_num") == 1)
    .select(
        lit(1).alias("PROV_DEA_SK"),
        lit("NA").alias("DEA_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CMN_PRCT_SK"),
        lit("X").alias("CLM_TRANS_ADD_IN"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("NA").alias("PROV_NM"),
        lit("NA").alias("ADDR_LN_1"),
        lit("NA").alias("ADDR_LN_2"),
        lit("NA").alias("CITY_NM"),
        lit(1).alias("PROV_DEA_ST_CD_SK"),
        lit("NA").alias("POSTAL_CD"),
        lit("NA").alias("NTNL_PROV_ID"),
        lit("NA").alias("NTNL_PROV_ID_SPEC_DESC"),
        lit("NA").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
        lit("NA").alias("NTNL_PROV_ID_PROV_CLS_DESC"),
        lit(1).alias("NTNL_PROV_SK")
    )
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_recycle_out = df_recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("PROV_DEA_SK"),
    rpad(col("DEA_NO"), <...>, " ").alias("DEA_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CMN_PRCT_CD"), 5, " ").alias("CMN_PRCT_CD"),
    rpad(col("CLM_TRANS_ADD_IN"), 1, " ").alias("CLM_TRANS_ADD_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    rpad(col("PROV_NM"), <...>, " ").alias("PROV_NM"),
    rpad(col("ADDR_LN_1"), <...>, " ").alias("ADDR_LN_1"),
    rpad(col("ADDR_LN_2"), <...>, " ").alias("ADDR_LN_2"),
    rpad(col("CITY_NM"), <...>, " ").alias("CITY_NM"),
    rpad(col("PROV_DEA_ST_CD"), 2, " ").alias("PROV_DEA_ST_CD"),
    rpad(col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    rpad(col("NTNL_PROV_ID"), <...>, " ").alias("NTNL_PROV_ID"),
    rpad(col("NTNL_PROV_ID_SPEC_DESC"), <...>, " ").alias("NTNL_PROV_ID_SPEC_DESC"),
    rpad(col("NTNL_PROV_ID_PROV_TYP_DESC"), <...>, " ").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
    rpad(col("NTNL_PROV_ID_PROV_CLS_DESC"), <...>, " ").alias("NTNL_PROV_ID_PROV_CLS_DESC")
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector_final = df_collector.select(
    col("PROV_DEA_SK"),
    rpad(col("DEA_NO"), <...>, " ").alias("DEA_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK"),
    rpad(col("CLM_TRANS_ADD_IN"), 1, " ").alias("CLM_TRANS_ADD_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    rpad(col("PROV_NM"), <...>, " ").alias("PROV_NM"),
    rpad(col("ADDR_LN_1"), <...>, " ").alias("ADDR_LN_1"),
    rpad(col("ADDR_LN_2"), <...>, " ").alias("ADDR_LN_2"),
    rpad(col("CITY_NM"), <...>, " ").alias("CITY_NM"),
    col("PROV_DEA_ST_CD_SK"),
    rpad(col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    rpad(col("NTNL_PROV_ID"), <...>, " ").alias("NTNL_PROV_ID"),
    rpad(col("NTNL_PROV_ID_SPEC_DESC"), <...>, " ").alias("NTNL_PROV_ID_SPEC_DESC"),
    rpad(col("NTNL_PROV_ID_PROV_TYP_DESC"), <...>, " ").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
    rpad(col("NTNL_PROV_ID_PROV_CLS_DESC"), <...>, " ").alias("NTNL_PROV_ID_PROV_CLS_DESC"),
    col("NTNL_PROV_SK")
)

write_files(
    df_collector_final,
    f"{adls_path}/load/PROV_DEA.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)