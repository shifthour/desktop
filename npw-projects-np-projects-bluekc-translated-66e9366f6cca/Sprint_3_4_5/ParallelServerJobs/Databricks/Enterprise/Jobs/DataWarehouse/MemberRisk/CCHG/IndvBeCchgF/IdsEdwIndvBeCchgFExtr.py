# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from IDS INDV_BE_CCHG table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2015-08-25       5460                              Initial Programming                                     EnterpriseDev2          Bhoomi Dasari           8/30/2015
# MAGIC Goutham K                11/13/2021         US-500022                    Added New Field VersionID                           IntegrateDev2          Reddy Sanam           04/13/2022

# MAGIC Read all the Data from IDS INDV_BE_CCHG Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Write INDV_BE_CCHG_F Data into a Sequential file for Load Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# db2_INDV_BE_CCHG_in
db2_INDV_BE_CCHG_in_sql = (
    f"SELECT \n"
    f"IBC.INDV_BE_CCHG_SK,\n"
    f"IBC.INDV_BE_KEY,\n"
    f"IBC.CCHG_STRT_YR_MO_SK,\n"
    f"IBC.PRCS_YR_MO_SK,\n"
    f"IBC.SRC_SYS_CD_SK,\n"
    f"IBC.CRT_RUN_CYC_EXCTN_SK,\n"
    f"IBC.LAST_UPDT_RUN_CYC_EXCTN_SK,\n"
    f"IBC.CCHG_MULT_CAT_GRP_PRI_SK,\n"
    f"IBC.CCHG_MULT_CAT_GRP_SEC_SK,\n"
    f"IBC.CCHG_MULT_CAT_GRP_TRTY_SK,\n"
    f"IBC.CCHG_MULT_CAT_GRP_4TH_SK,\n"
    f"IBC.CCHG_MULT_CAT_GRP_5TH_SK,\n"
    f"IBC.CCHG_END_YR_MO_SK,\n"
    f"IBC.CCHG_CT,\n"
    f"COALESCE(CD1.TRGT_CD, 'UNK') as SRC_SYS_CD,\n"
    f"IBC.VRSN_ID\n"
    f"FROM {IDSOwner}.INDV_BE_CCHG AS IBC\n"
    f"LEFT OUTER JOIN {IDSOwner}.CD_MPPNG AS CD1\n"
    f"ON IBC.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK\n"
    f"WHERE IBC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)

jdbc_url_db2_INDV_BE_CCHG_in, jdbc_props_db2_INDV_BE_CCHG_in = get_db_config(ids_secret_name)
df_db2_INDV_BE_CCHG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_INDV_BE_CCHG_in)
    .options(**jdbc_props_db2_INDV_BE_CCHG_in)
    .option("query", db2_INDV_BE_CCHG_in_sql)
    .load()
)

# db2_CCHG_MULT_CAT_GRP_In
db2_CCHG_MULT_CAT_GRP_In_sql = (
    f"SELECT \n"
    f"CCHG_MULT_CAT_GRP_SK,\n"
    f"CCHG_MULT_CAT_GRP_ID\n"
    f"FROM {IDSOwner}.CCHG_MULT_CAT_GRP"
)

jdbc_url_db2_CCHG_MULT_CAT_GRP_In, jdbc_props_db2_CCHG_MULT_CAT_GRP_In = get_db_config(ids_secret_name)
df_db2_CCHG_MULT_CAT_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CCHG_MULT_CAT_GRP_In)
    .options(**jdbc_props_db2_CCHG_MULT_CAT_GRP_In)
    .option("query", db2_CCHG_MULT_CAT_GRP_In_sql)
    .load()
)

# Copy stage -> produce Grp1, Grp2, Grp3, Grp4, Grp5
df_Grp1 = df_db2_CCHG_MULT_CAT_GRP_In.select(
    col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID")
)

df_Grp2 = df_db2_CCHG_MULT_CAT_GRP_In.select(
    col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID")
)

df_Grp3 = df_db2_CCHG_MULT_CAT_GRP_In.select(
    col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID")
)

df_Grp4 = df_db2_CCHG_MULT_CAT_GRP_In.select(
    col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID")
)

df_Grp5 = df_db2_CCHG_MULT_CAT_GRP_In.select(
    col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID")
)

# Lookup
df_Lookup = (
    df_db2_INDV_BE_CCHG_in.alias("IdsOut")
    .join(df_Grp1.alias("Grp1"), col("IdsOut.CCHG_MULT_CAT_GRP_PRI_SK") == col("Grp1.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_Grp2.alias("Grp2"), col("IdsOut.CCHG_MULT_CAT_GRP_SEC_SK") == col("Grp2.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_Grp3.alias("Grp3"), col("IdsOut.CCHG_MULT_CAT_GRP_TRTY_SK") == col("Grp3.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_Grp4.alias("Grp4"), col("IdsOut.CCHG_MULT_CAT_GRP_4TH_SK") == col("Grp4.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_Grp5.alias("Grp5"), col("IdsOut.CCHG_MULT_CAT_GRP_5TH_SK") == col("Grp5.CCHG_MULT_CAT_GRP_SK"), "left")
)

df_rules = df_Lookup.select(
    col("IdsOut.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
    col("IdsOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("IdsOut.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    col("IdsOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("IdsOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("IdsOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("IdsOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("IdsOut.CCHG_MULT_CAT_GRP_PRI_SK").alias("CCHG_MULT_CAT_GRP_PRI_SK"),
    col("IdsOut.CCHG_MULT_CAT_GRP_SEC_SK").alias("CCHG_MULT_CAT_GRP_SEC_SK"),
    col("IdsOut.CCHG_MULT_CAT_GRP_TRTY_SK").alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
    col("IdsOut.CCHG_MULT_CAT_GRP_4TH_SK").alias("CCHG_MULT_CAT_GRP_4TH_SK"),
    col("IdsOut.CCHG_MULT_CAT_GRP_5TH_SK").alias("CCHG_MULT_CAT_GRP_5TH_SK"),
    col("IdsOut.CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
    col("IdsOut.CCHG_CT").alias("CCHG_CT"),
    col("IdsOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Grp1.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_1"),
    col("Grp2.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_2"),
    col("Grp3.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_3"),
    col("Grp4.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_4"),
    col("Grp5.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_5"),
    col("IdsOut.VRSN_ID").alias("VRSN_ID")
)

# xfrm_BusinessLogic

# lnk_xfm_Data: rules.INDV_BE_CCHG_SK <> 1 And rules.INDV_BE_CCHG_SK <> 0
df_lnk_xfm_Data = (
    df_rules
    .filter((col("INDV_BE_CCHG_SK") != 1) & (col("INDV_BE_CCHG_SK") != 0))
    .withColumn("INDV_BE_CCHG_SK", col("INDV_BE_CCHG_SK"))
    .withColumn("INDV_BE_KEY", col("INDV_BE_KEY"))
    .withColumn("CCHG_STRT_YR_MO_SK", col("CCHG_STRT_YR_MO_SK"))
    .withColumn("PRCS_YR_MO_SK", col("PRCS_YR_MO_SK"))
    .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn("CCHG_MULT_CAT_GRP_PRI_SK", col("CCHG_MULT_CAT_GRP_PRI_SK"))
    .withColumn("CCHG_MULT_CAT_GRP_SEC_SK", col("CCHG_MULT_CAT_GRP_SEC_SK"))
    .withColumn("CCHG_MULT_CAT_GRP_TRTY_SK", col("CCHG_MULT_CAT_GRP_TRTY_SK"))
    .withColumn("CCHG_MULT_CAT_GRP_4TH_SK", col("CCHG_MULT_CAT_GRP_4TH_SK"))
    .withColumn("CCHG_MULT_CAT_GRP_5TH_SK", col("CCHG_MULT_CAT_GRP_5TH_SK"))
    .withColumn("CCHG_END_YR_MO_SK", col("CCHG_END_YR_MO_SK"))
    .withColumn("CCHG_CT", col("CCHG_CT"))
    .withColumn("CCHG_MULT_CAT_GRP_PRI_ID", when(col("CCHG_MULT_CAT_GRP_ID_1").isNull(), lit("UNK")).otherwise(col("CCHG_MULT_CAT_GRP_ID_1")))
    .withColumn("CCHG_MULT_CAT_GRP_SEC_ID", when(col("CCHG_MULT_CAT_GRP_ID_2").isNull(), lit("UNK")).otherwise(col("CCHG_MULT_CAT_GRP_ID_2")))
    .withColumn("CCHG_MULT_CAT_GRP_TRTY_ID", when(col("CCHG_MULT_CAT_GRP_ID_3").isNull(), lit("UNK")).otherwise(col("CCHG_MULT_CAT_GRP_ID_3")))
    .withColumn("CCHG_MULT_CAT_GRP_4TH_ID", when(col("CCHG_MULT_CAT_GRP_ID_4").isNull(), lit("UNK")).otherwise(col("CCHG_MULT_CAT_GRP_ID_4")))
    .withColumn("CCHG_MULT_CAT_GRP_5TH_ID", when(col("CCHG_MULT_CAT_GRP_ID_5").isNull(), lit("UNK")).otherwise(col("CCHG_MULT_CAT_GRP_ID_5")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("VRSN_ID", col("VRSN_ID"))
)

# lnk_NA_out: single row with constraint => "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
schema_NA = StructType([
    StructField("INDV_BE_CCHG_SK", IntegerType(), True),
    StructField("INDV_BE_KEY", IntegerType(), True),
    StructField("CCHG_STRT_YR_MO_SK", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_PRI_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_SEC_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_TRTY_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_4TH_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_5TH_SK", IntegerType(), True),
    StructField("CCHG_END_YR_MO_SK", StringType(), True),
    StructField("CCHG_CT", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_PRI_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_SEC_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_TRTY_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_4TH_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_5TH_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VRSN_ID", StringType(), True)
])

data_lnk_NA_out = [(1, 1, "175301", "175301", "NA", "1753-01-01", "1753-01-01", 1, 1, 1, 1, 1, "219912", 0, "NA", "NA", "NA", "NA", "NA", 100, 100, 100, "NA")]
df_lnk_NA_out = spark.createDataFrame(data_lnk_NA_out, schema=schema_NA)

# lnk_UNK_out: single row with constraint => same approach
schema_UNK = StructType([
    StructField("INDV_BE_CCHG_SK", IntegerType(), True),
    StructField("INDV_BE_KEY", IntegerType(), True),
    StructField("CCHG_STRT_YR_MO_SK", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_PRI_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_SEC_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_TRTY_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_4TH_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_5TH_SK", IntegerType(), True),
    StructField("CCHG_END_YR_MO_SK", StringType(), True),
    StructField("CCHG_CT", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_PRI_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_SEC_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_TRTY_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_4TH_ID", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_5TH_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VRSN_ID", StringType(), True)
])

data_lnk_UNK_out = [(0, 0, "175301", "175301", "UNK", "1753-01-01", "1753-01-01", 0, 0, 0, 0, 0, "219912", 0, "UNK", "UNK", "UNK", "UNK", "UNK", 100, 100, 100, "UNK")]
df_lnk_UNK_out = spark.createDataFrame(data_lnk_UNK_out, schema=schema_UNK)

# Funnel -> fnl_Data
df_fnl_Data = df_lnk_NA_out.unionByName(df_lnk_UNK_out).unionByName(df_lnk_xfm_Data)

# seq_INDV_BE_CCHG_F_load -> writing file
# Apply column order and rpad for char columns
df_final = (
    df_fnl_Data.select(
        "INDV_BE_CCHG_SK",
        "INDV_BE_KEY",
        "CCHG_STRT_YR_MO_SK",
        "PRCS_YR_MO_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CCHG_MULT_CAT_GRP_PRI_SK",
        "CCHG_MULT_CAT_GRP_SEC_SK",
        "CCHG_MULT_CAT_GRP_TRTY_SK",
        "CCHG_MULT_CAT_GRP_4TH_SK",
        "CCHG_MULT_CAT_GRP_5TH_SK",
        "CCHG_END_YR_MO_SK",
        "CCHG_CT",
        "CCHG_MULT_CAT_GRP_PRI_ID",
        "CCHG_MULT_CAT_GRP_SEC_ID",
        "CCHG_MULT_CAT_GRP_TRTY_ID",
        "CCHG_MULT_CAT_GRP_4TH_ID",
        "CCHG_MULT_CAT_GRP_5TH_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "VRSN_ID"
    )
    .withColumn("CCHG_STRT_YR_MO_SK", rpad(col("CCHG_STRT_YR_MO_SK"), 6, " "))
    .withColumn("PRCS_YR_MO_SK", rpad(col("PRCS_YR_MO_SK"), 6, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CCHG_END_YR_MO_SK", rpad(col("CCHG_END_YR_MO_SK"), 6, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/INDV_BE_CCHG_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)