# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 06/02/09 10:11:31 Batch  15129_36694 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 06/02/09 10:05:13 Batch  15129_36315 INIT bckcett:31540 testIDS dsadm bls for sa
# MAGIC ^1_1 05/22/09 13:25:41 Batch  15118_48362 PROMOTE bckcett:31540 testIDS u150906 TTR229_Sharon_testIds       Maddy
# MAGIC ^1_1 05/22/09 13:17:26 Batch  15118_47880 INIT bckcett:31540 devlIDS u150906 maddy
# MAGIC ^1_1 05/21/09 13:19:51 Batch  15117_48023 INIT bckcett:31540 devlIDS u150906 maddy
# MAGIC ^1_1 12/06/07 10:30:05 Batch  14585_37808 INIT bckcetl ids20 dsadm dadm
# MAGIC ^1_1 10/03/07 10:51:00 Batch  14521_39063 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/09/06 14:25:59 Batch  14162_51974 PROMOTE bckcetl ids20 dsadm Keith for Sharon
# MAGIC ^1_1 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_7 10/05/06 14:35:41 Batch  14158_52545 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_7 10/05/06 14:34:04 Batch  14158_52446 INIT bckcett devlIDS30 u10157 SA
# MAGIC ^1_6 10/05/06 14:31:17 Batch  14158_52278 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/21/06 16:08:28 Batch  14144_58111 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/20/06 23:19:38 Batch  14143_83979 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/20/06 22:15:19 Batch  14143_80124 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/19/06 09:53:29 Batch  14142_35614 INIT bckcett devlIDS30 u03651 steffy
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data the BlueBasicBusiness BBB for loading to the IDS AGNT_TERR table
# MAGIC                             Lookups are by the IDS Member Group and Subscriber table for the Marketing Terriroty information
# MAGIC       
# MAGIC INPUTS:       BBB Agent Profile system
# MAGIC                       tables:  PARTY, TERRITORY, P
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew   08/02/2006  -  Originally Programmed
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------    ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      -------------------------
# MAGIC Bhoomi Dasari    02/24/2009              Program logic changed                                                TTR229/15       devlIDS                             Steph Goddard         03/11/2009

# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of IDS Agent Addr extract from BlueQ sql server database BBB.
# MAGIC Builds table records with source system BBBAGNT
# MAGIC Extracts from the following system:
# MAGIC SQLServer: SQL10\\SQL10
# MAGIC Database: BlueQ_prod 
# MAGIC Tables:  PARTY, PARTY_TERRITORY, and TERRITORY.
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, upper, substring, to_date, date_format, concat, length, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value('RunCycle','100')
RunID = get_widget_value('RunID','2006070123456')
CurrDate = get_widget_value('CurrDate','2009-01-01')
BBBOwner = get_widget_value('BBBOwner','')
bbb_secret_name = get_widget_value('bbb_secret_name','')

jdbc_url, jdbc_props = get_db_config(bbb_secret_name)
extract_query = f"""SELECT 
TERRITORY.TERR_CK,
TERRITORY.TERR_CD, 
TERRITORY.MKT_SEG,
PARTY.SRC_SYS_ASG_ID, 
PARTY.EFF_DT, 
PARTY.INACTIVATE_DT, 
TYP.PARTY_TYP_DESC, 
PROD.PARTY_CK 

FROM 
((({BBBOwner}.PARTY PARTY INNER JOIN {BBBOwner}.PARTY_TERRITORY TERR ON PARTY.PARTY_CK = TERR.PARTY_CK) INNER JOIN {BBBOwner}.TERRITORY TERRITORY ON TERR.TERR_CK = TERRITORY.TERR_CK) LEFT JOIN {BBBOwner}.PRODUCER PROD ON PARTY.PARTY_CK = PROD.PARTY_CK) INNER JOIN {BBBOwner}.PARTY_TYPE TYP ON PARTY.PARTY_TYP_CK = TYP.PARTY_TYP_CK
"""
df_BlueBusiness = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BlueBusiness = df_BlueBusiness.select(
    col("TERR_CK").alias("TERR_CK"),
    col("TERR_CD").alias("TERR_CD"),
    col("MKT_SEG").alias("MKT_SEG"),
    col("SRC_SYS_ASG_ID").alias("SRC_SYS_ASG_ID"),
    col("EFF_DT").alias("EFF_DT"),
    col("INACTIVATE_DT").alias("INACTIVATE_DT"),
    col("PARTY_TYP_DESC").alias("PARTY_TYP_DESC"),
    col("PARTY_CK").alias("PARTY_CK")
)

df_StripFields = df_BlueBusiness.select(
    col("TERR_CK").alias("TERR_CK"),
    trim(strip_field(col("TERR_CD"))).alias("TERR_CD"),
    trim(strip_field(col("MKT_SEG"))).alias("MKT_SEG"),
    trim(strip_field(col("SRC_SYS_ASG_ID"))).alias("SRC_SYS_ASG_ID"),
    date_format(col("EFF_DT"), "yyyy-MM-dd").alias("EFF_DT"),
    date_format(col("INACTIVATE_DT"), "yyyy-MM-dd").alias("INACTIVATE_DT"),
    trim(strip_field(col("PARTY_TYP_DESC"))).alias("PARTY_TYP_DESC"),
    col("PARTY_CK").alias("PARTY_CK")
)

df_Strip1 = df_StripFields.filter(
    (
        (
            (
                (to_date(col("EFF_DT"), "yyyy-MM-dd") <= to_date(lit(CurrDate), "yyyy-MM-dd"))
                | col("EFF_DT").isNull()
            )
            & (
                (to_date(col("INACTIVATE_DT"), "yyyy-MM-dd") >= to_date(lit(CurrDate), "yyyy-MM-dd"))
                | col("INACTIVATE_DT").isNull()
            )
            & (col("PARTY_TYP_DESC") == lit("AGENT"))
            & (~col("PARTY_CK").isNull())
        )
        |
        (
            (
                (to_date(col("EFF_DT"), "yyyy-MM-dd") <= to_date(lit(CurrDate), "yyyy-MM-dd"))
                | col("EFF_DT").isNull()
            )
            & (
                (to_date(col("INACTIVATE_DT"), "yyyy-MM-dd") >= to_date(lit(CurrDate), "yyyy-MM-dd"))
                | col("INACTIVATE_DT").isNull()
            )
            & (col("PARTY_TYP_DESC") == lit("AGENCY"))
            & (substring(col("SRC_SYS_ASG_ID"), -4, 4) == "0000")
        )
    )
)

df_hf_agnt_mktng_terr_extr_dupes = df_Strip1.select(
    trim(col("SRC_SYS_ASG_ID")).alias("SRC_SYS_ASG_ID"),
    col("TERR_CD").alias("TERR_CD"),
    upper(col("MKT_SEG")).alias("AGNT_TERR_LVL_CD")
)

df_BusinessRulesInput = dedup_sort(
    df_hf_agnt_mktng_terr_extr_dupes,
    ["SRC_SYS_ASG_ID","TERR_CD","AGNT_TERR_LVL_CD"],
    []
)

df_BusinessRules = (
    df_BusinessRulesInput
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("BBBAGNT"))
    .withColumn("PRI_KEY_STRING", concat(lit("BBBAGNT;"), trim(col("SRC_SYS_ASG_ID")), lit(";"), trim(col("TERR_CD")), lit(";"), trim(col("AGNT_TERR_LVL_CD"))))
    .withColumn("AGNT_ID", when(length(trim(col("SRC_SYS_ASG_ID"))) == 0, lit("UNK")).otherwise(trim(col("SRC_SYS_ASG_ID"))))
    .withColumn("MKTNG_TERR_CD", col("TERR_CD"))
    .withColumn("MKTNG_TERR_CAT_CD", upper(col("AGNT_TERR_LVL_CD")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
)

df_hf_agnt_mktng_terr_lookup = spark.read.parquet("hf_agnt_mktng_terr_lookup.parquet")

df_PrimaryKeyJoined = df_BusinessRules.alias("Transform").join(
    df_hf_agnt_mktng_terr_lookup.alias("lkup"),
    (
        (trim(col("Transform.SRC_SYS_CD")) == trim(col("lkup.SRC_SYS_CD")))
        & (trim(col("Transform.AGNT_ID")) == trim(col("lkup.AGNT_ID")))
        & (trim(col("Transform.MKTNG_TERR_CD")) == trim(col("lkup.MKTNG_TERR_CD")))
        & (trim(col("Transform.MKTNG_TERR_CAT_CD")) == trim(col("lkup.MKTNG_TERR_CAT_CD")))
    ),
    how="left"
)

df_PrimaryKeyJoined = df_PrimaryKeyJoined.withColumn(
    "AGNT_TERR_SK_temp",
    when(col("lkup.AGNT_TERR_SK").isNull(), lit(None)).otherwise(col("lkup.AGNT_TERR_SK"))
)

df_enriched = df_PrimaryKeyJoined.withColumnRenamed("AGNT_TERR_SK_temp", "AGNT_TERR_SK")

df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "AGNT_TERR_SK",
    <schema>,
    <secret_name>
)

df_enriched2 = df_enriched.withColumn(
    "NewCrtRunCycExtcnSk",
    when(col("lkup.AGNT_TERR_SK").isNull(), lit(RunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_PrimaryKeyOut = df_enriched2.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("AGNT_TERR_SK").alias("AGNT_TERR_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("Transform.AGNT_ID").alias("AGNT_ID"),
    col("Transform.MKTNG_TERR_CD").alias("MKTNG_TERR_ID"),
    col("Transform.MKTNG_TERR_CAT_CD").alias("MKTNG_TERR_CAT_CD"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_updt = df_enriched2.filter(col("lkup.AGNT_ID").isNull()).select(
    trim(col("Transform.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    trim(col("Transform.AGNT_ID")).alias("AGNT_ID"),
    col("Transform.MKTNG_TERR_CD").alias("MKTNG_TERR_CD"),
    trim(col("Transform.MKTNG_TERR_CAT_CD")).alias("MKTNG__TERR_LVL_CD"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("AGNT_TERR_SK").alias("AGNT_TERR_SK")
)

df_updt_final = df_updt.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("AGNT_ID"), <...>, " ").alias("AGNT_ID"),
    rpad(col("MKTNG_TERR_CD"), 3, " ").alias("MKTNG_TERR_CD"),
    rpad(col("MKTNG__TERR_LVL_CD"), <...>, " ").alias("MKTNG__TERR_LVL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("AGNT_TERR_SK")
)

write_files(
    df_updt_final,
    "hf_agnt_mktng_terr.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_final = df_PrimaryKeyOut.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("AGNT_TERR_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("AGNT_ID"), <...>, " ").alias("AGNT_ID"),
    rpad(col("MKTNG_TERR_ID"), 3, " ").alias("MKTNG_TERR_ID"),
    rpad(col("MKTNG_TERR_CAT_CD"), <...>, " ").alias("MKTNG_TERR_CAT_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/key/BBBAgntMktngTerrExtr.AgntMktngTerr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)