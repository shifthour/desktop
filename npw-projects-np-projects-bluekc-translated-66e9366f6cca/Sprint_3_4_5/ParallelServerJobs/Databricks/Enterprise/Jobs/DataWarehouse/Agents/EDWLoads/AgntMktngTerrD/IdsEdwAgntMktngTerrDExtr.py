# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called from EDW Commissions 
# MAGIC  Full replace from IDS AGNT_MKTNG_TERR TO EDW.AGNT_MKTNG_TERR_D.
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids into edw 
# MAGIC   
# MAGIC       
# MAGIC                   IDS - read from source all records 
# MAGIC                   Provide default values for runcycle values
# MAGIC                   decode code SKs
# MAGIC                   No filter or variance logic
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                        Date               Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Landon Hall                    10-01-2008           161023                        Originally Programmed                               devlEDW
# MAGIC 
# MAGIC Bhoomi D                        24-02-2009          TTR-339/15                 Added IsNull Lkup's                                  devlEDW                     Steph Goddard            03/11/2009
# MAGIC 
# MAGIC Rama Kamjula                 12-10-2013           5114                           Rewritten from server to parallel version      EnterpriseWrhsDevl
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC                                                                                                                                                                                                                    
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rama Kamjula                  2013-11-25            5114                                                         Original Programming                                   EnterpriseWrhsDevl                                   Jag Yelavarthi                    2013-12-22           
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC JobName: IdsEdwAgntMktngTerrDExtr
# MAGIC 
# MAGIC This job creates load file for  EDW table : AGNT_MKTNG_TERR_D
# MAGIC IDS Extract from  AGNT_MKTNG_TERR
# MAGIC Load file for  AGNT_MKTNG_TERR_D
# MAGIC IDS AgntMktngTerr is actually created from DSADG000 and is not affliated with IDS Commisisons
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_AGNT_MKTNG_TERR = """SELECT 
AGNT_MKTNG_TERR.AGNT_MKTNG_TERR_SK,
COALESCE(CD.TRGT_CD, 'NA') SRC_SYS_CD,
AGNT_MKTNG_TERR.AGNT_ID,
AGNT_MKTNG_TERR.MKTNG_TERR_ID,
AGNT_MKTNG_TERR.MKTNG_TERR_CAT_CD_SK,
AGNT_MKTNG_TERR.LAST_UPDT_RUN_CYC_EXCTN_SK,
AGNT_MKTNG_TERR.AGNT_SK,
AGNT_MKTNG_TERR.MKTNG_TERR_SK
FROM {0}.AGNT_MKTNG_TERR AGNT_MKTNG_TERR
LEFT JOIN {0}.CD_MPPNG CD
  ON AGNT_MKTNG_TERR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
""".format(IDSOwner)
df_db2_AGNT_MKTNG_TERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_AGNT_MKTNG_TERR)
    .load()
)

extract_query_db2_CD_MPPNG = """SELECT
CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM
FROM {0}.CD_MPPNG
""".format(IDSOwner)
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

df_lkp_Codes = (
    df_db2_AGNT_MKTNG_TERR.alias("lnk_AgntMktngTerr_In")
    .join(
        df_db2_CD_MPPNG.alias("lnk_MktTerrCat"),
        F.col("lnk_AgntMktngTerr_In.MKTNG_TERR_CAT_CD_SK") == F.col("lnk_MktTerrCat.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_AgntMktngTerr_In.AGNT_MKTNG_TERR_SK").alias("AGNT_MKTNG_TERR_SK"),
        F.col("lnk_AgntMktngTerr_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_AgntMktngTerr_In.AGNT_ID").alias("AGNT_ID"),
        F.col("lnk_AgntMktngTerr_In.MKTNG_TERR_ID").alias("MKTNG_TERR_ID"),
        F.col("lnk_AgntMktngTerr_In.MKTNG_TERR_CAT_CD_SK").alias("MKTNG_TERR_CAT_CD_SK"),
        F.col("lnk_AgntMktngTerr_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_AgntMktngTerr_In.AGNT_SK").alias("AGNT_SK"),
        F.col("lnk_AgntMktngTerr_In.MKTNG_TERR_SK").alias("MKTNG_TERR_SK"),
        F.col("lnk_MktTerrCat.TRGT_CD").alias("TRGT_CD"),
        F.col("lnk_MktTerrCat.TRGT_CD_NM").alias("TRGT_CD_NM")
    )
)

df_lnk_AgntMktngTerr_Out = (
    df_lkp_Codes
    .filter((F.col("AGNT_MKTNG_TERR_SK") != 0) & (F.col("AGNT_MKTNG_TERR_SK") != 1))
    .select(
        F.col("AGNT_MKTNG_TERR_SK").alias("AGNT_MKTNG_TERR_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("MKTNG_TERR_ID").alias("MKTNG_TERR_ID"),
        F.when(
            F.col("TRGT_CD").isNull() | (trim(F.col("TRGT_CD")) == ""), 
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD")).alias("MKTNG_TERR_CAT_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("AGNT_SK").alias("AGNT_SK"),
        F.col("MKTNG_TERR_SK").alias("MKTNG_TERR_SK"),
        F.when(
            F.col("TRGT_CD_NM").isNull() | (trim(F.col("TRGT_CD_NM")) == ""), 
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_NM")).alias("MKTNG_TERR_CAT_NM"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MKTNG_TERR_CAT_CD_SK").alias("MKTNG_TERR_CAT_CD_SK")
    )
)

df_lnk_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "NA",
            "NA",
            "1753-01-01",
            "1753-01-01",
            1,
            1,
            "NA",
            100,
            100,
            1
        )
    ],
    [
        "AGNT_MKTNG_TERR_SK",
        "SRC_SYS_CD",
        "AGNT_ID",
        "MKTNG_TERR_ID",
        "MKTNG_TERR_CAT_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_SK",
        "MKTNG_TERR_SK",
        "MKTNG_TERR_CAT_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MKTNG_TERR_CAT_CD_SK"
    ]
).limit(1)

df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            "UNK",
            100,
            100,
            0
        )
    ],
    [
        "AGNT_MKTNG_TERR_SK",
        "SRC_SYS_CD",
        "AGNT_ID",
        "MKTNG_TERR_ID",
        "MKTNG_TERR_CAT_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_SK",
        "MKTNG_TERR_SK",
        "MKTNG_TERR_CAT_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MKTNG_TERR_CAT_CD_SK"
    ]
).limit(1)

df_fnl_Combine = (
    df_lnk_AgntMktngTerr_Out
    .unionByName(df_lnk_NA)
    .unionByName(df_lnk_UNK)
)

df_fnl_Combine_ordered = df_fnl_Combine.select(
    "AGNT_MKTNG_TERR_SK",
    "SRC_SYS_CD",
    "AGNT_ID",
    "MKTNG_TERR_ID",
    "MKTNG_TERR_CAT_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "AGNT_SK",
    "MKTNG_TERR_SK",
    "MKTNG_TERR_CAT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MKTNG_TERR_CAT_CD_SK"
)

df_final = (
    df_fnl_Combine_ordered
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/AGNT_MKTNG_TERR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)