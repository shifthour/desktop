# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiPlnNtwkProfFctlyExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Net Cat Prof Ref and Net Cat Fac Ref to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiRefExtrSeq
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                    Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                                                 -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl        Kalyan Neelam          2013-10-31
# MAGIC Praveen Annam                                          2014-07-14           5115 BHI                               FTP stage added to transfer in binary mode               EnterpriseNewDevl        Bhoomi Dasari           7/15/2014
# MAGIC Praneeth Kakarla                                        2019-07-29           US-136671                             Codinging from MA Datamart to BCA                            EnterpriseDev2          Kalyan Neelam          2019-08-09
# MAGIC                                                                                                                              (Blue Cross Association) for Plan Network Category Facility file
# MAGIC Mohan Karnati                                            2020-08-03               US-252543               Including High Performance n/w  in product short    
# MAGIC                                                                                                                                            while extracting in the source query                               EnterpriseDev1             Hugh Sisson              2020-08-18
# MAGIC 
# MAGIC Mohan Karnati                                           2021-12-05           US 343836                          Added MA Products 'BMADVH','BMADVP' to EDW source    EnterpriseDev2           Jeyaprasanna            2021-04-20
# MAGIC                                                                                                                                            Removed MA Datamart Source extract

# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC Extract Plan Network Category Proof and Facility data, these two files share the same data with different column and file names.
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# ----------------------------------------------------------------
# Retrieve parameter values
EDWOwner = get_widget_value('EDWOwner','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')
MADataMartAsstOwner = get_widget_value('MADataMartAsstOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# ----------------------------------------------------------------
# DB2ConnectorPX Stage: Prod
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_Prod = f"""
SELECT DISTINCT 
    PROD_ABBR,
    PROD_SH_NM
FROM {EDWOwner}.PROD_D
WHERE PROD_SH_NM IN ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+','HP','BMADVH','BMADVP')
  AND SRC_SYS_CD = 'FACETS'
"""

df_Prod = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Prod)
    .load()
)

# ----------------------------------------------------------------
# CTransformerStage: Trns
df_Trns = df_Prod.select(
    F.lit('240').alias("BHI_HOME_PLN_ID"),
    F.col("PROD_ABBR").alias("PLN_NTWK_CAT_CD_FCLTY"),
    # User-defined function PadString used directly
    PadString(F.col("PROD_SH_NM"), ' ', F.lit(30)).alias("PLN_NTWK_CAT_DESC_FCLTY")
)

# ----------------------------------------------------------------
# PxCopy Stage: Copy
df_Prof_Extr = df_Trns.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("PLN_NTWK_CAT_CD_FCLTY").alias("PLN_NTWK_CAT_CD_PROF"),
    F.col("PLN_NTWK_CAT_DESC_FCLTY").alias("PLN_NTWK_CAT_DESC_PROF")
)

df_Fac_Extr = df_Trns.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("PLN_NTWK_CAT_CD_FCLTY").alias("PLN_NTWK_CAT_CD_FCLTY"),
    F.col("PLN_NTWK_CAT_DESC_FCLTY").alias("PLN_NTWK_CAT_DESC_FCLTY")
)

df_Count = df_Trns.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID")
)

# ----------------------------------------------------------------
# PxSequentialFile Stage: net_cat_fac_ref (writing df_Fac_Extr)
df_Fac_Extr_final = df_Fac_Extr.withColumn(
    "BHI_HOME_PLN_ID", rpad("BHI_HOME_PLN_ID", 3, " ")
).withColumn(
    "PLN_NTWK_CAT_CD_FCLTY", rpad("PLN_NTWK_CAT_CD_FCLTY", 2, " ")
).withColumn(
    "PLN_NTWK_CAT_DESC_FCLTY", rpad("PLN_NTWK_CAT_DESC_FCLTY", 30, " ")
).select(
    "BHI_HOME_PLN_ID",
    "PLN_NTWK_CAT_CD_FCLTY",
    "PLN_NTWK_CAT_DESC_FCLTY"
)

write_files(
    df_Fac_Extr_final,
    f"{adls_path_publish}/external/net_cat_fac_ref",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# PxSequentialFile Stage: net_cat_prof_ref (writing df_Prof_Extr)
df_Prof_Extr_final = df_Prof_Extr.withColumn(
    "BHI_HOME_PLN_ID", rpad("BHI_HOME_PLN_ID", 3, " ")
).withColumn(
    "PLN_NTWK_CAT_CD_PROF", rpad("PLN_NTWK_CAT_CD_PROF", 2, " ")
).withColumn(
    "PLN_NTWK_CAT_DESC_PROF", rpad("PLN_NTWK_CAT_DESC_PROF", 30, " ")
).select(
    "BHI_HOME_PLN_ID",
    "PLN_NTWK_CAT_CD_PROF",
    "PLN_NTWK_CAT_DESC_PROF"
)

write_files(
    df_Prof_Extr_final,
    f"{adls_path_publish}/external/net_cat_prof_ref",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# PxAggregator Stage: Aggregator (counting rows)
df_Aggregator = df_Count.groupBy("BHI_HOME_PLN_ID").agg(F.count(F.lit(1)).alias("COUNT"))

# ----------------------------------------------------------------
# CTransformerStage: Trns_cntrl
# Simulating the loop with ITERATION = 1,2
df_iter = spark.createDataFrame([(1,), (2,)], ["ITERATION"])

df_Trns_cntrl = (
    df_Aggregator
    .crossJoin(df_iter)
    .withColumn(
        "LoopVar",
        F.when(F.col("ITERATION") == 1, F.lit("STD_NET_CAT_FAC_REF"))
         .when(F.col("ITERATION") == 2, F.lit("STD_NET_CAT_PROF_REF"))
         .otherwise(F.lit(""))
    )
    .withColumn(
        "BHI_HOME_PLN_ID", F.col("BHI_HOME_PLN_ID")
    )
    .withColumn(
        "EXTR_NM",
        # User-defined function PadString used directly
        PadString(F.col("LoopVar"), ' ', F.lit(30))
    )
    .withColumn(
        "MIN_CLM_PROCESSED_DT",
        Trim(F.lit(StartDate), '-', 'A')
    )
    .withColumn(
        "MAX_CLM_PRCS_DT",
        Trim(F.lit(EndDate), '-', 'A')
    )
    .withColumn(
        "SUBMSN_DT",
        Trim(F.lit(CurrDate), '-', 'A')
    )
    .withColumn(
        "RCRD_CT",
        F.concat(
            F.expr("Str('0', 10 - LEN(COUNT))"),
            F.col("COUNT").cast(StringType())
        )
    )
    .withColumn(
        "TOT_SUBMT_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_NONCOV_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_ALW_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_PD_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COB_TPL_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COINS_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COPAY_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_DEDCT_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_FFS_EQVLNT_AMT",
        F.when(
            F.expr("Left('00000000000000', 1) <> '-'"),
            F.concat(F.lit('+'), F.lit('00000000000000'))
        ).otherwise(F.lit('00000000000000'))
    )
)

df_Trns_cntrl_final = (
    df_Trns_cntrl
    .withColumn("BHI_HOME_PLN_ID", rpad("BHI_HOME_PLN_ID", 3, " "))
    .withColumn("EXTR_NM", rpad("EXTR_NM", 30, " "))
    .withColumn("MIN_CLM_PROCESSED_DT", rpad("MIN_CLM_PROCESSED_DT", 8, " "))
    .withColumn("MAX_CLM_PRCS_DT", rpad("MAX_CLM_PRCS_DT", 8, " "))
    .withColumn("SUBMSN_DT", rpad("SUBMSN_DT", 8, " "))
    .withColumn("RCRD_CT", rpad("RCRD_CT", 10, " "))
    .withColumn("TOT_SUBMT_AMT", rpad("TOT_SUBMT_AMT", 15, " "))
    .withColumn("TOT_NONCOV_AMT", rpad("TOT_NONCOV_AMT", 15, " "))
    .withColumn("TOT_ALW_AMT", rpad("TOT_ALW_AMT", 15, " "))
    .withColumn("TOT_PD_AMT", rpad("TOT_PD_AMT", 15, " "))
    .withColumn("TOT_COB_TPL_AMT", rpad("TOT_COB_TPL_AMT", 15, " "))
    .withColumn("TOT_COINS_AMT", rpad("TOT_COINS_AMT", 15, " "))
    .withColumn("TOT_COPAY_AMT", rpad("TOT_COPAY_AMT", 15, " "))
    .withColumn("TOT_DEDCT_AMT", rpad("TOT_DEDCT_AMT", 15, " "))
    .withColumn("TOT_FFS_EQVLNT_AMT", rpad("TOT_FFS_EQVLNT_AMT", 15, " "))
    .select(
        "BHI_HOME_PLN_ID",
        "EXTR_NM",
        "MIN_CLM_PROCESSED_DT",
        "MAX_CLM_PRCS_DT",
        "SUBMSN_DT",
        "RCRD_CT",
        "TOT_SUBMT_AMT",
        "TOT_NONCOV_AMT",
        "TOT_ALW_AMT",
        "TOT_PD_AMT",
        "TOT_COB_TPL_AMT",
        "TOT_COINS_AMT",
        "TOT_COPAY_AMT",
        "TOT_DEDCT_AMT",
        "TOT_FFS_EQVLNT_AMT"
    )
)

# ----------------------------------------------------------------
# PxSequentialFile Stage: submission_control
write_files(
    df_Trns_cntrl_final,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)