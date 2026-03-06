# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiProdExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extract Product data to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  None
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
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam            2013-08-01               5115 BHI                                       Original programming                                                  EnterpriseNewDevl          Kalyan Neelam          2013-10-31                                  
# MAGIC Praveen Annam            2014-07-14               5115 BHI                                       FTP stage added to transfer in binary mode               EnterpriseNewDevl          Kalyan Neelam          2014-07-14
# MAGIC Praneeth Kakarla          2019-08-07              US-143252                                     Adding MA Datamart to BCA  for Product file             EnterpriseDev2                Jaideep Mankala       10/09/2019
# MAGIC 
# MAGIC Mohan Karnati              2020-11-30              US#318377                          Modifying logic for BHI_PROD_CAT in Trns stage
# MAGIC                                                                                                                    and including 'HP' members in source extraction                 EnterpriseDev2              Sravya Gorla                12/01/2020 
# MAGIC Mohan Karnati             2021-12-05                 US 343836                          Added MA Products 'BMADVH','BMADVP' to EDW source          EnterpriseDev2                Jeyaprasanna            2021-04-20
# MAGIC                                                                                                                    MA Datamart removed from Source

# MAGIC Extract Prod data,
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')
MADataMartAsstOwner = get_widget_value('MADataMartAsstOwner','')
madatamartasst_secret_name = get_widget_value('madatamartasst_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = (
    "SELECT PROD_ID,\n"
    "       PROD_DESC,\n"
    "       PROD_SH_NM_DLVRY_METH_CD,\n"
    "       PROD_SH_NM\n"
    f"FROM {EDWOwner}.PROD_D\n"
    "WHERE PROD_SH_NM IN ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+','HP','BMADVH','BMADVP')\n"
    "  AND EXPRNC_CAT_CD <> 'FEP'"
)

df_Prod = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trns = (
    df_Prod
    .withColumn("BHI_HOME_PLN_ID", F.lit('240'))
    .withColumn("HOME_PLN_PROD_ID", PadString(F.col("PROD_ID"), F.lit(' '), F.lit(15)))
    .withColumn("HOME_PLN_PROD_NM", PadString(F.col("PROD_DESC"), F.lit(' '), F.lit(50)))
    .withColumn(
        "BHI_PROD_CAT",
        F.when(F.col("PROD_SH_NM") == 'HP', 'EPO')
         .when(F.col("PROD_SH_NM") == 'BMADVH', 'MMO')
         .when(F.col("PROD_SH_NM") == 'BMADVP', 'MPO')
         .otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD"))
    )
    .withColumn("TRACEABILITY_FLD", F.lit('FCTS '))
    .selectExpr(
        "BHI_HOME_PLN_ID",
        "HOME_PLN_PROD_ID",
        "HOME_PLN_PROD_NM",
        "BHI_PROD_CAT",
        "TRACEABILITY_FLD"
    )
)

df_Copy = df_Trns

df_Extract = df_Copy.selectExpr(
    "BHI_HOME_PLN_ID",
    "HOME_PLN_PROD_ID",
    "HOME_PLN_PROD_NM",
    "BHI_PROD_CAT",
    "TRACEABILITY_FLD"
)

df_Count = df_Copy.selectExpr("BHI_HOME_PLN_ID")

df_std_product = df_Extract.select(
    F.rpad("BHI_HOME_PLN_ID", 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad("HOME_PLN_PROD_ID", 15, " ").alias("HOME_PLN_PROD_ID"),
    F.rpad("HOME_PLN_PROD_NM", 50, " ").alias("HOME_PLN_PROD_NM"),
    F.rpad("BHI_PROD_CAT", 3, " ").alias("BHI_PROD_CAT"),
    F.rpad("TRACEABILITY_FLD", 5, " ").alias("TRACEABILITY_FLD")
)

write_files(
    df_std_product,
    f"{adls_path_publish}/external/std_product",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Aggregator = df_Count.groupBy("BHI_HOME_PLN_ID").agg(F.count("*").alias("COUNT"))

df_Trns_cntrl = (
    df_Aggregator
    .withColumn("EXTR_NM", PadString(F.lit("STD_PRODUCT"), F.lit(' '), F.lit(30)))
    .withColumn("MIN_CLM_PROCESSED_DT", Trim(F.lit(StartDate), F.lit('-'), F.lit('A')))
    .withColumn("MAX_CLM_PRCS_DT", Trim(F.lit(EndDate), F.lit('-'), F.lit('A')))
    .withColumn("SUBMSN_DT", Trim(F.lit(CurrDate), F.lit('-'), F.lit('A')))
    .withColumn(
        "RCRD_CT",
        Str(F.lit('0'), F.lit(10) - LEN(F.col("COUNT"))) + F.col("COUNT")
    )
    .withColumn(
        "TOT_SUBMT_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_NONCOV_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_ALW_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_PD_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COB_TPL_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COINS_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_COPAY_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_DEDCT_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
    .withColumn(
        "TOT_FFS_EQVLNT_AMT",
        F.when(
            Left(F.lit('00000000000000'), F.lit(1)) != F.lit('-'),
            F.lit('+') + F.lit('00000000000000')
        ).otherwise(F.lit('00000000000000'))
    )
)

df_submission_control = df_Trns_cntrl.select(
    F.rpad("BHI_HOME_PLN_ID", 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad("EXTR_NM", 30, " ").alias("EXTR_NM"),
    F.rpad("MIN_CLM_PROCESSED_DT", 8, " ").alias("MIN_CLM_PROCESSED_DT"),
    F.rpad("MAX_CLM_PRCS_DT", 8, " ").alias("MAX_CLM_PRCS_DT"),
    F.rpad("SUBMSN_DT", 8, " ").alias("SUBMSN_DT"),
    F.rpad("RCRD_CT", 10, " ").alias("RCRD_CT"),
    F.rpad("TOT_SUBMT_AMT", 15, " ").alias("TOT_SUBMT_AMT"),
    F.rpad("TOT_NONCOV_AMT", 15, " ").alias("TOT_NONCOV_AMT"),
    F.rpad("TOT_ALW_AMT", 15, " ").alias("TOT_ALW_AMT"),
    F.rpad("TOT_PD_AMT", 15, " ").alias("TOT_PD_AMT"),
    F.rpad("TOT_COB_TPL_AMT", 15, " ").alias("TOT_COB_TPL_AMT"),
    F.rpad("TOT_COINS_AMT", 15, " ").alias("TOT_COINS_AMT"),
    F.rpad("TOT_COPAY_AMT", 15, " ").alias("TOT_COPAY_AMT"),
    F.rpad("TOT_DEDCT_AMT", 15, " ").alias("TOT_DEDCT_AMT"),
    F.rpad("TOT_FFS_EQVLNT_AMT", 15, " ").alias("TOT_FFS_EQVLNT_AMT")
)

write_files(
    df_submission_control,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)