# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya Raju               2013-08-20          5114                             Original Programming                                                                             IntegrateWrhsDevl    Peter Marshall              10/18/2013  
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela        2014-08-05           5345                           Changed reject file name                                                                       IntegrateNewDevl     Jag Yelavarthi               2014-08-07

# MAGIC Job Name: IdsDmMbrshDmClsPlnCustSvcIdCardPhnLoad
# MAGIC Read Load File created in the IdsDmMbrshDmClsPlnCustSvcIdCardPhnExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("CLS_PLN_CUST_SVC_IDCRD_EFF_DT", TimestampType(), True),
    StructField("CLS_PLN_PROD_CAT_CD", StringType(), True),
    StructField("CLS_PLN_PROD_CAT_NM", StringType(), True),
    StructField("CLS_PLN_CUST_SVC_IDCRD_TERM_DT", TimestampType(), True),
    StructField("CUST_SVC_LOCAL_PHN_NO", StringType(), True),
    StructField("CUST_SVC_TLLFR_PHN_NO", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_csv_load = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_EFF_DT").alias("CLS_PLN_CUST_SVC_IDCRD_EFF_DT"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("CLS_PLN_PROD_CAT_NM").alias("CLS_PLN_PROD_CAT_NM"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_TERM_DT").alias("CLS_PLN_CUST_SVC_IDCRD_TERM_DT"),
    F.col("CUST_SVC_LOCAL_PHN_NO").alias("CUST_SVC_LOCAL_PHN_NO"),
    F.col("CUST_SVC_TLLFR_PHN_NO").alias("CUST_SVC_TLLFR_PHN_NO"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout = df_cpy_forBuffer.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("SUBGRP_ID"), <...>, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("CLS_ID"), <...>, " ").alias("CLS_ID"),
    F.rpad(F.col("CLS_PLN_ID"), <...>, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_EFF_DT").alias("CLS_PLN_CUST_SVC_IDCRD_EFF_DT"),
    F.rpad(F.col("CLS_PLN_PROD_CAT_CD"), <...>, " ").alias("CLS_PLN_PROD_CAT_CD"),
    F.rpad(F.col("CLS_PLN_PROD_CAT_NM"), <...>, " ").alias("CLS_PLN_PROD_CAT_NM"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_TERM_DT").alias("CLS_PLN_CUST_SVC_IDCRD_TERM_DT"),
    F.rpad(F.col("CUST_SVC_LOCAL_PHN_NO"), <...>, " ").alias("CUST_SVC_LOCAL_PHN_NO"),
    F.rpad(F.col("CUST_SVC_TLLFR_PHN_NO"), <...>, " ").alias("CUST_SVC_TLLFR_PHN_NO"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmCustSvcldCardPhnLoad_Odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_temp",
    jdbc_url,
    jdbc_props
)

df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmCustSvcldCardPhnLoad_Odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE INTO MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN AS T
USING STAGING.IdsDmCustSvcldCardPhnLoad_Odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.GRP_ID = S.GRP_ID
    AND T.SUBGRP_ID = S.SUBGRP_ID
    AND T.CLS_ID = S.CLS_ID
    AND T.CLS_PLN_ID = S.CLS_PLN_ID
    AND T.PROD_ID = S.PROD_ID
    AND T.CLS_PLN_CUST_SVC_IDCRD_EFF_DT = S.CLS_PLN_CUST_SVC_IDCRD_EFF_DT
WHEN MATCHED THEN
    UPDATE SET
        T.CLS_PLN_PROD_CAT_CD = S.CLS_PLN_PROD_CAT_CD,
        T.CLS_PLN_PROD_CAT_NM = S.CLS_PLN_PROD_CAT_NM,
        T.CLS_PLN_CUST_SVC_IDCRD_TERM_DT = S.CLS_PLN_CUST_SVC_IDCRD_TERM_DT,
        T.CUST_SVC_LOCAL_PHN_NO = S.CUST_SVC_LOCAL_PHN_NO,
        T.CUST_SVC_TLLFR_PHN_NO = S.CUST_SVC_TLLFR_PHN_NO,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        GRP_ID,
        SUBGRP_ID,
        CLS_ID,
        CLS_PLN_ID,
        PROD_ID,
        CLS_PLN_CUST_SVC_IDCRD_EFF_DT,
        CLS_PLN_PROD_CAT_CD,
        CLS_PLN_PROD_CAT_NM,
        CLS_PLN_CUST_SVC_IDCRD_TERM_DT,
        CUST_SVC_LOCAL_PHN_NO,
        CUST_SVC_TLLFR_PHN_NO,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.GRP_ID,
        S.SUBGRP_ID,
        S.CLS_ID,
        S.CLS_PLN_ID,
        S.PROD_ID,
        S.CLS_PLN_CUST_SVC_IDCRD_EFF_DT,
        S.CLS_PLN_PROD_CAT_CD,
        S.CLS_PLN_PROD_CAT_NM,
        S.CLS_PLN_CUST_SVC_IDCRD_TERM_DT,
        S.CUST_SVC_LOCAL_PHN_NO,
        S.CUST_SVC_TLLFR_PHN_NO,
        S.LAST_UPDT_RUN_CYC_NO
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("CLS_PLN_CUST_SVC_IDCRD_EFF_DT", TimestampType(), True),
    StructField("CLS_PLN_PROD_CAT_CD", StringType(), True),
    StructField("CLS_PLN_PROD_CAT_NM", StringType(), True),
    StructField("CLS_PLN_CUST_SVC_IDCRD_TERM_DT", TimestampType(), True),
    StructField("CUST_SVC_LOCAL_PHN_NO", StringType(), True),
    StructField("CUST_SVC_TLLFR_PHN_NO", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej_empty = spark.createDataFrame([], schema_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej)

df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej = df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej_empty.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("SUBGRP_ID"), <...>, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("CLS_ID"), <...>, " ").alias("CLS_ID"),
    F.rpad(F.col("CLS_PLN_ID"), <...>, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_EFF_DT").alias("CLS_PLN_CUST_SVC_IDCRD_EFF_DT"),
    F.rpad(F.col("CLS_PLN_PROD_CAT_CD"), <...>, " ").alias("CLS_PLN_PROD_CAT_CD"),
    F.rpad(F.col("CLS_PLN_PROD_CAT_NM"), <...>, " ").alias("CLS_PLN_PROD_CAT_NM"),
    F.col("CLS_PLN_CUST_SVC_IDCRD_TERM_DT").alias("CLS_PLN_CUST_SVC_IDCRD_TERM_DT"),
    F.rpad(F.col("CUST_SVC_LOCAL_PHN_NO"), <...>, " ").alias("CUST_SVC_LOCAL_PHN_NO"),
    F.rpad(F.col("CUST_SVC_TLLFR_PHN_NO"), <...>, " ").alias("CUST_SVC_TLLFR_PHN_NO"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHNout_rej,
    f"{adls_path}/load/MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)