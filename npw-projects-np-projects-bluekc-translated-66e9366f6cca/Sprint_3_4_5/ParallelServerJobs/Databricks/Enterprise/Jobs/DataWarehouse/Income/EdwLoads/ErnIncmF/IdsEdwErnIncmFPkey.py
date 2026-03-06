# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING : Primary Key job for ERN_INCM_F table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Srikanth Mettpalli      10/17/2013        5114                              Original Programming                                                                            EnterpriseWrhsDevl   Peter Marshall             12/12/2013  
# MAGIC Kalyan Neelam          2014-06-20          5235                              Added 2 new columns SUB_SBSDY_AMT and SUB_RESP_AMT     EnterpriseNewDevl   Bhoomi Dasari             6/22/2014 
# MAGIC                                                                                                    on end

# MAGIC Job Name: IdsEdwErnIncmFPkey
# MAGIC Read ERN_INCM_F.ds Dataset created in the IdsEdwErnIncmFExtr Job.
# MAGIC Copy for creating two output streams
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC Read K_ERN_INCM_F Table to pull the Natural Keys and the Skey.
# MAGIC Left Join on Natural Keys
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Write ERN_INCM_F.dat for the IdsEdwErnIncmFLoad Job.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
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


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

schema_ds_ERN_INCM_F_out = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("ERN_INCM_DT_SK", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("BILL_ENTY_UNIQ_KEY", StringType(), True),
    StructField("BILL_INVC_ID", StringType(), True),
    StructField("ERN_INCM_TYP_CD", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("PROD_BILL_CMPNT_ID", StringType(), True),
    StructField("ERN_INCM_DISP_CD", StringType(), True),
    StructField("FEE_DSCNT_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("BILL_ENTY_SK", StringType(), True),
    StructField("CLS_SK", StringType(), True),
    StructField("CLS_PLN_SK", StringType(), True),
    StructField("EXPRNC_CAT_SK", StringType(), True),
    StructField("FEE_DSCNT_SK", StringType(), True),
    StructField("FNCL_LOB_SK", StringType(), True),
    StructField("GRP_SK", StringType(), True),
    StructField("PROD_SK", StringType(), True),
    StructField("PROD_SH_NM_SK", StringType(), True),
    StructField("SUB_SK", StringType(), True),
    StructField("SUBGRP_SK", StringType(), True),
    StructField("BILL_ENTY_LVL_CD", StringType(), True),
    StructField("BILL_ENTY_LVL_NM", StringType(), True),
    StructField("INVC_TYP_CD", StringType(), True),
    StructField("INVC_TYP_NM", StringType(), True),
    StructField("DP_IN", StringType(), True),
    StructField("INVC_CUR_RCRD_IN", StringType(), True),
    StructField("ERN_INCM_YR_MO_SK", StringType(), True),
    StructField("INVC_BILL_DUE_DT_SK", StringType(), True),
    StructField("INVC_BILL_DUE_YR_MO_SK", StringType(), True),
    StructField("INVC_BILL_END_DT_SK", StringType(), True),
    StructField("INVC_BILL_END_YR_MO_SK", StringType(), True),
    StructField("INVC_CRT_DT_SK", StringType(), True),
    StructField("ERN_INCM_AMT", StringType(), True),
    StructField("SUB_UNIQ_KEY", StringType(), True),
    StructField("ERN_INCM_DISP_NM", StringType(), True),
    StructField("ERN_INCM_TYP_NM", StringType(), True),
    StructField("EXPRNC_CAT_CD", StringType(), True),
    StructField("FNCL_LOB_CD", StringType(), True),
    StructField("PROD_SH_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("BILL_ENTY_LVL_CD_SK", StringType(), True),
    StructField("ERN_INCM_DISP_CD_SK", StringType(), True),
    StructField("ERN_INCM_TYP_CD_SK", StringType(), True),
    StructField("INVC_TYP_CD_SK", StringType(), True),
    StructField("SUB_SBSDY_AMT", StringType(), True),
    StructField("SUB_RESP_AMT", StringType(), True)
])

df_ds_ERN_INCM_F_out = spark.read.schema(schema_ds_ERN_INCM_F_out).parquet(f"{adls_path}/ds/ERN_INCM_F.parquet")

df_lnk_ErnIncmData_L_in = df_ds_ERN_INCM_F_out
df_lnk_rdp_naturalkeys_in = df_ds_ERN_INCM_F_out

df_rdp_Natural_Keys_out = dedup_sort(
    df_lnk_rdp_naturalkeys_in,
    [
        "SRC_SYS_CD","ERN_INCM_DT_SK","GRP_ID","SUBGRP_ID","BILL_ENTY_UNIQ_KEY",
        "BILL_INVC_ID","ERN_INCM_TYP_CD","CLS_ID","CLS_PLN_ID","PROD_ID",
        "PROD_BILL_CMPNT_ID","ERN_INCM_DISP_CD","FEE_DSCNT_ID"
    ],
    []
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_K_ERN_INCM_F_in = f"""
SELECT
SRC_SYS_CD,
ERN_INCM_DT_SK,
GRP_ID,
SUBGRP_ID,
BILL_ENTY_UNIQ_KEY,
BILL_INVC_ID,
ERN_INCM_TYP_CD,
CLS_ID,
CLS_PLN_ID,
PROD_ID,
PROD_BILL_CMPNT_ID,
ERN_INCM_DISP_CD,
FEE_DSCNT_ID,
ERN_INCM_SK
FROM {EDWOwner}.K_ERN_INCM_F
"""

df_db2_K_ERN_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_ERN_INCM_F_in)
    .load()
)

df_jn_ErnIncmF_join = df_rdp_Natural_Keys_out.alias("lnk_ErnIncmF_L_in").join(
    df_db2_K_ERN_INCM_F_in.alias("lnk_KErnIncmFPkey_out"),
    on=[
        F.col("lnk_ErnIncmF_L_in.SRC_SYS_CD")==F.col("lnk_KErnIncmFPkey_out.SRC_SYS_CD"),
        F.col("lnk_ErnIncmF_L_in.ERN_INCM_DT_SK")==F.col("lnk_KErnIncmFPkey_out.ERN_INCM_DT_SK"),
        F.col("lnk_ErnIncmF_L_in.GRP_ID")==F.col("lnk_KErnIncmFPkey_out.GRP_ID"),
        F.col("lnk_ErnIncmF_L_in.SUBGRP_ID")==F.col("lnk_KErnIncmFPkey_out.SUBGRP_ID"),
        F.col("lnk_ErnIncmF_L_in.BILL_ENTY_UNIQ_KEY")==F.col("lnk_KErnIncmFPkey_out.BILL_ENTY_UNIQ_KEY"),
        F.col("lnk_ErnIncmF_L_in.BILL_INVC_ID")==F.col("lnk_KErnIncmFPkey_out.BILL_INVC_ID"),
        F.col("lnk_ErnIncmF_L_in.ERN_INCM_TYP_CD")==F.col("lnk_KErnIncmFPkey_out.ERN_INCM_TYP_CD"),
        F.col("lnk_ErnIncmF_L_in.CLS_ID")==F.col("lnk_KErnIncmFPkey_out.CLS_ID"),
        F.col("lnk_ErnIncmF_L_in.CLS_PLN_ID")==F.col("lnk_KErnIncmFPkey_out.CLS_PLN_ID"),
        F.col("lnk_ErnIncmF_L_in.PROD_ID")==F.col("lnk_KErnIncmFPkey_out.PROD_ID"),
        F.col("lnk_ErnIncmF_L_in.PROD_BILL_CMPNT_ID")==F.col("lnk_KErnIncmFPkey_out.PROD_BILL_CMPNT_ID"),
        F.col("lnk_ErnIncmF_L_in.ERN_INCM_DISP_CD")==F.col("lnk_KErnIncmFPkey_out.ERN_INCM_DISP_CD"),
        F.col("lnk_ErnIncmF_L_in.FEE_DSCNT_ID")==F.col("lnk_KErnIncmFPkey_out.FEE_DSCNT_ID")
    ],
    how="left"
)

df_jn_ErnIncmF = df_jn_ErnIncmF_join.select(
    F.col("lnk_ErnIncmF_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_ErnIncmF_L_in.ERN_INCM_DT_SK").alias("ERN_INCM_DT_SK"),
    F.col("lnk_ErnIncmF_L_in.GRP_ID").alias("GRP_ID"),
    F.col("lnk_ErnIncmF_L_in.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_ErnIncmF_L_in.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_ErnIncmF_L_in.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("lnk_ErnIncmF_L_in.ERN_INCM_TYP_CD").alias("ERN_INCM_TYP_CD"),
    F.col("lnk_ErnIncmF_L_in.CLS_ID").alias("CLS_ID"),
    F.col("lnk_ErnIncmF_L_in.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_ErnIncmF_L_in.PROD_ID").alias("PROD_ID"),
    F.col("lnk_ErnIncmF_L_in.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_ErnIncmF_L_in.ERN_INCM_DISP_CD").alias("ERN_INCM_DISP_CD"),
    F.col("lnk_ErnIncmF_L_in.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("lnk_KErnIncmFPkey_out.ERN_INCM_SK").alias("ERN_INCM_SK")
)

df_xfrm_PKEYgen_renamed = df_jn_ErnIncmF.withColumnRenamed("ERN_INCM_SK", "ERN_INCM_SK_old")
df_enriched = df_xfrm_PKEYgen_renamed
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ERN_INCM_SK",<schema>,<secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ERN_INCM_DT_SK").alias("ERN_INCM_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("ERN_INCM_TYP_CD").alias("ERN_INCM_TYP_CD"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ERN_INCM_DISP_CD").alias("ERN_INCM_DISP_CD"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("ERN_INCM_SK").alias("ERN_INCM_SK")
)

df_lnk_KErnIncmF_out = df_enriched.filter(F.col("ERN_INCM_SK_old").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ERN_INCM_DT_SK").alias("ERN_INCM_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("ERN_INCM_TYP_CD").alias("ERN_INCM_TYP_CD"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ERN_INCM_DISP_CD").alias("ERN_INCM_DISP_CD"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ERN_INCM_SK").alias("ERN_INCM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnk_KErnIncmF_out.createOrReplaceTempView("<DO_NOT_USE_TEMP_VIEW_NAME>")
F.spark_hint = None  # Just a placeholder to avoid function definitions

temp_table_name = "STAGING.IdsEdwErnIncmFPkey_db2_K_ERN_INCM_F_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_lnk_KErnIncmF_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_ERN_INCM_F_load = f"""
MERGE {EDWOwner}.K_ERN_INCM_F AS T
USING {temp_table_name} AS S
ON 
T.SRC_SYS_CD=S.SRC_SYS_CD
AND T.ERN_INCM_DT_SK=S.ERN_INCM_DT_SK
AND T.GRP_ID=S.GRP_ID
AND T.SUBGRP_ID=S.SUBGRP_ID
AND T.BILL_ENTY_UNIQ_KEY=S.BILL_ENTY_UNIQ_KEY
AND T.BILL_INVC_ID=S.BILL_INVC_ID
AND T.ERN_INCM_TYP_CD=S.ERN_INCM_TYP_CD
AND T.CLS_ID=S.CLS_ID
AND T.CLS_PLN_ID=S.CLS_PLN_ID
AND T.PROD_ID=S.PROD_ID
AND T.PROD_BILL_CMPNT_ID=S.PROD_BILL_CMPNT_ID
AND T.ERN_INCM_DISP_CD=S.ERN_INCM_DISP_CD
AND T.FEE_DSCNT_ID=S.FEE_DSCNT_ID
WHEN MATCHED THEN UPDATE SET
T.CRT_RUN_CYC_EXCTN_DT_SK=S.CRT_RUN_CYC_EXCTN_DT_SK,
T.ERN_INCM_SK=S.ERN_INCM_SK,
T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
(
SRC_SYS_CD,
ERN_INCM_DT_SK,
GRP_ID,
SUBGRP_ID,
BILL_ENTY_UNIQ_KEY,
BILL_INVC_ID,
ERN_INCM_TYP_CD,
CLS_ID,
CLS_PLN_ID,
PROD_ID,
PROD_BILL_CMPNT_ID,
ERN_INCM_DISP_CD,
FEE_DSCNT_ID,
CRT_RUN_CYC_EXCTN_DT_SK,
ERN_INCM_SK,
CRT_RUN_CYC_EXCTN_SK
)
VALUES
(
S.SRC_SYS_CD,
S.ERN_INCM_DT_SK,
S.GRP_ID,
S.SUBGRP_ID,
S.BILL_ENTY_UNIQ_KEY,
S.BILL_INVC_ID,
S.ERN_INCM_TYP_CD,
S.CLS_ID,
S.CLS_PLN_ID,
S.PROD_ID,
S.PROD_BILL_CMPNT_ID,
S.ERN_INCM_DISP_CD,
S.FEE_DSCNT_ID,
S.CRT_RUN_CYC_EXCTN_DT_SK,
S.ERN_INCM_SK,
S.CRT_RUN_CYC_EXCTN_SK
);
"""

execute_dml(merge_sql_db2_K_ERN_INCM_F_load, jdbc_url, jdbc_props)

df_jn_Pkey_join = df_lnk_ErnIncmData_L_in.alias("lnk_ErnIncmData_L_in").join(
    df_lnk_Pkey_out.alias("lnk_Pkey_out"),
    [
        F.col("lnk_ErnIncmData_L_in.SRC_SYS_CD")==F.col("lnk_Pkey_out.SRC_SYS_CD"),
        F.col("lnk_ErnIncmData_L_in.ERN_INCM_DT_SK")==F.col("lnk_Pkey_out.ERN_INCM_DT_SK"),
        F.col("lnk_ErnIncmData_L_in.GRP_ID")==F.col("lnk_Pkey_out.GRP_ID"),
        F.col("lnk_ErnIncmData_L_in.SUBGRP_ID")==F.col("lnk_Pkey_out.SUBGRP_ID"),
        F.col("lnk_ErnIncmData_L_in.BILL_ENTY_UNIQ_KEY")==F.col("lnk_Pkey_out.BILL_ENTY_UNIQ_KEY"),
        F.col("lnk_ErnIncmData_L_in.BILL_INVC_ID")==F.col("lnk_Pkey_out.BILL_INVC_ID"),
        F.col("lnk_ErnIncmData_L_in.ERN_INCM_TYP_CD")==F.col("lnk_Pkey_out.ERN_INCM_TYP_CD"),
        F.col("lnk_ErnIncmData_L_in.CLS_ID")==F.col("lnk_Pkey_out.CLS_ID"),
        F.col("lnk_ErnIncmData_L_in.CLS_PLN_ID")==F.col("lnk_Pkey_out.CLS_PLN_ID"),
        F.col("lnk_ErnIncmData_L_in.PROD_ID")==F.col("lnk_Pkey_out.PROD_ID"),
        F.col("lnk_ErnIncmData_L_in.PROD_BILL_CMPNT_ID")==F.col("lnk_Pkey_out.PROD_BILL_CMPNT_ID"),
        F.col("lnk_ErnIncmData_L_in.ERN_INCM_DISP_CD")==F.col("lnk_Pkey_out.ERN_INCM_DISP_CD"),
        F.col("lnk_ErnIncmData_L_in.FEE_DSCNT_ID")==F.col("lnk_Pkey_out.FEE_DSCNT_ID")
    ],
    how="left"
)

df_jn_Pkey = df_jn_Pkey_join.select(
    F.col("lnk_Pkey_out.ERN_INCM_SK").alias("ERN_INCM_SK"),
    F.col("lnk_ErnIncmData_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_DT_SK").alias("ERN_INCM_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.GRP_ID").alias("GRP_ID"),
    F.col("lnk_ErnIncmData_L_in.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_ErnIncmData_L_in.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_ErnIncmData_L_in.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_TYP_CD").alias("ERN_INCM_TYP_CD"),
    F.col("lnk_ErnIncmData_L_in.CLS_ID").alias("CLS_ID"),
    F.col("lnk_ErnIncmData_L_in.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_ErnIncmData_L_in.PROD_ID").alias("PROD_ID"),
    F.col("lnk_ErnIncmData_L_in.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_DISP_CD").alias("ERN_INCM_DISP_CD"),
    F.col("lnk_ErnIncmData_L_in.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("lnk_ErnIncmData_L_in.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_ErnIncmData_L_in.CLS_SK").alias("CLS_SK"),
    F.col("lnk_ErnIncmData_L_in.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_ErnIncmData_L_in.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_ErnIncmData_L_in.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("lnk_ErnIncmData_L_in.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_ErnIncmData_L_in.GRP_SK").alias("GRP_SK"),
    F.col("lnk_ErnIncmData_L_in.PROD_SK").alias("PROD_SK"),
    F.col("lnk_ErnIncmData_L_in.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_ErnIncmData_L_in.SUB_SK").alias("SUB_SK"),
    F.col("lnk_ErnIncmData_L_in.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_ErnIncmData_L_in.BILL_ENTY_LVL_CD").alias("BILL_ENTY_LVL_CD"),
    F.col("lnk_ErnIncmData_L_in.BILL_ENTY_LVL_NM").alias("BILL_ENTY_LVL_NM"),
    F.col("lnk_ErnIncmData_L_in.INVC_TYP_CD").alias("INVC_TYP_CD"),
    F.col("lnk_ErnIncmData_L_in.INVC_TYP_NM").alias("INVC_TYP_NM"),
    F.col("lnk_ErnIncmData_L_in.DP_IN").alias("DP_IN"),
    F.col("lnk_ErnIncmData_L_in.INVC_CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_YR_MO_SK").alias("ERN_INCM_YR_MO_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_AMT").alias("ERN_INCM_AMT"),
    F.col("lnk_ErnIncmData_L_in.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_DISP_NM").alias("ERN_INCM_DISP_NM"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_TYP_NM").alias("ERN_INCM_TYP_NM"),
    F.col("lnk_ErnIncmData_L_in.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("lnk_ErnIncmData_L_in.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("lnk_ErnIncmData_L_in.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("lnk_ErnIncmData_L_in.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ErnIncmData_L_in.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ErnIncmData_L_in.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_LVL_CD_SK"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_DISP_CD_SK").alias("ERN_INCM_DISP_CD_SK"),
    F.col("lnk_ErnIncmData_L_in.ERN_INCM_TYP_CD_SK").alias("ERN_INCM_TYP_CD_SK"),
    F.col("lnk_ErnIncmData_L_in.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    F.col("lnk_ErnIncmData_L_in.SUB_SBSDY_AMT").alias("SUB_SBSDY_AMT"),
    F.col("lnk_ErnIncmData_L_in.SUB_RESP_AMT").alias("SUB_RESP_AMT")
)

df_seq_ERN_INCM_F_csv_load = (
    df_jn_Pkey
    .withColumn("ERN_INCM_DT_SK", F.rpad(F.col("ERN_INCM_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("DP_IN", F.rpad(F.col("DP_IN"), 1, " "))
    .withColumn("INVC_CUR_RCRD_IN", F.rpad(F.col("INVC_CUR_RCRD_IN"), 1, " "))
    .withColumn("ERN_INCM_YR_MO_SK", F.rpad(F.col("ERN_INCM_YR_MO_SK"), 6, " "))
    .withColumn("INVC_BILL_DUE_DT_SK", F.rpad(F.col("INVC_BILL_DUE_DT_SK"), 10, " "))
    .withColumn("INVC_BILL_DUE_YR_MO_SK", F.rpad(F.col("INVC_BILL_DUE_YR_MO_SK"), 6, " "))
    .withColumn("INVC_BILL_END_DT_SK", F.rpad(F.col("INVC_BILL_END_DT_SK"), 10, " "))
    .withColumn("INVC_BILL_END_YR_MO_SK", F.rpad(F.col("INVC_BILL_END_YR_MO_SK"), 6, " "))
    .withColumn("INVC_CRT_DT_SK", F.rpad(F.col("INVC_CRT_DT_SK"), 10, " "))
    .select(
        "ERN_INCM_SK",
        "SRC_SYS_CD",
        "ERN_INCM_DT_SK",
        "GRP_ID",
        "SUBGRP_ID",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_INVC_ID",
        "ERN_INCM_TYP_CD",
        "CLS_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "ERN_INCM_DISP_CD",
        "FEE_DSCNT_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "EXPRNC_CAT_SK",
        "FEE_DSCNT_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "PROD_SK",
        "PROD_SH_NM_SK",
        "SUB_SK",
        "SUBGRP_SK",
        "BILL_ENTY_LVL_CD",
        "BILL_ENTY_LVL_NM",
        "INVC_TYP_CD",
        "INVC_TYP_NM",
        "DP_IN",
        "INVC_CUR_RCRD_IN",
        "ERN_INCM_YR_MO_SK",
        "INVC_BILL_DUE_DT_SK",
        "INVC_BILL_DUE_YR_MO_SK",
        "INVC_BILL_END_DT_SK",
        "INVC_BILL_END_YR_MO_SK",
        "INVC_CRT_DT_SK",
        "ERN_INCM_AMT",
        "SUB_UNIQ_KEY",
        "ERN_INCM_DISP_NM",
        "ERN_INCM_TYP_NM",
        "EXPRNC_CAT_CD",
        "FNCL_LOB_CD",
        "PROD_SH_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_LVL_CD_SK",
        "ERN_INCM_DISP_CD_SK",
        "ERN_INCM_TYP_CD_SK",
        "INVC_TYP_CD_SK",
        "SUB_SBSDY_AMT",
        "SUB_RESP_AMT"
    )
)

write_files(
    df_seq_ERN_INCM_F_csv_load,
    f"{adls_path}/load/ERN_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)