# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Srikanth Mettpalli      10/21/2013        5114                              Original Programming                                                                            EnterpriseWrhsDevl  Peter Marshall               12/12/2013

# MAGIC Job Name: IdsEdwFeeDscntIncmFPkey
# MAGIC Read FEE_DSCNT_INCM_F.ds Dataset created in the IdsEdwFeeDscntIncmFExtr Job.
# MAGIC Copy for creating two output streams
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC Read K_FEE_DSCNT_INCM_F Table to pull the Natural Keys and the Skey.
# MAGIC Left Join on Natural Keys
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Write FEE_DSCNT_INCM_F.dat for the IdsEdwCmnPrctNtwkCtFLoad Job.
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

df_ds_FEE_DSCNT_INCM_F_out = spark.read.parquet(f"{adls_path}/ds/FEE_DSCNT_INCM_F.parquet")

df_cpy_Multistreams = df_ds_FEE_DSCNT_INCM_F_out

df_lnk_FeeDscntIncmData_L_in = df_cpy_Multistreams.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("INVC_TYP_CD"),
    F.col("DSCNT_IN"),
    F.col("INVC_CUR_RCRD_IN"),
    F.col("FEE_DSCNT_YR_MO_SK"),
    F.col("INVC_BILL_DUE_DT_SK"),
    F.col("INVC_BILL_DUE_YR_MO_SK"),
    F.col("INVC_BILL_END_DT_SK"),
    F.col("INVC_BILL_END_YR_MO_SK"),
    F.col("INVC_CRT_DT_SK"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("INVC_FEE_DSCNT_SK"),
    F.col("INVC_SK"),
    F.col("INVC_TYP_CD_SK"),
    F.col("SUB_SK"),
    F.col("PROD_SK")
)

df_lnk_rdp_naturalkeys_in = df_cpy_Multistreams.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID")
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"SELECT SRC_SYS_CD,BILL_INVC_ID,FEE_DSCNT_ID,INVC_FEE_DSCNT_BILL_DISP_CD,INVC_FEE_DSCNT_SRC_CD,GRP_ID,SUBGRP_ID,CLS_ID,CLS_PLN_ID,PROD_ID,FEE_DSCNT_INCM_SK FROM {EDWOwner}.K_FEE_DSCNT_INCM_F"

df_db2_FEE_DSCNT_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_rdp_Natural_Keys = dedup_sort(
    df_lnk_rdp_naturalkeys_in,
    ["SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD","INVC_FEE_DSCNT_SRC_CD","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID"],
    [
      ("SRC_SYS_CD","A"),("BILL_INVC_ID","A"),("FEE_DSCNT_ID","A"),
      ("INVC_FEE_DSCNT_BILL_DISP_CD","A"),("INVC_FEE_DSCNT_SRC_CD","A"),
      ("GRP_ID","A"),("SUBGRP_ID","A"),("CLS_ID","A"),
      ("CLS_PLN_ID","A"),("PROD_ID","A")
    ]
)

df_lnk_FeeDscntIncmFData_L_in2 = df_rdp_Natural_Keys.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID")
)

df_jn_FeeDscntIncmF = df_lnk_FeeDscntIncmFData_L_in2.alias("L").join(
    df_db2_FEE_DSCNT_INCM_F_in.alias("R"),
    (
      (F.col("L.SRC_SYS_CD") == F.col("R.SRC_SYS_CD")) &
      (F.col("L.BILL_INVC_ID") == F.col("R.BILL_INVC_ID")) &
      (F.col("L.FEE_DSCNT_ID") == F.col("R.FEE_DSCNT_ID")) &
      (F.col("L.INVC_FEE_DSCNT_BILL_DISP_CD") == F.col("R.INVC_FEE_DSCNT_BILL_DISP_CD")) &
      (F.col("L.INVC_FEE_DSCNT_SRC_CD") == F.col("R.INVC_FEE_DSCNT_SRC_CD")) &
      (F.col("L.GRP_ID") == F.col("R.GRP_ID")) &
      (F.col("L.SUBGRP_ID") == F.col("R.SUBGRP_ID")) &
      (F.col("L.CLS_ID") == F.col("R.CLS_ID")) &
      (F.col("L.CLS_PLN_ID") == F.col("R.CLS_PLN_ID")) &
      (F.col("L.PROD_ID") == F.col("R.PROD_ID"))
    ),
    "left"
)

df_jn_FeeDscntIncmF = df_jn_FeeDscntIncmF.select(
    F.col("L.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("L.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("L.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("L.INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("L.INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("L.GRP_ID").alias("GRP_ID"),
    F.col("L.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("L.CLS_ID").alias("CLS_ID"),
    F.col("L.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("L.PROD_ID").alias("PROD_ID"),
    F.col("R.FEE_DSCNT_INCM_SK").alias("FEE_DSCNT_INCM_SK")
)

df_xfrm_PKEYgen = df_jn_FeeDscntIncmF.withColumnRenamed("FEE_DSCNT_INCM_SK","ORIG_FEE_DSCNT_INCM_SK").withColumn("FEE_DSCNT_INCM_SK",F.col("ORIG_FEE_DSCNT_INCM_SK"))

df_enriched = df_xfrm_PKEYgen
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"FEE_DSCNT_INCM_SK",<schema>,<secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("FEE_DSCNT_INCM_SK").alias("FEE_DSCNT_INCM_SK")
)

df_lnk_KFeeDscntIncmF_out = df_enriched.filter(F.col("ORIG_FEE_DSCNT_INCM_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("FEE_DSCNT_INCM_SK").alias("FEE_DSCNT_INCM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

temp_table = "STAGING.IdsEdwFeeDscntIncmFPkey_db2_K_FEE_DSCNT_INCM_F_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
  df_lnk_KFeeDscntIncmF_out
  .write
  .format("jdbc")
  .option("url", jdbc_url)
  .options(**jdbc_props)
  .option("dbtable", temp_table)
  .mode("append")
  .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_FEE_DSCNT_INCM_F AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.BILL_INVC_ID = S.BILL_INVC_ID
AND T.FEE_DSCNT_ID = S.FEE_DSCNT_ID
AND T.INVC_FEE_DSCNT_BILL_DISP_CD = S.INVC_FEE_DSCNT_BILL_DISP_CD
AND T.INVC_FEE_DSCNT_SRC_CD = S.INVC_FEE_DSCNT_SRC_CD
AND T.GRP_ID = S.GRP_ID
AND T.SUBGRP_ID = S.SUBGRP_ID
AND T.CLS_ID = S.CLS_ID
AND T.CLS_PLN_ID = S.CLS_PLN_ID
AND T.PROD_ID = S.PROD_ID
WHEN MATCHED THEN
  UPDATE SET SRC_SYS_CD = T.SRC_SYS_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    BILL_INVC_ID,
    FEE_DSCNT_ID,
    INVC_FEE_DSCNT_BILL_DISP_CD,
    INVC_FEE_DSCNT_SRC_CD,
    GRP_ID,
    SUBGRP_ID,
    CLS_ID,
    CLS_PLN_ID,
    PROD_ID,
    CRT_RUN_CYC_EXCTN_DT_SK,
    FEE_DSCNT_INCM_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.BILL_INVC_ID,
    S.FEE_DSCNT_ID,
    S.INVC_FEE_DSCNT_BILL_DISP_CD,
    S.INVC_FEE_DSCNT_SRC_CD,
    S.GRP_ID,
    S.SUBGRP_ID,
    S.CLS_ID,
    S.CLS_PLN_ID,
    S.PROD_ID,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.FEE_DSCNT_INCM_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_Pkey = df_lnk_FeeDscntIncmData_L_in.alias("L").join(
    df_lnk_Pkey_out.alias("R"),
    (
      (F.col("L.SRC_SYS_CD") == F.col("R.SRC_SYS_CD")) &
      (F.col("L.BILL_INVC_ID") == F.col("R.BILL_INVC_ID")) &
      (F.col("L.FEE_DSCNT_ID") == F.col("R.FEE_DSCNT_ID")) &
      (F.col("L.INVC_FEE_DSCNT_BILL_DISP_CD") == F.col("R.INVC_FEE_DSCNT_BILL_DISP_CD")) &
      (F.col("L.INVC_FEE_DSCNT_SRC_CD") == F.col("R.INVC_FEE_DSCNT_SRC_CD")) &
      (F.col("L.GRP_ID") == F.col("R.GRP_ID")) &
      (F.col("L.SUBGRP_ID") == F.col("R.SUBGRP_ID")) &
      (F.col("L.CLS_ID") == F.col("R.CLS_ID")) &
      (F.col("L.CLS_PLN_ID") == F.col("R.CLS_PLN_ID")) &
      (F.col("L.PROD_ID") == F.col("R.PROD_ID"))
    ),
    "left"
)

df_jn_Pkey = df_jn_Pkey.select(
    F.col("R.FEE_DSCNT_INCM_SK").alias("FEE_DSCNT_INCM_SK"),
    F.col("L.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("L.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("L.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("L.INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("L.INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("L.GRP_ID").alias("GRP_ID"),
    F.col("L.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("L.CLS_ID").alias("CLS_ID"),
    F.col("L.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("L.PROD_ID").alias("PROD_ID"),
    F.col("L.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("L.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("L.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("L.CLS_SK").alias("CLS_SK"),
    F.col("L.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("L.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("L.GRP_SK").alias("GRP_SK"),
    F.col("L.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("L.INVC_TYP_CD").alias("INVC_TYP_CD"),
    F.col("L.DSCNT_IN").alias("DSCNT_IN"),
    F.col("L.INVC_CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
    F.col("L.FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
    F.col("L.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    F.col("L.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.col("L.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    F.col("L.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
    F.col("L.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    F.col("L.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("L.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("L.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("L.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("L.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.col("L.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("L.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
    F.col("L.INVC_SK").alias("INVC_SK"),
    F.col("L.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    F.col("L.SUB_SK").alias("SUB_SK"),
    F.col("L.PROD_SK").alias("PROD_SK")
)

df_final = df_jn_Pkey.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "DSCNT_IN",
    rpad(F.col("DSCNT_IN"), 1, " ")
).withColumn(
    "INVC_CUR_RCRD_IN",
    rpad(F.col("INVC_CUR_RCRD_IN"), 1, " ")
).withColumn(
    "FEE_DSCNT_YR_MO_SK",
    rpad(F.col("FEE_DSCNT_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_BILL_DUE_DT_SK",
    rpad(F.col("INVC_BILL_DUE_DT_SK"), 10, " ")
).withColumn(
    "INVC_BILL_DUE_YR_MO_SK",
    rpad(F.col("INVC_BILL_DUE_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_BILL_END_DT_SK",
    rpad(F.col("INVC_BILL_END_DT_SK"), 10, " ")
).withColumn(
    "INVC_BILL_END_YR_MO_SK",
    rpad(F.col("INVC_BILL_END_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_CRT_DT_SK",
    rpad(F.col("INVC_CRT_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/FEE_DSCNT_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)