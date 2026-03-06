# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                            
# MAGIC DEVELOPER                          DATE                  PROJECT                                                    DESCRIPTION                                                   DATASTAGE                                        CODE                                      DATE 
# MAGIC                                                                                                                                                                                                                            ENVIRONMENT                                   REVIEWER                             REVIEW
# MAGIC ----------------------------------------       --------------------        -------------------------------------------------                   -----------------------------------------------------------                 -----------------------------------------------               ------------------------------                   --------------------
# MAGIC Lee Moore                              2013-09-13           5114                                                           rewrite in parallel                                                   EnterpriseWrhsDevl                                Peter Marshall                           12/19/2013
# MAGIC 
# MAGIC Pooja Sunkara                       2014-01-21           5114 Project - Daptiv#688                          Fixed the Nullability of column CLM_DIAG_SK     EnterpriseWrhsDevl                                Jag Yelavarthi                         2014-01-21
# MAGIC                                                                                                                                                on Lookup file

# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target DB2 table here. 
# MAGIC 
# MAGIC Load Type; "Update" .
# MAGIC Copy For Buffer
# MAGIC JobName: IdsEdwClmDiagISkUpdate 
# MAGIC This job compare key of current diag with key of last run of diag  to determine if primary key need to be updated.
# MAGIC Code SK lookups for Denormalization
# MAGIC Read from source table 
# MAGIC CLM_DIAG_I.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

schema_seq_CLM_DIAG_Sk_csv = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), True),
    StructField("CLM_DIAG_SK", IntegerType(), True)
])

df_seq_CLM_DIAG_Sk_csv = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DIAG_Sk_csv)
    .load(f"{adls_path}/load/CLM_DIAG_SK.dat")
)

extract_query_db2_CLM_DIAG_in = f"""
SELECT c.CLM_DIAG_SK,
       c.SRC_SYS_CD,
       c.CLM_ID,
       c.CLM_DIAG_ORDNL_CD
FROM {EDWOwner}.CLM_DIAG_I c,
     {EDWOwner}.B_CLM_F2 b
WHERE c.SRC_SYS_CD = b.SRC_SYS_CD
  AND c.CLM_ID = b.CLM_ID
ORDER BY c.SRC_SYS_CD,
         c.CLM_ID,
         c.CLM_DIAG_ORDNL_CD
"""

jdbc_url_db2_CLM_DIAG_in, jdbc_props_db2_CLM_DIAG_in = get_db_config(edw_secret_name)

df_db2_CLM_DIAG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_DIAG_in)
    .options(**jdbc_props_db2_CLM_DIAG_in)
    .option("query", extract_query_db2_CLM_DIAG_in)
    .load()
)

df_lkp_Codes = (
    df_db2_CLM_DIAG_in.alias("lnk_IdsEdwClmDiagIExtr_InABC")
    .join(
        df_seq_CLM_DIAG_Sk_csv.alias("lnk_Clm_Diag_Sk"),
        (
            F.col("lnk_IdsEdwClmDiagIExtr_InABC.SRC_SYS_CD") == F.col("lnk_Clm_Diag_Sk.SRC_SYS_CD")
        )
        & (
            F.col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_ID") == F.col("lnk_Clm_Diag_Sk.CLM_ID")
        )
        & (
            F.col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_ORDNL_CD")
            == F.col("lnk_Clm_Diag_Sk.CLM_DIAG_ORDNL_CD")
        ),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwClmDiagIExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
        F.col("lnk_Clm_Diag_Sk.CLM_DIAG_SK").alias("CLM_DIAG_SK"),
        F.col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_SK").alias("CLM_DIAG_SK_1")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .filter(
        (F.col("CLM_DIAG_SK").isNotNull())
        & (F.col("CLM_DIAG_SK") != 0)
        & (F.col("CLM_DIAG_SK") != F.col("CLM_DIAG_SK_1"))
    )
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_DIAG_ORDNL_CD"),
        F.when(F.col("CLM_DIAG_SK").isNull(), F.lit(0)).otherwise(F.col("CLM_DIAG_SK")).alias("CLM_DIAG_SK")
    )
)

df_db2_CLM_DIAG_I_out_in = (
    df_xfrm_BusinessLogic
    .select(
        F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")).alias("SRC_SYS_CD"),
        F.rpad(F.col("CLM_ID"), F.lit(<...>), F.lit(" ")).alias("CLM_ID"),
        F.rpad(F.col("CLM_DIAG_ORDNL_CD"), F.lit(<...>), F.lit(" ")).alias("CLM_DIAG_ORDNL_CD"),
        F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK")
    )
)

drop_table_sql = "DROP TABLE IF EXISTS STAGING.EdwClmDiagISkUpdate_db2_CLM_DIAG_I_out_temp"
execute_dml(drop_table_sql, jdbc_url_db2_CLM_DIAG_in, jdbc_props_db2_CLM_DIAG_in)

(
    df_db2_CLM_DIAG_I_out_in.write
    .format("jdbc")
    .option("url", jdbc_url_db2_CLM_DIAG_in)
    .options(**jdbc_props_db2_CLM_DIAG_in)
    .option("dbtable", "STAGING.EdwClmDiagISkUpdate_db2_CLM_DIAG_I_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql_db2_CLM_DIAG_I_out = f"""
MERGE INTO {EDWOwner}.CLM_DIAG_I AS T
USING STAGING.EdwClmDiagISkUpdate_db2_CLM_DIAG_I_out_temp AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
    AND T.CLM_DIAG_ORDNL_CD = S.CLM_DIAG_ORDNL_CD
)
WHEN MATCHED THEN
    UPDATE SET
        T.CLM_DIAG_SK = S.CLM_DIAG_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLM_ID,
        CLM_DIAG_ORDNL_CD,
        CLM_DIAG_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLM_ID,
        S.CLM_DIAG_ORDNL_CD,
        S.CLM_DIAG_SK
    );
"""
execute_dml(merge_sql_db2_CLM_DIAG_I_out, jdbc_url_db2_CLM_DIAG_in, jdbc_props_db2_CLM_DIAG_in)

df_db2_CLM_DIAG_I_out_rej = (
    df_db2_CLM_DIAG_I_out_in
    .withColumn("ERRORCODE", F.lit(None).cast(StringType()))
    .withColumn("ERRORTEXT", F.lit(None).cast(StringType()))
)

df_seq_CLM_DIAG_I_csv_rej = df_db2_CLM_DIAG_I_out_rej.select(
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")).alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), F.lit(<...>), F.lit(" ")).alias("CLM_ID"),
    F.rpad(F.col("CLM_DIAG_ORDNL_CD"), F.lit(<...>), F.lit(" ")).alias("CLM_DIAG_ORDNL_CD"),
    F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.rpad(F.col("ERRORCODE"), F.lit(<...>), F.lit(" ")).alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), F.lit(<...>), F.lit(" ")).alias("ERRORTEXT")
)

write_files(
    df_seq_CLM_DIAG_I_csv_rej,
    f"{adls_path}/load/CLM_DIAG_I_SK_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

params = {
    "EnvProjectPath": f"dap/<...>/edw",
    "File_Path": "load",
    "File_Name": "CLM_DIAG_SK.dat"
}
dbutils.notebook.run("../../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)