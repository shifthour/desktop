# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:    IdsEdwCustSvcBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                               Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              06/29/2007         3264                              Originally Programmed                             devlEDW10      
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to        devlEDW10                 Steph Goddard             10/21/2007
# MAGIC                                                                                                          Snapshot table extract                                  
# MAGIC 
# MAGIC Bhupinder Kaur                12/09/2013          5114                               Create Load File for                              EnterpriseWhseDevl     Jag Yelavarthi               2014-02-25
# MAGIC 
# MAGIC                                                                                                       DW Table B_CUST_SVC_TASK_F

# MAGIC Loading Balancing Snapshot Table
# MAGIC Job: IdsEdwCustSvcCustSvcTaskFBalExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_IdsCustSvc_Snapshot_csv_load = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("CUST_ID", StringType(), False)
])

df_seq_IdsCustSvc_Snapshot_csv_load = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_seq_IdsCustSvc_Snapshot_csv_load)
    .load(f"{adls_path}/balancing/snapshot/IDS_CUST_SVC_TASK.uniq")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_GRP_in = f"SELECT GRP.GRP_SK, GRP.GRP_ID FROM {IDSOwner}.GRP GRP"
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_lkup_CdMppng = (
    df_seq_IdsCustSvc_Snapshot_csv_load.alias("lnk_Snapshot_Extr_In")
    .join(
        df_db2_GRP_in.alias("ref_GrpLkup"),
        col("lnk_Snapshot_Extr_In.GRP_SK") == col("ref_GrpLkup.GRP_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG_in.alias("ref_SrcSysCdLkup"),
        col("lnk_Snapshot_Extr_In.SRC_SYS_CD_SK") == col("ref_SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("ref_SrcSysCdLkup.TRGT_CD").alias("TRGT_CD"),
        col("lnk_Snapshot_Extr_In.CUST_SVC_ID").alias("CUST_SVC_ID"),
        col("lnk_Snapshot_Extr_In.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        col("ref_GrpLkup.GRP_ID").alias("GRP_ID")
    )
)

df_xfrm_Businesslogic = df_lkup_CdMppng.select(
    when(
        (col("TRGT_CD").isNull()) | (length(trim(col("TRGT_CD"))) == 0),
        "NA"
    ).otherwise(col("TRGT_CD")).alias("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    when(
        (col("GRP_ID").isNull()) | (length(trim(col("GRP_ID"))) == 0),
        "NA"
    ).otherwise(col("GRP_ID")).alias("GRP_ID")
)

df_seq_BCustSvcTaskF_csv_load = df_xfrm_Businesslogic.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CUST_SVC_ID"), <...>, " ").alias("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID")
)

write_files(
    df_seq_BCustSvcTaskF_csv_load,
    f"{adls_path}/load/B_CUST_SVC_TASK_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)