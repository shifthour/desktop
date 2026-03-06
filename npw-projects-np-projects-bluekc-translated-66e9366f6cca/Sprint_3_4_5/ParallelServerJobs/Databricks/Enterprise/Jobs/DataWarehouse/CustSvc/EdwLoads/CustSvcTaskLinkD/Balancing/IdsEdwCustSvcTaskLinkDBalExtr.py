# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsEdwCustSvcBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              08/01/2007         3264                              Originally Programmed                                 devlEDW10    
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to            devlEDW10                  Steph Goddard            10/21/2007
# MAGIC                                                                                                          Snapshot table extract                                        
# MAGIC 
# MAGIC                                                                                                          Snapshot table extract                             
# MAGIC Archana Palivela               12/06/2013          5114                          Create Load File for                                       EnterpriseWhseDevl              Jag Yelavarthi              2014-02-26
# MAGIC                                                                                                         EDW Table B_CUST_SVC_TASK_FUND_F

# MAGIC Loading Balancing Snapshot Table
# MAGIC Job: IdsEdwCustSvcTaskFundFBalExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

seq_IdsEdwCustSvcTaskLinkDBal_Snapshot_csv_load_schema = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_LINK_TYP_CD_SK", IntegerType(), False),
    StructField("LINK_RCRD_ID", StringType(), False),
    StructField("APL_SK", IntegerType(), False)
])

df_seq_IdsEdwCustSvcTaskLinkDBal_Snapshot_csv_load = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(seq_IdsEdwCustSvcTaskLinkDBal_Snapshot_csv_load_schema)
    .load(f"{adls_path}/balancing/snapshot/IDS_CUST_SVC_TASK_LINK.uniq")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Copy_515_in = df_db2_CD_MPPNG_in

df_Lnk_SrcSysCd_Out = df_Copy_515_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Lnk_CustSvcSrcCd = df_Copy_515_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_Snapshot_Extr_In = df_seq_IdsEdwCustSvcTaskLinkDBal_Snapshot_csv_load

df_lkup_CdMppng = (
    df_lnk_Snapshot_Extr_In.alias("lnk_Snapshot_Extr_In")
    .join(
        df_Lnk_SrcSysCd_Out.alias("Lnk_SrcSysCd_Out"),
        F.col("lnk_Snapshot_Extr_In.SRC_SYS_CD_SK") == F.col("Lnk_SrcSysCd_Out.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Lnk_CustSvcSrcCd.alias("Lnk_CustSvcSrcCd"),
        F.col("lnk_Snapshot_Extr_In.CUST_SVC_TASK_LINK_TYP_CD_SK") == F.col("Lnk_CustSvcSrcCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("Lnk_SrcSysCd_Out.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Snapshot_Extr_In.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_Snapshot_Extr_In.TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.col("Lnk_CustSvcSrcCd.TRGT_CD").alias("CUST_SVC_TASK_LINK_TYP_CD"),
        F.col("lnk_Snapshot_Extr_In.LINK_RCRD_ID").alias("CUST_SVC_TASK_LINK_RCRD_ID"),
        F.col("lnk_Snapshot_Extr_In.APL_SK").alias("APL_SK")
    )
)

df_xfrm_Businesslogic = df_lkup_CdMppng.select(
    F.when(trim("SRC_SYS_CD") == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.when(trim("CUST_SVC_TASK_LINK_TYP_CD") == "", F.lit("UNK")).otherwise(F.col("CUST_SVC_TASK_LINK_TYP_CD")).alias("CUST_SVC_TASK_LINK_TYP_CD"),
    F.col("CUST_SVC_TASK_LINK_RCRD_ID").alias("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.col("APL_SK").alias("APL_SK")
)

df_final = df_xfrm_Businesslogic.select(
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")).alias("SRC_SYS_CD"),
    F.rpad(F.col("CUST_SVC_ID"), F.lit(<...>), F.lit(" ")).alias("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_TYP_CD"), F.lit(<...>), F.lit(" ")).alias("CUST_SVC_TASK_LINK_TYP_CD"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_RCRD_ID"), F.lit(<...>), F.lit(" ")).alias("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.col("APL_SK").alias("APL_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/B_CUST_SVC_TASK_LINK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)