# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdePartdPaymtReconCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PARTD_RECON* and loads the data into EDW Table PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                 Jaideep Mankala         02/24/2022                                                 
# MAGIC 
# MAGIC Rekha Radhakrishna                     2022-04-19           408843                               Updated the Metadata for                                            EnterpriseDev3                Jaideep Mankala          04/30/2022
# MAGIC                                                                                                                                SUBMT_CMS_CNTR_ID Column


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
InFile = get_widget_value('InFile','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = """SELECT 
FILE_ID,
CNTR_SEQ_ID,
PLN_BNF_PCKG_SEQ_ID,
DTL_SEQ_ID,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_DT_SK,
PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK
FROM {}.K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F""".format(EDWOwner)
df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F = StructType([
    StructField("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK", IntegerType(), False),
    StructField("FILE_ID", StringType(), False),
    StructField("CNTR_SEQ_ID", StringType(), False),
    StructField("PLN_BNF_PCKG_SEQ_ID", StringType(), False),
    StructField("DTL_SEQ_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK", IntegerType(), False),
    StructField("CMS_CNTR_ID", StringType(), False),
    StructField("PLN_BNF_PCKG_ID", StringType(), False),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), False),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), False),
    StructField("NET_GROS_DRUG_CST_BELOW_AMT", DecimalType(), False),
    StructField("NET_GROS_DRUG_CST_ABOVE_AMT", DecimalType(), False),
    StructField("NET_TOT_GROS_DRUG_CST_AMT", DecimalType(), False),
    StructField("NET_LOW_INCM_CST_SHARING_AMT", DecimalType(), False),
    StructField("NET_COV_PLN_PD_AMT", DecimalType(), False),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), False),
    StructField("SUBMT_DUE_AMT", DecimalType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])
df_Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F)
    .csv(f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F.txt")
)

df_Xfm_DET = df_Seq_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F.select(
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Copy_for_Lnk_Remove_Dup = df_Xfm_DET.select(
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Copy_for_Lnk_AllCol_Join = df_Xfm_DET.select(
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Remove_Duplicates = dedup_sort(
    df_Copy_for_Lnk_Remove_Dup,
    ["FILE_ID","CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","DTL_SEQ_ID","SRC_SYS_CD"],
    [
        ("FILE_ID","A"),
        ("CNTR_SEQ_ID","A"),
        ("PLN_BNF_PCKG_SEQ_ID","A"),
        ("DTL_SEQ_ID","A"),
        ("SRC_SYS_CD","A")
    ]
)

df_Lnk_RmDup = df_Remove_Duplicates.select(
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Jn1_NKey = (
    df_Lnk_RmDup.alias("L")
    .join(
        df_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F.alias("R"),
        (
            (F.col("L.FILE_ID") == F.col("R.FILE_ID")) &
            (F.col("L.CNTR_SEQ_ID") == F.col("R.CNTR_SEQ_ID")) &
            (F.col("L.PLN_BNF_PCKG_SEQ_ID") == F.col("R.PLN_BNF_PCKG_SEQ_ID")) &
            (F.col("L.DTL_SEQ_ID") == F.col("R.DTL_SEQ_ID")) &
            (F.col("L.SRC_SYS_CD") == F.col("R.SRC_SYS_CD"))
        ),
        "left"
    )
    .select(
        F.col("L.FILE_ID").alias("FILE_ID"),
        F.col("L.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("L.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("L.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
        F.col("L.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("R.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("R.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK")
    ]
)

df_Transformer_in = df_Jn1_NKey

df_Transformer_in = df_Transformer_in.withColumn(
    "_pde_val_for_null_check",
    F.when(
        (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").isNull()) |
        (F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK") == 0),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_enriched = df_Transformer_in
df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK",
    <schema>,
    <secret_name>
)

df_Lnk_KTableLoad = (
    df_enriched
    .filter(F.col("_pde_val_for_null_check") == True)
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK")
    )
)

df_Lnk_Jn = (
    df_enriched
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.when(
            F.col("_pde_val_for_null_check") == True,
            F.lit(CurrRunCycleDate)
        ).otherwise(
            F.col("CRT_RUN_CYC_EXCTN_DT_SK")
        ).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK")
    )
)

table_temp = "STAGING.PdePartdPaymtReconCntrPlnBnfPckgDtlFExtr_Db2_K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F_Load_temp"
spark.sql(f"DROP TABLE IF EXISTS {table_temp}")

df_Lnk_KTableLoad.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", table_temp) \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO {owner}.K_PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F as T
USING {table_temp} as S
ON
    T.FILE_ID = S.FILE_ID AND
    T.CNTR_SEQ_ID = S.CNTR_SEQ_ID AND
    T.PLN_BNF_PCKG_SEQ_ID = S.PLN_BNF_PCKG_SEQ_ID AND
    T.DTL_SEQ_ID = S.DTL_SEQ_ID AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK = S.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK
WHEN NOT MATCHED THEN
    INSERT (
        FILE_ID,
        CNTR_SEQ_ID,
        PLN_BNF_PCKG_SEQ_ID,
        DTL_SEQ_ID,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_DT_SK,
        PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK
    )
    VALUES (
        S.FILE_ID,
        S.CNTR_SEQ_ID,
        S.PLN_BNF_PCKG_SEQ_ID,
        S.DTL_SEQ_ID,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK
    );
""".format(owner=EDWOwner, table_temp=table_temp)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Jn2Nkey = (
    df_Copy_for_Lnk_AllCol_Join.alias("L")
    .join(
        df_Lnk_Jn.alias("R"),
        (
            (F.col("L.FILE_ID") == F.col("R.FILE_ID")) &
            (F.col("L.CNTR_SEQ_ID") == F.col("R.CNTR_SEQ_ID")) &
            (F.col("L.PLN_BNF_PCKG_SEQ_ID") == F.col("R.PLN_BNF_PCKG_SEQ_ID")) &
            (F.col("L.DTL_SEQ_ID") == F.col("R.DTL_SEQ_ID")) &
            (F.col("L.SRC_SYS_CD") == F.col("R.SRC_SYS_CD"))
        ),
        "inner"
    )
    .select(
        F.col("R.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK"),
        F.col("L.FILE_ID").alias("FILE_ID"),
        F.col("L.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("L.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
        F.col("L.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
        F.col("L.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("R.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("L.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("L.PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
        F.col("L.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        F.col("L.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("L.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
        F.col("L.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
        F.col("L.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        F.col("L.NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
        F.col("L.NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
        F.col("L.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
        F.col("L.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
        F.col("L.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
        F.col("L.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
        F.col("L.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
        F.col("L.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("L.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_Copy5 = df_Jn2Nkey

df_Lnk_SeqExtr = df_Copy5.select(
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK").alias("PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_SK"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("NET_GROS_DRUG_CST_BELOW_AMT").alias("NET_GROS_DRUG_CST_BELOW_AMT"),
    F.col("NET_GROS_DRUG_CST_ABOVE_AMT").alias("NET_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

final_cols = df_Lnk_SeqExtr.schema.fields
def rpad_if_needed(df, field):
    col_name = field.name
    data_type = field.dataType
    if isinstance(data_type, StringType):
        length_info = 255
        # If it's char(10) specifically:
        if col_name in ["CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK"]:
            length_info = 10
        return F.rpad(F.col(col_name), length_info, " ").alias(col_name)
    return F.col(col_name).alias(col_name)

df_final = df_Lnk_SeqExtr.select([rpad_if_needed(df_Lnk_SeqExtr, f) for f in final_cols])

write_files(
    df_final,
    f"{adls_path}/verified/PDE_PARTD_PAYMT_RECON_CNTR_PLN_BNF_PCKG_DTL_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)