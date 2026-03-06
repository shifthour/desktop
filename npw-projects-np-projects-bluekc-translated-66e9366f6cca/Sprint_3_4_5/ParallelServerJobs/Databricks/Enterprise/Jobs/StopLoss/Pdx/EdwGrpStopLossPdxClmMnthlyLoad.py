# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:   BcbsscEdwGrpStopLossPdxClmMnthlyCntl, EdwEdwGrpStopLossPdxClmMnthlyCntl, SavRXEdwGrpStopLossPdxClmMnthlyCntl and MedTrakEdwGrpStopLossPdxClmMnthlyCntl
# MAGIC 
# MAGIC PROCESSING:     To Load EDW table GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                         Development Project\(9)            Code Reviewer             Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                            -----------------------------------------------------------------------                  ------------------------------\(9)            -------------------------------      ----------------------------       
# MAGIC Shanmugam A.          2017-08-08\(9)5828-StopLoss-Bcbssc                                Original Programming\(9)\(9)\(9)                EnterpriseDev2                       Kalyan Neelam            2017-08-09  
# MAGIC Shanmugam A.          2017-12-01\(9)5828-StopLoss-Bcbssc                                Introduced SrcSysCd, AmtTyp parameter                            EnterpriseDev2                       Kalyan Neelam            2017-12-05

# MAGIC Job to load the Target EDW table GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
# MAGIC Read the load file created by one of the below jobs BcbsscEdwGrpStopLossPdxClmMnthlyExtr or 
# MAGIC EdwEdwGrpStopLossPdxClmMnthlyCntl or 
# MAGIC SavRXEdwGrpStopLossPdxClmMnthlyCntl or 
# MAGIC MedTrakEdwGrpStopLossPdxClmMnthlyCntl
# MAGIC Load the file to the EDW table GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
AmtTyp = get_widget_value('AmtTyp','')

schema_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM = StructType([
    StructField("FILE_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_GRP_ID", StringType(), nullable=False),
    StructField("RCRD_TYP_NM", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SRC_SYS_COV_NO", StringType(), nullable=False),
    StructField("RCVD_TOT_RCRD_CT", IntegerType(), nullable=False),
    StructField("RCVD_TOT_INGR_CST_AMT", DecimalType(10,2), nullable=False),
    StructField("RCVD_TOT_DRUG_ADM_FEE_AMT", DecimalType(10,2), nullable=False),
    StructField("RCVD_TOT_COPAY_AMT", DecimalType(10,2), nullable=False),
    StructField("RCVD_TOT_PD_AMT", DecimalType(10,2), nullable=False),
    StructField("RCVD_TOT_QTY_CT", DecimalType(10,2), nullable=False),
    StructField("CALC_TOT_RCRD_CT", IntegerType(), nullable=True),
    StructField("CALC_TOT_INGR_CST_AMT", DecimalType(10,2), nullable=True),
    StructField("CALC_TOT_DRUG_ADM_FEE_AMT", DecimalType(10,2), nullable=True),
    StructField("CALC_TOT_COPAY_AMT", DecimalType(10,2), nullable=True),
    StructField("CALC_TOT_PD_AMT", DecimalType(10,2), nullable=True),
    StructField("CALC_TOT_QTY_CT", DecimalType(10,2), nullable=True)
])

df_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM)
    .csv(f"{adls_path}/load/{SrcSysCd}_{AmtTyp}_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM.dat")
)

df_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM = df_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM.select(
    "FILE_DT_SK",
    "SRC_SYS_GRP_ID",
    "RCRD_TYP_NM",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_ID",
    "SRC_SYS_COV_NO",
    "RCVD_TOT_RCRD_CT",
    "RCVD_TOT_INGR_CST_AMT",
    "RCVD_TOT_DRUG_ADM_FEE_AMT",
    "RCVD_TOT_COPAY_AMT",
    "RCVD_TOT_PD_AMT",
    "RCVD_TOT_QTY_CT",
    "CALC_TOT_RCRD_CT",
    "CALC_TOT_INGR_CST_AMT",
    "CALC_TOT_DRUG_ADM_FEE_AMT",
    "CALC_TOT_COPAY_AMT",
    "CALC_TOT_PD_AMT",
    "CALC_TOT_QTY_CT"
)

df_cpy_forBuffer = (
    df_Seq_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
    .repartition("FILE_DT_SK","SRC_SYS_GRP_ID","RCRD_TYP_NM","SRC_SYS_CD")
    .select(
        F.col("FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("SRC_SYS_COV_NO").alias("SRC_SYS_COV_NO"),
        F.col("RCVD_TOT_RCRD_CT").alias("RCVD_TOT_RCRD_CT"),
        F.col("RCVD_TOT_INGR_CST_AMT").alias("RCVD_TOT_INGR_CST_AMT"),
        F.col("RCVD_TOT_DRUG_ADM_FEE_AMT").alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
        F.col("RCVD_TOT_COPAY_AMT").alias("RCVD_TOT_COPAY_AMT"),
        F.col("RCVD_TOT_PD_AMT").alias("RCVD_TOT_PD_AMT"),
        F.col("RCVD_TOT_QTY_CT").alias("RCVD_TOT_QTY_CT"),
        F.col("CALC_TOT_RCRD_CT").alias("CALC_TOT_RCRD_CT"),
        F.col("CALC_TOT_INGR_CST_AMT").alias("CALC_TOT_INGR_CST_AMT"),
        F.col("CALC_TOT_DRUG_ADM_FEE_AMT").alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
        F.col("CALC_TOT_COPAY_AMT").alias("CALC_TOT_COPAY_AMT"),
        F.col("CALC_TOT_PD_AMT").alias("CALC_TOT_PD_AMT"),
        F.col("CALC_TOT_QTY_CT").alias("CALC_TOT_QTY_CT")
    )
)

df_EDW_DB2ConnectorPX = df_cpy_forBuffer.select(
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), F.lit(<...>), " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("RCRD_TYP_NM"), F.lit(<...>), " ").alias("RCRD_TYP_NM"),
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("GRP_ID"), F.lit(<...>), " ").alias("GRP_ID"),
    F.rpad(F.col("SRC_SYS_COV_NO"), F.lit(<...>), " ").alias("SRC_SYS_COV_NO"),
    F.col("RCVD_TOT_RCRD_CT"),
    F.col("RCVD_TOT_INGR_CST_AMT"),
    F.col("RCVD_TOT_DRUG_ADM_FEE_AMT"),
    F.col("RCVD_TOT_COPAY_AMT"),
    F.col("RCVD_TOT_PD_AMT"),
    F.col("RCVD_TOT_QTY_CT"),
    F.col("CALC_TOT_RCRD_CT"),
    F.col("CALC_TOT_INGR_CST_AMT"),
    F.col("CALC_TOT_DRUG_ADM_FEE_AMT"),
    F.col("CALC_TOT_COPAY_AMT"),
    F.col("CALC_TOT_PD_AMT"),
    F.col("CALC_TOT_QTY_CT")
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwGrpStopLossPdxClmMnthlyLoad_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM_temp",
    jdbc_url,
    jdbc_props
)

(
    df_EDW_DB2ConnectorPX
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwGrpStopLossPdxClmMnthlyLoad_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM AS T
USING STAGING.EdwGrpStopLossPdxClmMnthlyLoad_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM_temp AS S
ON 
    T.FILE_DT_SK = S.FILE_DT_SK
    AND T.SRC_SYS_GRP_ID = S.SRC_SYS_GRP_ID
    AND T.RCRD_TYP_NM = S.RCRD_TYP_NM
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.FILE_DT_SK = S.FILE_DT_SK,
    T.SRC_SYS_GRP_ID = S.SRC_SYS_GRP_ID,
    T.RCRD_TYP_NM = S.RCRD_TYP_NM,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.GRP_ID = S.GRP_ID,
    T.SRC_SYS_COV_NO = S.SRC_SYS_COV_NO,
    T.RCVD_TOT_RCRD_CT = S.RCVD_TOT_RCRD_CT,
    T.RCVD_TOT_INGR_CST_AMT = S.RCVD_TOT_INGR_CST_AMT,
    T.RCVD_TOT_DRUG_ADM_FEE_AMT = S.RCVD_TOT_DRUG_ADM_FEE_AMT,
    T.RCVD_TOT_COPAY_AMT = S.RCVD_TOT_COPAY_AMT,
    T.RCVD_TOT_PD_AMT = S.RCVD_TOT_PD_AMT,
    T.RCVD_TOT_QTY_CT = S.RCVD_TOT_QTY_CT,
    T.CALC_TOT_RCRD_CT = S.CALC_TOT_RCRD_CT,
    T.CALC_TOT_INGR_CST_AMT = S.CALC_TOT_INGR_CST_AMT,
    T.CALC_TOT_DRUG_ADM_FEE_AMT = S.CALC_TOT_DRUG_ADM_FEE_AMT,
    T.CALC_TOT_COPAY_AMT = S.CALC_TOT_COPAY_AMT,
    T.CALC_TOT_PD_AMT = S.CALC_TOT_PD_AMT,
    T.CALC_TOT_QTY_CT = S.CALC_TOT_QTY_CT
WHEN NOT MATCHED THEN INSERT (
    FILE_DT_SK,
    SRC_SYS_GRP_ID,
    RCRD_TYP_NM,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_DT_SK,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    GRP_ID,
    SRC_SYS_COV_NO,
    RCVD_TOT_RCRD_CT,
    RCVD_TOT_INGR_CST_AMT,
    RCVD_TOT_DRUG_ADM_FEE_AMT,
    RCVD_TOT_COPAY_AMT,
    RCVD_TOT_PD_AMT,
    RCVD_TOT_QTY_CT,
    CALC_TOT_RCRD_CT,
    CALC_TOT_INGR_CST_AMT,
    CALC_TOT_DRUG_ADM_FEE_AMT,
    CALC_TOT_COPAY_AMT,
    CALC_TOT_PD_AMT,
    CALC_TOT_QTY_CT
) VALUES (
    S.FILE_DT_SK,
    S.SRC_SYS_GRP_ID,
    S.RCRD_TYP_NM,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    S.GRP_ID,
    S.SRC_SYS_COV_NO,
    S.RCVD_TOT_RCRD_CT,
    S.RCVD_TOT_INGR_CST_AMT,
    S.RCVD_TOT_DRUG_ADM_FEE_AMT,
    S.RCVD_TOT_COPAY_AMT,
    S.RCVD_TOT_PD_AMT,
    S.RCVD_TOT_QTY_CT,
    S.CALC_TOT_RCRD_CT,
    S.CALC_TOT_INGR_CST_AMT,
    S.CALC_TOT_DRUG_ADM_FEE_AMT,
    S.CALC_TOT_COPAY_AMT,
    S.CALC_TOT_PD_AMT,
    S.CALC_TOT_QTY_CT
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)