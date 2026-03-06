
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def run_McdClmDtlPK(
    df_AllCol: DataFrame,
    df_TransformIt: DataFrame,
    params: dict,
) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    execute_dml(
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_MCAID_CLM_DTL_TEMP')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    (
        df_TransformIt.write.mode("append")
        .jdbc(url=ids_jdbc_url, table=f"{IDSOwner}.K_MCAID_CLM_DTL_TEMP", properties=ids_jdbc_props)
    )

    extract_query = f"""
    SELECT k.MCAID_CLM_DTL_SK,
           w.SRC_SYS_CD_SK,
           w.MCAID_ICN_NO,
           w.MCAID_CLM_PAYMT_TYP_CD,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MCAID_CLM_DTL_TEMP w
    JOIN {IDSOwner}.K_MCAID_CLM_DTL k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.MCAID_ICN_NO = k.MCAID_ICN_NO
     AND w.MCAID_CLM_PAYMT_TYP_CD = k.MCAID_CLM_PAYMT_TYP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MCAID_ICN_NO,
           w2.MCAID_CLM_PAYMT_TYP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_MCAID_CLM_DTL_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MCAID_CLM_DTL k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.MCAID_ICN_NO = k2.MCAID_ICN_NO
          AND w2.MCAID_CLM_PAYMT_TYP_CD = k2.MCAID_CLM_PAYMT_TYP_CD
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "RAR_ICN_NBR", "RAR_RCRD_TYP_CDE"],
        [],
    )

    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(F.col("MCAID_CLM_DTL_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("MCAID_CLM_DTL_SK")),
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>,
    )

    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("MCAID_ICN_NO"),
            F.col("MCAID_CLM_PAYMT_TYP_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MCAID_CLM_DTL_SK"),
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_MCAID_CLM_DTL.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    df_Keys = df_enriched.select(
        F.col("svSK").alias("MCAID_CLM_DTL_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("MCAID_ICN_NO"),
        F.col("MCAID_CLM_PAYMT_TYP_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )

    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("MCAID_ICN_NO"),
        F.col("MCAID_CLM_PAYMT_TYP_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CUST_SVC_TASK_LINK_SK"),
    )

    write_files(
        df_updt,
        f"{adls_path}/McdClmDtlPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    df_key = (
        df_AllCol_dedup.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK"),
            "left",
        )
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.MCAID_CLM_DTL_SK").alias("MCAID_CLM_DTL_SK"),
            F.col("Keys.MCAID_ICN_NO").alias("MCAID_ICN_NO"),
            F.col("Keys.MCAID_CLM_PAYMT_TYP_CD").alias("MCAID_CLM_PAYMT_TYP_CD"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.CLM_SK").alias("CLM_SK"),
            F.col("AllColOut.CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
            F.col("AllColOut.CLM_ID").alias("CLM_ID"),
            F.col("AllColOut.RAR_LI_FRST_SVC_DTE").alias("FIRST_SVC_DT_SK"),
            F.col("AllColOut.RAR_LI_LAST_SVC_DTE").alias("LAST_SVC_DT_SK"),
            F.col("AllColOut.RAR_RCPNT_DCN").alias("MCAID_RECPNT_NO"),
            F.col("AllColOut.RAR_RCPNT_LST_NME").alias("MCAID_RECPNT_LAST_NM"),
            F.col("AllColOut.RAR_RCPNT_FRST_NME").alias("MCAID_RECPNT_FIRST_NM"),
            F.col("AllColOut.PRCS_DT_SK").alias("PRCS_DT_SK"),
            F.col("AllColOut.RAR_REIM_AMT").alias("TRANS_AMT"),
        )
    )

    return df_key
