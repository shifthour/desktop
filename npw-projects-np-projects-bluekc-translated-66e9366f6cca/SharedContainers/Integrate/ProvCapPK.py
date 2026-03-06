# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from functools import reduce
from typing import Tuple

def run_ProvCapPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> Tuple[DataFrame, DataFrame]:
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------  load temp table --------------------
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PROV_CAP_TEMP")
        .mode("overwrite")
        .save()
    )
    # --------------------  extract keys --------------------
    extract_query = f"""
    SELECT k.PROV_CAP_SK,
           w.SRC_SYS_CD_SK,
           w.PD_DT_SK,
           w.CAP_PROV_ID,
           w.PD_PROV_ID,
           w.PROV_CAP_PAYMT_LOB_CD,
           w.PROV_CAP_PAYMT_METH_CD,
           w.PROV_CAP_PAYMT_CAP_TYP_CD,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_PROV_CAP_TEMP w
    JOIN {IDSOwner}.K_PROV_CAP k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.PD_DT_SK = k.PD_DT_SK
     AND w.CAP_PROV_ID = k.CAP_PROV_ID
     AND w.PD_PROV_ID = k.PD_PROV_ID
     AND w.PROV_CAP_PAYMT_LOB_CD = k.PROV_CAP_PAYMT_LOB_CD
     AND w.PROV_CAP_PAYMT_METH_CD = k.PROV_CAP_PAYMT_METH_CD
     AND w.PROV_CAP_PAYMT_CAP_TYP_CD = k.PROV_CAP_PAYMT_CAP_TYP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.PD_DT_SK,
           w2.CAP_PROV_ID,
           w2.PD_PROV_ID,
           w2.PROV_CAP_PAYMT_LOB_CD,
           w2.PROV_CAP_PAYMT_METH_CD,
           w2.PROV_CAP_PAYMT_CAP_TYP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_PROV_CAP_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_PROV_CAP k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.PD_DT_SK = k2.PD_DT_SK
          AND w2.CAP_PROV_ID = k2.CAP_PROV_ID
          AND w2.PD_PROV_ID = k2.PD_PROV_ID
          AND w2.PROV_CAP_PAYMT_LOB_CD = k2.PROV_CAP_PAYMT_LOB_CD
          AND w2.PROV_CAP_PAYMT_METH_CD = k2.PROV_CAP_PAYMT_METH_CD
          AND w2.PROV_CAP_PAYMT_CAP_TYP_CD = k2.PROV_CAP_PAYMT_CAP_TYP_CD
    )
    """
    df_w_extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # --------------------  primary key transformation --------------------
    df_enriched = (
        df_w_extract
        .withColumn("svInstUpdt", when(col("PROV_CAP_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svCrtRunCycExctnSk", when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svSK", col("PROV_CAP_SK"))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # --------------------  build outputs from transformer --------------------
    df_updt = df_enriched.select(
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        "PD_DT_SK",
        "CAP_PROV_ID",
        "PD_PROV_ID",
        "PROV_CAP_PAYMT_LOB_CD",
        "PROV_CAP_PAYMT_METH_CD",
        "PROV_CAP_PAYMT_CAP_TYP_CD",
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("PROV_CAP_SK")
    )
    df_newKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "PD_DT_SK",
            "CAP_PROV_ID",
            "PD_PROV_ID",
            "PROV_CAP_PAYMT_LOB_CD",
            "PROV_CAP_PAYMT_METH_CD",
            "PROV_CAP_PAYMT_CAP_TYP_CD",
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("PROV_CAP_SK")
        )
    )
    df_keys = df_enriched.select(
        col("svSK").alias("PROV_CAP_SK"),
        "SRC_SYS_CD_SK",
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        "PD_DT_SK",
        "CAP_PROV_ID",
        "PD_PROV_ID",
        "PROV_CAP_PAYMT_LOB_CD",
        "PROV_CAP_PAYMT_METH_CD",
        "PROV_CAP_PAYMT_CAP_TYP_CD",
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------  dedup intermediate hash file --------------------
    key_cols = [
        "SRC_SYS_CD_SK",
        "PD_DT_SK",
        "CAP_PROV_ID",
        "PD_PROV_ID",
        "PROV_CAP_PAYMT_LOB_CD",
        "PROV_CAP_PAYMT_METH_CD",
        "PROV_CAP_PAYMT_CAP_TYP_CD"
    ]
    df_allcol_dedup = dedup_sort(df_AllCol, key_cols, [])
    # --------------------  merge --------------------
    join_expr = reduce(lambda x, y: x & y, [df_allcol_dedup[c] == df_keys[c] for c in key_cols])
    df_merge = df_allcol_dedup.alias("AllColOut").join(df_keys.alias("Keys"), join_expr, "left")
    df_lnkOut = df_merge.select(
        col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
        col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("AllColOut.ERR_CT").alias("ERR_CT"),
        col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
        col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("Keys.PROV_CAP_SK").alias("PROV_CAP_SK"),
        col("AllColOut.PD_DT_SK").alias("PD_DT_SK"),
        col("AllColOut.CAP_PROV_ID").alias("CAP_PROV_ID"),
        col("AllColOut.PD_PROV_ID").alias("PD_PROV_ID"),
        col("AllColOut.PROV_CAP_PAYMT_LOB_CD").alias("PROV_CAP_PAYMT_LOB_CD"),
        col("AllColOut.PROV_CAP_PAYMT_METH_CD").alias("PROV_CAP_PAYMT_METH_CD"),
        col("AllColOut.PROV_CAP_PAYMT_CAP_TYP_CD").alias("PROV_CAP_PAYMT_CAP_TYP_CD"),
        col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AllColOut.PAYMT_SUM_CD").alias("PAYMT_SUM_CD"),
        col("AllColOut.AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
        col("AllColOut.CUR_CAP_AMT").alias("CUR_CAP_AMT"),
        col("AllColOut.MAN_ADJ_AMT").alias("MAN_ADJ_AMT"),
        col("AllColOut.NET_AMT").alias("NET_AMT"),
        col("AllColOut.AUTO_ADJ_MBR_MO_CT").alias("AUTO_ADJ_MBR_MO_CT"),
        col("AllColOut.CUR_MBR_MO_CT").alias("CUR_MBR_MO_CT"),
        col("AllColOut.MNL_ADJ_MBR_MO_CT").alias("MNL_ADJ_MBR_MO_CT")
    )
    df_paymntsum = df_allcol_dedup.select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("PAYMT_SUM_CD"),
        col("PROV_CAP_PAYMT_LOB_CD")
    )
    # --------------------  write outputs --------------------
    write_files(
        df_newKeys,
        f"{adls_path}/load/K_PROV_CAP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )
    write_files(
        df_updt,
        f"{adls_path}/ProvCapPK_updt.parquet",
        mode="overwrite",
        is_pqruet=True
    )
    return df_lnkOut, df_paymntsum