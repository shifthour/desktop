
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# Databricks notebook source
"""
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
    IDS Primary Key Container for QHP_ENR
    Hash file (hf_qhp_enr_allcol) cleared in calling job
    SQL joins temp table with key table to assign known keys
    Temp table is truncated before load and runstat done after load
    Load IDS temp. table
    join primary key info with table info
    update primary key table (QHP_ENR) with new keys created today
    primary key hash file only contains current run keys and is cleared before writing
    Assign primary surrogate key

CALLED BY : FctsBCBSQhpEnrExtr
PROCESSING:

MODIFICATIONS:
Developer               Date            Change Description              Project/Altius #      Development Project       Code Reviewer      Date Reviewed
--------------------    -------------   ----------------------------    -------------------   ------------------------   ----------------   -------------
Santosh Bokka           2013-12-31      Initial program                5235 Primary Key       Integratenewdevl          Kalyan Neelam      2014-01-10
"""

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple, List

# COMMAND ----------

def run_QhpEnrPK(
    df_ClmNotePK: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared Container: QhpEnrPK
    Performs primary-key generation and related enrichment for QHP_ENR records.

    Parameters
    ----------
    df_ClmNotePK : DataFrame
        Input stream from the calling job (link “ClmNotePK”).
    params : dict
        Runtime parameters passed from the parent job.

    Returns
    -------
    DataFrame
        Output stream “Key” back to the calling job.
    """

    # --------------------------------------------------
    # 1. Unpack parameters (exactly once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    IDSOwner          = params["IDSOwner"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # --------------------------------------------------
    # 2. Replace intermediate hashed-files (Scenario a)
    #    a) hf_qhp_enr_allcol  -> df_allcol_out
    #    b) hf_qhpenr_pk_dedupe -> df_dslink69
    # --------------------------------------------------
    key_cols_allcol: List[str] = [
        "QHP_ID",
        "QHP_EFF_DT_SK",
        "GRP_ID",
        "SUBGRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "PEDI_SUB_IN",
        "QHP_ENR_EFF_DT_SK",
        "PEDI_DNTL_CLS_PLN_ID"
    ]
    # Deduplicate full-column stream
    df_allcol_out = dedup_sort(
        df_ClmNotePK,
        partition_cols=key_cols_allcol,
        sort_cols=[]
    )

    # Deduplicate PK-only stream
    key_cols_dslink69: List[str] = key_cols_allcol + ["SRC_SYS_CD_SK"]
    df_dslink69 = dedup_sort(
        df_ClmNotePK.select(*key_cols_dslink69),
        partition_cols=key_cols_dslink69,
        sort_cols=[]
    )

    # --------------------------------------------------
    # 3. Load temp table {IDSOwner}.K_QHP_ENR_TEMP
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_QHP_ENR_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_dslink69.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_QHP_ENR_TEMP")
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # 4. Extract W_Extract result-set from IDS
    # --------------------------------------------------
    extract_query = f"""
    SELECT  
            w.QHP_ID,
            w.QHP_EFF_DT_SK,
            w.GRP_ID,
            w.SUBGRP_ID,
            w.CLS_ID,
            w.CLS_PLN_ID,
            w.PEDI_SUB_IN,
            w.QHP_ENR_EFF_DT_SK,
            w.PEDI_DNTL_CLS_PLN_ID,
            w.SRC_SYS_CD_SK,
            k.QHP_ENR_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_QHP_ENR_TEMP w,
         {IDSOwner}.K_QHP_ENR k
    WHERE w.SRC_SYS_CD_SK            = k.SRC_SYS_CD_SK
      AND w.QHP_ID                   = k.QHP_ID
      AND w.QHP_EFF_DT_SK            = k.QHP_EFF_DT_SK
      AND w.GRP_ID                   = k.GRP_ID
      AND w.SUBGRP_ID                = k.SUBGRP_ID
      AND w.CLS_ID                   = k.CLS_ID
      AND w.CLS_PLN_ID               = k.CLS_PLN_ID
      AND w.PEDI_SUB_IN              = k.PEDI_SUB_IN
      AND w.QHP_ENR_EFF_DT_SK        = k.QHP_ENR_EFF_DT_SK
      AND w.PEDI_DNTL_CLS_PLN_ID     = k.PEDI_DNTL_CLS_PLN_ID
    UNION
    SELECT 
            w2.QHP_ID,
            w2.QHP_EFF_DT_SK,
            w2.GRP_ID,
            w2.SUBGRP_ID,
            w2.CLS_ID,
            w2.CLS_PLN_ID,
            w2.PEDI_SUB_IN,
            w2.QHP_ENR_EFF_DT_SK,
            w2.PEDI_DNTL_CLS_PLN_ID,
            w2.SRC_SYS_CD_SK,
            -1,
            {CurrRunCycle}
    FROM {IDSOwner}.K_QHP_ENR_TEMP w2
    WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_QHP_ENR k2
            WHERE w2.SRC_SYS_CD_SK            = k2.SRC_SYS_CD_SK
              AND w2.QHP_ID                   = k2.QHP_ID
              AND w2.QHP_EFF_DT_SK            = k2.QHP_EFF_DT_SK
              AND w2.GRP_ID                   = k2.GRP_ID
              AND w2.SUBGRP_ID                = k2.SUBGRP_ID
              AND w2.CLS_ID                   = k2.CLS_ID
              AND w2.CLS_PLN_ID               = k2.CLS_PLN_ID
              AND w2.PEDI_SUB_IN              = k2.PEDI_SUB_IN
              AND w2.QHP_ENR_EFF_DT_SK        = k2.QHP_ENR_EFF_DT_SK
              AND w2.PEDI_DNTL_CLS_PLN_ID     = k2.PEDI_DNTL_CLS_PLN_ID
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # 5. PrimaryKey Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("QHP_ENR_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # SurrogateKeyGen replacement for KeyMgtGetNextValueConcurrent
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'QHP_ENR_SK',<schema>,<secret_name>)

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    ).withColumn(
        "SRC_SYS_CD",
        F.lit(SrcSysCd)
    )

    # Updt link (to hf_qhp_enr)
    cols_updt = [
        "QHP_ID","QHP_EFF_DT_SK","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PEDI_SUB_IN",
        "QHP_ENR_EFF_DT_SK","PEDI_DNTL_CLS_PLN_ID","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK",
        "QHP_ENR_SK"
    ]
    df_updt = df_enriched.select(*cols_updt)

    # NewKeys link (insert only)
    cols_newkeys = [
        "QHP_ID","QHP_EFF_DT_SK","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PEDI_SUB_IN",
        "QHP_ENR_EFF_DT_SK","PEDI_DNTL_CLS_PLN_ID","SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK","QHP_ENR_SK"
    ]
    df_new_keys = df_enriched.filter(F.col("svInstUpdt") == F.lit("I")).select(*cols_newkeys)

    # Keys link
    cols_keys = [
        "QHP_ENR_SK","SRC_SYS_CD_SK","SRC_SYS_CD","QHP_ID","QHP_EFF_DT_SK","GRP_ID","SUBGRP_ID",
        "CLS_ID","CLS_PLN_ID","PEDI_SUB_IN","QHP_ENR_EFF_DT_SK","PEDI_DNTL_CLS_PLN_ID",
        "svInstUpdt","svCrtRunCycExctnSk"
    ]
    df_keys = (
        df_enriched
        .withColumnRenamed("svInstUpdt", "INSRT_UPDT_CD")
        .withColumnRenamed("svCrtRunCycExctnSk", "CRT_RUN_CYC_EXCTN_SK")
        .select(*[
            "QHP_ENR_SK","SRC_SYS_CD_SK","SRC_SYS_CD","QHP_ID","QHP_EFF_DT_SK","GRP_ID","SUBGRP_ID",
            "CLS_ID","CLS_PLN_ID","PEDI_SUB_IN","QHP_ENR_EFF_DT_SK","PEDI_DNTL_CLS_PLN_ID",
            "INSRT_UPDT_CD","CRT_RUN_CYC_EXCTN_SK"
        ])
    )

    # --------------------------------------------------
    # 6. Merge Transformer logic
    # --------------------------------------------------
    join_expr = [
        df_allcol_out[col] == df_keys[col] for col in [
            "QHP_ID","QHP_EFF_DT_SK","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID",
            "PEDI_SUB_IN","QHP_ENR_EFF_DT_SK","PEDI_DNTL_CLS_PLN_ID","SRC_SYS_CD_SK"
        ]
    ]
    df_merge = (
        df_allcol_out.alias("AllColOut")
        .join(df_keys.alias("Keys"), join_expr, "left")
    )

    df_Key_output = (
        df_merge
        .select(
            "AllColOut.JOB_EXCTN_RCRD_ERR_SK",
            "Keys.INSRT_UPDT_CD",
            "AllColOut.DISCARD_IN",
            "AllColOut.PASS_THRU_IN",
            "AllColOut.FIRST_RECYC_DT",
            "AllColOut.ERR_CT",
            "AllColOut.RECYCLE_CT",
            "Keys.SRC_SYS_CD",
            "AllColOut.PRI_KEY_STRING",
            "Keys.QHP_ENR_SK",
            "AllColOut.QHP_ID",
            "AllColOut.QHP_EFF_DT_SK",
            "AllColOut.GRP_ID",
            "AllColOut.SUBGRP_ID",
            "AllColOut.CLS_ID",
            "AllColOut.CLS_PLN_ID",
            "AllColOut.PEDI_SUB_IN",
            "AllColOut.QHP_ENR_EFF_DT_SK",
            "AllColOut.PEDI_DNTL_CLS_PLN_ID",
            "AllColOut.SRC_SYS_CD_SK",
            "Keys.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "AllColOut.PEDI_DNTL_PROD_ID",
            "AllColOut.PROD_ID",
            "AllColOut.CLS_PROD_CAT_CD",
            "AllColOut.PEDI_DNTL_CAT_CD",
            "AllColOut.QHP_ENR_TERM_DT_SK",
            "AllColOut.PEDI_DNTL_MAX_AGE_NO"
        )
    )

    # --------------------------------------------------
    # 7. Egress handling
    #    a) Write hf_qhp_enr (Parquet, scenario c)
    #    b) Write K_QHP_ENR.dat (CSV)
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/hf_qhp_enr_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    write_files(
        df_new_keys,
        f"{adls_path}/load/K_QHP_ENR.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # 8. Return container output stream
    # --------------------------------------------------
    return df_Key_output
