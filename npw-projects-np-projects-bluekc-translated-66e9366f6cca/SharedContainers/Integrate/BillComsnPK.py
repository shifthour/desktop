# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
BillComsnPK – Shared container used for Primary Keying of Commission Bill Comsn job.

DESCRIPTION:
    Shared container used for Primary Keying of Comm Bill Comsn job
    CALLED BY : FctsComsnBillComsnExtr

ANNOTATIONS:
    IDS Primary Key Container for Commission Bill Comsn
    Used by  FctsComsnBillComsnExtr
    Hash file (hf_bill_comsn_allcol) cleared in calling job
    SQL joins temp table with key table to assign known keys
    Temp table is truncated before load and runstat done after load
    Load IDS temp. table
    join primary key info with table info
    update primary key table (K_BILL_COMSN) with new keys created today
    primary key hash file only contains current run keys and is cleared before writing
    Assign primary surrogate key

COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def run_BillComsnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the primary-key assignment logic for BillComsnPK shared container.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Input link “AllCol” – all bill commission columns from upstream job.
    df_Transform: DataFrame
        Input link “Transform” – source rows to stage into temp table K_BILL_COMSN_TEMP.
    params      : dict
        Runtime parameters and JDBC/config dictionaries.

    Returns
    -------
    DataFrame
        Output link “Key” – enriched rows containing primary-key information.
    """

    # ──────────────────────────────────────────────────────────────────────────────
    # 1. Parameter unpacking
    # ──────────────────────────────────────────────────────────────────────────────
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    ids_secret_name     = params["ids_secret_name"]

    # ──────────────────────────────────────────────────────────────────────────────
    # 2. Handle intermediate hash file hf_bill_comsn_allcol  (scenario a)
    #    Replace with de-duplication logic
    # ──────────────────────────────────────────────────────────────────────────────
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "BLEI_CK", "CSPI_ID", "PMFA_ID", "BLCO_SEQ_NO"],
        []
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 3. Stage input rows into IDS.K_BILL_COMSN_TEMP and collect W_Extract rows
    # ──────────────────────────────────────────────────────────────────────────────
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_BILL_COMSN_TEMP")
        .mode("overwrite")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_BILL_COMSN_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
    SELECT  k.BILL_COMSN_SK,
            w.SRC_SYS_CD_SK,
            w.BILL_ENTY_UNIQ_KEY,
            w.CLS_PLN_ID,
            w.FEE_DSCNT_ID,
            w.SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM   {IDSOwner}.K_BILL_COMSN_TEMP w,
           {IDSOwner}.K_BILL_COMSN       k
    WHERE  w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
      AND  w.BILL_ENTY_UNIQ_KEY   = k.BILL_ENTY_UNIQ_KEY
      AND  w.CLS_PLN_ID           = k.CLS_PLN_ID
      AND  w.FEE_DSCNT_ID         = k.FEE_DSCNT_ID
      AND  w.SEQ_NO               = k.SEQ_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.BILL_ENTY_UNIQ_KEY,
           w2.CLS_PLN_ID,
           w2.FEE_DSCNT_ID,
           w2.SEQ_NO,
           {CurrRunCycle}
    FROM   {IDSOwner}.K_BILL_COMSN_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM   {IDSOwner}.K_BILL_COMSN k2
        WHERE  w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
          AND  w2.BILL_ENTY_UNIQ_KEY = k2.BILL_ENTY_UNIQ_KEY
          AND  w2.FEE_DSCNT_ID       = k2.FEE_DSCNT_ID
          AND  w2.CLS_PLN_ID         = k2.CLS_PLN_ID
          AND  w2.SEQ_NO             = k2.SEQ_NO
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 4. Primary-key logic (Transformer “PrimaryKey”)
    # ──────────────────────────────────────────────────────────────────────────────
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("BILL_COMSN_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svClsPlnId",  F.trim(F.col("CLS_PLN_ID")))
        .withColumn("svFeeDscntId", F.trim(F.col("FEE_DSCNT_ID")))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast(T.LongType()))
             .otherwise(F.col("BILL_COMSN_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(int(CurrRunCycle))).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ──────────────────────────────────────────────────────────────────────────────
    # 5. Build output links from transformer
    # ──────────────────────────────────────────────────────────────────────────────
    df_updt = (
        df_enriched.select(
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            F.col("BILL_ENTY_UNIQ_KEY"),
            F.col("svClsPlnId").alias("CLS_PLN_ID"),
            F.col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            F.col("SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("BILL_COMSN_SK")
        )
    )

    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I").select(
            "SRC_SYS_CD_SK",
            "BILL_ENTY_UNIQ_KEY",
            F.col("svClsPlnId").alias("CLS_PLN_ID"),
            F.col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            "SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("BILL_COMSN_SK")
        )
    )

    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("BILL_COMSN_SK"),
            "SRC_SYS_CD_SK",
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            "BILL_ENTY_UNIQ_KEY",
            F.col("svClsPlnId").alias("CLS_PLN_ID"),
            F.col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            "SEQ_NO",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 6. Persist hash-file output (hf_comsn_bill_comsn) as Parquet (scenario c)
    # ──────────────────────────────────────────────────────────────────────────────
    write_files(
        df_updt,
        f"{adls_path}/BillComsnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 7. Persist NewKeys sequential file
    # ──────────────────────────────────────────────────────────────────────────────
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_BILL_COMSN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 8. Final Merge transformer to create container output “Key”
    # ──────────────────────────────────────────────────────────────────────────────
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.BLEI_CK")       == F.col("k.BILL_ENTY_UNIQ_KEY")) &
        (F.col("all.CSPI_ID")      == F.col("k.CLS_PLN_ID")) &
        (F.col("all.PMFA_ID")      == F.col("k.FEE_DSCNT_ID")) &
        (F.col("all.BLCO_SEQ_NO")  == F.col("k.SEQ_NO"))
    )

    df_result = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.BILL_COMSN_SK"),
            F.col("k.SRC_SYS_CD_SK"),
            F.col("all.BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
            F.col("all.CSPI_ID").alias("CLS_PLN_ID"),
            F.col("all.PMFA_ID").alias("FEE_DSCNT_ID"),
            F.col("all.BLCO_SEQ_NO").alias("SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(int(CurrRunCycle)).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.BLEI_CK").alias("BILL_ENTY_SK"),
            F.col("all.CSPI_ID").alias("CLS_PLN_SK"),
            F.col("all.COAR_ID").alias("COMSN_ARGMT"),
            F.col("all.PMFA_ID").alias("FEE_DSCNT_SK"),
            F.col("all.BLCO_EFF_DT").alias("EFF_DT_SK"),
            F.col("all.BLCO_TERM_DT").alias("TERM_DT_SK"),
            F.col("all.BLCO_PCT").alias("PRM_PCT")
        )
    )

    # ──────────────────────────────────────────────────────────────────────────────
    # 9. Return container output
    # ──────────────────────────────────────────────────────────────────────────────
    return df_result