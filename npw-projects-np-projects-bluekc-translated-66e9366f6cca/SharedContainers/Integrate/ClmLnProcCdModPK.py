
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmLnProcCdModPK
Description:
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Clm Ln Proc Cd Mod job

CALLED BY : FctsClmLnProcCdModExtr and NascoClmLnProcCdModExtr
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lit

# COMMAND ----------
def run_ClmLnProcCdModPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    adls_path          = params["adls_path"]
    # ------------------------------------------------------------------
    # Stage: hf_clm_ln_proc_cd_mod_allcol  (intermediate hash-file → dedup)
    partition_cols = [
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD"
    ]
    df_allcol_out = dedup_sort(df_AllCol, partition_cols, [])
    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_PROC_CD_MOD_TEMP  (load temp table then extract keys)
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_CLM_LN_PROC_CD_MOD_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform.write
        .format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_PROC_CD_MOD_TEMP")
        .save()
    )

    execute_dml(
        f"""CALL SYSPROC.ADMIN_CMD(
              'runstats on table {IDSOwner}.K_CLM_LN_PROC_CD_MOD_TEMP
               on key columns with distribution on key columns
               and detailed indexes all allow write access')""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
    SELECT k.CLM_LN_PROC_CD_MOD_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           w.CLM_LN_PROC_CD_MOD_ORDNL_CD,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CLM_LN_PROC_CD_MOD_TEMP w
      JOIN {IDSOwner}.K_CLM_LN_PROC_CD_MOD k
        ON w.SRC_SYS_CD_SK              = k.SRC_SYS_CD_SK
       AND w.CLM_ID                     = k.CLM_ID
       AND w.CLM_LN_SEQ_NO              = k.CLM_LN_SEQ_NO
       AND w.CLM_LN_PROC_CD_MOD_ORDNL_CD= k.CLM_LN_PROC_CD_MOD_ORDNL_CD
    UNION
    SELECT -1                 AS CLM_LN_PROC_CD_MOD_SK,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           w2.CLM_LN_PROC_CD_MOD_ORDNL_CD,
           {CurrRunCycle}      AS CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CLM_LN_PROC_CD_MOD_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_CLM_LN_PROC_CD_MOD k2
            WHERE w2.SRC_SYS_CD_SK              = k2.SRC_SYS_CD_SK
              AND w2.CLM_ID                     = k2.CLM_ID
              AND w2.CLM_LN_SEQ_NO              = k2.CLM_LN_SEQ_NO
              AND w2.CLM_LN_PROC_CD_MOD_ORDNL_CD= k2.CLM_LN_PROC_CD_MOD_ORDNL_CD )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # ------------------------------------------------------------------
    # Stage: PrimaryKey  (transformations & surrogate keys)
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("CLM_LN_PROC_CD_MOD_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svClmId", trim(col("CLM_ID")))
        .withColumn("svClmLnProcCdModOrdnlCd", trim(col("CLM_LN_PROC_CD_MOD_ORDNL_CD")))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svSK", col("CLM_LN_PROC_CD_MOD_SK"))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # Output link: updt  (hash-file → parquet)
    df_updt = df_enriched.select(
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("svClmId").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("svClmLnProcCdModOrdnlCd").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("CLM_LN_PROC_CD_MOD_SK")
    )

    file_path_hf = f"{adls_path}/ClmLnProcCdModPK_updt.parquet"
    write_files(df_updt, file_path_hf, ',', 'overwrite', True, True, '"', None)
    # ------------------------------------------------------------------
    # Output link: NewKeys  (sequential file)
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            col("svClmId").alias("CLM_ID"),
            "CLM_LN_SEQ_NO",
            col("svClmLnProcCdModOrdnlCd").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_LN_PROC_CD_MOD_SK")
        )
    )

    file_path_seq = f"{adls_path}/load/K_CLM_LN_PROC_CD_MOD.dat"
    write_files(df_newkeys, file_path_seq, ',', 'overwrite', False, True, '"', None)
    # ------------------------------------------------------------------
    # Output link: Keys  (to Merge)
    df_keys = df_enriched.select(
        col("svSK").alias("CLM_LN_PROC_CD_MOD_SK"),
        "SRC_SYS_CD_SK",
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("svClmId").alias("CLM_ID"),
        "CLM_LN_SEQ_NO",
        col("svClmLnProcCdModOrdnlCd").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ------------------------------------------------------------------
    # Stage: Merge  (join Keys with AllColOut)
    df_merge = (
        df_allcol_out.alias("a")
        .join(
            df_keys.alias("k"),
            [
                col("a.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK"),
                col("a.CLM_ID") == col("k.CLM_ID"),
                col("a.CLM_LN_SEQ_NO") == col("k.CLM_LN_SEQ_NO"),
                col("a.CLM_LN_PROC_CD_MOD_ORDNL_CD") == col("k.CLM_LN_PROC_CD_MOD_ORDNL_CD")
            ],
            "left"
        )
    )

    df_final = df_merge.select(
        col("a.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("a.DISCARD_IN").alias("DISCARD_IN"),
        col("a.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("a.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("a.ERR_CT").alias("ERR_CT"),
        col("a.RECYCLE_CT").alias("RECYCLE_CT"),
        col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("a.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("k.CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
        col("a.CLM_ID").alias("CLM_ID"),
        col("a.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("a.CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("a.PROC_CD_MOD_CD").alias("PROC_CD_MOD_CD")
    )
    # ------------------------------------------------------------------
    return df_final
# COMMAND ----------
