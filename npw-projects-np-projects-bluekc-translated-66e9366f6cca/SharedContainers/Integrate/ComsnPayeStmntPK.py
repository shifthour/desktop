# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
ComsnPayeStmntPK
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Comm Paye Stmtn job
CALLED BY : FctsComsnPayeStmntExtr

* VC LOGS *
^1_1 03/04/09 10:05:04 Batch  15039_36308 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 03/04/09 09:57:49 Batch  15039_35874 INIT bckcett testIDS dsadm BLS FOR SA
^1_1 01/13/09 09:00:06 Batch  14989_32412 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

Annotations:
- IDS Primary Key Container for Commission Paye Statement
- Used by FctsComsnPayeStmntExtr
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_COMSN_PAYE_STMNT) with new keys created today
- Assign primary surrogate key
- Hash file (hf_comsn_paye_stmt_allcol) cleared in calling job
- join primary key info with table info
- Load IDS temp. table
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    substring
)


def run_ComsnPayeStmntPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the ComsnPayeStmntPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input DataFrame corresponding to link `AllCol`.
    df_Transform : DataFrame
        Input DataFrame corresponding to link `Transform`.
    params : dict
        Runtime parameters dictionary.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to link `Key`.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each unpacked exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    RunID               = params["RunID"]
    CurrentDate         = params["CurrentDate"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_comsn_paye_stmt_allcol  (Scenario a – intermediate hash)
    #  - Deduplicate on key columns before further processing
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "SRC_SYS_CD_SK",
        "INSTNC_ID",
        "AGENT_ID",
        "BLEI_CK",
        "PRM_DUE_DT",
        "ARGMT_ID",
        "TAX_ID",
        "ROW_NBR"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=key_cols_allcol,
        sort_cols=[]
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_PAYE_STMNT_TEMP  (DB2 ‑ write temp table & extract)
    # ------------------------------------------------------------------
    # truncate temp table
    execute_dml(
        query=f"TRUNCATE TABLE {IDSOwner}.K_COMSN_PAYE_STMNT_TEMP",
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    # load temp table
    (
        df_Transform.write
        .format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_COMSN_PAYE_STMNT_TEMP")
        .save()
    )

    # runstats
    execute_dml(
        query=f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_PAYE_STMNT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    # extract W_Extract query
    extract_query = f"""
    SELECT  k.COMSN_PAYE_STMNT_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_BTCH_INST_ID,
            w.AGNT_ID,
            w.BILL_ENTY_UNIQ_KEY,
            w.PRM_DUE_DT,
            w.COMSN_ARGMT_ID,
            w.TAX_ID,
            w.COMSN_PAYE_STMNT_SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_PAYE_STMNT_TEMP w,
         {IDSOwner}.K_COMSN_PAYE_STMNT k
    WHERE w.SRC_SYS_CD_SK             = k.SRC_SYS_CD_SK
      AND w.COMSN_BTCH_INST_ID        = k.COMSN_BTCH_INST_ID
      AND w.AGNT_ID                   = k.AGNT_ID
      AND w.BILL_ENTY_UNIQ_KEY        = k.BILL_ENTY_UNIQ_KEY
      AND w.PRM_DUE_DT                = k.PRM_DUE_DT
      AND w.COMSN_ARGMT_ID            = k.COMSN_ARGMT_ID
      AND w.TAX_ID                    = k.TAX_ID
      AND w.COMSN_PAYE_STMNT_SEQ_NO   = k.COMSN_PAYE_STMNT_SEQ_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_BTCH_INST_ID,
           w2.AGNT_ID,
           w2.BILL_ENTY_UNIQ_KEY,
           w2.PRM_DUE_DT,
           w2.COMSN_ARGMT_ID,
           w2.TAX_ID,
           w2.COMSN_PAYE_STMNT_SEQ_NO,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_PAYE_STMNT_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_COMSN_PAYE_STMNT k2
        WHERE w2.SRC_SYS_CD_SK           = k2.SRC_SYS_CD_SK
          AND w2.COMSN_BTCH_INST_ID      = k2.COMSN_BTCH_INST_ID
          AND w2.AGNT_ID                 = k2.AGNT_ID
          AND w2.BILL_ENTY_UNIQ_KEY      = k2.BILL_ENTY_UNIQ_KEY
          AND w2.PRM_DUE_DT              = k2.PRM_DUE_DT
          AND w2.COMSN_ARGMT_ID          = k2.COMSN_ARGMT_ID
          AND w2.TAX_ID                  = k2.TAX_ID
          AND w2.COMSN_PAYE_STMNT_SEQ_NO = k2.COMSN_PAYE_STMNT_SEQ_NO )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer) – Derivations
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("COMSN_PAYE_STMNT_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svAgntd", trim(col("AGNT_ID")))
        .withColumn("svComsnArgmtId", trim(col("COMSN_ARGMT_ID")))
        .withColumn("svTaxId", trim(col("TAX_ID")))
        .withColumn("svComsnBtchId", trim(col("COMSN_BTCH_INST_ID")))
        .withColumn("svSK", lit(0))
    )

    # surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svSK',
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Derive output links from transformer
    # ------------------------------------------------------------------
    # Link: updt
    df_updt = (
        df_enriched
        .select(
            col("SRC_SYS_CD_SK"),
            col("svComsnBtchId").alias("COMSN_BTCH_INST_ID"),
            col("svAgntd").alias("AGNT_ID"),
            col("BILL_ENTY_UNIQ_KEY"),
            col("PRM_DUE_DT"),
            col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            col("svTaxId").alias("TAX_ID"),
            col("COMSN_PAYE_STMNT_SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("COMSN_PAYE_STMNT_SK"),
            lit(SrcSysCd).alias("SRC_SYS_CD")
        )
    )

    # Link: NewKeys (constraint svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svComsnBtchId").alias("COMSN_BTCH_INST_ID"),
            col("svAgntd").alias("AGNT_ID"),
            col("BILL_ENTY_UNIQ_KEY"),
            col("PRM_DUE_DT"),
            col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            col("svTaxId").alias("TAX_ID"),
            col("COMSN_PAYE_STMNT_SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("COMSN_PAYE_STMNT_SK")
        )
    )

    # Link: Keys
    df_Keys = (
        df_enriched
        .select(
            col("svSK").alias("COMSN_PAYE_STMNT_SK"),
            col("SRC_SYS_CD_SK"),
            col("svComsnBtchId").alias("COMSN_BTCH_INST_ID"),
            col("svAgntd").alias("AGNT_ID"),
            col("BILL_ENTY_UNIQ_KEY"),
            col("PRM_DUE_DT"),
            col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            col("svTaxId").alias("TAX_ID"),
            col("COMSN_PAYE_STMNT_SEQ_NO"),
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer) – join AllColOut & Keys
    # ------------------------------------------------------------------
    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (
                (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
                (trim(col("all.INSTNC_ID")) == col("k.COMSN_BTCH_INST_ID")) &
                (trim(col("all.AGENT_ID")) == col("k.AGNT_ID")) &
                (col("all.BLEI_CK") == col("k.BILL_ENTY_UNIQ_KEY")) &
                (col("all.PRM_DUE_DT") == col("k.PRM_DUE_DT")) &
                (trim(col("all.ARGMT_ID")) == col("k.COMSN_ARGMT_ID")) &
                (trim(col("all.TAX_ID")) == col("k.TAX_ID")) &
                (col("all.ROW_NBR") == col("k.COMSN_PAYE_STMNT_SEQ_NO"))
            ),
            "left"
        )
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("k.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.COMSN_PAYE_STMNT_SK").alias("COMSN_PAYMT_STMNT_SK"),
            col("k.SRC_SYS_CD_SK"),
            trim(col("all.INSTNC_ID")).alias("COMSN_BTCH_INST_ID"),
            trim(col("all.AGENT_ID")).alias("AGNT_ID"),
            col("all.BLEI_CK").alias("BILL_ENTY_ID"),
            col("all.PRM_DUE_DT").alias("PRM_DUE_DT_SK"),
            trim(col("all.ARGMT_ID")).alias("COMSN_ARGMT_ID"),
            trim(col("all.TAX_ID")).alias("TAX_DMGRPHC_ID"),
            col("all.ROW_NBR").alias("COMSN_PAYE_STMNT_SEQ_NO"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            trim(col("all.AGENT_ID")).alias("PAYE_AGNT_ID"),
            trim(col("all.BUS_TYP_CD")).alias("COMSN_STMNT_SRC_TYP_CD"),
            substring(col("all.EFF_DT"), 1, 10).alias("EFF_DT"),
            substring(col("all.PAID_TO_DT"), 1, 10).alias("PAID_TO_DT"),
            substring(col("all.PAYMT_DT"), 1, 10).alias("PAYMT_DT_SK"),
            col("all.BM_BONUS_AM").alias("BM_BNS_AMT"),
            col("all.BL_ELIG_PRM_AM").alias("BM_ELIG_PRM_AMT"),
            col("all.ADJ_AM").alias("COMSN_ADJ_AMT"),
            col("all.COMSN_AM").alias("COMSN_AMT"),
            col("all.CURR_PAID_PRM_AM").alias("CUR_PD_PRM_ANOUNT"),
            col("all.EST_ANUL_PRM_AM").alias("EST_ANUL_PRM_AMT"),
            col("all.NET_AMT"),
            col("all.BM_BONUS_PTS_CT").alias("BM_BNS_PT_CT"),
            col("all.BONUS_PT_ADJ_AM").alias("BNS_PT_ADJ_QTY"),
            col("all.COMSN_PCT"),
            col("all.LIVES_CT").alias("PRSN_CT"),
            col("all.ADJ_DSC").alias("COMSN_ADJ_DESC"),
            trim(col("all.PAYEE_ID")).alias("PAYEE_ID")
        )
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_PAYE_STMNT  (Sequential file write)
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        file_path=f"{adls_path}/load/K_COMSN_PAYE_STMNT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: hf_comsn_paye_stmt  (Scenario c – write as parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        file_path=f"{adls_path}/ComsnPayeStmntPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return final output link
    # ------------------------------------------------------------------
    return df_merge