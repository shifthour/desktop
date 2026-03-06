# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ComsnDtlPK – Shared container converted from IBM DataStage

DESCRIPTION:
  Shared container used for Primary Keying of Commission Detail job.
  Called by: FctsComsnDtlExtr

ANNOTATIONS:
  IDS Primary Key Container for Commission Detail
  Used by FctsComsnDtlExtr
  Hash file (hf_comsn_dtl_allcol) cleared in calling job
  SQL joins temp table with key table to assign known keys
  Temp table is truncated before load and runstat done after load
  Load IDS temp. table
  Join primary key info with table info
  Update primary key table (K_COMSN_DTL) with new keys created today
  Primary key hash file only contains current run keys and is cleared before writing
  Assign primary surrogate key
"""

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# COMMAND ----------
def run_ComsnDtlPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ComsnDtlPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming stream matching link “AllCol”.
    df_Transform : DataFrame
        Incoming stream matching link “Transform”.
    params : dict
        Runtime parameters and secrets.

    Returns
    -------
    DataFrame
        Stream corresponding to container output link “Key”.
    """

    # ------------------------------------------------------------------
    # Un-pack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]

    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]

    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_comsn_dtl_allcol  (scenario a  – intermediate hash file)
    # Replace with duplicate-removal logic
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "SYIN_INST",
            "LOBD_ID_COMM",
            "COCE_ID_PAYEE",
            "CODE_SEQ_NO",
            "COCE_ID",
            "COAR_ID",
        ],
        sort_cols=[("SRC_SYS_CD_SK", "A")],
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_DTL_TEMP  – write incoming rows to temp table
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_COMSN_DTL_TEMP",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_COMSN_DTL_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        "CALL SYSPROC.ADMIN_CMD('runstats on table {0}.K_COMSN_DTL_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')".format(
            IDSOwner
        ),
        ids_jdbc_url,
        ids_jdbc_props,
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_DTL_TEMP  – extract W_Extract link via SQL
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.COMSN_DTL_SK,
                w.SRC_SYS_CD_SK,
                w.COMSN_BTCH_INST_ID,
                w.COMSN_DTL_COMSN_LOB_CD,
                w.PAYE_AGNT_ID,
                w.SEQ_NO,
                w.AGNT_ID,
                w.COMSN_ARGMT_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM   {IDSOwner}.K_COMSN_DTL_TEMP w,
               {IDSOwner}.K_COMSN_DTL k
        WHERE  w.SRC_SYS_CD_SK           = k.SRC_SYS_CD_SK
          AND  w.COMSN_BTCH_INST_ID      = k.COMSN_BTCH_INST_ID
          AND  w.COMSN_DTL_COMSN_LOB_CD  = k.COMSN_DTL_COMSN_LOB_CD
          AND  w.PAYE_AGNT_ID            = k.PAYE_AGNT_ID
          AND  w.SEQ_NO                  = k.SEQ_NO
          AND  w.AGNT_ID                 = k.AGNT_ID
          AND  w.COMSN_ARGMT_ID          = k.COMSN_ARGMT_ID

        UNION

        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.COMSN_BTCH_INST_ID,
               w2.COMSN_DTL_COMSN_LOB_CD,
               w2.PAYE_AGNT_ID,
               w2.SEQ_NO,
               w2.AGNT_ID,
               w2.COMSN_ARGMT_ID,
               {CurrRunCycle}
        FROM   {IDSOwner}.K_COMSN_DTL_TEMP w2
        WHERE  NOT EXISTS (
                SELECT 1
                FROM   {IDSOwner}.K_COMSN_DTL k2
                WHERE  w2.SRC_SYS_CD_SK          = k2.SRC_SYS_CD_SK
                  AND  w2.COMSN_DTL_COMSN_LOB_CD = k2.COMSN_DTL_COMSN_LOB_CD
                  AND  w2.COMSN_BTCH_INST_ID     = k2.COMSN_BTCH_INST_ID
                  AND  w2.PAYE_AGNT_ID           = k2.PAYE_AGNT_ID
                  AND  w2.SEQ_NO                 = k2.SEQ_NO
                  AND  w2.AGNT_ID                = k2.AGNT_ID
                  AND  w2.COMSN_ARGMT_ID         = k2.COMSN_ARGMT_ID
              )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey  – derive columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("COMSN_DTL_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == F.lit("I"),
                F.lit(CurrRunCycle),
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")),
        )
        .withColumn("svComsnBtchInstd", F.trim(F.col("COMSN_BTCH_INST_ID")))
        .withColumn("svPayeAgntId", F.trim(F.col("PAYE_AGNT_ID")))
        .withColumn("svAgntId", F.trim(F.col("AGNT_ID")))
        .withColumn("svComsnArgmtId", F.trim(F.col("COMSN_ARGMT_ID")))
        .withColumn("svSK", F.col("COMSN_DTL_SK"))  # placeholder, will be overwritten
        .withColumn("INSRT_UPDT_CD", F.col("svInstUpdt"))
    )

    # Surrogate key generation (mandatory placeholder call)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>,
    )

    # ------------------------------------------------------------------
    # Build downstream streams
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            F.col("svComsnBtchInstd").alias("COMSN_BTCH_INST_ID"),
            F.col("COMSN_DTL_COMSN_LOB_CD"),
            F.col("svPayeAgntId").alias("PAYE_AGNT_ID"),
            F.col("SEQ_NO"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_DTL_SK"),
            F.col("INSRT_UPDT_CD"),
        )
    )

    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I")).select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svComsnBtchInstd").alias("COMSN_BTCH_INST_ID"),
            F.col("COMSN_DTL_COMSN_LOB_CD"),
            F.col("svPayeAgntId").alias("PAYE_AGNT_ID"),
            F.col("SEQ_NO"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_DTL_SK"),
        )
    )

    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("COMSN_DTL_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("svComsnBtchInstd").alias("COMSN_BTCH_INST_ID"),
            F.col("COMSN_DTL_COMSN_LOB_CD"),
            F.col("svPayeAgntId").alias("PAYE_AGNT_ID"),
            F.col("SEQ_NO"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            F.col("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ------------------------------------------------------------------
    # Stage: hf_comsn_dtl (scenario c  – parquet write)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ComsnDtlPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_DTL  – sequential file write
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_COMSN_DTL.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Stage: Merge – join AllColOut & Keys
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("Keys.SRC_SYS_CD_SK") == F.col("AllColOut.SRC_SYS_CD_SK"))
        & (F.col("Keys.COMSN_BTCH_INST_ID") == F.col("AllColOut.SYIN_INST"))
        & (
            F.col("Keys.COMSN_DTL_COMSN_LOB_CD")
            == F.col("AllColOut.LOBD_ID_COMM")
        )
        & (F.col("Keys.PAYE_AGNT_ID") == F.col("AllColOut.COCE_ID_PAYEE"))
        & (F.col("Keys.SEQ_NO") == F.col("AllColOut.CODE_SEQ_NO"))
        & (F.col("Keys.AGNT_ID") == F.col("AllColOut.COCE_ID"))
        & (F.col("Keys.COMSN_ARGMT_ID") == F.col("AllColOut.COAR_ID"))
    )

    df_Key = (
        df_allcol_dedup.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_expr, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT"),
            F.col("AllColOut.RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING"),
            F.col("Keys.COMSN_DTL_SK"),
            F.col("AllColOut.SYIN_INST").alias("COMSN_BTCH_INST_ID"),
            F.col("AllColOut.LOBD_ID_COMM"),
            F.col("AllColOut.COCE_ID_PAYEE").alias("PAYE_AGNT_ID"),
            F.col("AllColOut.CODE_SEQ_NO").alias("SEQ_NO"),
            F.col("AllColOut.COCE_ID").alias("AGNT_ID"),
            F.col("AllColOut.COAR_ID").alias("COMSN_ARGMT_ID"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.BLEI_CK"),
            F.col("AllColOut.CSPI_ID"),
            F.col("AllColOut.COAG_EFF_DT"),
            F.col("AllColOut.COSC_ID"),
            F.col("AllColOut.PMFA_ID"),
            F.col("AllColOut.PDPD_ID"),
            F.col("AllColOut.COSC_CALC_METH"),
            F.col("AllColOut.COBL_SOURCE_CD"),
            F.col("AllColOut.CODE_DISP_CD"),
            F.col("AllColOut.LOBD_ID"),
            F.col("AllColOut.CODE_PREM_TYPE"),
            F.col("AllColOut.CODE_SOURCE"),
            F.col("AllColOut.BLBL_DUE_DT").alias("BILL_DUE_DT_SK"),
            F.col("AllColOut.COSD_EFF_DT").alias("DURATN_EFF_DT_SK"),
            F.col("AllColOut.COEC_COMM_AMT_EARN").alias("ERN_COMSN_AMT"),
            F.col("AllColOut.COCC_BASIS").alias("COMSN_BSS_AMT"),
            F.col("AllColOut.COCC_SOURCE_AMT").alias("COMSN_CALC_SRC_INPT_AMT"),
            F.col("AllColOut.COEC_SOURCE_AMT").alias("COMSN_ERN_SRC_INPT_AMT"),
            F.col("AllColOut.COST_AMT").alias("COMSN_SCHD_TIER_AMT"),
            F.col("AllColOut.COEC_COMM_AMT_PAID").alias("PD_COMSN_AMT"),
            F.col("AllColOut.COST_FROM_AMT").alias("PRM_FROM_THRSHLD_AMT"),
            F.col("AllColOut.COBL_SOURCE_CK").alias("COMSN_BILL_REL_SRC_UNIQ_KEY"),
            F.col("AllColOut.COST_PCT").alias("COMSN_SCHD_TIER_PCT"),
            F.col("AllColOut.CODE_LVS_DP").alias("DPNDT_CT"),
            F.col("AllColOut.COSD_DUR_START").alias("DURATN_STRT_PERD_NO"),
            F.col("AllColOut.CODE_LVS_SB").alias("SUB_CT"),
            F.col("AllColOut.PDBL_ID").alias("PROD_BILL_CMPNT_ID"),
        )
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key

# COMMAND ----------