# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ComsnStmntPK – Shared container used for Primary Keying of Comm Stmt job

* VC LOGS *
^1_1 03/04/09 10:05:04 Batch  15039_36308 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 03/04/09 09:57:49 Batch  15039_35874 INIT bckcett testIDS dsadm BLS FOR SA
^1_1 01/13/09 09:00:06 Batch  14989_32412 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Comm Stmt job

CALLED BY : FctsComsnStmntExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-15               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          09/22/2008
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lit


def run_ComsnStmntPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack required parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_comsn_stmnt_allcol  (Scenario-a intermediate hash file)
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "SYIN_INST", "LOBD_ID_COMM", "COCE_ID_PAYEE", "COSS_SEQ_NO"],
        []
    )
    # ------------------------------------------------------------------
    # Stage: K_COMSN_STMNT_TEMP  (load temp table, runstats)
    # ------------------------------------------------------------------
    temp_table_fullname = f"{IDSOwner}.K_COMSN_STMNT_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table_fullname}", ids_jdbc_url, ids_jdbc_props)
    (
        df_Transform.write.mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table_fullname)
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_STMNT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # Stage: K_COMSN_STMNT_TEMP  (extract W_Extract)
    # ------------------------------------------------------------------
    w_extract_query = f"""
    SELECT  k.COMSN_STMNT_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_BTCH_INST_ID,
            w.COMSN_STMNT_LOB_CD,
            w.PAYE_AGNT_ID,
            w.SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_STMNT_TEMP w,
         {IDSOwner}.K_COMSN_STMNT k
    WHERE w.SRC_SYS_CD_SK         = k.SRC_SYS_CD_SK
      AND w.COMSN_BTCH_INST_ID    = k.COMSN_BTCH_INST_ID
      AND w.COMSN_STMNT_LOB_CD    = k.COMSN_STMNT_LOB_CD
      AND w.PAYE_AGNT_ID          = k.PAYE_AGNT_ID
      AND w.SEQ_NO                = k.SEQ_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_BTCH_INST_ID,
           w2.COMSN_STMNT_LOB_CD,
           w2.PAYE_AGNT_ID,
           w2.SEQ_NO,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_STMNT_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_COMSN_STMNT k2
        WHERE w2.SRC_SYS_CD_SK         = k2.SRC_SYS_CD_SK
          AND w2.COMSN_BTCH_INST_ID    = k2.COMSN_BTCH_INST_ID
          AND w2.PAYE_AGNT_ID          = k2.PAYE_AGNT_ID
          AND w2.COMSN_STMNT_LOB_CD    = k2.COMSN_STMNT_LOB_CD
          AND w2.SEQ_NO                = k2.SEQ_NO
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", w_extract_query)
        .load()
    )
    # ------------------------------------------------------------------
    # Stage: PrimaryKey (transform)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("COMSN_STMNT_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn("svComsnBtchInstId", trim(col("COMSN_BTCH_INST_ID")))
        .withColumn("svPayeAgntId", trim(col("PAYE_AGNT_ID")))
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == "U", col("COMSN_STMNT_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("SrcSysCd", lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK','<schema>','<secret_name>')
    # ------------------------------------------------------------------
    # Output-link: updt  -> hf_comsn_stmnt (scenario-c → Parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("svComsnBtchInstId").alias("COMSN_BTCH_INST_ID"),
        col("COMSN_STMNT_LOB_CD"),
        col("svPayeAgntId").alias("PAYE_AGNT_ID"),
        col("SEQ_NO"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("COMSN_STMNT_SK")
    )
    parquet_path_updt = f"{adls_path}/ComsnStmntPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Output-link: NewKeys -> K_COMSN_STMNT (seq file)
    # ------------------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == "I")
        .select(
            col("SRC_SYS_CD_SK"),
            col("svComsnBtchInstId").alias("COMSN_BTCH_INST_ID"),
            col("COMSN_STMNT_LOB_CD"),
            col("svPayeAgntId").alias("PAYE_AGNT_ID"),
            col("SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("COMSN_STMNT_SK")
        )
    )
    seq_file_path = f"{adls_path}/load/K_COMSN_STMNT.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Output-link: Keys (used in Merge)
    # ------------------------------------------------------------------
    df_Keys = df_enriched.select(
        col("svSK").alias("COMSN_STMNT_SK"),
        col("SRC_SYS_CD_SK"),
        col("svComsnBtchInstId").alias("COMSN_BTCH_INST_ID"),
        col("COMSN_STMNT_LOB_CD"),
        col("svPayeAgntId").alias("PAYE_AGNT_ID"),
        col("SEQ_NO"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    merge_expr = (
        (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
        (col("all.SYIN_INST")     == col("k.COMSN_BTCH_INST_ID")) &
        (col("all.LOBD_ID_COMM")  == col("k.COMSN_STMNT_LOB_CD")) &
        (col("all.COCE_ID_PAYEE") == col("k.PAYE_AGNT_ID")) &
        (col("all.COSS_SEQ_NO")   == col("k.SEQ_NO"))
    )
    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), merge_expr, "left")
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
            col("k.COMSN_STMNT_SK"),
            col("all.SYIN_INST").alias("COMSN_BTCH_INST_ID"),
            col("all.LOBD_ID_COMM"),
            col("all.COCE_ID_PAYEE").alias("PAYE_AGNT_ID"),
            col("all.COSS_SEQ_NO").alias("SEQ_NO"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.COCE_ID"),
            col("all.COAR_ID"),
            col("all.COAG_EFF_DT"),
            col("all.COSS_LOBD_ID"),
            col("all.COCE_PAY_CO_METH"),
            col("all.COSS_SOURCE_TABLE"),
            col("all.BLBL_DUE_DT"),
            col("all.COPY_PAY_DT"),
            col("all.COEC_COMM_AMT_EARN").alias("ERN_COMSN_AMT"),
            col("all.COSS_NET_CHECK_AMT").alias("NET_CHK_AMT"),
            col("all.COEC_COMM_AMT_PAID").alias("PD_COMSN_AMT"),
            col("all.COEC_SOURCE_AMT").alias("SRC_INCM_AMT"),
            col("all.COSS_LVS").alias("PRSN_CT"),
            col("all.COSS_DESC").alias("STMNT_DESC")
        )
    )
    # ------------------------------------------------------------------
    # Return the container output
    # ------------------------------------------------------------------
    return df_Key