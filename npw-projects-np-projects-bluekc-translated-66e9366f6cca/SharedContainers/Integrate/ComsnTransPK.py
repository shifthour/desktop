# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: ComsnTransPK
Folder Path     : Shared Containers/PrimaryKey
Description     :  Shared container used for Primary Keying of Commission Transaction job
Called By       :  PSComsnTransExtr

* VC LOGS *
^1_1 02/06/09 13:37:20 Batch  15013_49062 PROMOTE bckcetl ids20 dsadm rc for steph
^1_1 02/06/09 13:28:52 Batch  15013_48548 INIT bckcett testIDS dsadm rc for steph 
^1_2 01/23/09 15:14:34 Batch  14999_54898 PROMOTE bckcett testIDS u03651 steph - primary key
^1_2 01/23/09 14:36:26 Batch  14999_52588 INIT bckcett devlIDS u03651 steffy
^1_1 01/08/09 06:44:33 Batch  14984_24277 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Commission Transaction job

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-08-18               Initial program                                                                             3567(Primary Key)               devlIDS         Steph Goddard         08/22/2008

Annotations from DataStage Design:
- This container is used in:
  PSComsnTransExtr
  These programs need to be re-compiled when logic changes
- Hash file (hf_comsn_trans_allcol) cleared in calling program
- join primary key info with table info
- update primary key table (K_COMSN_TRANS) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_ComsnTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage: hf_comsn_trans_allcol  (scenario a – intermediate hash file)
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "COMSN_TRANS_CK", "ACCTG_DT_SK"],
        []
    )
    # --------------------------------------------------
    # Stage: K_COMSN_TRANS_TEMP  (load temp table then extract)
    execute_dml(
        f"DELETE FROM {IDSOwner}.K_COMSN_TRANS_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform
        .select(
            "SRC_SYS_CD_SK",
            "COMSN_TRANS_CK",
            "ACCTG_DT_SK"
        )
        .write
        .format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_COMSN_TRANS_TEMP")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_TRANS_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    extract_query = f"""
    SELECT k.COMSN_TRANS_SK,
           w.SRC_SYS_CD_SK,
           w.COMSN_TRANS_CK,
           w.ACCTG_DT_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_TRANS_TEMP w
    JOIN {IDSOwner}.K_COMSN_TRANS k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.COMSN_TRANS_CK = k.COMSN_TRANS_CK
     AND w.ACCTG_DT_SK   = k.ACCTG_DT_SK
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_TRANS_CK,
           w2.ACCTG_DT_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_TRANS_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_COMSN_TRANS k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.COMSN_TRANS_CK = k2.COMSN_TRANS_CK
          AND w2.ACCTG_DT_SK   = k2.ACCTG_DT_SK
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
    # Stage: PrimaryKey transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("COMSN_TRANS_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSrcSysCd",
            F.lit(SrcSysCd)
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "U", F.col("COMSN_TRANS_SK")).otherwise(F.lit(None))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svSK',
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------
    # Build DataFrames corresponding to transformer output links
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("COMSN_TRANS_CK"),
        F.col("ACCTG_DT_SK"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("COMSN_TRANS_SK")
    )
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "COMSN_TRANS_CK",
            "ACCTG_DT_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_TRANS_SK")
        )
    )
    df_keys = df_enriched.select(
        F.col("svSK").alias("COMSN_TRANS_SK"),
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "COMSN_TRANS_CK",
        "ACCTG_DT_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------
    # Stage: hf_comsn_trans (scenario c – write to parquet)
    write_files(
        df_updt,
        f"{adls_path}/ComsnTransPK_updt.parquet",
        ',',
        'overwrite',
        True,
        True,
        '\"',
        None
    )
    # --------------------------------------------------
    # Stage: K_COMSN_TRANS sequential file
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_COMSN_TRANS.dat",
        ',',
        'overwrite',
        False,
        False,
        '\"',
        None
    )
    # --------------------------------------------------
    # Stage: Merge transformer logic
    join_expr = [
        F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
        F.col("all.COMSN_TRANS_CK") == F.col("k.COMSN_TRANS_CK"),
        F.col("all.ACCTG_DT_SK")   == F.col("k.ACCTG_DT_SK")
    ]
    df_Key = (
        df_AllColOut.alias("all")
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
            F.col("k.COMSN_TRANS_SK"),
            F.col("all.COMSN_TRANS_CK"),
            F.col("all.ACCTG_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.FNCL_LOB"),
            F.col("all.GRP"),
            F.col("all.ORIG_FNCL_LOB"),
            F.col("all.PROD"),
            F.col("all.RVSED_FNCL_LOB"),
            F.col("all.SUBGRP"),
            F.col("all.COMSN_TRANS_BILL_LVL_CD"),
            F.col("all.COMSN_TRANS_LOB_CD"),
            F.col("all.FIRST_YR_IN"),
            F.col("all.BILL_DUE_DT"),
            F.col("all.CRT_DT"),
            F.col("all.POSTING_DT"),
            F.col("all.BILL_CMPNT_TOT_CK"),
            F.col("all.BILL_ENTY_CK"),
            F.col("all.BILL_LVL_CK"),
            F.col("all.BILL_CMPNT_ID")
        )
    )
    # --------------------------------------------------
    return df_Key