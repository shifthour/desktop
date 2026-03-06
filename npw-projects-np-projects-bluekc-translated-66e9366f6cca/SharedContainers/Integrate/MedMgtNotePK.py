# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
MedMgtNotePK – IBM DataStage shared-container converted to PySpark.

* VC LOGS *
^1_1 07/31/08 13:47:05 Batch  14823_49643 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 07/31/08 13:32:03 Batch  14823_48732 INIT bckcett testIDS dsadm rc for brent 
^1_1 07/31/08 07:45:39 Batch  14823_27944 PROMOTE bckcett testIDS u08717 Brent
^1_1 07/31/08 07:38:12 Batch  14823_27497 INIT bckcett devlIDS u08717 Brent
^1_1 07/11/08 08:18:14 Batch  14803_29933 INIT bckcett devlIDS u06640 Ralph promoted to test for Brent

original coding by Ralph Tucker
code walkthru by Steph Goddard changes required 6/30/08

Annotations:
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
- SQL joins temp table with key table to assign known keys
- primary key hash file only contains current run keys
- update primary key table (K_MED_MGT_NOTE) with new keys created today
- join primary key info with table info
- Assign primary surrogate key
- Hash file (hf_med_mgt_note_allcol) cleared in calling program
- Used by FctsMedMgtNoteExtr
- IDS Primary Key Container for Medical Management
"""
# COMMAND ----------
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
# COMMAND ----------
def run_MedMgtNotePK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the MedMgtNotePK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container-input link “AllCol”.
    df_Transform : DataFrame
        Container-input link “Transform”.
    params : dict
        Runtime parameters and context variables.

    Returns
    -------
    DataFrame
        Container-output link “Key”.
    """

    # --------------------------------------------------
    # unpack parameters (each exactly once)
    CurrRunCycle          = params["CurrRunCycle"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    SrcSysCd              = params["SrcSysCd"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    # --------------------------------------------------
    # 1. Replace intermediate hash-file hf_med_mgt_note_allcol (scenario a)
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MED_MGT_NOTE_DTM", "MED_MGT_NOTE_INPT_DTM"],
        []
    )

    # --------------------------------------------------
    # 2. Read existing keys table K_MED_MGT_NOTE
    extract_query_k = f"""
        SELECT
            MED_MGT_NOTE_SK,
            SRC_SYS_CD_SK,
            MED_MGT_NOTE_DTM,
            MED_MGT_NOTE_INPT_DTM,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MED_MGT_NOTE
    """
    df_k_med_mgt_note = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_k)
            .load()
    )

    # --------------------------------------------------
    # 3. Emulate W_Extract (SQL logic encoded in DataStage)
    df_w_extract = (
        df_Transform.alias("w")
        .join(
            df_k_med_mgt_note.alias("k"),
            (
                col("w.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")
            )
            & (
                col("w.MED_MGT_NOTE_DTM") == col("k.MED_MGT_NOTE_DTM")
            )
            & (
                col("w.MED_MGT_NOTE_INPT_DTM") == col("k.MED_MGT_NOTE_INPT_DTM")
            ),
            "left"
        )
        .select(
            col("k.MED_MGT_NOTE_SK"),
            col("w.SRC_SYS_CD_SK"),
            col("w.MED_MGT_NOTE_DTM"),
            col("w.MED_MGT_NOTE_INPT_DTM"),
            col("k.CRT_RUN_CYC_EXCTN_SK")
        )
        .withColumn(
            "MED_MGT_NOTE_SK",
            when(col("MED_MGT_NOTE_SK").isNull(), lit(-1))
            .otherwise(col("MED_MGT_NOTE_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("CRT_RUN_CYC_EXCTN_SK").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------
    # 4. PrimaryKey transformer derivations
    df_enriched = (
        df_w_extract
        .withColumn(
            "INSRT_UPDT_CD",
            when(col("MED_MGT_NOTE_SK") == lit(-1), lit("I"))
            .otherwise(lit("U"))
        )
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumn(
            "MED_MGT_NOTE_SK",
            when(col("MED_MGT_NOTE_SK") == lit(-1), lit(None))
            .otherwise(col("MED_MGT_NOTE_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("INSRT_UPDT_CD") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------
    # 5. Surrogate-key generation (replaces KeyMgtGetNextValueConcurrent)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "MED_MGT_NOTE_SK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # 6. Split outputs from PrimaryKey transformer
    # 6a. updt link → hf_med_mgt_note (written as parquet)
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "MED_MGT_NOTE_SK"
    )
    write_files(
        df_updt,
        f"{adls_path}/MedMgtNotePK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # 6b. NewKeys link → sequential file
    df_newkeys = (
        df_enriched
        .filter(col("INSRT_UPDT_CD") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MED_MGT_NOTE_DTM",
            "MED_MGT_NOTE_INPT_DTM",
            "CRT_RUN_CYC_EXCTN_SK",
            "MED_MGT_NOTE_SK"
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MED_MGT_NOTE.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # 6c. Keys link → for Merge transformer
    df_keys = df_enriched.select(
        "MED_MGT_NOTE_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # --------------------------------------------------
    # 7. Merge transformer logic
    df_key_output = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            (
                col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")
            )
            & (
                col("all.MED_MGT_NOTE_DTM") == col("k.MED_MGT_NOTE_DTM")
            )
            & (
                col("all.MED_MGT_NOTE_INPT_DTM") == col("k.MED_MGT_NOTE_INPT_DTM")
            ),
            "left"
        )
        .select(
            lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
            col("all.MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
            col("all.MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.INPT_USER_SK").alias("INPT_USER_SK"),
            col("all.UPDT_USER_SK").alias("UPDT_USER_SK"),
            col("all.MED_MGT_NOTE_CAT_CD_SK").alias("MED_MGT_NOTE_CAT_CD_SK"),
            col("all.MED_MGT_NOTE_SUBJ_CD_SK").alias("MED_MGT_NOTE_SUBJ_CD_SK"),
            col("all.UPDT_DTM").alias("UPDT_DTM"),
            col("all.CNTCT_NM").alias("CNTCT_NM"),
            col("all.CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
            col("all.CNTCT_PHN_NO_EXT").alias("CNTCT_PHN_NO_EXT"),
            col("all.CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
            col("all.CNTCT_FAX_NO_EXT").alias("CNTCT_FAX_NO_EXT"),
            col("all.SUM_DESC").alias("SUM_DESC")
        )
    )

    # --------------------------------------------------
    # Container output
    return df_key_output
# COMMAND ----------