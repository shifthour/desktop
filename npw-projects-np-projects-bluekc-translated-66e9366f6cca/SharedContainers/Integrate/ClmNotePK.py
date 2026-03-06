# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: ClmNotePK
DESCRIPTION: Shared container used for Primary Keying of Claim Note
VC LOGS:
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

ANNOTATIONS:
Load IDS temp. table
join primary key info with table info
Writing Sequential File to /key
IDS Primary Key Container for Claim Note
Used by  FctsClmNoteExtr
Hash file (hf_clm_note_allcol) cleared in the calling program - FctsClmNoteExtr
SQL joins temp table with key table to assign known keys
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_CLM_NOTE) with new keys created today
Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
def run_ClmNotePK(df_AllCol: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    SrcSysCd = params["SrcSysCd"]
    df_allcol_dedup = dedup_sort(df_AllCol, ["SRC_SYS_CD_SK", "CLM_ID", "CLM_NOTE_SEQ"], [])
    df_temp = df_allcol_dedup.select(
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_NOTE_SEQ").alias("CLM_NOTE_SEQ_NO")
    )
    extract_query = f"""
    SELECT CLM_NOTE_SK,
           SRC_SYS_CD_SK,
           CLM_ID,
           CLM_NOTE_SEQ_NO,
           CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_NOTE
    """
    df_k_clm_note = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    join_cond = (
        (df_temp["SRC_SYS_CD_SK"] == df_k_clm_note["SRC_SYS_CD_SK"]) &
        (df_temp["CLM_ID"] == df_k_clm_note["CLM_ID"]) &
        (df_temp["CLM_NOTE_SEQ_NO"] == df_k_clm_note["CLM_NOTE_SEQ_NO"])
    )
    df_match = df_temp.alias("w").join(df_k_clm_note.alias("k"), join_cond, "inner").select(
        col("k.CLM_NOTE_SK"),
        col("w.SRC_SYS_CD_SK"),
        col("w.CLM_ID"),
        col("w.CLM_NOTE_SEQ_NO"),
        col("k.CRT_RUN_CYC_EXCTN_SK")
    )
    df_nomatch = df_temp.alias("w2").join(
        df_k_clm_note.alias("k2"),
        (col("w2.SRC_SYS_CD_SK") == col("k2.SRC_SYS_CD_SK")) &
        (col("w2.CLM_ID") == col("k2.CLM_ID")) &
        (col("w2.CLM_NOTE_SEQ_NO") == col("k2.CLM_NOTE_SEQ_NO")),
        "left_anti"
    ).select(
        lit(-1).alias("CLM_NOTE_SK"),
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_NOTE_SEQ_NO"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
    )
    df_w_extract = df_match.unionByName(df_nomatch)
    df_enriched = df_w_extract.withColumn(
        "INSRT_UPDT_CD",
        when(col("CLM_NOTE_SK") == -1, lit("I")).otherwise(lit("U"))
    ).withColumn(
        "SRC_SYS_CD",
        lit(SrcSysCd)
    ).withColumn(
        "CLM_NOTE_SK",
        when(col("INSRT_UPDT_CD") == "I", lit(None)).otherwise(col("CLM_NOTE_SK"))
    ).withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(col("INSRT_UPDT_CD") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CLM_NOTE_SK",<schema>,<secret_name>)
    df_updt = df_enriched.select(
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_NOTE_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_NOTE_SK")
    )
    df_newkeys = df_enriched.filter(col("INSRT_UPDT_CD") == "I").select(
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_NOTE_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_NOTE_SK")
    )
    df_keys = df_enriched.select(
        col("CLM_NOTE_SK"),
        col("SRC_SYS_CD_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_NOTE_SEQ_NO"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("CRT_RUN_CYC_EXCTN_SK")
    )
    df_key = df_allcol_dedup.alias("all").join(
        df_keys.alias("k"),
        (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
        (col("all.CLM_ID") == col("k.CLM_ID")) &
        (col("all.CLM_NOTE_SEQ") == col("k.CLM_NOTE_SEQ_NO")),
        "left"
    ).select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("all.DISCARD_IN"),
        col("all.PASS_THRU_IN"),
        col("all.FIRST_RECYC_DT"),
        col("all.ERR_CT"),
        col("all.RECYCLE_CT"),
        col("all.SRC_SYS_CD"),
        col("all.PRI_KEY_STRING"),
        col("k.CLM_NOTE_SK"),
        col("all.CLM_ID"),
        col("all.CLM_NOTE_SEQ").alias("CLM_NOTE_SEQ_NO"),
        col("k.CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("all.LAST_UPDT_USER_ID"),
        col("all.CLM_NOTE_TYP_CD"),
        col("all.LAST_UPDT_DT"),
        col("all.NOTE_DESC"),
        col("all.NOTE_TX")
    )
    out_path_updt = f"{adls_path}/ClmNotePK_updt.parquet"
    write_files(df_updt, out_path_updt, delimiter=',', mode='overwrite', is_pqruet=True, header=True, quote='"', nullValue=None)
    file_path_newkeys = f"{adls_path}/load/K_CLM_NOTE.dat"
    write_files(df_newkeys, file_path_newkeys, delimiter=',', mode='overwrite', is_pqruet=False, header=False, quote='"', nullValue=None)
    return df_key