# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container : ClmAtchmtPK  (Job Type: Server Job)

* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
^1_1 07/24/08 13:21:35 Batch  14816_48098 INIT bckcett devlIDS u08717 Brent

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:  Shared container used for Primary Keying of Claim Attachment job

CALLED BY : FctsClmAtchmtExtr

PROCESSING: 
   

MODIFICATIONS:
Developer           Date                         Change Description                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-07-21               Initial program                                                                   3567 Primary Key    devlIDS                                Steph Goddard            07/25/2008

Annotations:
- IDS Primary Key Container for Claim Atchmt
- Used by FctsClmAtchmtExtr
- Hash file (hf_clm_atchmt_allcol) cleared in calling job
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
- join primary key info with table info
- Assign primary surrogate key
- update primary key table (K_CLM_ATCHMT) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
def run_ClmAtchmtPK(
    df_Allcol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ClmAtchmtPK shared container.
    Parameters
    ----------
    df_Allcol   : DataFrame  # link “Allcol”
    df_Transform: DataFrame  # link “Transform”
    params      : dict       # runtime parameters and jdbc configs
    Returns
    -------
    DataFrame   # link “Key”
    """
    # --------------------------------------------------
    # Un-pack required parameters exactly once
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------------------------------------
    # Scenario a : Intermediate hashed file -> dedup
    df_AllColOut = dedup_sort(
        df_Allcol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_ATCHMT_TYP_CD"],
        [("SRC_SYS_CD_SK", "A")]
    )
    # --------------------------------------------------
    # Read key table from IDS
    extract_query = f"""
        SELECT  CLM_ATCHMT_SK,
                SRC_SYS_CD_SK,
                CLM_ID,
                CLM_ATCHMT_TYP_CD,
                CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CLM_ATCHMT
    """
    df_key_tbl = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # --------------------------------------------------
    # Replicate W_Extract logic (assign existing/new keys)
    cond_join = [
        df_Transform["SRC_SYS_CD_SK"] == df_key_tbl["SRC_SYS_CD_SK"],
        df_Transform["CLM_ID"] == df_key_tbl["CLM_ID"],
        df_Transform["CLM_ATCHMT_TYP_CD"] == df_key_tbl["CLM_ATCHMT_TYP_CD"]
    ]
    df_join = df_Transform.alias("w").join(df_key_tbl.alias("k"), cond_join, "left")
    df_W_extract = (
        df_join
        .select(
            F.when(F.col("k.CLM_ATCHMT_SK").isNull(), F.lit(-1))
             .otherwise(F.col("k.CLM_ATCHMT_SK")).alias("CLM_ATCHMT_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.CLM_ID"),
            F.col("w.CLM_ATCHMT_TYP_CD"),
            F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # PrimaryKey transformer logic
    df_enriched = (
        df_W_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_ATCHMT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit("FACETS"))
        .withColumn("svClmId", F.trim(F.col("CLM_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("CLM_ATCHMT_SK") == F.lit(-1), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("CLM_ATCHMT_SK") == F.lit(-1), F.lit(None))
             .otherwise(F.col("CLM_ATCHMT_SK"))
        )
    )
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------
    # Build updt DataFrame (for hf_clm_atchmt)
    df_updt = (
        df_enriched
        .select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("CLM_ATCHMT_TYP_CD").alias("CLM_ATCHMT_TYP_CD_SK"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_ATCHMT_SK")
        )
    )
    # Write parquet for hash‐file replacement
    parquet_path_updt = f"{adls_path}/ClmAtchmtPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Build NewKeys DataFrame and write sequential file
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            F.col("svClmId").alias("CLM_ID"),
            "CLM_ATCHMT_TYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_ATCHMT_SK")
        )
    )
    seq_path = f"{adls_path}/load/K_CLM_ATCHMT.dat"
    write_files(
        df_NewKeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Build Keys DataFrame for merge
    df_keys_for_merge = (
        df_enriched
        .select(
            F.col("svSK").alias("CLM_ATCHMT_SK"),
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            "CLM_ATCHMT_TYP_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # Merge transformer logic
    cond_merge = [
        df_AllColOut["SRC_SYS_CD_SK"] == df_keys_for_merge["SRC_SYS_CD_SK"],
        df_AllColOut["CLM_ID"] == df_keys_for_merge["CLM_ID"],
        df_AllColOut["CLM_ATCHMT_TYP_CD"] == df_keys_for_merge["CLM_ATCHMT_TYP_CD"]
    ]
    df_merge = (
        df_AllColOut.alias("AllColOut")
        .join(df_keys_for_merge.alias("Keys"), cond_merge, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.CLM_ATCHMT_SK").alias("CLM_ATCHMT_SK"),
            F.col("AllColOut.CLM_ID").alias("CLM_ID"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.CLM_ATCHMT_TYP_CD").alias("ATCHMT_IND"),
            F.col("AllColOut.ATCHMT_REF_ID").alias("ATCHMT_REF_ID")
        )
    )
    # --------------------------------------------------
    return df_merge
# COMMAND ----------