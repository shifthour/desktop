# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
ClmSttusAuditPK
Converted from IBM DataStage shared container “ClmSttusAuditPK”.

Original Description:
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

Called by:
    FctsClmSttusAuditExtr, NascoClmSttusAuditExtr

Processing notes:
    hf_clm_sttus_audit_allcol is cleared by the calling job
    SQL joins temp table with key table to assign known keys
    The primary key hash file contains only current run keys and is cleared before writing
    Update primary key table K_CLM_STTUS_AUDIT with new keys created today
    Assign primary surrogate key

Annotations:
IDS Primary Key Container for Claim Status Audit
Used by FctsClmSttusAuditExtr
Load IDS temp table
Join primary key info with table info
Hash file (hf_clm_sttus_audit_allcol) cleared in the calling program
SQL joins temp table with key table to assign known keys
The primary key hash file contains only current run keys and is cleared before writing
Update primary key table
K_CLM_STTUS_AUDIT
with new keys created today
Assign primary surrogate key
This container is used in:
FctsClmSttusAuditExtr
NascoClmSttusAuditTrns

These programs need to be re-compiled when logic changes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

def run_ClmSttusAuditPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ---------------------------------------------------
    # unpack runtime parameters (once only)
    # ---------------------------------------------------
    IDSOwner         = params["IDSOwner"]
    ids_secret_name  = params["ids_secret_name"]
    CurrRunCycle     = params["CurrRunCycle"]
    ids_jdbc_url     = params["ids_jdbc_url"]
    ids_jdbc_props   = params["ids_jdbc_props"]
    adls_path        = params["adls_path"]
    adls_path_raw    = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # ---------------------------------------------------
    # Hashed-file scenario a  –  hf_clm_sttus_audit_allcol
    # replace with dedup logic on key columns
    # ---------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_STTUS_AUDIT_SEQ_NO"],
        []
    )
    # ---------------------------------------------------
    # Write incoming rows to temp table in IDS
    # ---------------------------------------------------
    df_Transform.write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_CLM_STTUS_AUDIT_TEMP") \
        .mode("overwrite") \
        .save()
    # ---------------------------------------------------
    # Extract existing / new key information
    # ---------------------------------------------------
    extract_query = f"""
    SELECT
           k.CLM_STTUS_AUDIT_SK,
           t.SRC_SYS_CD_SK,
           t.CLM_ID,
           t.CLM_STTUS_AUDIT_SEQ_NO,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM
           {IDSOwner}.K_CLM_STTUS_AUDIT_TEMP t
           JOIN {IDSOwner}.K_CLM_STTUS_AUDIT k
             ON  t.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
             AND t.CLM_ID        = k.CLM_ID
             AND t.CLM_STTUS_AUDIT_SEQ_NO = k.CLM_STTUS_AUDIT_SEQ_NO
    UNION
    SELECT
           -1,
           t2.SRC_SYS_CD_SK,
           t2.CLM_ID,
           t2.CLM_STTUS_AUDIT_SEQ_NO,
           {CurrRunCycle}
    FROM
           {IDSOwner}.K_CLM_STTUS_AUDIT_TEMP t2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_STTUS_AUDIT k2
        WHERE t2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND t2.CLM_ID        = k2.CLM_ID
          AND t2.CLM_STTUS_AUDIT_SEQ_NO = k2.CLM_STTUS_AUDIT_SEQ_NO
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # ---------------------------------------------------
    # Primary Key transformer logic
    # ---------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            when(col("CLM_STTUS_AUDIT_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("SRC_SYS_CD", lit("FACETS"))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("INSRT_UPDT_CD") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CLM_STTUS_AUDIT_SK",
        <schema>,
        <secret_name>
    )
    # ---------------------------------------------------
    # updt -> hf_clm_sttus_audit  (hashed-file scenario c → parquet)
    # ---------------------------------------------------
    df_updt = df_enriched.select(
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_STTUS_AUDIT_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_STTUS_AUDIT_SK")
    )
    write_files(
        df_updt,
        f"{adls_path}/ClmSttusAuditPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------
    # NewKeys -> sequential file K_CLM_STTUS_AUDIT.dat
    # ---------------------------------------------------
    df_newkeys = df_enriched.filter(col("INSRT_UPDT_CD") == lit("I")).select(
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_STTUS_AUDIT_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_STTUS_AUDIT_SK")
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_STTUS_AUDIT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------
    # Keys for merge
    # ---------------------------------------------------
    df_keys = df_enriched.select(
        col("CLM_STTUS_AUDIT_SK"),
        col("SRC_SYS_CD_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_STTUS_AUDIT_SEQ_NO"),
        col("INSRT_UPDT_CD"),
        col("CRT_RUN_CYC_EXCTN_SK")
    )
    # ---------------------------------------------------
    # Merge transformer logic
    # ---------------------------------------------------
    df_merge = (
        df_keys.alias("Keys")
        .join(
            df_AllCol_dedup.alias("AllColOut"),
            [
                col("Keys.SRC_SYS_CD") == col("AllColOut.SRC_SYS_CD"),
                col("Keys.CLM_ID") == col("AllColOut.CLM_ID"),
                col("Keys.CLM_STTUS_AUDIT_SEQ_NO") == col("AllColOut.CLM_STTUS_AUDIT_SEQ_NO")
            ],
            "left"
        )
    )
    df_Key = df_merge.select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
        col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("AllColOut.ERR_CT").alias("ERR_CT"),
        col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
        col("AllColOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("Keys.CLM_STTUS_AUDIT_SK").alias("CLM_STTUS_AUDIT_SK"),
        col("AllColOut.CLM_ID").alias("CLM_ID"),
        col("Keys.CLM_STTUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
        col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AllColOut.CLM_SK").alias("CLM_SK"),
        col("AllColOut.CRT_BY_APP_USER").alias("CRT_BY_APP_USER"),
        col("AllColOut.RTE_TO_APP_USER").alias("RTE_TO_APP_USER"),
        col("AllColOut.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        col("AllColOut.CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        col("AllColOut.CLM_STTUS_DT_SK").alias("CLM_STTUS_DT_SK"),
        col("AllColOut.CLM_STTUS_DTM").alias("CLM_STTUS_DTM"),
        col("AllColOut.TRNSMSN_SRC_CD").alias("TRNSMSN_SRC_CD")
    )
    # ---------------------------------------------------
    # return final output DataFrame
    # ---------------------------------------------------
    return df_Key