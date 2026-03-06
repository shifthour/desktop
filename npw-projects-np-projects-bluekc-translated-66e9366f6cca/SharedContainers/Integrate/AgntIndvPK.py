# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared container: AgntIndvPK
Original DataStage Description:

* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Comm Agnt Indv job

CALLED BY : FctsAgntIndvExtr

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-16               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          09/22/2008

Annotations:
- IDS Primary Key Container for Commission Agent Indv
- Used by  FctsAgntIndvExtr
- Hash file (hf_agnt_indv_allcol) cleared in calling job
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
- join primary key info with table info
- update primary key table (K_AGNT_INDV) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim


def run_AgntIndvPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the AgntIndvPK shared container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link 'AllCol'.
    df_Transform : DataFrame
        Container input link 'Transform'.
    params : dict
        Runtime parameters and JDBC / path configurations.

    Returns
    -------
    DataFrame
        Output link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle       = int(params["CurrRunCycle"])
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]

    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # STEP 1 : Replace intermediate hash file (hf_agnt_indv_allcol)
    #          with duplicate-removal logic
    # ------------------------------------------------------------------
    df_AllCol_dedup = df_AllCol.dropDuplicates(
        ["SRC_SYS_CD_SK", "AGNT_INDV_ID"]
    )

    # ------------------------------------------------------------------
    # STEP 2 : Retrieve existing keys from IDS
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  AGNT_INDV_SK,
                SRC_SYS_CD_SK,
                AGNT_INDV_ID,
                CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_AGNT_INDV
    """

    df_k_agnt_indv = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # STEP 3 : Emulate W_Extract (join / union logic)
    # ------------------------------------------------------------------
    df_join = (
        df_Transform.alias("w")
        .join(
            df_k_agnt_indv.alias("k"),
            (col("w.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
            (col("w.AGNT_INDV_ID")  == col("k.AGNT_INDV_ID")),
            "left"
        )
    )

    df_W_Extract = (
        df_join
        .select(
            when(col("k.AGNT_INDV_SK").isNull(), lit(-1))
                .otherwise(col("k.AGNT_INDV_SK")).alias("AGNT_INDV_SK"),
            col("w.SRC_SYS_CD_SK"),
            col("w.AGNT_INDV_ID"),
            when(col("k.AGNT_INDV_SK").isNull(), lit(CurrRunCycle))
                .otherwise(col("k.CRT_RUN_CYC_EXCTN_SK"))
                .alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # STEP 4 : PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("AGNT_INDV_SK") == lit(-1), lit('I')).otherwise(lit('U'))
        )
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn("svAgntIndvId", trim(col("AGNT_INDV_ID")))
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == lit('I'), lit(None).cast("long"))
            .otherwise(col("AGNT_INDV_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit('I'), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # STEP 5 : Create derivative streams
    # ------------------------------------------------------------------
    # updt link (to hf_agnt_indv)
    df_updt = (
        df_enriched
        .select(
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svAgntIndvId").alias("AGNT_INDV_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("AGNT_INDV_SK")
        )
    )

    # Persist hf_agnt_indv as Parquet
    parquet_path_updt = f"{adls_path}/AgntIndvPK_updt.parquet"
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

    # NewKeys link (to sequential file)
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit('I'))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svAgntIndvId").alias("AGNT_INDV_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("AGNT_INDV_SK")
        )
    )

    seq_file_path = f"{adls_path}/load/K_AGNT_INDV.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # Keys link (for merging)
    df_keys = (
        df_enriched
        .select(
            col("svSK").alias("AGNT_INDV_SK"),
            col("SRC_SYS_CD_SK"),
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svAgntIndvId").alias("AGNT_INDV_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # STEP 6 : Merge transformer logic
    # ------------------------------------------------------------------
    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
            (col("all.AGNT_INDV_ID")  == col("k.AGNT_INDV_ID")),
            "left"
        )
    )

    df_Key = (
        df_merge
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.AGNT_INDV_SK").alias("AGNT_INDV_SK"),
            col("all.AGNT_INDV_ID").alias("AGNT_INDV_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.AGNT_INDV_ENTY_TYP_CD").alias("AGNT_INDV_ENTY_TYP_CD"),
            col("all.SSN").alias("SSN"),
            col("all.FIRST_NM").alias("FIRST_NM"),
            col("all.MIDINIT").alias("MIDINIT"),
            col("all.LAST_NM").alias("LAST_NM"),
            col("all.INDV_TTL").alias("INDV_TTL")
        )
    )

    return df_Key