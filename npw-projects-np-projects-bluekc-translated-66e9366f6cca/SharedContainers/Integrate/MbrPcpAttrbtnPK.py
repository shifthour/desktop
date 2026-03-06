# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

def run_MbrPcpAttrbtnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
    
    DESCRIPTION:   Shared container used for Primary Keying of Mbr Vbb Cmpnt Enr
    
    MODIFICATIONS:
    Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
    -----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
    Raja Gummadi    2013-05-22              Initial Programming                                                      4963 VBB Phase III     IntegrateNewDevl              Bhoomi Dasari           7/8/2013
    
    Annotations:
    - join primary key info with table info
    - update primary key table (K_MBR_PCP_ATTRBTN) with new keys created today
    - Assign primary surrogate key
    - primary key hash file only contains current run keys and is cleared before writing
    - Temp table is tuncated before load and runstat done after load
    - SQL joins temp table with key table to assign known keys
    - Load IDS temp. table
    - Hashed files (hf_mbr_pcp_attrbtn_allcol & hf_mbr_pcp_attrbtn_trnsfrm) cleared in the calling program - TreoMbrPcpAttrbtnExtr
    - Hashfile cleared in calling program
    - This container is used in:
      TreoMbrPcpAttrbtnExtr
      These programs need to be re-compiled when logic changes
    """
    # ------------------------------------------------------------------
    # Unpack parameters
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params.get("adls_path_raw", adls_path)
    adls_path_publish  = params.get("adls_path_publish", adls_path)
    # ------------------------------------------------------------------
    # Stage: hf_mbr_pcp_attrbtn_allcol  (scenario a - dedup only)
    key_cols_hash = [
        "MBR_UNIQ_KEY",
        "ATTRBTN_BCBS_PLN_CD",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK"
    ]
    df_allcol_dedup = df_AllCol.dropDuplicates(key_cols_hash)
    # ------------------------------------------------------------------
    # Stage: hf_mbr_pcp_attrbtn_trnsfrm (scenario a - dedup only)
    df_transform_dedup = df_Transform.dropDuplicates(key_cols_hash)
    # ------------------------------------------------------------------
    # Stage: K_MBR_PCP_ATTRBTN_TEMP  (write temp table then read back)
    temp_table = f"{IDSOwner}.K_MBR_PCP_ATTRBTN_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table}", ids_jdbc_url, ids_jdbc_props)
    (
        df_transform_dedup
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("append")
        .save()
    )
    extract_query = f"""
    SELECT k.MBR_PCP_ATTRBTN_SK,
           w.MBR_UNIQ_KEY,
           w.ATTRBTN_BCBS_PLN_CD,
           w.ROW_EFF_DT_SK,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_PCP_ATTRBTN_TEMP w
    JOIN {IDSOwner}.K_MBR_PCP_ATTRBTN k
      ON w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
     AND w.ROW_EFF_DT_SK      = k.ROW_EFF_DT_SK
     AND w.MBR_UNIQ_KEY       = k.MBR_UNIQ_KEY
     AND w.ATTRBTN_BCBS_PLN_CD = k.ATTRBTN_BCBS_PLN_CD
    UNION
    SELECT -1,
           w2.MBR_UNIQ_KEY,
           w2.ATTRBTN_BCBS_PLN_CD,
           w2.ROW_EFF_DT_SK,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_MBR_PCP_ATTRBTN_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_PCP_ATTRBTN k2
        WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
          AND w2.ROW_EFF_DT_SK       = k2.ROW_EFF_DT_SK
          AND w2.MBR_UNIQ_KEY        = k2.MBR_UNIQ_KEY
          AND w2.ATTRBTN_BCBS_PLN_CD = k2.ATTRBTN_BCBS_PLN_CD
    )
    """
    df_W_extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # ------------------------------------------------------------------
    # Stage: PrimaryKey Transformer
    df_enriched = (
        df_W_extract
        .withColumn(
            "MBR_PCP_ATTRBTN_SK",
            when(col("MBR_PCP_ATTRBTN_SK") == -1, lit(None)).otherwise(col("MBR_PCP_ATTRBTN_SK"))
        )
        .withColumn(
            "svInstUpdt",
            when(col("MBR_PCP_ATTRBTN_SK").isNull(), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
    )
    key_col = "MBR_PCP_ATTRBTN_SK"
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("svSK", col("MBR_PCP_ATTRBTN_SK")) \
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    # updt link
    df_updt = df_enriched.select(
        "MBR_UNIQ_KEY",
        "ATTRBTN_BCBS_PLN_CD",
        "ROW_EFF_DT_SK",
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("MBR_PCP_ATTRBTN_SK")
    )
    # NewKeys link
    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == "I")
        .select(
            "MBR_UNIQ_KEY",
            "ATTRBTN_BCBS_PLN_CD",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            col("svSK").alias("MBR_PCP_ATTRBTN_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # Keys link
    df_keys = df_enriched.select(
        col("svSK").alias("MBR_PCP_ATTRBTN_SK"),
        "MBR_UNIQ_KEY",
        "ATTRBTN_BCBS_PLN_CD",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ------------------------------------------------------------------
    # Write outputs
    parquet_path_updt = f"{adls_path}/MbrPcpAttrbtnPK_updt.parquet"
    write_files(df_updt, parquet_path_updt, delimiter=",", mode="overwrite", is_pqruet=True, header=True, quote='"', nullValue=None)
    seq_file_path = f"{adls_path}/load/K_MBR_PCP_ATTRBTN.dat"
    write_files(df_newkeys, seq_file_path, delimiter=",", mode="overwrite", is_pqruet=False, header=False, quote='"', nullValue=None)
    # ------------------------------------------------------------------
    # Stage: Merge Transformer
    join_condition = (
        (col("AllColOut.MBR_UNIQ_KEY") == col("Keys.MBR_UNIQ_KEY")) &
        (col("AllColOut.ATTRBTN_BCBS_PLN_CD") == col("Keys.ATTRBTN_BCBS_PLN_CD")) &
        (col("AllColOut.ROW_EFF_DT_SK") == col("Keys.ROW_EFF_DT_SK")) &
        (col("AllColOut.SRC_SYS_CD_SK") == col("Keys.SRC_SYS_CD_SK"))
    )
    df_Key = (
        df_allcol_dedup.alias("AllColOut")
        .join(df_keys.alias("Keys"), join_condition, "left")
        .select(
            col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            col("Keys.INSRT_UPDT_CD"),
            col("AllColOut.DISCARD_IN"),
            col("AllColOut.PASS_THRU_IN"),
            col("AllColOut.FIRST_RECYC_DT"),
            col("AllColOut.ERR_CT"),
            col("AllColOut.RECYCLE_CT"),
            col("Keys.SRC_SYS_CD"),
            col("AllColOut.PRI_KEY_STRING"),
            col("Keys.MBR_PCP_ATTRBTN_SK"),
            col("AllColOut.MBR_UNIQ_KEY"),
            col("AllColOut.ATTRBTN_BCBS_PLN_CD"),
            col("AllColOut.ROW_EFF_DT_SK"),
            col("AllColOut.SRC_SYS_CD_SK"),
            col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("AllColOut.MBR_SK"),
            col("AllColOut.PROV_SK"),
            col("AllColOut.REL_GRP_PROV_SK"),
            col("AllColOut.COB_IN"),
            col("AllColOut.LAST_EVAL_AND_MNG_SVC_DT_SK"),
            col("AllColOut.INDV_BE_KEY"),
            col("AllColOut.MBR_PCP_MO_NO"),
            col("AllColOut.MED_HOME_ID"),
            col("AllColOut.MED_HOME_DESC"),
            col("AllColOut.MED_HOME_GRP_ID"),
            col("AllColOut.MED_HOME_GRP_DESC"),
            col("AllColOut.MED_HOME_LOC_ID"),
            col("AllColOut.MED_HOME_LOC_DESC"),
            col("AllColOut.ATTRBTN_BCBS_PLN_CD_SRC_CD")
        )
    )
    # ------------------------------------------------------------------
    return df_Key