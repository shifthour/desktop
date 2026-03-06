# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
IDS Primary Key Container for Vendor Group Invoice
Used by

PSVndrGrpInvcExtr
Hash file (hf_vndr_grp_invc_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (K_VNDR_GRP_INVC) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key

***************************************************************************************************************************************************************
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Vendor Group Invoice job

CALLED BY : PSVndrGrpInvcExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2009-01-09               Initial program                                                               3244 Primary Key    devlIDS                                 Steph Goddard         01/14/2009
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

def run_PSVndrGrpInvcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # unpack parameters
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params.get("SrcSysCd", "")
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # --------------------------------------------------
    # step 1: deduplicate AllCol (hash file eliminated – scenario a)
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "GRP_ID", "FNCL_LOB_CD", "INVC_DT_SK", "INVC_DESC"],
        sort_cols=[("<…>", "D")]
    )
    # --------------------------------------------------
    # step 2: load K_VNDR_GRP_INVC_TEMP
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_VNDR_GRP_INVC_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_VNDR_GRP_INVC_TEMP")
        .mode("append")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_VNDR_GRP_INVC_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # --------------------------------------------------
    # step 3: extract W_Extract dataset
    extract_query = f"""
    SELECT  k.VNDR_GRP_INVC_SK,
            w.SRC_SYS_CD_SK,
            w.GRP_ID,
            w.FNCL_LOB_CD,
            w.INVC_DT_SK,
            w.INVC_DESC,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_VNDR_GRP_INVC_TEMP w,
         {IDSOwner}.K_VNDR_GRP_INVC k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.GRP_ID        = k.GRP_ID
      AND w.FNCL_LOB_CD   = k.FNCL_LOB_CD
      AND w.INVC_DT_SK    = k.INVC_DT_SK
      AND w.INVC_DESC     = k.INVC_DESC
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.GRP_ID,
           w2.FNCL_LOB_CD,
           w2.INVC_DT_SK,
           w2.INVC_DESC,
           {CurrRunCycle}
    FROM {IDSOwner}.K_VNDR_GRP_INVC_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.VNDR_GRP_INVC_SK
        FROM {IDSOwner}.K_VNDR_GRP_INVC k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.GRP_ID        = k2.GRP_ID
          AND w2.FNCL_LOB_CD   = k2.FNCL_LOB_CD
          AND w2.INVC_DT_SK    = k2.INVC_DT_SK
          AND w2.INVC_DESC     = k2.INVC_DESC
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
    # step 4: PrimaryKey transformer – derive columns
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("VNDR_GRP_INVC_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
        .withColumn("svSK", F.col("VNDR_GRP_INVC_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("VNDR_GRP_INVC_SK") == -1, F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # --------------------------------------------------
    # step 5: updt DataFrame → hf_vndr_grp_invc (parquet)
    df_updt = df_enriched.select(
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("GRP_ID"),
        F.col("FNCL_LOB_CD"),
        F.col("INVC_DT_SK"),
        F.col("INVC_DESC"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("VNDR_GRP_INVC_SK")
    )
    write_files(
        df_updt,
        f"{adls_path}/PSVndrGrpInvcPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # step 6: NewKeys DataFrame → sequential file
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("GRP_ID"),
            F.col("FNCL_LOB_CD"),
            F.col("INVC_DT_SK"),
            F.col("INVC_DESC"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VNDR_GRP_INVC_SK")
        )
    )
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_VNDR_GRP_INVC.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # step 7: Keys DataFrame (for merge)
    df_Keys = df_enriched.select(
        F.col("svSK").alias("VNDR_GRP_INVC_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("GRP_ID"),
        F.col("FNCL_LOB_CD"),
        F.col("INVC_DT_SK"),
        F.col("INVC_DESC"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------
    # step 8: Merge transformer – create Key DataFrame
    df_Key = (
        df_AllColOut.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.GRP_ID") == F.col("k.GRP_ID")) &
            (F.col("all.FNCL_LOB_CD") == F.col("k.FNCL_LOB_CD")) &
            (F.col("all.INVC_DT_SK") == F.col("k.INVC_DT_SK")) &
            (F.col("all.INVC_DESC") == F.col("k.INVC_DESC")),
            "left"
        )
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
            F.col("k.VNDR_GRP_INVC_SK"),
            F.col("all.GRP_ID"),
            F.col("all.FNCL_LOB_CD"),
            F.col("all.INVC_DT_SK"),
            F.col("all.INVC_DESC"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.INVOICE_PAID_DATE"),
            F.col("all.PD_AMT"),
            F.col("all.INVC_QTY"),
            F.col("all.CHK_NO"),
            F.col("all.SVC_DESC"),
            F.col("all.SUB_ACCT_NO"),
            F.col("all.VNDR_ID"),
            F.col("all.VNDR_NM"),
            F.col("all.LOB"),
            F.col("all.PROD_SH_NM_SK")
        )
    )
    # --------------------------------------------------
    return df_Key