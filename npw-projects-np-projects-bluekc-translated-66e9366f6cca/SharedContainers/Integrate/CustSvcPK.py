# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
CustSvcPK – Shared Container
* VC LOGS *
^1_1 10/21/08 10:37:16 Batch  14905_38245 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 10/21/08 10:33:47 Batch  14905_38033 INIT bckcett testIDS dsadm rc for brent
^1_1 10/20/08 12:44:46 Batch  14904_45890 PROMOTE bckcett testIDS u08717 Brent
^1_1 10/20/08 12:41:40 Batch  14904_45702 INIT bckcett devlIDS u08717 Brent
^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent

CALLED BY:
    FctsCustSvcExtr
    NascoCustSvcExtr

PROCESSING:
    Assign primary surrogate key to input records

MODIFICATIONS:
Developer        Date         Project/Altiris #   Change Description
Brent Leland     2008-02-26   3567 Primary Key    Original Programming. Changed counter from IDS_SK to CUST_SVC_SK
"""
# IDS Primary Key Container for Customer Service
# Used by FctsCustSvcExtr, NascoCustSvcExtr
# Assign primary surrogate key
# join primary key info with table info
# update primary key table (K_CUST_SVC) with new keys created today
# primary key hash file only contains current run keys
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# Load IDS temp. table
# Hash file cleared in calling job
# COMMAND ----------
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
# COMMAND ----------
def run_CustSvcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
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
    # hf_custsvc_sum_allcol – scenario a (intermediate hash file)
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CUST_SVC_ID"],
        [("SRC_SYS_CD_SK", "A"), ("CUST_SVC_ID", "A")]
    )
    # ------------------------------------------------------------------
    # K_CUST_SVC_TEMP – load temp table
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_CUST_SVC_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform.select("SRC_SYS_CD_SK", "CUST_SVC_ID")
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CUST_SVC_TEMP")
        .mode("append")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CUST_SVC_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # W_Extract – query known/unknown keys
    extract_query = f"""
    SELECT k.CUST_SVC_SK,
           w.SRC_SYS_CD_SK,
           w.CUST_SVC_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CUST_SVC_TEMP w,
         {IDSOwner}.K_CUST_SVC k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.CUST_SVC_ID   = k.CUST_SVC_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CUST_SVC_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CUST_SVC_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.CUST_SVC_SK
        FROM {IDSOwner}.K_CUST_SVC k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CUST_SVC_ID   = k2.CUST_SVC_ID
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
    # PrimaryKey transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("CUST_SVC_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CUST_SVC_SK",
            F.when(F.col("INSRT_UPDT_CD") == "I", F.lit(None).cast("long")).otherwise(F.col("CUST_SVC_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CUST_SVC_SK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # Derive output link DataFrames
    df_NewKeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_SK"
        )
    )
    df_Keys = df_enriched.select(
        "CUST_SVC_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_SK"
    )
    # ------------------------------------------------------------------
    # hf_cust_svc_updt – scenario c (write parquet)
    parquet_path = f"{adls_path}/CustSvcPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # K_CUST_SVC sequential file write
    seq_file_path = f"{adls_path}/load/K_CUST_SVC.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Merge transformer
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.CUST_SVC_ID")   == F.col("k.CUST_SVC_ID")),
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
            F.col("k.CUST_SVC_SK"),
            F.col("all.CUST_SVC_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CUST_SVC_CNTCT_RELSHP_CD"),
            F.col("all.CUST_SVC_EXCL_CD"),
            F.col("all.CUST_SVC_METH_CD"),
            F.col("all.CUST_SVC_SATSFCTN_LVL_CD"),
            F.col("all.DISCLMR_IN"),
            F.col("all.CNTCT_INFO_TX"),
            F.col("all.CNTCT_RQST_DESC"),
            F.col("all.CSTK_SEQ_NO")
        )
    )
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------