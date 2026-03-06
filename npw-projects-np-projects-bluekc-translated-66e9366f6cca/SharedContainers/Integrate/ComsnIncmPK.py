
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from functools import reduce

"""
COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Comm Income job

CALLED BY : FctsComsnIncmExtr

PROCESSING:    


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #                            Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ------------------------------------------------     ------------------------------------       ----------------------------      -------------------------
Ralph Tucker    2012-08-20                Initial program                                                               4873 - Commissions Reporting     IntegrateNewDevl               Kalyan Neelam          2012-11-06
T.Sieg               2021-02-19                 Adding 16 new columns for ACA and MA activity          335378                                           IntegrateDev2

Annotation:
IDS Primary Key Container for Commission Incm
Used by FctsComsnIncmExtr
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_COMSN_INCM) with new keys created today
Assign primary surrogate key
Hash file (hf_comsn_incm_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
"""

def run_ComsnIncmPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # --------------------------------------------------
    # Write df_Transform to {IDSOwner}.K_COMSN_INCM_TEMP
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_COMSN_INCM_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    df_Transform.write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_COMSN_INCM_TEMP") \
        .mode("append") \
        .save()
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_AGMNT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # --------------------------------------------------
    # Read W_Extract from DB
    extract_query = f"""
        SELECT
            k.COMSN_INCM_SK,
            w.PD_AGNT_ID,
            w.ERN_AGNT_ID,
            w.COMSN_PD_DT_SK,
            w.BILL_ENTY_UNIQ_KEY,
            w.BILL_INCM_RCPT_BILL_DUE_DT_SK,
            w.COMSN_INCM_SEQ_NO,
            w.CLS_PLN_ID,
            w.PROD_ID,
            w.COMSN_DTL_INCM_DISP_CD,
            w.COMSN_TYP_CD,
            w.SRC_SYS_CD_SK,
            k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_COMSN_INCM_TEMP   w
        JOIN {IDSOwner}.K_COMSN_INCM        k
          ON w.SRC_SYS_CD_SK                   = k.SRC_SYS_CD_SK
         AND w.PD_AGNT_ID                      = k.PD_AGNT_ID
         AND w.ERN_AGNT_ID                     = k.ERN_AGNT_ID
         AND w.COMSN_PD_DT_SK                  = k.COMSN_PD_DT_SK
         AND w.BILL_ENTY_UNIQ_KEY              = k.BILL_ENTY_UNIQ_KEY
         AND w.BILL_INCM_RCPT_BILL_DUE_DT_SK   = k.BILL_INCM_RCPT_BILL_DUE_DT_SK
         AND w.COMSN_INCM_SEQ_NO               = k.COMSN_INCM_SEQ_NO
         AND w.PROD_ID                         = k.PROD_ID
         AND w.CLS_PLN_ID                      = k.CLS_PLN_ID
         AND w.COMSN_DTL_INCM_DISP_CD          = k.COMSN_DTL_INCM_DISP_CD
         AND w.COMSN_TYP_CD                    = k.COMSN_TYP_CD
        UNION
        SELECT
            -1 AS COMSN_INCM_SK,
            w2.PD_AGNT_ID,
            w2.ERN_AGNT_ID,
            w2.COMSN_PD_DT_SK,
            w2.BILL_ENTY_UNIQ_KEY,
            w2.BILL_INCM_RCPT_BILL_DUE_DT_SK,
            w2.COMSN_INCM_SEQ_NO,
            w2.CLS_PLN_ID,
            w2.PROD_ID,
            w2.COMSN_DTL_INCM_DISP_CD,
            w2.COMSN_TYP_CD,
            w2.SRC_SYS_CD_SK,
            {CurrRunCycle} AS CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_COMSN_INCM_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
              FROM {IDSOwner}.K_COMSN_INCM k2
             WHERE w2.SRC_SYS_CD_SK                 = k2.SRC_SYS_CD_SK
               AND w2.PD_AGNT_ID                    = k2.PD_AGNT_ID
               AND w2.ERN_AGNT_ID                   = k2.ERN_AGNT_ID
               AND w2.COMSN_PD_DT_SK                = k2.COMSN_PD_DT_SK
               AND w2.BILL_ENTY_UNIQ_KEY            = k2.BILL_ENTY_UNIQ_KEY
               AND w2.BILL_INCM_RCPT_BILL_DUE_DT_SK = k2.BILL_INCM_RCPT_BILL_DUE_DT_SK
               AND w2.COMSN_INCM_SEQ_NO             = k2.COMSN_INCM_SEQ_NO
               AND w2.PROD_ID                       = k2.PROD_ID
               AND w2.CLS_PLN_ID                    = k2.CLS_PLN_ID
               AND w2.COMSN_DTL_INCM_DISP_CD        = k2.COMSN_DTL_INCM_DISP_CD
               AND w2.COMSN_TYP_CD                  = k2.COMSN_TYP_CD
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
    # PrimaryKey Transformer Logic
    df_enriched = (
        df_W_Extract
        .withColumn("INSRT_UPDT_CD",
                    F.when(F.col("COMSN_INCM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")))
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn("COMSN_INCM_SK",
                    F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(None).cast("long")).otherwise(F.col("COMSN_INCM_SK")))
        .withColumn("CRT_RUN_CYC_EXCTN_SK",
                    F.when(F.col("INSRT_UPDT_CD") == F.lit("I"),
                           F.lit(int(CurrRunCycle))).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"COMSN_INCM_SK",<schema>,<secret_name>)
    # --------------------------------------------------
    # Build Updt, Keys, NewKeys DataFrames
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "PD_AGNT_ID",
        "ERN_AGNT_ID",
        "COMSN_PD_DT_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_INCM_RCPT_BILL_DUE_DT_SK",
        "COMSN_INCM_SEQ_NO",
        "PROD_ID",
        "CLS_PLN_ID",
        "COMSN_DTL_INCM_DISP_CD",
        "COMSN_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "COMSN_INCM_SK"
    )
    df_Keys = df_enriched.select(
        "COMSN_INCM_SK",
        "SRC_SYS_CD_SK",
        "PD_AGNT_ID",
        "ERN_AGNT_ID",
        "COMSN_PD_DT_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_INCM_RCPT_BILL_DUE_DT_SK",
        "COMSN_INCM_SEQ_NO",
        "CLS_PLN_ID",
        "PROD_ID",
        "COMSN_DTL_INCM_DISP_CD",
        "COMSN_TYP_CD",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    df_NewKeys = df_enriched.select(
        "PD_AGNT_ID",
        "ERN_AGNT_ID",
        "COMSN_PD_DT_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_INCM_RCPT_BILL_DUE_DT_SK",
        "COMSN_INCM_SEQ_NO",
        "CLS_PLN_ID",
        "PROD_ID",
        "COMSN_DTL_INCM_DISP_CD",
        "COMSN_TYP_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "COMSN_INCM_SK"
    )
    # --------------------------------------------------
    # Scenario-a hash file replacement: dedup_sort
    key_cols = [
        "SRC_SYS_CD_SK",
        "PD_AGNT_ID",
        "ERN_AGNT_ID",
        "COMSN_PD_DT_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_INCM_RCPT_BILL_DUE_DT_SK",
        "COMSN_INCM_SEQ_NO",
        "CLS_PLN_ID",
        "PROD_ID",
        "COMSN_DTL_INCM_DISP_CD",
        "COMSN_TYP_CD"
    ]
    sort_cols = [(c, "A") for c in key_cols]
    df_AllColOut = dedup_sort(df_AllCol, key_cols, sort_cols)
    # --------------------------------------------------
    # Merge Transformer Logic
    join_conditions = reduce(
        lambda acc, col_nm: acc & (F.col(f"all.{col_nm}") == F.col(f"keys.{col_nm}")),
        key_cols[1:],
        F.col(f"all.{key_cols[0]}") == F.col(f"keys.{key_cols[0]}")
    )
    df_Key = df_AllColOut.alias("all").join(df_Keys.alias("keys"), join_conditions, "left")
    df_Key = (
        df_Key
        .withColumn("INSRT_UPDT_CD", F.col("keys.INSRT_UPDT_CD"))
        .withColumn("SRC_SYS_CD",     F.col("keys.SRC_SYS_CD"))
        .withColumn("COMSN_INCM_SK",  F.col("keys.COMSN_INCM_SK"))
        .withColumn("CRT_RUN_CYC_EXTCN_SK",  F.col("keys.CRT_RUN_CYC_EXCTN_SK"))
        .withColumn("LAST_UPDT_RUN_CYC_EXTCN_SK", F.lit(int(CurrRunCycle)))
    )
    # --------------------------------------------------
    # Scenario-c hash file write as parquet
    write_files(
        df_updt,
        f"{adls_path}/ComsnIncmPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # Sequential file write
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_COMSN_INCM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    return df_Key
