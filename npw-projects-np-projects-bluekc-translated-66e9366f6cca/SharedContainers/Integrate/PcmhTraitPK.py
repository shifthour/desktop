# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
PcmhTraitPK Shared Container
Copyright 2011 Blue Cross and Blue Shield of Kansas City

Called by:
                    MddatacorPcmhMbrPgmExtr 

Processing:
                    *

Control Job Rerun Information: 
                    Previous Run Successful:    What needs to happen before a special run can occur?
                    Previous Run Aborted:         Restart, no other steps necessary

Modifications:                        
                                                 Project/                                                                                                                                                       Code                   Date
Developer           Date              Altiris #     Change Description                                                                                     Project                    Reviewer            Reviewed
-------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -----------------------------  -------------------------  -------------------
Steph Goddard   2011-05-03    4663        Original program                                                                                            IntegrateCurDevl    Brent Leland      5-11-2011
"""
# Assign primary surrogate key
# update primary key table (K_PCMH_TRAIT) with new keys created today
# join primary key info with table info
# Load IDS temp. table
# Temp table is tuncated before load and runstat done after load
# IDS Primary Key Container for PCMH_TRAIT
# This container is used in:
# PCMHTraitExtr
# Hash file (hf_pcmh_trait_allcol) cleared in the calling program
# SQL joins temp table with key table to assign known keys
# primary key hash file only contains current run keys and is cleared before writing
# This program is called in MddatacorPcmhTraitExtr.  It needs to be recompiled if logic changes.
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_PcmhTraitPK(
    df_Allcol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # hf_pcmh_trait_allcol – scenario a (intermediate hash file)
    df_Allcol_dedup = dedup_sort(
        df_Allcol,
        ["SRC_SYS_CD_SK", "TRAIT_TYP_KEY"],
        []
    )

    # ------------------------------------------------------------------
    # K_PCMH_TRAIT_TEMP – truncate and load
    truncate_stmt = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_TRAIT_TEMP')"
    execute_dml(truncate_stmt, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .select(
            F.col("PCMH_TRAIT_ID"),
            F.col("SRC_SYS_CD_SK")
        )
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PCMH_TRAIT_TEMP")
        .mode("append")
        .save()
    )

    extract_query = f"""
    SELECT  k.PCMH_TRAIT_SK,
            w.SRC_SYS_CD_SK,
            w.PCMH_TRAIT_ID,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_PCMH_TRAIT_TEMP w,
         {IDSOwner}.K_PCMH_TRAIT k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.PCMH_TRAIT_ID = k.PCMH_TRAIT_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.PCMH_TRAIT_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_PCMH_TRAIT_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_PCMH_TRAIT k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.PCMH_TRAIT_ID = k2.PCMH_TRAIT_ID
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
            "svInstUpdt",
            F.when(F.col("PCMH_TRAIT_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "PCMH_TRAIT_SK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("PCMH_TRAIT_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PCMH_TRAIT_SK",<schema>,<secret_name>)

    df_updt = df_enriched.select(
        F.col("PCMH_TRAIT_ID"),
        F.lit("MDDATACOR").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("PCMH_TRAIT_SK")
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("PCMH_TRAIT_ID"),
            F.col("SRC_SYS_CD_SK"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("PCMH_TRAIT_SK")
        )
    )

    df_Keys = df_enriched.select(
        F.col("PCMH_TRAIT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("PCMH_TRAIT_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Write sequential file (K_PCMH_TRAIT)
    seq_file_path = f"{adls_path}/load/K_PCMH_TRAIT.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Write parquet for hf_pcmh_trait (scenario c)
    parquet_path = f"{adls_path}/PcmhTraitPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Merge transformer
    df_merge = (
        df_Allcol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.TRAIT_TYP_KEY") == F.col("k.PCMH_TRAIT_ID")),
            "left"
        )
    )

    df_key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("all.INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN"),
        F.col("all.PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT"),
        F.col("all.ERR_CT"),
        F.col("all.RECYCLE_CT"),
        F.col("all.SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING"),
        F.col("k.PCMH_TRAIT_SK"),
        F.col("k.PCMH_TRAIT_ID"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.TRAIT_NM")
    )

    return df_key