# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

def run_MbrCareOppPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]

    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK", "CARE_OPP_ID"],
        [("SRC_SYS_CD_SK", "A")]
    )

    execute_dml(f"DROP TABLE IF EXISTS {IDSOwner}.K_MBR_CARE_OPP_TEMP", ids_jdbc_url, ids_jdbc_props)
    execute_dml(
        f"""
CREATE TABLE {IDSOwner}.K_MBR_CARE_OPP_TEMP (
    SRC_SYS_CD_SK INT NOT NULL,
    MBR_UNIQ_KEY VARCHAR(20) NOT NULL,
    PRCS_YR_MO_SK INT NOT NULL,
    CARE_OPP_ID VARCHAR(20) NOT NULL,
    PRIMARY KEY (SRC_SYS_CD_SK, MBR_UNIQ_KEY, PRCS_YR_MO_SK, CARE_OPP_ID)
)
""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    df_Transform.select(
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        "CARE_OPP_ID"
    ).write.mode("append").format("jdbc").option("url", ids_jdbc_url).options(**ids_jdbc_props).option("dbtable", f"{IDSOwner}.K_MBR_CARE_OPP_TEMP").save()

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MBR_CARE_OPP_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
SELECT  k.MBR_CARE_OPP_SK,
        w.SRC_SYS_CD_SK,
        w.MBR_UNIQ_KEY,
        w.PRCS_YR_MO_SK,
        w.CARE_OPP_ID,
        k.CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_MBR_CARE_OPP_TEMP w
JOIN {IDSOwner}.K_MBR_CARE_OPP k
  ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
 AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
 AND w.PRCS_YR_MO_SK = k.PRCS_YR_MO_SK
 AND w.CARE_OPP_ID   = k.CARE_OPP_ID
UNION
SELECT -1,
       w2.SRC_SYS_CD_SK,
       w2.MBR_UNIQ_KEY,
       w2.PRCS_YR_MO_SK,
       w2.CARE_OPP_ID,
       {CurrRunCycle}
FROM {IDSOwner}.K_MBR_CARE_OPP_TEMP w2
WHERE NOT EXISTS (
    SELECT 1
    FROM {IDSOwner}.K_MBR_CARE_OPP k2
    WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
      AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
      AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
      AND w2.CARE_OPP_ID   = k2.CARE_OPP_ID
)
"""

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("MBR_CARE_OPP_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svCareOppId", F.trim(F.col("CARE_OPP_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("MBR_CARE_OPP_SK") == -1, F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_CARE_OPP_SK",<schema>,<secret_name>)

    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("MBR_UNIQ_KEY"),
        F.col("PRCS_YR_MO_SK"),
        F.col("svCareOppId").alias("CARE_OPP_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_CARE_OPP_SK")
    )

    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("MBR_UNIQ_KEY"),
            F.col("PRCS_YR_MO_SK"),
            F.col("svCareOppId").alias("CARE_OPP_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("MBR_CARE_OPP_SK")
        )
    )

    df_Keys = df_enriched.select(
        F.col("MBR_CARE_OPP_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("MBR_UNIQ_KEY"),
        F.col("PRCS_YR_MO_SK"),
        F.col("svCareOppId").alias("CARE_OPP_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_MBR_CARE_OPP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    write_files(
        df_updt,
        f"{adls_path}/MbrCareOppPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY")) &
            (F.col("all.PRCS_YR_MO_SK") == F.col("k.PRCS_YR_MO_SK")) &
            (F.col("all.CARE_OPP_ID") == F.col("k.CARE_OPP_ID")),
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
            F.col("k.MBR_CARE_OPP_SK"),
            F.col("all.MBR_UNIQ_KEY"),
            F.col("all.PRCS_YR_MO_SK"),
            F.col("all.CARE_OPP_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CARE_OPP_SK"),
            F.col("all.MBR_MED_MESRS_SK")
        )
    )

    return df_Key