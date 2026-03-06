# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Tuple
# COMMAND ----------
def run_EpsdTreatGrpPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    df_AllCol = dedup_sort(df_AllCol,["SRC_SYS_CD_SK","EPSD_TREAT_GRP_CD"],[])
    df_k = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", f"SELECT EPSD_TREAT_GRP_SK, SRC_SYS_CD_SK, EPSD_TREAT_GRP_CD, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_EPSD_TREAT_GRP")
             .load()
    )
    df_W_Extract = (
        df_Transform.alias("w")
        .join(df_k.alias("k"), ["SRC_SYS_CD_SK","EPSD_TREAT_GRP_CD"], "left")
        .select(
            F.when(F.col("k.EPSD_TREAT_GRP_SK").isNull(), F.lit(-1)).otherwise(F.col("k.EPSD_TREAT_GRP_SK")).alias("EPSD_TREAT_GRP_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.EPSD_TREAT_GRP_CD"),
            F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_primary = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("EPSD_TREAT_GRP_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("EPSD_TREAT_GRP_SK", F.when(F.col("EPSD_TREAT_GRP_SK") == -1, F.lit(None).cast("long")).otherwise(F.col("EPSD_TREAT_GRP_SK")))
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn("svCrtRunCycExctnSk", F.when(F.col("svInstUpdt") == 'I', F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svEpsdTrtGrpCd", F.trim(F.col("EPSD_TREAT_GRP_CD")))
    )
    df_enriched = df_primary
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EPSD_TREAT_GRP_SK",<schema>,<secret_name>)
    df_updt = df_enriched.select(
        F.col("SRC_SYS_CD"),
        F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("EPSD_TREAT_GRP_SK")
    )
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == 'I')
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("EPSD_TREAT_GRP_SK")
        )
    )
    df_Keys = (
        df_enriched.select(
            F.col("EPSD_TREAT_GRP_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("SRC_SYS_CD"),
            F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_Key = (
        df_AllCol.alias("all")
        .join(df_Keys.alias("k"), ["SRC_SYS_CD_SK","EPSD_TREAT_GRP_CD"], "left")
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
            F.col("k.EPSD_TREAT_GRP_SK"),
            F.col("all.EPSD_TREAT_GRP_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MAJ_PRCTC_CAT_CD_SK"),
            F.col("all.EPSD_TREAT_GRP_DESC"),
            F.col("all.EPSD_TREAT_GRP_LABEL")
        )
    )
    write_files(df_updt,f"{adls_path}/EpsdTreatGrpPK_updt.parquet",',','overwrite',True,True,'"',None)
    write_files(df_NewKeys,f"{adls_path}/load/K_EPSD_TREAT_GRP.dat",',','overwrite',False,False,'"',None)
    return df_Key