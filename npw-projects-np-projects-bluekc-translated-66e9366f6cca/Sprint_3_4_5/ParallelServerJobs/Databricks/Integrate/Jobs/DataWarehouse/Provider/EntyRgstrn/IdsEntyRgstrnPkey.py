# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja Sunkara          2014-07-08               5345                             Original Programming                                                                        IntegrateWrhsDevl     Kalyan Neelam            2015-01-07
# MAGIC 
# MAGIC Revathi BoojiReddy  2022-03-30          US 503869                    Added  11 new columns to the                                                             IntegrateDevB           Goutham Kalidindi         2022-05-02
# MAGIC                                                                                                           ENTY_RGSTRN table

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_ENTY_RGSTRN.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','')

# Read from ds_ENTY_RGSTRN_Xfrm (.ds → .parquet)
df_ds_ENTY_RGSTRN_Xfrm = spark.read.parquet(f"{adls_path}/ds/ENTY_RGSTRN.{SrcSysCd}.xfrm.{RunID}.parquet")

# cpy_MultiStreams outputs
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_ENTY_RGSTRN_Xfrm.select(
    F.col("ENTY_ID").alias("ENTY_ID"),
    F.col("ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_ENTY_RGSTRN_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("ENTY_ID").alias("ENTY_ID"),
    F.col("ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRRG_MCTR_TYPE").alias("PRRG_MCTR_TYPE"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CMN_PRCT_IN").alias("CMN_PRCT_IN"),
    F.col("Lkup_PRCR_ID").alias("Lkup_PRCR_ID"),
    F.col("ENTY_RGSTRN_ST_CD").alias("ENTY_RGSTRN_ST_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("RGSTRN_ID").alias("RGSTRN_ID"),
    F.col("PRRG_LAST_VER_NAME").alias("PRRG_LAST_VER_NAME"),
    F.col("FACETS_SRC_SYS_CD_CMN_PRCT").alias("FACETS_SRC_SYS_CD_CMN_PRCT"),
    F.col("CACTUS_SRC_SYS_CD_CMN_PRCT").alias("CACTUS_SRC_SYS_CD_CMN_PRCT"),
    F.col("PRRG_INIT_VER_DT").alias("PRRG_INIT_VER_DT"),
    F.col("PRRG_LAST_VER_DT").alias("PRRG_LAST_VER_DT"),
    F.col("PRRG_NEXT_VER_DT").alias("PRRG_NEXT_VER_DT"),
    F.col("PRRG_RCVD_VER_DT").alias("PRRG_RCVD_VER_DT"),
    F.col("PRRG_MCTR_VSRC").alias("PRRG_MCTR_VSRC"),
    F.col("PRRG_MCTR_VRSL").alias("PRRG_MCTR_VRSL"),
    F.col("PRRG_MCTR_VMTH").alias("PRRG_MCTR_VMTH"),
    F.col("PRRG_MCTR_SPEC").alias("PRRG_MCTR_SPEC"),
    F.col("PRRG_STS").alias("PRRG_STS"),
    F.col("PRRG_CERT_IND").alias("PRRG_CERT_IND"),
    F.col("PRRG_PRIM_VER_IND").alias("PRRG_PRIM_VER_IND")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_lnkRemDupDataOut = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    partition_cols=["ENTY_ID","ENTY_RGSTRN_TYP_CD","RGSTRN_SEQ_NO","SRC_SYS_CD"],
    sort_cols=[]
)

# db2_K_ENTY_RGSTRN_In (DB2ConnectorPX) - Reading from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT ENTY_ID, ENTY_RGSTRN_TYP_CD, RGSTRN_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, ENTY_RGSTRN_SK FROM {IDSOwner}.K_ENTY_RGSTRN"
df_db2_K_ENTY_RGSTRN_In = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

# jn_EntyRgstrn (left outer join)
df_jn_EntyRgstrn = df_rdp_NaturalKeys_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_K_ENTY_RGSTRN_In.alias("lnk_KEntyRgstrn_Extr"),
    [
        F.col("lnkRemDupDataOut.ENTY_ID") == F.col("lnk_KEntyRgstrn_Extr.ENTY_ID"),
        F.col("lnkRemDupDataOut.ENTY_RGSTRN_TYP_CD") == F.col("lnk_KEntyRgstrn_Extr.ENTY_RGSTRN_TYP_CD"),
        F.col("lnkRemDupDataOut.RGSTRN_SEQ_NO") == F.col("lnk_KEntyRgstrn_Extr.RGSTRN_SEQ_NO"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_KEntyRgstrn_Extr.SRC_SYS_CD")
    ],
    "left"
)
df_jn_EntyRgstrn = df_jn_EntyRgstrn.select(
    F.col("lnkRemDupDataOut.ENTY_ID").alias("ENTY_ID"),
    F.col("lnkRemDupDataOut.ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("lnkRemDupDataOut.RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KEntyRgstrn_Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KEntyRgstrn_Extr.ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK")
)

# xfm_PKEYgen (CTransformerStage)
df_xfm_PKEYgen_temp = df_jn_EntyRgstrn.withColumn("pre_EntyRgstrnSK", F.col("ENTY_RGSTRN_SK"))
df_enriched = df_xfm_PKEYgen_temp
# SurrogateKeyGen for NextSurrogateKey logic
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'ENTY_RGSTRN_SK',<schema>,<secret_name>)

# lnk_KEntyRgstrn_New (constraint: isNull(original ENTY_RGSTRN_SK))
df_xfm_PKEYgen_lnk_KEntyRgstrn_New = df_enriched.filter(F.col("pre_EntyRgstrnSK").isNull()).select(
    F.col("ENTY_ID").alias("ENTY_ID"),
    F.col("ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK")
)

# lnkPKEYxfmOut (all rows, applying transform logic)
df_xfm_PKEYgen_lnkPKEYxfmOut = df_enriched.select(
    F.col("ENTY_ID").alias("ENTY_ID"),
    F.col("ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(F.col("pre_EntyRgstrnSK").isNull(), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# db2_K_ENTY_RGSTRN_Load (DB2ConnectorPX) - merge into {IDSOwner}.K_ENTY_RGSTRN
temp_table_name = "STAGING.IdsEntyRgstrnPkey_db2_K_ENTY_RGSTRN_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_xfm_PKEYgen_lnk_KEntyRgstrn_New.write.jdbc(
    url=jdbc_url,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {IDSOwner}.K_ENTY_RGSTRN AS T
USING {temp_table_name} AS S
ON 
    T.ENTY_ID = S.ENTY_ID AND
    T.ENTY_RGSTRN_TYP_CD = S.ENTY_RGSTRN_TYP_CD AND
    T.RGSTRN_SEQ_NO = S.RGSTRN_SEQ_NO AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN 
    UPDATE SET 
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.ENTY_RGSTRN_SK = S.ENTY_RGSTRN_SK
WHEN NOT MATCHED THEN 
    INSERT (
        ENTY_ID,
        ENTY_RGSTRN_TYP_CD,
        RGSTRN_SEQ_NO,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_SK,
        ENTY_RGSTRN_SK
    )
    VALUES (
        S.ENTY_ID,
        S.ENTY_RGSTRN_TYP_CD,
        S.RGSTRN_SEQ_NO,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.ENTY_RGSTRN_SK
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs (inner join)
df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        F.col("lnkFullDataJnIn.ENTY_ID") == F.col("lnkPKEYxfmOut.ENTY_ID"),
        F.col("lnkFullDataJnIn.ENTY_RGSTRN_TYP_CD") == F.col("lnkPKEYxfmOut.ENTY_RGSTRN_TYP_CD"),
        F.col("lnkFullDataJnIn.RGSTRN_SEQ_NO") == F.col("lnkPKEYxfmOut.RGSTRN_SEQ_NO"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    "inner"
)

df_jn_PKEYs = df_jn_PKEYs.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.ENTY_ID").alias("ENTY_ID"),
    F.col("lnkFullDataJnIn.ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    F.col("lnkFullDataJnIn.RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    F.col("lnkFullDataJnIn.PRRG_MCTR_TYPE").alias("PRRG_MCTR_TYPE"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkFullDataJnIn.CMN_PRCT_IN").alias("CMN_PRCT_IN"),
    F.col("lnkFullDataJnIn.Lkup_PRCR_ID").alias("Lkup_PRCR_ID"),
    F.col("lnkFullDataJnIn.ENTY_RGSTRN_ST_CD").alias("ENTY_RGSTRN_ST_CD"),
    F.col("lnkFullDataJnIn.EFF_DT").alias("EFF_DT"),
    F.col("lnkFullDataJnIn.TERM_DT").alias("TERM_DT"),
    F.col("lnkFullDataJnIn.RGSTRN_ID").alias("RGSTRN_ID"),
    F.col("lnkFullDataJnIn.PRRG_LAST_VER_NAME").alias("PRRG_LAST_VER_NAME"),
    F.col("lnkFullDataJnIn.FACETS_SRC_SYS_CD_CMN_PRCT").alias("FACETS_SRC_SYS_CD_CMN_PRCT"),
    F.col("lnkFullDataJnIn.CACTUS_SRC_SYS_CD_CMN_PRCT").alias("CACTUS_SRC_SYS_CD_CMN_PRCT"),
    F.col("lnkFullDataJnIn.PRRG_INIT_VER_DT").alias("PRRG_INIT_VER_DT"),
    F.col("lnkFullDataJnIn.PRRG_LAST_VER_DT").alias("PRRG_LAST_VER_DT"),
    F.col("lnkFullDataJnIn.PRRG_NEXT_VER_DT").alias("PRRG_NEXT_VER_DT"),
    F.col("lnkFullDataJnIn.PRRG_RCVD_VER_DT").alias("PRRG_RCVD_VER_DT"),
    F.col("lnkFullDataJnIn.PRRG_MCTR_VSRC").alias("PRRG_MCTR_VSRC"),
    F.col("lnkFullDataJnIn.PRRG_MCTR_VRSL").alias("PRRG_MCTR_VRSL"),
    F.col("lnkFullDataJnIn.PRRG_MCTR_VMTH").alias("PRRG_MCTR_VMTH"),
    F.col("lnkFullDataJnIn.PRRG_MCTR_SPEC").alias("PRRG_MCTR_SPEC"),
    F.col("lnkFullDataJnIn.PRRG_STS").alias("PRRG_STS"),
    F.col("lnkFullDataJnIn.PRRG_CERT_IND").alias("PRRG_CERT_IND"),
    F.col("lnkFullDataJnIn.PRRG_PRIM_VER_IND").alias("PRRG_PRIM_VER_IND")
)

# seq_ENTY_RGSTRN_Pkey (PxSequentialFile) - final write
df_final = df_jn_PKEYs
df_final = df_final.withColumn("CMN_PRCT_IN", F.rpad(F.col("CMN_PRCT_IN"), 1, " "))
df_final = df_final.withColumn("EFF_DT", F.rpad(F.col("EFF_DT"), 10, " "))
df_final = df_final.withColumn("TERM_DT", F.rpad(F.col("TERM_DT"), 10, " "))
df_final = df_final.withColumn("PRRG_INIT_VER_DT", F.rpad(F.col("PRRG_INIT_VER_DT"), 10, " "))
df_final = df_final.withColumn("PRRG_LAST_VER_DT", F.rpad(F.col("PRRG_LAST_VER_DT"), 10, " "))
df_final = df_final.withColumn("PRRG_NEXT_VER_DT", F.rpad(F.col("PRRG_NEXT_VER_DT"), 10, " "))
df_final = df_final.withColumn("PRRG_RCVD_VER_DT", F.rpad(F.col("PRRG_RCVD_VER_DT"), 10, " "))
df_final = df_final.withColumn("PRRG_MCTR_SPEC", F.rpad(F.col("PRRG_MCTR_SPEC"), 4, " "))
df_final = df_final.withColumn("PRRG_CERT_IND", F.rpad(F.col("PRRG_CERT_IND"), 1, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), 0, " "))

write_files(
    df_final,
    f"{adls_path}/key/ENTY_RGSTRN.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)