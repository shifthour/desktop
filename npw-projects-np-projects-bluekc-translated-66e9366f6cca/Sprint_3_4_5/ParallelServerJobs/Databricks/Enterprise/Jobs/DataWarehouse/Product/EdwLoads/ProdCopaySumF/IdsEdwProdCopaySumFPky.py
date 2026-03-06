# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                         DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                             ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ----------------------------------------------                        -------------------------------    ------------------------------       --------------------
# MAGIC Leandrew Moore          6/11/2013             P5114                         REWRITE IN PARALELL                          Enterprisewarehouse  Peter Marshall              8/8/2013
# MAGIC 
# MAGIC Aishwarya                    03/15/2016            5600                           DRUG_NPRFR_SPEC_COPAY_AMT column EnterpriseDevl      Jag Yelavarthi             2016-04-06
# MAGIC                                                                                                        added in the target

# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: IdsEdwProdCopaySumFPky
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_KProdCopaySumFRead = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT \n \nSRC_SYS_CD,\nPROD_ID,\nPROD_CMPNT_EFF_DT_SK,\nCRT_RUN_CYC_EXCTN_DT_SK,\nPROD_COPAY_SUM_SK ,\nCRT_RUN_CYC_EXCTN_SK\n\nFROM\n#$EDWOwner#.K_PROD_COPAY_SUM_F"
    )
    .load()
)

df_ds_ProdCopaySumFExtr = spark.read.parquet(f"{adls_path}/ds/PROD_COPAY_SUM_F.parquet")

df_cpy_MultiStreams_1 = df_ds_ProdCopaySumFExtr.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("DRUG_GNRC_COPAY_AMT").alias("DRUG_GNRC_COPAY_AMT"),
    F.col("DRUG_NM_BRND_COPAY_AMT").alias("DRUG_NM_BRND_COPAY_AMT"),
    F.col("DRUG_PRFRD_COPAY_AMT").alias("DRUG_PRFRD_COPAY_AMT"),
    F.col("DRUG_NPRFR_COPAY_AMT").alias("DRUG_NPRFR_COPAY_AMT"),
    F.col("ELTRNC_PHYS_VST_COPAY_AMT").alias("ELTRNC_PHYS_VST_COPAY_AMT"),
    F.col("ER_IN_NTWK_COPAY_AMT").alias("ER_IN_NTWK_COPAY_AMT"),
    F.col("ER_OUT_NTWK_COPAY_AMT").alias("ER_OUT_NTWK_COPAY_AMT"),
    F.col("HOME_HLTH_VST_COPAY_AMT").alias("HOME_HLTH_VST_COPAY_AMT"),
    F.col("IN_HOSP_IN_OUT_NTWK_COPAY_AMT").alias("IN_HOSP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("IP_HSPC_COPAY_AMT").alias("IP_HSPC_COPAY_AMT"),
    F.col("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT").alias("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT"),
    F.col("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT").alias("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT"),
    F.col("MRI_CT_PET_MRA_COPAY_AMT").alias("MRI_CT_PET_MRA_COPAY_AMT"),
    F.col("OV_IN_OUT_NTWK_COPAY_AMT").alias("OV_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OV_PCP_IN_OUT_NTWK_COPAY_AMT").alias("OV_PCP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OV_SPLST_IN_OUT_NTWK_COPAY_AMT").alias("OV_SPLST_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OP_HOSP_IN_OUT_NTWK_COPAY_AMT").alias("OP_HOSP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("SKILL_NURSE_COPAY_AMT").alias("SKILL_NURSE_COPAY_AMT"),
    F.col("UC_IN_NTWK_COPAY_AMT").alias("UC_IN_NTWK_COPAY_AMT"),
    F.col("VSN_IN_NTWK_COPAY_AMT").alias("VSN_IN_NTWK_COPAY_AMT"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_NPRFR_SPEC_COPAY_AMT").alias("DRUG_NPRFR_SPEC_COPAY_AMT")
)

df_cpy_MultiStreams_2 = df_ds_ProdCopaySumFExtr.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_2,
    ["SRC_SYS_CD", "PROD_ID", "PROD_CMPNT_EFF_DT_SK"],
    [("SRC_SYS_CD", "A"), ("PROD_ID", "A"), ("PROD_CMPNT_EFF_DT_SK", "A")]
)

df_jn_ProdCopaySumF = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_KProdCopaySumFRead.alias("lnkKProdCopaySumFIn"),
        (
            (F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKProdCopaySumFIn.SRC_SYS_CD"))
            & (F.col("lnkRemDupDataOut.PROD_ID") == F.col("lnkKProdCopaySumFIn.PROD_ID"))
            & (F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK") == F.col("lnkKProdCopaySumFIn.PROD_CMPNT_EFF_DT_SK"))
        ),
        "left"
    )
    .select(
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
        F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        F.col("lnkKProdCopaySumFIn.PROD_COPAY_SUM_SK").alias("PROD_COPAY_SUM_SK"),
        F.col("lnkKProdCopaySumFIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("lnkKProdCopaySumFIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_temp_xfm_PKEYgen = df_jn_ProdCopaySumF.withColumn("svProdCopSumFSK", F.col("PROD_COPAY_SUM_SK"))
df_enriched = SurrogateKeyGen(df_temp_xfm_PKEYgen,<DB sequence name>,"svProdCopSumFSK",<schema>,<secret_name>)

df_lnkInsKProdCopaySumFOut = df_enriched.filter(F.col("PROD_COPAY_SUM_SK").isNull()).select(
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svProdCopSumFSK").alias("PROD_COPAY_SUM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("svProdCopSumFSK").alias("PROD_COPAY_SUM_SK"),
    F.when(
        F.col("PROD_COPAY_SUM_SK").isNull(),
        F.lit(EDWRunCycleDate)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.col("PROD_COPAY_SUM_SK").isNull(),
        F.lit(EDWRunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# Write (insert-only) into #$EDWOwner#.K_PROD_COPAY_SUM_F via merge
df_insert_temp_table_name = "STAGING.IdsEdwProdCopaySumFPky_db2_KProdCopaySumFLoad_temp"
execute_dml(f"DROP TABLE IF EXISTS {df_insert_temp_table_name}", jdbc_url, jdbc_props)

df_lnkInsKProdCopaySumFOut.write.jdbc(
    url=jdbc_url,
    table=df_insert_temp_table_name,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_db2_KProdCopaySumFLoad = f"""
MERGE #$EDWOwner#.K_PROD_COPAY_SUM_F AS T
USING {df_insert_temp_table_name} AS S
ON 
    T.PROD_ID = S.PROD_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PROD_CMPNT_EFF_DT_SK = S.PROD_CMPNT_EFF_DT_SK
WHEN NOT MATCHED THEN
    INSERT (
        PROD_ID,
        SRC_SYS_CD,
        PROD_CMPNT_EFF_DT_SK,
        CRT_RUN_CYC_EXCTN_DT_SK,
        PROD_COPAY_SUM_SK,
        CRT_RUN_CYC_EXCTN_SK
    )
    VALUES (
        S.PROD_ID,
        S.SRC_SYS_CD,
        S.PROD_CMPNT_EFF_DT_SK,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.PROD_COPAY_SUM_SK,
        S.CRT_RUN_CYC_EXCTN_SK
    );
"""
execute_dml(merge_sql_db2_KProdCopaySumFLoad, jdbc_url, jdbc_props)

df_jn_PKEYs = (
    df_cpy_MultiStreams_1.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        (
            (F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"))
            & (F.col("lnkFullDataJnIn.PROD_ID") == F.col("lnkPKEYxfmOut.PROD_ID"))
            & (F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK") == F.col("lnkPKEYxfmOut.PROD_CMPNT_EFF_DT_SK"))
        ),
        "left"
    )
    .select(
        F.col("lnkPKEYxfmOut.PROD_COPAY_SUM_SK").alias("PROD_COPAY_SUM_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
        F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
        F.col("lnkFullDataJnIn.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
        F.col("lnkFullDataJnIn.DRUG_GNRC_COPAY_AMT").alias("DRUG_GNRC_COPAY_AMT"),
        F.col("lnkFullDataJnIn.DRUG_NM_BRND_COPAY_AMT").alias("DRUG_NM_BRND_COPAY_AMT"),
        F.col("lnkFullDataJnIn.DRUG_PRFRD_COPAY_AMT").alias("DRUG_PRFRD_COPAY_AMT"),
        F.col("lnkFullDataJnIn.DRUG_NPRFR_COPAY_AMT").alias("DRUG_NPRFR_COPAY_AMT"),
        F.col("lnkFullDataJnIn.ELTRNC_PHYS_VST_COPAY_AMT").alias("ELTRNC_PHYS_VST_COPAY_AMT"),
        F.col("lnkFullDataJnIn.ER_IN_NTWK_COPAY_AMT").alias("ER_IN_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.ER_OUT_NTWK_COPAY_AMT").alias("ER_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.HOME_HLTH_VST_COPAY_AMT").alias("HOME_HLTH_VST_COPAY_AMT"),
        F.col("lnkFullDataJnIn.IN_HOSP_IN_OUT_NTWK_COPAY_AMT").alias("IN_HOSP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.IP_HSPC_COPAY_AMT").alias("IP_HSPC_COPAY_AMT"),
        F.col("lnkFullDataJnIn.MNTL_HLTH_IP_IN_NTWK_COPAY_AMT").alias("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.MNTL_HLTH_OP_IN_NTWK_COPAY_AMT").alias("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.MRI_CT_PET_MRA_COPAY_AMT").alias("MRI_CT_PET_MRA_COPAY_AMT"),
        F.col("lnkFullDataJnIn.OV_IN_OUT_NTWK_COPAY_AMT").alias("OV_IN_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.OV_PCP_IN_OUT_NTWK_COPAY_AMT").alias("OV_PCP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.OV_SPLST_IN_OUT_NTWK_COPAY_AMT").alias("OV_SPLST_IN_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.OP_HOSP_IN_OUT_NTWK_COPAY_AMT").alias("OP_HOSP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.SKILL_NURSE_COPAY_AMT").alias("SKILL_NURSE_COPAY_AMT"),
        F.col("lnkFullDataJnIn.UC_IN_NTWK_COPAY_AMT").alias("UC_IN_NTWK_COPAY_AMT"),
        F.col("lnkFullDataJnIn.VSN_IN_NTWK_COPAY_AMT").alias("VSN_IN_NTWK_COPAY_AMT"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnkFullDataJnIn.DRUG_NPRFR_SPEC_COPAY_AMT").alias("DRUG_NPRFR_SPEC_COPAY_AMT")
    )
)

df_seq_ProdCopaySumFPKey = df_jn_PKEYs.select(
    F.col("PROD_COPAY_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROD_ID"),
    F.rpad(F.col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("DRUG_GNRC_COPAY_AMT"),
    F.col("DRUG_NM_BRND_COPAY_AMT"),
    F.col("DRUG_PRFRD_COPAY_AMT"),
    F.col("DRUG_NPRFR_COPAY_AMT"),
    F.col("ELTRNC_PHYS_VST_COPAY_AMT"),
    F.col("ER_IN_NTWK_COPAY_AMT"),
    F.col("ER_OUT_NTWK_COPAY_AMT"),
    F.col("HOME_HLTH_VST_COPAY_AMT"),
    F.col("IN_HOSP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("IP_HSPC_COPAY_AMT"),
    F.col("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT"),
    F.col("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT"),
    F.col("MRI_CT_PET_MRA_COPAY_AMT"),
    F.col("OV_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OV_PCP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OV_SPLST_IN_OUT_NTWK_COPAY_AMT"),
    F.col("OP_HOSP_IN_OUT_NTWK_COPAY_AMT"),
    F.col("SKILL_NURSE_COPAY_AMT"),
    F.col("UC_IN_NTWK_COPAY_AMT"),
    F.col("VSN_IN_NTWK_COPAY_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_NPRFR_SPEC_COPAY_AMT")
)

write_files(
    df_seq_ProdCopaySumFPKey,
    f"{adls_path}/load/PROD_COPAY_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)