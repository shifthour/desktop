# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSAClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      BCBSA Med Claim pre-processing. Builds Claim Id, Matches the members from the input file to the warehouse,
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                 Project/Altiris #          Change Description                                                       Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     ------------------------      -----------------------------------------------------------------------                     --------------------------------           -------------------------------   ----------------------------      
# MAGIC Karthik Chintalapani                    2016-02-21     5212                          Original Programming                                                              IntegrateDev2                 Kalyan Neelam          2016-05-11  
# MAGIC                                                   
# MAGIC Karthik Chintalapani                    2016-08-12     5212                Changed the length of the last column MM_ID from                         IntegrateDev1                 Kalyan Neelam          2016-08-15                                          
# MAGIC                                                                                                    Char(23) TO Char(22) and changed the logic to build 
# MAGIC                                                                                                    the Claim Id's based on the new mapping changes.

# MAGIC Append header record to audit file.
# MAGIC Total claim paid amounts and write to unix Drug_Header_Record.dat file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','100')
InFile = get_widget_value('InFile','BCBSA_BHI.PI.240_RX.201601.dat')
CurrDate = get_widget_value('CurrDate','2016-05-11')

# Read from hf_etrnl_mbr_sk (Scenario C -> read parquet)
df_hf_etrnl_mbr_sk = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_sk.parquet")

# Read from BCBSA_PDX_DATA (delimited file, define schema)
schema_BCBSA_PDX_DATA = StructType([
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True)
])
df_BCBSA_PDX_DATA = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCBSA_PDX_DATA)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Read from P_MBR_BCBSA_SUPLMT (JDBC with query)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
P_MBR_BCBSA_SUPLMT.BCBSA_HOME_PLN_CONSIS_MBR_ID,
P_MBR_BCBSA_SUPLMT.MBR_UNIQ_KEY,
P_MBR_BCBSA_SUPLMT.MBR_SK
FROM {IDSOwner}.P_MBR_BCBSA_SUPLMT P_MBR_BCBSA_SUPLMT,
     {IDSOwner}.MBR MBR
WHERE P_MBR_BCBSA_SUPLMT.MBR_SK = MBR.MBR_SK
AND P_MBR_BCBSA_SUPLMT.MBR_UNIQ_KEY <>0
"""
df_P_MBR_BCBSA_SUPLMT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer_399
df_Transformer_399 = df_P_MBR_BCBSA_SUPLMT.select(
    trim(F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID")).alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SK").alias("MBR_SK")
)

# hf_bcbsa_rxclm_preproc_mbr_lkup (Scenario A: deduplicate on primary key = BCBSA_HOME_PLN_CONSIS_MBR_ID)
df_hf_bcbsa_rxclm_preproc_mbr_lkup = dedup_sort(
    df_Transformer_399,
    ["BCBSA_HOME_PLN_CONSIS_MBR_ID"],
    []
)

# Mbr_Lookup
#   Primary link: BCBSA_PDX_DATA
#   Lookup link: df_hf_bcbsa_rxclm_preproc_mbr_lkup (left join on Trim(df_BCBSA_PDX_DATA.CONSIS_MBR_ID) == BCBSA_HOME_PLN_CONSIS_MBR_ID)
df_Mbr_Lookup_join = (
    df_BCBSA_PDX_DATA.alias("Extract")
    .join(
        df_hf_bcbsa_rxclm_preproc_mbr_lkup.alias("MbrLkup"),
        trim(F.col("Extract.CONSIS_MBR_ID")) == F.col("MbrLkup.BCBSA_HOME_PLN_CONSIS_MBR_ID"),
        "left"
    )
)

# Output link (Lnk_Xfm) => keep records where MbrLkup.MBR_UNIQ_KEY is not null
df_Mbr_Lookup_Lnk_Xfm_pre = df_Mbr_Lookup_join.filter(F.col("MbrLkup.MBR_UNIQ_KEY").isNotNull())
df_Mbr_Lookup_Lnk_Xfm = df_Mbr_Lookup_Lnk_Xfm_pre.select(
    trim(F.col("Extract.NDW_HOME_PLN_ID")).alias("NDW_HOME_PLN_ID"),
    trim(F.col("Extract.CLM_ID")).alias("CLM_ID"),
    trim(F.col("Extract.CLM_LN_NO")).alias("CLM_LN_NO"),
    trim(F.col("Extract.TRACEABILITY_FLD")).alias("TRACEABILITY_FLD"),
    trim(F.col("Extract.ADJ_SEQ_NO")).alias("ADJ_SEQ_NO"),
    trim(F.col("Extract.HOST_PLN_ID")).alias("HOST_PLN_ID"),
    trim(F.col("Extract.HOME_PLAN_PROD_ID_CD")).alias("HOME_PLAN_PROD_ID_CD"),
    trim(F.col("Extract.MBR_ID")).alias("MBR_ID"),
    trim(F.col("Extract.MBR_ZIP_CD_ON_CLM")).alias("MBR_ZIP_CD_ON_CLM"),
    trim(F.col("Extract.MBR_CTRY_ON_CLM")).alias("MBR_CTRY_ON_CLM"),
    trim(F.col("Extract.NPI_REND_PROV_ID")).alias("NPI_REND_PROV_ID"),
    trim(F.col("Extract.PRSCRB_PROV_ID")).alias("PRSCRB_PROV_ID"),
    trim(F.col("Extract.NPI_PRSCRB_PROV_ID")).alias("NPI_PRSCRB_PROV_ID"),
    trim(F.col("Extract.BNF_PAYMT_STTUS_CD")).alias("BNF_PAYMT_STTUS_CD"),
    trim(F.col("Extract.PDX_CARVE_OUT_SUBMSN_IN")).alias("PDX_CARVE_OUT_SUBMSN_IN"),
    trim(F.col("Extract.CAT_OF_SVC")).alias("CAT_OF_SVC"),
    trim(F.col("Extract.CLM_PAYMT_STTUS")).alias("CLM_PAYMT_STTUS"),
    trim(F.col("Extract.DAW_CD")).alias("DAW_CD"),
    trim(F.col("Extract.DAYS_SUPL")).alias("DAYS_SUPL"),
    trim(F.col("Extract.FRMLRY_IN")).alias("FRMLRY_IN"),
    trim(F.col("Extract.PLN_SPEC_DRUG_IN")).alias("PLN_SPEC_DRUG_IN"),
    trim(F.col("Extract.PROD_SVC_ID")).alias("PROD_SVC_ID"),
    trim(F.col("Extract.QTY_DISPNS")).alias("QTY_DISPNS"),
    trim(F.col("Extract.CLM_PD_DT")).alias("CLM_PD_DT"),
    trim(F.col("Extract.DT_OF_SVC")).alias("DT_OF_SVC"),
    F.col("Extract.AVG_WHLSL_PRICE_SUBMT_AMT").alias("AVG_WHLSL_PRICE_SUBMT_AMT"),
    trim(F.col("Extract.CONSIS_MBR_ID")).alias("CONSIS_MBR_ID"),
    trim(F.col("Extract.MMI_ID")).alias("MMI_ID"),
    F.col("MbrLkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MbrLkup.MBR_SK").alias("MBR_SK")
)

# Reject link => records where MbrLkup.MBR_UNIQ_KEY is null
df_Mbr_Lookup_Reject_pre = df_Mbr_Lookup_join.filter(F.col("MbrLkup.MBR_UNIQ_KEY").isNull())
df_Mbr_Lookup_Reject = df_Mbr_Lookup_Reject_pre.select(
    F.col("Extract.NDW_HOME_PLN_ID").alias("NDW_HOME_PLN_ID"),
    F.col("Extract.CLM_ID").alias("CLM_ID"),
    F.col("Extract.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Extract.TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    F.col("Extract.ADJ_SEQ_NO").alias("ADJ_SEQ_NO"),
    F.col("Extract.HOST_PLN_ID").alias("HOST_PLN_ID"),
    F.col("Extract.MBR_ID").alias("MBR_ID"),
    F.col("Extract.MBR_ZIP_CD_ON_CLM").alias("MBR_ZIP_CD_ON_CLM"),
    F.col("Extract.MBR_CTRY_ON_CLM").alias("MBR_CTRY_ON_CLM"),
    F.col("Extract.NPI_REND_PROV_ID").alias("NPI_REND_PROV_ID"),
    F.col("Extract.PRSCRB_PROV_ID").alias("PRSCRB_PROV_ID"),
    F.col("Extract.NPI_PRSCRB_PROV_ID").alias("NPI_PRSCRB_PROV_ID"),
    F.col("Extract.BNF_PAYMT_STTUS_CD").alias("BNF_PAYMT_STTUS_CD"),
    F.col("Extract.PDX_CARVE_OUT_SUBMSN_IN").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    F.col("Extract.CAT_OF_SVC").alias("CAT_OF_SVC"),
    F.col("Extract.CLM_PAYMT_STTUS").alias("CLM_PAYMT_STTUS"),
    F.col("Extract.DAW_CD").alias("DAW_CD"),
    F.col("Extract.DAYS_SUPL").alias("DAYS_SUPL"),
    F.col("Extract.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Extract.PLN_SPEC_DRUG_IN").alias("PLN_SPEC_DRUG_IN"),
    F.col("Extract.PROD_SVC_ID").alias("PROD_SVC_ID"),
    F.col("Extract.QTY_DISPNS").alias("QTY_DISPNS"),
    F.col("Extract.CLM_PD_DT").alias("CLM_PD_DT"),
    F.col("Extract.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("Extract.AVG_WHLSL_PRICE_SUBMT_AMT").alias("AVG_WHLSL_PRICE_SUBMT_AMT"),
    F.col("Extract.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Extract.MMI_ID").alias("MMI_ID")
)

# ErrorFile (writes df_Mbr_Lookup_Reject)
write_files(
    df_Mbr_Lookup_Reject,
    f"{adls_path_publish}/external/BCBSA_RxCLM_MbrMatchRejects.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BuildClmId:
#   Primary link: df_Mbr_Lookup_Lnk_Xfm
#   Lookup link: df_hf_etrnl_mbr_sk (left join on MBR_SK)
df_BuildClmId_prejoin = df_Mbr_Lookup_Lnk_Xfm.alias("Lnk_Xfm").join(
    df_hf_etrnl_mbr_sk.alias("mbr_sk_lkup"),
    F.col("Lnk_Xfm.MBR_SK") == F.col("mbr_sk_lkup.MBR_SK"),
    "left"
)

# StageVariables logic in PySpark columns
svClmId_expr = F.when(
    (F.trim(F.col("Lnk_Xfm.CLM_ID")).substr(1, 1) == F.lit("9")) &
    (F.length(F.trim(F.col("Lnk_Xfm.CLM_ID"))) > 20),
    F.concat(
        F.trim(F.col("Lnk_Xfm.CLM_ID")).substr(3, 18),
        F.trim(F.col("Lnk_Xfm.CLM_LN_NO")).substr(2, 2)
    )
).otherwise(
    F.when(
        F.length(F.trim(F.col("Lnk_Xfm.CLM_ID"))) > 18,
        F.concat(
            F.trim(F.col("Lnk_Xfm.CLM_ID")).substr(-18, 18),
            F.trim(F.col("Lnk_Xfm.CLM_LN_NO")).substr(2, 2)
        )
    ).otherwise(
        F.concat(
            F.trim(F.col("Lnk_Xfm.CLM_ID")),
            F.trim(F.col("Lnk_Xfm.CLM_LN_NO")).substr(2, 2)
        )
    )
)

svMbrUniqKey_expr = F.when(
    F.col("mbr_sk_lkup.MBR_UNIQ_KEY").isNull(),
    F.lit(0)
).otherwise(
    F.col("mbr_sk_lkup.MBR_UNIQ_KEY")
)

df_BuildClmId = df_BuildClmId_prejoin \
    .withColumn("svClmId", svClmId_expr) \
    .withColumn("svMbrUniqKey", svMbrUniqKey_expr) \
    .withColumn("SvCurrKey", F.lit(None)) \
    .withColumn("SvCounter", F.lit(None)) \
    .withColumn("SvPrevKey", F.lit(None))

# Output link: Landing => BCBSADrugClm_Land
df_BuildClmId_Landing = df_BuildClmId.select(
    F.col("svClmId").alias("CLM_ID"),
    F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_Xfm.NDW_HOME_PLN_ID").alias("NDW_HOME_PLN_ID"),
    F.col("Lnk_Xfm.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Lnk_Xfm.TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    F.col("Lnk_Xfm.ADJ_SEQ_NO").alias("ADJ_SEQ_NO"),
    F.col("Lnk_Xfm.HOST_PLN_ID").alias("HOST_PLN_ID"),
    F.col("Lnk_Xfm.HOME_PLAN_PROD_ID_CD").alias("HOME_PLAN_PROD_ID_CD"),
    F.col("Lnk_Xfm.MBR_ID").alias("MBR_ID"),
    F.col("Lnk_Xfm.MBR_ZIP_CD_ON_CLM").alias("MBR_ZIP_CD_ON_CLM"),
    F.col("Lnk_Xfm.MBR_CTRY_ON_CLM").alias("MBR_CTRY_ON_CLM"),
    F.col("Lnk_Xfm.NPI_REND_PROV_ID").alias("NPI_REND_PROV_ID"),
    F.col("Lnk_Xfm.PRSCRB_PROV_ID").alias("PRSCRB_PROV_ID"),
    F.col("Lnk_Xfm.NPI_PRSCRB_PROV_ID").alias("NPI_PRSCRB_PROV_ID"),
    F.col("Lnk_Xfm.BNF_PAYMT_STTUS_CD").alias("BNF_PAYMT_STTUS_CD"),
    F.col("Lnk_Xfm.PDX_CARVE_OUT_SUBMSN_IN").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    F.col("Lnk_Xfm.CAT_OF_SVC").alias("CAT_OF_SVC"),
    F.col("Lnk_Xfm.CLM_PAYMT_STTUS").alias("CLM_PAYMT_STTUS"),
    F.col("Lnk_Xfm.DAW_CD").alias("DAW_CD"),
    F.col("Lnk_Xfm.DAYS_SUPL").alias("DAYS_SUPL"),
    F.col("Lnk_Xfm.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Lnk_Xfm.PLN_SPEC_DRUG_IN").alias("PLN_SPEC_DRUG_IN"),
    F.col("Lnk_Xfm.PROD_SVC_ID").alias("PROD_SVC_ID"),
    F.col("Lnk_Xfm.QTY_DISPNS").alias("QTY_DISPNS"),
    F.concat_ws("-", F.col("Lnk_Xfm.CLM_PD_DT").substr(1,4), F.col("Lnk_Xfm.CLM_PD_DT").substr(5,2), F.col("Lnk_Xfm.CLM_PD_DT").substr(7,2)).alias("CLM_PD_DT"),
    F.concat_ws("-", F.col("Lnk_Xfm.DT_OF_SVC").substr(1,4), F.col("Lnk_Xfm.DT_OF_SVC").substr(5,2), F.col("Lnk_Xfm.DT_OF_SVC").substr(7,2)).alias("DT_OF_SVC"),
    F.regexp_replace(F.col("Lnk_Xfm.AVG_WHLSL_PRICE_SUBMT_AMT"), "^[+]+", "").alias("AVG_WHLSL_PRICE_SUBMT_AMT"),
    F.col("Lnk_Xfm.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Lnk_Xfm.MMI_ID").alias("MMI_ID"),
    F.when(F.col("mbr_sk_lkup.SUBGRP_SK").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.SUBGRP_SK")).alias("SUBGRP_SK"),
    F.when(F.col("mbr_sk_lkup.SUBGRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("mbr_sk_lkup.SUBGRP_ID")).alias("SUBGRP_ID"),
    F.when(F.col("mbr_sk_lkup.SUB_SK").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.SUB_SK")).alias("SUB_SK"),
    F.when(F.col("mbr_sk_lkup.SUB_ID").isNull(), F.lit("UNK")).otherwise(F.col("mbr_sk_lkup.SUB_ID")).alias("SUB_ID"),
    F.when(F.col("mbr_sk_lkup.MBR_SFX_NO").isNull(), F.lit("00")).otherwise(F.col("mbr_sk_lkup.MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.when(F.col("mbr_sk_lkup.MBR_GNDR_CD_SK").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.MBR_GNDR_CD_SK")).alias("MBR_GNDR_CD_SK"),
    F.when(F.col("mbr_sk_lkup.MBR_GNDR_CD").isNull(), F.lit("UNK")).otherwise(F.col("mbr_sk_lkup.MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    F.when(F.col("mbr_sk_lkup.BRTH_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("mbr_sk_lkup.BRTH_DT_SK")).alias("BRTH_DT_SK"),
    F.when(F.col("mbr_sk_lkup.SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.SUB_UNIQ_KEY")).alias("SUB_UNIQ_KEY"),
    F.when(F.col("mbr_sk_lkup.GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("mbr_sk_lkup.GRP_ID")).alias("GRP_ID")
)

# Output link: link1 => W_DRUG_ENR
df_BuildClmId_link1 = df_BuildClmId.select(
    F.col("svClmId").alias("CLM_ID"),
    F.concat_ws("-", F.col("Lnk_Xfm.DT_OF_SVC").substr(1,4), F.col("Lnk_Xfm.DT_OF_SVC").substr(5,2), F.col("Lnk_Xfm.DT_OF_SVC").substr(7,2)).alias("DT_FILLED"),
    F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY")
)

# BCBSADrugClm_Land => write to "verified" folder with .dat.#RunID#
# Apply rpad for any char/varchar with explicit length from the job metadata
df_BCBSADrugClm_Land_padded = df_BuildClmId_Landing \
    .withColumn("NDW_HOME_PLN_ID", F.rpad("NDW_HOME_PLN_ID", 3, " ")) \
    .withColumn("CLM_LN_NO", F.rpad("CLM_LN_NO", 3, " ")) \
    .withColumn("TRACEABILITY_FLD", F.rpad("TRACEABILITY_FLD", 5, " ")) \
    .withColumn("ADJ_SEQ_NO", F.rpad("ADJ_SEQ_NO", 8, " ")) \
    .withColumn("HOST_PLN_ID", F.rpad("HOST_PLN_ID", 3, " ")) \
    .withColumn("HOME_PLAN_PROD_ID_CD", F.rpad("HOME_PLAN_PROD_ID_CD", 15, " ")) \
    .withColumn("MBR_ID", F.rpad("MBR_ID", 22, " ")) \
    .withColumn("MBR_ZIP_CD_ON_CLM", F.rpad("MBR_ZIP_CD_ON_CLM", 5, " ")) \
    .withColumn("MBR_CTRY_ON_CLM", F.rpad("MBR_CTRY_ON_CLM", 2, " ")) \
    .withColumn("NPI_REND_PROV_ID", F.rpad("NPI_REND_PROV_ID", 10, " ")) \
    .withColumn("PRSCRB_PROV_ID", F.rpad("PRSCRB_PROV_ID", 27, " ")) \
    .withColumn("NPI_PRSCRB_PROV_ID", F.rpad("NPI_PRSCRB_PROV_ID", 10, " ")) \
    .withColumn("BNF_PAYMT_STTUS_CD", F.rpad("BNF_PAYMT_STTUS_CD", 1, " ")) \
    .withColumn("PDX_CARVE_OUT_SUBMSN_IN", F.rpad("PDX_CARVE_OUT_SUBMSN_IN", 1, " ")) \
    .withColumn("CAT_OF_SVC", F.rpad("CAT_OF_SVC", 14, " ")) \
    .withColumn("CLM_PAYMT_STTUS", F.rpad("CLM_PAYMT_STTUS", 1, " ")) \
    .withColumn("DAW_CD", F.rpad("DAW_CD", 2, " ")) \
    .withColumn("DAYS_SUPL", F.rpad("DAYS_SUPL", 3, " ")) \
    .withColumn("FRMLRY_IN", F.rpad("FRMLRY_IN", 1, " ")) \
    .withColumn("PLN_SPEC_DRUG_IN", F.rpad("PLN_SPEC_DRUG_IN", 1, " ")) \
    .withColumn("PROD_SVC_ID", F.rpad("PROD_SVC_ID", 11, " ")) \
    .withColumn("QTY_DISPNS", F.rpad("QTY_DISPNS", 13, " ")) \
    .withColumn("CLM_PD_DT", F.rpad("CLM_PD_DT", 10, " ")) \
    .withColumn("DT_OF_SVC", F.rpad("DT_OF_SVC", 10, " ")) \
    .withColumn("MBR_SFX_NO", F.rpad("MBR_SFX_NO", 2, " ")) \
    .withColumn("BRTH_DT_SK", F.rpad("BRTH_DT_SK", 10, " "))

write_files(
    df_BCBSADrugClm_Land_padded,
    f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# W_DRUG_ENR => writes to "load/W_DRUG_ENR.dat"
df_W_DRUG_ENR_padded = df_BuildClmId_link1 \
    .withColumn("CLM_ID", F.col("CLM_ID")) \
    .withColumn("DT_FILLED", F.col("DT_FILLED")) \
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
write_files(
    df_W_DRUG_ENR_padded,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BCBSA_PDX_DATA_1 (similar read as BCBSA_PDX_DATA)
schema_BCBSA_PDX_DATA_1 = StructType([
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True)
])
df_BCBSA_PDX_DATA_1 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCBSA_PDX_DATA_1)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# get_detials transformer => output link "details" with constraint @INROWNUM > 1
#   PROCESS_DATE=FORMAT.DATE(CurrDate, 'DATE', 'DATE', 'CCYYMMDD')
#   INROW_NBR=@INROWNUM
#   AVG_WHLSL_PRICE_SUBMT_AMT=Extract.AVG_WHLSL_PRICE_SUBMT_AMT
# We'll simulate row number by using a monotonically_increasing_id + 1; then filter > 1
df_get_details_pre = df_BCBSA_PDX_DATA_1.withColumn("_ROWNUM", F.monotonically_increasing_id() + F.lit(1))
df_get_details_filtered = df_get_details_pre.filter(F.col("_ROWNUM") > 1)
df_get_details = df_get_details_filtered.select(
    F.date_format(F.lit(CurrDate), "yyyyMMdd").alias("PROCESS_DATE"),
    F.col("_ROWNUM").alias("INROW_NBR"),
    F.col("AVG_WHLSL_PRICE_SUBMT_AMT").alias("AVG_WHLSL_PRICE_SUBMT_AMT")
)

# hf_bcbsa_drugclmpreproc_land_details (Scenario A => deduplicate on [PROCESS_DATE, INROW_NBR, AVG_WHLSL_PRICE_SUBMT_AMT])
df_hf_bcbsa_land_details_dedup = dedup_sort(
    df_get_details,
    ["PROCESS_DATE","INROW_NBR","AVG_WHLSL_PRICE_SUBMT_AMT"],
    []
)

# Aggregator => group by PROCESS_DATE => sum(AVG_WHLSL_PRICE_SUBMT_AMT) as TOTAL_AVG_WHLSL_PRICE_SUBMT_AMT, max(INROW_NBR) as TOTAL_REC_NBR
df_Aggregator = (
    df_hf_bcbsa_land_details_dedup
    .groupBy("PROCESS_DATE")
    .agg(
        F.sum(F.col("AVG_WHLSL_PRICE_SUBMT_AMT").cast(DecimalType(38,10))).alias("TOTAL_AVG_WHLSL_PRICE_SUBMT_AMT"),
        F.max(F.col("INROW_NBR")).alias("TOTAL_REC_NBR")
    )
)

# hf_bcbsa_drugclmpreproc_land_drvr_audit_sum (Scenario A => deduplicate on [PROCESS_DATE])
df_hf_bcbsa_drugclmpreproc_land_drvr_audit_sum = dedup_sort(
    df_Aggregator,
    ["PROCESS_DATE"],
    []
)

# format_to_output => output columns:
#   Run_Date = CurrDate
#   Filler1 = Space(2)
#   Frequency = "MONTHLY"
#   Filler2 = "*  "
#   Program = "BCA "
#   File_date = total_pd.PROCESS_DATE
#   Row_Count = total_pd.TOTAL_REC_NBR -1
#   Pd_amount = total_pd.TOTAL_AVG_WHLSL_PRICE_SUBMT_AMT
df_format_to_output = df_hf_bcbsa_drugclmpreproc_land_drvr_audit_sum.select(
    F.lit(CurrDate).alias("Run_Date"),
    F.lpad(F.lit(""), 2, " ").alias("Filler1"),
    F.lit("MONTHLY").alias("Frequency"),
    F.lit("*  ").alias("Filler2"),
    F.lit("BCA ").alias("Program"),
    F.col("PROCESS_DATE").alias("File_date"),
    (F.col("TOTAL_REC_NBR") - F.lit(1)).alias("Row_Count"),
    F.col("TOTAL_AVG_WHLSL_PRICE_SUBMT_AMT").alias("Pd_amount")
)

# Drug_Header_Record => append to landing/Drug_Header_Record.dat
# Apply rpad to columns that are char with lengths from stage metadata:
df_Drug_Header_Record_padded = df_format_to_output \
    .withColumn("Run_Date", F.rpad("Run_Date", 10, " ")) \
    .withColumn("Filler1", F.rpad("Filler1", 2, " ")) \
    .withColumn("Frequency", F.rpad("Frequency", 9, " ")) \
    .withColumn("Filler2", F.rpad("Filler2", 3, " ")) \
    .withColumn("Program", F.rpad("Program", 9, " ")) \
    .withColumn("File_date", F.rpad("File_date", 9, " ")) \
    .withColumn("Row_Count", F.rpad(F.col("Row_Count").cast(StringType()), 8, " ")) \
    .withColumn("Pd_amount", F.rpad(F.col("Pd_amount").cast(StringType()), 12, " "))

write_files(
    df_Drug_Header_Record_padded,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AfterJobRoutine placeholder if any (the JSON indicates "AfterJobRoutine": "1", but no details):
# Example of calling DB2StatsSubroutine or Move_File if needed; real parameters are unknown, so put <...> if not default:
# (No explicit instruction given other than we have AfterJobRoutine=1)
params = {
  "EnvProjectPath": f"dap/<...>/<...>",
  "File_Path": "<...>",
  "File_Name": "<...>"
}
dbutils.notebook.run("../../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)