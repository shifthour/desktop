# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY : BCADrugExtrSeq
# MAGIC 
# MAGIC PROCESSING : Reads the BCA Drug Land file and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC 
# MAGIC Developer                    Date         Project #                     Change Description                                        Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ------------------      ----------------                  --------------------------------------------------------------         ------------------------------------       ----------------------------            ----------------
# MAGIC Kalyan Neelam        2013-10-29   5056 FEP Claims         Initial Programming                                          IntegrateNewDevl               Bhoomi Dasari                11/30/2013
# MAGIC Dan Long                2014-04-18   5082                           Changed lenghth of CLM_ID go into
# MAGIC                                                                                        contaner ClmProvPK from 18 to 20 and           IntegrateNewDevl                Kalyan Neelam               2014-04-21
# MAGIC                                                                                        type to VAR, Changed CLM_PROV_ROLE_TYPE_CD
# MAGIC                                                                                        length to 20.
# MAGIC                                                                                        Changed PROV_ID type to VARCHAR. 
# MAGIC Abhiram Dasarathy	2017-02-27   5568 - HEDIS	       Changed the transformation rule for 		IntegrateDev2	           Kalyan Neelam	  2017-02-27
# MAGIC 					       DISPNS_PROV_ID
# MAGIC Sudhir Bomshetty    2017-07-20    5781 - HEDIS          The FEP Pharmacy file is populated with            IntegrateDev1                   Kalyan Neelam                2017-07-26
# MAGIC                                                                                      prescriber provider ID. To accommodate new
# MAGIC                                                                                      dataset PROV_ID field is updated
# MAGIC 
# MAGIC Madhavan B	2017-06-20   5788 - BHI Updates  Added new column                                           IntegrateDev2                    Kalyan Neelam                2017-07-07
# MAGIC                                                                                       SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC 
# MAGIC Sudhir Bomshetty     2018-03-16  5781 HEDIS           Updated busniess logic for PROV_ID, TAX_ID   IntegrateDev2                    Kalyan Neelam                2018-03-20    
# MAGIC 
# MAGIC Sudhir Bomshetty     2018-03-16  5781 HEDIS           Added last 4 columns in 'BCAClmLand' stage      IntegrateDev2                    Jaideep Mankala             04/04/2018
# MAGIC 
# MAGIC Sudhir Bomshetty     2018-04-13    5781 - HEDIS            Added the NTNL_PROV_ID                           IntegrateDev1                   Kalyan Neelam                2018-04-23
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                                            IntegrateDev1	     Kalyan Neelam     2019-09-05          
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID    
# MAGIC                                                                                  for implementing reversals logic

# MAGIC BCA Claim Provider Extract
# MAGIC The file is created in the BCADrugClmPreProcExtr job
# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSSCClmProvExtr
# MAGIC BCBSSCMedClmProvExtr
# MAGIC BCAClmProvExtr
# MAGIC BCAFEPClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# --------------------------------------------------------------------------------
# STAGE: PROV_DEA (DB2Connector, database=IDS)
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_PROV_DEA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT PROV.PROV_ID, PROV.TAX_ID FROM {IDSOwner}.PROV PROV"
    )
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_bca_clmprov_taxid (CHashedFileStage, scenario A dedup by primary key PROV_ID)
# --------------------------------------------------------------------------------
df_hf_bca_clmprov_taxid = dedup_sort(
    df_PROV_DEA,
    partition_cols=["PROV_ID"],
    sort_cols=[("PROV_ID","A")]
).select("PROV_ID","TAX_ID")
df_taxid_lkup = df_hf_bca_clmprov_taxid

# --------------------------------------------------------------------------------
# STAGE: BCAClmLand (CSeqFileStage) - read from .dat file with explicit schema
# --------------------------------------------------------------------------------
schema_BCAClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(38,10), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(38,10), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(38,10), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("LGCY_SRC_CD", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRSCRB_NTWK_CD", StringType(), True),
    StructField("PRSCRB_PROV_PLN_CD", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("BRND_NM", StringType(), True),
    StructField("LABEL_NM", StringType(), True),
    StructField("THRPTC_CAT_DESC", StringType(), True),
    StructField("GNRC_NM_DRUG_IN", StringType(), True),
    StructField("METH_DRUG_ADM", StringType(), True),
    StructField("RX_CST_EQVLNT", DecimalType(38,10), True),
    StructField("METRIC_UNIT", DecimalType(38,10), True),
    StructField("NON_METRIC_UNIT", DecimalType(38,10), True),
    StructField("DAYS_SUPPLIED", DecimalType(38,10), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

df_BCAClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCAClmLand)
    .load(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

# --------------------------------------------------------------------------------
# STAGE: Transformer_34
# --------------------------------------------------------------------------------
df_Transformer_34 = df_BCAClmLand.withColumn(
    "svDispnsProvId",
    F.instr(F.col("PDX_NTNL_PROV_ID"), 'NBP')
)

df_Transformer_34_BcaData = (
    df_Transformer_34.select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
        F.col("ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
        F.col("PDX_SVC_ID").alias("PDX_SVC_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("RX_FILLED_DT").alias("RX_FILLED_DT"),
        F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
        F.col("FEP_CNTR_ID").alias("FEP_CNTR_ID"),
        F.col("LGCY_MBR_ID").alias("LGCY_MBR_ID"),
        F.col("DOB").alias("DOB"),
        F.col("PATN_AGE").alias("PATN_AGE"),
        F.col("GNDR_CD").alias("GNDR_CD"),
        F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("LGCY_SRC_CD").alias("LGCY_SRC_CD"),
        F.col("PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_ID"),
        F.col("PRSCRB_NTWK_CD").alias("PRSCRB_NTWK_CD"),
        F.col("PRSCRB_PROV_PLN_CD").alias("PRSCRB_PROV_PLN_CD"),
        F.col("NDC").alias("NDC"),
        F.col("BRND_NM").alias("BRND_NM"),
        F.col("LABEL_NM").alias("LABEL_NM"),
        F.col("THRPTC_CAT_DESC").alias("THRPTC_CAT_DESC"),
        F.col("GNRC_NM_DRUG_IN").alias("GNRC_NM_DRUG_IN"),
        F.col("METH_DRUG_ADM").alias("METH_DRUG_ADM"),
        F.col("RX_CST_EQVLNT").alias("RX_CST_EQVLNT"),
        F.col("METRIC_UNIT").alias("METRIC_UNIT"),
        F.col("NON_METRIC_UNIT").alias("NON_METRIC_UNIT"),
        F.col("DAYS_SUPPLIED").alias("DAYS_SUPPLIED"),
        F.col("CLM_PD_DT").alias("CLM_PD_DT"),
        F.col("CLM_LOAD_DT").alias("CLM_LOAD_DT"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# --------------------------------------------------------------------------------
# STAGE: IDS (DB2Connector) reading 6 queries
# --------------------------------------------------------------------------------
df_PROV_DEA_PCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV_DEA.DEA_NO,
    MIN(PROV.PROV_ID)
FROM {IDSOwner}.PROV_DEA PROV_DEA, {IDSOwner}.PROV PROV
WHERE
    PROV_DEA.CMN_PRCT_SK = PROV.CMN_PRCT_SK
    AND PROV.PROV_ID <> 'UNK'
    AND PROV.CMN_PRCT_SK  NOT IN (1,0)
    AND PROV_DEA.CMN_PRCT_SK NOT IN (1,0)
GROUP BY PROV_DEA.DEA_NO
"""
    )
    .load()
)
df_PROV_DEA_PCT = df_PROV_DEA_PCT.select(
    F.col("DEA_NO").alias("DEA_NO"),
    F.col("MIN(PROV.PROV_ID)").alias("PROV_ID")
)

df_PROV_DEA_NPID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV_DEA.NTNL_PROV_ID,
    MIN(PROV.PROV_ID)
FROM {IDSOwner}.PROV_DEA PROV_DEA, {IDSOwner}.PROV PROV
WHERE
    PROV_DEA.CMN_PRCT_SK = PROV.CMN_PRCT_SK
    AND PROV.PROV_ID <> 'UNK'
    AND PROV.CMN_PRCT_SK  NOT IN (1,0)
    AND PROV_DEA.CMN_PRCT_SK NOT IN (1,0)
GROUP BY PROV_DEA.NTNL_PROV_ID
"""
    )
    .load()
)
df_PROV_DEA_NPID = df_PROV_DEA_NPID.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("MIN(PROV.PROV_ID)").alias("PROV_ID")
)

df_PROV_DEA_NT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV_DEA.DEA_NO,
    MIN(PROV.PROV_ID)
FROM {IDSOwner}.PROV_DEA PROV_DEA, {IDSOwner}.PROV PROV
WHERE
    PROV_DEA.NTNL_PROV_ID = PROV.NTNL_PROV_ID
    AND PROV.PROV_ID <> 'UNK'
    AND PROV_DEA.NTNL_PROV_ID NOT IN ('NA', 'UNK')
    AND PROV.NTNL_PROV_ID > '-1'
GROUP BY PROV_DEA.DEA_NO
"""
    )
    .load()
)
df_PROV_DEA_NT = df_PROV_DEA_NT.select(
    F.col("DEA_NO").alias("DEA_NO"),
    F.col("MIN(PROV.PROV_ID)").alias("PROV_ID")
)

df_PROV_NPID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV.NTNL_PROV_ID,
    MIN(PROV.PROV_ID)
FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
   PROV.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
   AND SRC_CD = 'FACETS'
GROUP BY PROV.NTNL_PROV_ID
"""
    )
    .load()
)
df_PROV_NPID = df_PROV_NPID.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("MIN(PROV.PROV_ID)").alias("PROV_ID")
)

df_PROV_FAC_VCAC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
    PRCT.NTNL_PROV_ID,
    MPPNG.SRC_CD,
    MIN(PROV.PROV_ID)
FROM {IDSOwner}.PROV PROV,
     {IDSOwner}.CMN_PRCT PRCT,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE
    PRCT.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
    AND PRCT.CMN_PRCT_SK = PROV.CMN_PRCT_SK
GROUP BY
    PRCT.NTNL_PROV_ID,
    MPPNG.SRC_CD
"""
    )
    .load()
)
df_PROV_FAC_VCAC = df_PROV_FAC_VCAC.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("MIN(PROV.PROV_ID)").alias("PROV_ID")
)

df_DEA_NPI = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV_DEA.DEA_NO,
    PROV_DEA.NTNL_PROV_ID
FROM {IDSOwner}.PROV_DEA PROV_DEA
"""
    )
    .load()
)
df_DEA_NPI = df_DEA_NPI.select(
    F.col("DEA_NO").alias("DEA_NO"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: hf_bca_clmprov_dea (CHashedFileStage, scenario A for each link)
# Multiple input links => produce 7 output sets with possible filtering
# --------------------------------------------------------------------------------
# 1) hf_prov_dea_pct from df_PROV_DEA_PCT (key=DEA_NO)
df_hf_prov_dea_pct = dedup_sort(
    df_PROV_DEA_PCT,
    partition_cols=["DEA_NO"],
    sort_cols=[("DEA_NO","A")]
).select("DEA_NO","PROV_ID")

# 2) hf_Provdea_npid from df_PROV_DEA_NPID (key=NTNL_PROV_ID)
df_hf_Provdea_npid = dedup_sort(
    df_PROV_DEA_NPID,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
).select("NTNL_PROV_ID","PROV_ID")

# 3) hf_prov_dea_ntnl from df_PROV_DEA_NT (key=DEA_NO)
df_hf_prov_dea_ntnl = dedup_sort(
    df_PROV_DEA_NT,
    partition_cols=["DEA_NO"],
    sort_cols=[("DEA_NO","A")]
).select("DEA_NO","PROV_ID")

# 4) hf_prov_ntnl from df_PROV_NPID (key=NTNL_PROV_ID)
df_hf_prov_ntnl = dedup_sort(
    df_PROV_NPID,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
).select("NTNL_PROV_ID","PROV_ID")

# 5) from df_PROV_FAC_VCAC => produce hf_prct_facets and hf_prct_vcac (both dedup by NTNL_PROV_ID, SRC_CD)
df_prov_facets = df_PROV_FAC_VCAC.filter(F.col("SRC_CD")==F.lit("FACETS"))
df_prov_vcac = df_PROV_FAC_VCAC.filter(F.col("SRC_CD")==F.lit("VCAC"))

df_hf_prct_facets = dedup_sort(
    df_prov_facets,
    partition_cols=["NTNL_PROV_ID","SRC_CD"],
    sort_cols=[("NTNL_PROV_ID","A"),("SRC_CD","A")]
).select("NTNL_PROV_ID","SRC_CD","PROV_ID")

df_hf_prct_vcac = dedup_sort(
    df_prov_vcac,
    partition_cols=["NTNL_PROV_ID","SRC_CD"],
    sort_cols=[("NTNL_PROV_ID","A"),("SRC_CD","A")]
).select("NTNL_PROV_ID","SRC_CD","PROV_ID")

# 6) hf_dea_npi from df_DEA_NPI (key=DEA_NO)
df_hf_dea_npi = dedup_sort(
    df_DEA_NPI,
    partition_cols=["DEA_NO"],
    sort_cols=[("DEA_NO","A")]
).select("DEA_NO","NTNL_PROV_ID")

# --------------------------------------------------------------------------------
# STAGE: IDS_PROV (DB2Connector, database=IDS)
# --------------------------------------------------------------------------------
df_IDS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
   PROV.NTNL_PROV_ID, SRC_CD, MIN(PROV_ADDR_ID) AS PROV_ADDR_ID, MIN(PROV_ID) AS PROV_ID
FROM {IDSOwner}.PROV PROV,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE
   PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
GROUP BY NTNL_PROV_ID, SRC_CD
ORDER BY NTNL_PROV_ID, PROV_ID
"""
    )
    .load()
)
df_IDS_PROV = df_IDS_PROV.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ID").alias("PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: IDS_PROV_ADDR (DB2Connector, database=IDS)
# --------------------------------------------------------------------------------
df_IDS_PROV_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
    PROV_ADDR_ID, ADDR_LN_1, ADDR_LN_2, CITY_NM, PROV_ADDR_ST_CD_SK, POSTAL_CD
FROM {IDSOwner}.PROV_ADDR PROV_ADDR
"""
    )
    .load()
)
df_IDS_PROV_ADDR = df_IDS_PROV_ADDR.select(
    "PROV_ADDR_ID","ADDR_LN_1","ADDR_LN_2","CITY_NM","PROV_ADDR_ST_CD_SK","POSTAL_CD"
)

# --------------------------------------------------------------------------------
# STAGE: Xfm_Prov_Addr
# --------------------------------------------------------------------------------
df_Xfm_Prov_Addr = df_IDS_PROV_ADDR.withColumn(
    "svAddr",
    F.when(
        F.col("ADDR_LN_1").isNull() & F.col("ADDR_LN_2").isNull() & F.col("CITY_NM").isNull()
        & F.col("PROV_ADDR_ST_CD_SK").isNull() & F.col("POSTAL_CD").isNull(),
        1
    ).otherwise(
        F.when(
            (F.col("ADDR_LN_1")==F.lit("NA"))|
            (F.col("ADDR_LN_2")==F.lit("NA"))|
            (F.col("CITY_NM")==F.lit("NA"))|
            (F.col("PROV_ADDR_ST_CD_SK")==F.lit("1"))|
            (F.col("PROV_ADDR_ST_CD_SK")==F.lit("0"))|
            (F.col("POSTAL_CD")==F.lit("NA")),
            1
        ).otherwise(2)
    )
)
df_Xfm_Prov_Addr_DSLink192 = df_Xfm_Prov_Addr.filter(F.col("svAddr")==2).select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID")
)

# --------------------------------------------------------------------------------
# STAGE: hf_prov_addr_vld (CHashedFileStage, scenario A, key=PROV_ADDR_ID)
# --------------------------------------------------------------------------------
df_hf_prov_addr_vld = dedup_sort(
    df_Xfm_Prov_Addr_DSLink192,
    partition_cols=["PROV_ADDR_ID"],
    sort_cols=[("PROV_ADDR_ID","A")]
).select("PROV_ADDR_ID")

# --------------------------------------------------------------------------------
# STAGE: xfm_prov
# PrimaryLink: df_IDS_PROV => alias Prov_Nabp
# LookupLink: df_hf_prov_addr_vld => alias DSLink193, left join on Prov_Nabp.PROV_ADDR_ID=DSLink193.PROV_ADDR_ID
# Output pins: nabp, facets, bca with constraints
# --------------------------------------------------------------------------------
df_xfm_prov_joined = df_IDS_PROV.alias("Prov_Nabp").join(
    df_hf_prov_addr_vld.alias("DSLink193"),
    on=[F.col("Prov_Nabp.PROV_ADDR_ID")==F.col("DSLink193.PROV_ADDR_ID")],
    how="left"
)

df_xfm_prov_nabp = df_xfm_prov_joined.filter(F.col("Prov_Nabp.SRC_CD")==F.lit("NABP")).select(
    F.col("Prov_Nabp.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Prov_Nabp.PROV_ID").alias("PROV_ID")
)
df_xfm_prov_facets = df_xfm_prov_joined.filter(F.col("Prov_Nabp.SRC_CD")==F.lit("FACETS")).select(
    F.col("Prov_Nabp.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Prov_Nabp.PROV_ID").alias("PROV_ID")
)
df_xfm_prov_bca = df_xfm_prov_joined.filter(F.col("Prov_Nabp.SRC_CD")==F.lit("BCA")).select(
    F.col("Prov_Nabp.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Prov_Nabp.PROV_ID").alias("PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: hf_bca_clm_ln_prov (CHashedFileStage, scenario A)
# Inputs: nabp, facets, bca => each dedup by key=NTNL_PROV_ID
# --------------------------------------------------------------------------------
df_hf_prov_nabp = dedup_sort(
    df_xfm_prov_nabp,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
).select("NTNL_PROV_ID","PROV_ID")

df_hf_prov_fcets = dedup_sort(
    df_xfm_prov_facets,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
).select("NTNL_PROV_ID","PROV_ID")

df_hf_prov_bca = dedup_sort(
    df_xfm_prov_bca,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
).select("NTNL_PROV_ID","PROV_ID")

# --------------------------------------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
# PrimaryLink: BcaData => df_Transformer_34_BcaData
# 7 lookups:
#   hf_prov_dea_pct => df_hf_prov_dea_pct
#   hf_prct_vcac => df_hf_prct_vcac
#   hf_prct_facets => df_hf_prct_facets
#   hf_Provdea_npid => df_hf_Provdea_npid
#   hf_prov_dea_ntnl => df_hf_prov_dea_ntnl
#   hf_prov_ntnl => df_hf_prov_ntnl
#   hf_dea_npi => df_hf_dea_npi
# Also 3 more lookups:
#   hf_prov_bca => df_hf_prov_bca
#   hf_prov_fcets => df_hf_prov_fcets
#   hf_prov_nabp => df_hf_prov_nabp
# We chain left joins in order.
# --------------------------------------------------------------------------------
df_BusinessRules_LJ = df_Transformer_34_BcaData.alias("BcaData") \
    .join(df_hf_prov_dea_pct.alias("hf_prov_dea_pct"), F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_prov_dea_pct.DEA_NO"), "left") \
    .join(df_hf_prct_vcac.alias("hf_prct_vcac"), [F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_prct_vcac.NTNL_PROV_ID"), F.lit("VCAC")==F.col("hf_prct_vcac.SRC_CD")], "left") \
    .join(df_hf_prct_facets.alias("hf_prct_facets"), [F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_prct_facets.NTNL_PROV_ID"), F.lit("FACETS")==F.col("hf_prct_facets.SRC_CD")], "left") \
    .join(df_hf_Provdea_npid.alias("hf_Provdea_npid"), F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_Provdea_npid.NTNL_PROV_ID"), "left") \
    .join(df_hf_prov_dea_ntnl.alias("hf_prov_dea_ntnl"), F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_prov_dea_ntnl.DEA_NO"), "left") \
    .join(df_hf_prov_ntnl.alias("hf_prov_ntnl"), F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_prov_ntnl.NTNL_PROV_ID"), "left") \
    .join(df_hf_prov_bca.alias("hf_prov_bca"), F.col("BcaData.PDX_NTNL_PROV_ID")==F.col("hf_prov_bca.NTNL_PROV_ID"), "left") \
    .join(df_hf_prov_fcets.alias("hf_prov_fcets"), F.col("BcaData.PDX_NTNL_PROV_ID")==F.col("hf_prov_fcets.NTNL_PROV_ID"), "left") \
    .join(df_hf_prov_nabp.alias("hf_prov_nabp"), F.col("BcaData.PDX_NTNL_PROV_ID")==F.col("hf_prov_nabp.NTNL_PROV_ID"), "left") \
    .join(df_hf_dea_npi.alias("hf_dea_npi"), F.col("BcaData.PRSCRB_PROV_ID")==F.col("hf_dea_npi.DEA_NO"), "left")

# For SVC output link => filter none, just select
df_svc = df_BusinessRules_LJ.select(
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("BcaData.CLM_ID").alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("BcaData.PDX_NTNL_PROV_ID").isNotNull()) & (F.length("BcaData.PDX_NTNL_PROV_ID")==10),
        F.when(F.col("hf_prov_nabp.PROV_ID").isNotNull(), F.col("hf_prov_nabp.PROV_ID"))
         .otherwise(
            F.when(F.col("hf_prov_fcets.PROV_ID").isNotNull(), F.col("hf_prov_fcets.PROV_ID"))
             .otherwise(
                F.when(F.col("hf_prov_bca.PROV_ID").isNotNull(), F.col("hf_prov_bca.PROV_ID"))
                 .otherwise(F.lit("UNK"))
             )
         )
    ).otherwise(F.lit("NA")).alias("PROV_ID"),
    F.lit(0).cast(StringType()).alias("TAX_ID"),  # length=9 => '0' or 'NA' in DS logic is 0
    F.when(
        (F.col("BcaData.PDX_NTNL_PROV_ID").isNull()) | (F.col("BcaData.PDX_NTNL_PROV_ID")==F.lit("UNK")),
        F.lit("NA")
    ).otherwise(
        F.when(F.expr("length(BcaData.PDX_NTNL_PROV_ID) = 10"), F.col("BcaData.PDX_NTNL_PROV_ID"))
         .otherwise(F.lit("NA"))
    ).alias("NTNL_PROV_ID")
)

df_prscrb_out = df_BusinessRules_LJ.select(
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("BcaData.CLM_ID").alias("CLM_ID"),
    F.lit("PRSCRB").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("BcaData.PRSCRB_PROV_ID").isNull(),
        F.lit("NA")
    ).otherwise(
        F.when(
            F.expr("cast(substr(BcaData.PRSCRB_PROV_ID,1,1) as int) is null"),
            F.when(F.col("hf_prov_dea_pct.PROV_ID").isNotNull(), F.col("hf_prov_dea_pct.PROV_ID"))
             .otherwise(
                F.when(F.col("hf_prov_dea_ntnl.PROV_ID").isNotNull(), F.col("hf_prov_dea_ntnl.PROV_ID"))
                 .otherwise(F.lit("UNK"))
             )
        ).otherwise(
            F.when(F.col("hf_prct_facets.PROV_ID").isNotNull(), F.col("hf_prct_facets.PROV_ID"))
             .otherwise(
                F.when(F.col("hf_prct_vcac.PROV_ID").isNotNull(), F.col("hf_prct_vcac.PROV_ID"))
                 .otherwise(
                    F.when(F.col("hf_Provdea_npid.PROV_ID").isNotNull(), F.col("hf_Provdea_npid.PROV_ID"))
                     .otherwise(
                        F.when(F.col("hf_prov_ntnl.PROV_ID").isNotNull(), F.col("hf_prov_ntnl.PROV_ID"))
                         .otherwise(F.lit("UNK"))
                     )
                 )
             )
        )
    ).alias("PROV_ID"),
    F.lit(0).cast(StringType()).alias("TAX_ID"),
    F.when(
        F.col("BcaData.PRSCRB_PROV_ID").isNull(),
        F.lit("NA")
    ).otherwise(
        F.when(
            F.expr("cast(substr(BcaData.PRSCRB_PROV_ID,1,1) as int) is null"),
            F.col("hf_dea_npi.NTNL_PROV_ID")
        ).otherwise(F.col("BcaData.PRSCRB_PROV_ID"))
    ).alias("NTNL_PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: hf_bca_clmprov_svcprov (CHashedFileStage, scenario A, key=[CLM_PROV_SK,CLM_ID,CLM_PROV_ROLE_TYP_CD])
# --------------------------------------------------------------------------------
df_hf_bca_clmprov_svcprov = dedup_sort(
    df_svc,
    partition_cols=["CLM_PROV_SK","CLM_ID","CLM_PROV_ROLE_TYP_CD"],
    sort_cols=[("CLM_PROV_SK","A"),("CLM_ID","A"),("CLM_PROV_ROLE_TYP_CD","A")]
).select("CLM_PROV_SK","CLM_ID","CLM_PROV_ROLE_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","PROV_ID","TAX_ID","NTNL_PROV_ID")

# --------------------------------------------------------------------------------
# STAGE: hf_bca_clmprov_prscrbprov (CHashedFileStage, scenario A)
# key=[CLM_PROV_SK,CLM_ID,CLM_PROV_ROLE_TYP_CD]
# --------------------------------------------------------------------------------
df_hf_bca_clmprov_prscrbprov = dedup_sort(
    df_prscrb_out,
    partition_cols=["CLM_PROV_SK","CLM_ID","CLM_PROV_ROLE_TYP_CD"],
    sort_cols=[("CLM_PROV_SK","A"),("CLM_ID","A"),("CLM_PROV_ROLE_TYP_CD","A")]
).select("CLM_PROV_SK","CLM_ID","CLM_PROV_ROLE_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","PROV_ID","TAX_ID","NTNL_PROV_ID")

# --------------------------------------------------------------------------------
# STAGE: Link_Collector (CCollector, Round-Robin => effectively a union)
# --------------------------------------------------------------------------------
df_Collector = df_hf_bca_clmprov_prscrbprov.unionByName(df_hf_bca_clmprov_svcprov, allowMissingColumns=True).select(
    F.col("CLM_PROV_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID"),
    F.col("TAX_ID"),
    F.col("NTNL_PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: LookupTaxId
# Lookup link: df_taxid_lkup => join on ClmProvData.PROV_ID=Taxid_lkup.PROV_ID
# --------------------------------------------------------------------------------
df_LookupTaxId_join = df_Collector.alias("ClmProvData").join(
    df_taxid_lkup.alias("Taxid_lkup"), 
    F.col("ClmProvData.PROV_ID")==F.col("Taxid_lkup.PROV_ID"),
    how="left"
)

df_LookupTaxId_out = df_LookupTaxId_join.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),  # The job used "CurrDate", assume we map to a parameter
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"),F.lit(";"),F.col("ClmProvData.CLM_ID"),F.lit(";"),F.col("ClmProvData.CLM_PROV_ROLE_TYP_CD")).alias("PRI_KEY_STRING"),
    F.col("ClmProvData.CLM_PROV_SK").alias("CLM_PROV_SK"),
    F.col("ClmProvData.CLM_ID").alias("CLM_ID"),
    F.col("ClmProvData.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("ClmProvData.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ClmProvData.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmProvData.PROV_ID").alias("PROV_ID"),
    F.when(F.col("Taxid_lkup.TAX_ID").isNotNull(), F.col("Taxid_lkup.TAX_ID")).otherwise(F.lit(None)).alias("TAX_ID"),
    F.when(F.col("ClmProvData.NTNL_PROV_ID").isNull(), F.lit("NA")).otherwise(F.col("ClmProvData.NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: Snapshot
# --------------------------------------------------------------------------------
df_Snapshot = df_LookupTaxId_out.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # The job used "SrcSysCdSk"
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK").alias("CLM_PROV_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.col("TAX_ID").alias("TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID")
).alias("AllCol")

df_Snapshot_SnapShot = df_LookupTaxId_out.select(
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").cast(StringType()).alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("PROV_ID").cast(StringType()).alias("PROV_ID")
).alias("SnapShot")

df_Snapshot_Transform = df_LookupTaxId_out.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD")
)

# --------------------------------------------------------------------------------
# STAGE: Transformer
# --------------------------------------------------------------------------------
df_Transformer = df_Snapshot_SnapShot.withColumn(
    "svClmProvRole",
    F.lit(None)  # "GetFkeyCodes('BCA', 0, \"CLAIM PROVIDER ROLE TYPE\", SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')" is a UDF, assume available
).select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svClmProvRole").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
).alias("RowCount")

# Because we must preserve the actual expressions exactly:
df_Transformer_fixed = df_Snapshot_SnapShot.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.expr("GetFkeyCodes('BCA', 0, \"CLAIM PROVIDER ROLE TYPE\", PROV_ID, 'X')").alias("???_placeholder") 
)  # Not actually used; we replicate the pattern that the job indicates.

df_Transformer_out = df_Snapshot_SnapShot.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.expr("GetFkeyCodes('BCA', 0, \"CLAIM PROVIDER ROLE TYPE\", SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
)

# In DataStage, the final columns from "Transformer" -> "RowCount" were:
df_Transformer_RowCount = df_Transformer_out.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD_SK").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").cast(StringType()).alias("PROV_ID")
)

# --------------------------------------------------------------------------------
# STAGE: B_CLM_PROV (CSeqFileStage) from df_Transformer_RowCount
# Write with no header, quote="\"", delimiter=","
# File path is "load/B_CLM_PROV.#SrcSysCd#.dat.#RunID#"
# --------------------------------------------------------------------------------
df_B_CLM_PROV_write = df_Transformer_RowCount.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID"
)
write_files(
    df_B_CLM_PROV_write,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# STAGE: ClmProvPK (CContainerStage => shared container)
# Two input pins: AllCol (df_Snapshot) and Transform (df_Snapshot_Transform)
# One output pin: Key
# --------------------------------------------------------------------------------
params_ClmProvPK = {
    "CurrRunCycle": f"{CurrRunCycle}",
    "SrcSysCd": f"{SrcSysCd}",
    "$IDSOwner": f"{IDSOwner}"
}
# Call the shared container function with the two dataframes and parameters
df_ClmProvPK_Key = ClmProvPK(
    df_Snapshot.alias("AllCol"),
    df_Snapshot_Transform.alias("Transform"),
    params_ClmProvPK
)

# --------------------------------------------------------------------------------
# STAGE: BCAClmProvExtr (CSeqFileStage) from ClmProvPK output
# Path => "key/BCAClmProvExtr.DrugClmProv.dat.#RunID#"
# --------------------------------------------------------------------------------
df_BCAClmProvExtr_write = df_ClmProvPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID",
    "NTNL_PROV_ID"
)
write_files(
    df_BCAClmProvExtr_write,
    f"{adls_path}/key/BCAClmProvExtr.DrugClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)