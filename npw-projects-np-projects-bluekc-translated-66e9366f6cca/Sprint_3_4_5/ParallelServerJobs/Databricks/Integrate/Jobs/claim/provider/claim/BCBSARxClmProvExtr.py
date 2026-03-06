# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2015 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION: Runs BCBSA PDX Claims  Provider extract.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC KChintalapani        2016-03-01        5212                           Original Programming                                                                 IntegrateDevl2                Kalyan Neelam          2016-05-11
# MAGIC 
# MAGIC Madhavan B         2017-06-20        5788 - BHI Updates    Added new column                                                                    IntegrateDev2                Kalyan Neelam          2017-07-07
# MAGIC                                                                                            SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC Manasa A   \(9)2018-03-05     TFS 21142             Modified logic to correct populating the PROV_ID field in BusinessRules  IntegrateDev2     Kalyan Neelam          2018-03-12
# MAGIC \(9)\(9)\(9)                                     stage instead of NTNL_PROV_ID in the Npi_Prov_Prscrb_Lkup link.
# MAGIC  
# MAGIC Sudhir Bomshetty   2018-04-27       5781 - HEDIS            Added the NTNL_PROV_ID                                                        IntegrateDev2               Kalyan Neelam          2018-05-01

# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')

schema_BCBSARxClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("NDW_HOME_PLN_ID", StringType(), True),
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
    StructField("MMI_ID", StringType(), True),
    StructField("SUBGRP_SK", IntegerType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), False),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False)
])

df_ClmHdr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_BCBSARxClmLanding)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)

query_PROV_SVC = f"""SELECT 
PROV.NTNL_PROV_ID,
PROV.PROV_ID,
PROV.TAX_ID

FROM 
{IDSOwner}.PROV PROV, 
{IDSOwner}.PROV_ADDR PROV_ADDR, 
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE 
PROV.PROV_ADDR_ID = PROV_ADDR.PROV_ADDR_ID AND 
PROV_ADDR.PROV_ADDR_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK AND
PROV_ADDR.TERM_DT_SK > '{CurrentDate}' AND
PROV.NTNL_PROV_ID <> 'NA'  AND
CD_MPPNG.TRGT_CD = 'P' AND
PROV.PROV_ID =(SELECT MIN(P.PROV_ID) 
FROM 
{IDSOwner}.PROV P
WHERE 
P.NTNL_PROV_ID = PROV.NTNL_PROV_ID)
ORDER BY PROV_ID ASC

{IDSOwner}.PROV PROV
"""

df_PROV_SVC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_PROV_SVC)
    .load()
)

df_Prov_Svc_Lkup = dedup_sort(
    df_PROV_SVC,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("PROV_ID","A")]
)

query_PROV_PRSCRB = f"""SELECT 
PROV.PROV_ID,
PROV.NTNL_PROV_ID,
PROV.TAX_ID
FROM 
{IDSOwner}.PROV PROV, 
{IDSOwner}.PROV_ADDR PROV_ADDR, 
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE 
PROV.PROV_ADDR_ID = PROV_ADDR.PROV_ADDR_ID AND 
PROV_ADDR.PROV_ADDR_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK AND
PROV_ADDR.TERM_DT_SK > '{CurrentDate}' AND
PROV.NTNL_PROV_ID <> 'NA'  AND
CD_MPPNG.TRGT_CD = 'P' AND
PROV.PROV_ID =(SELECT MIN(P.PROV_ID) 
FROM 
{IDSOwner}.PROV P
WHERE 
P.NTNL_PROV_ID = PROV.NTNL_PROV_ID)
ORDER BY PROV_ID ASC

{IDSOwner}.PROV PROV
"""

df_PROV_PRSCRB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_PROV_PRSCRB)
    .load()
)

df_Prov_Prscrb_Lkup = dedup_sort(
    df_PROV_PRSCRB,
    partition_cols=["PROV_ID"],
    sort_cols=[("PROV_ID","A")]
)

query_NPI_PROV_PRSCRB = f"""SELECT 
PROV.NTNL_PROV_ID,    PROV.PROV_ID,
PROV.TAX_ID
FROM 
{IDSOwner}.PROV PROV, 
{IDSOwner}.PROV_ADDR PROV_ADDR, 
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE 
PROV.PROV_ADDR_ID = PROV_ADDR.PROV_ADDR_ID AND 
PROV_ADDR.PROV_ADDR_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK AND
PROV_ADDR.TERM_DT_SK > '{CurrentDate}' AND
PROV.NTNL_PROV_ID <> 'NA'  AND
CD_MPPNG.TRGT_CD = 'P' AND
PROV.PROV_ID =(SELECT MIN(P.PROV_ID) 
FROM 
{IDSOwner}.PROV P
WHERE 
P.NTNL_PROV_ID = PROV.NTNL_PROV_ID)
ORDER BY PROV_ID ASC
"""

df_NPI_PROV_PRSCRB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_NPI_PROV_PRSCRB)
    .load()
)

df_Npi_Prov_Prscrb_Lkup = dedup_sort(
    df_NPI_PROV_PRSCRB,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("PROV_ID","A")]
)

df_BusinessRules = (
    df_ClmHdr.alias("ClmHdr")
    .join(
        df_Prov_Svc_Lkup.alias("Prov_Svc_Lkup"),
        F.col("ClmHdr.NPI_REND_PROV_ID") == F.col("Prov_Svc_Lkup.NTNL_PROV_ID"),
        "left"
    )
    .join(
        df_Prov_Prscrb_Lkup.alias("Prov_Prscrb_Lkup"),
        (
            (F.col("ClmHdr.PRSCRB_PROV_ID") == F.col("Prov_Prscrb_Lkup.PROV_ID")) &
            (F.col("ClmHdr.NPI_PRSCRB_PROV_ID") == F.col("Prov_Prscrb_Lkup.NTNL_PROV_ID"))
        ),
        "left"
    )
    .join(
        df_Npi_Prov_Prscrb_Lkup.alias("Npi_Prov_Prscrb_Lkup"),
        (
            (F.col("ClmHdr.NPI_PRSCRB_PROV_ID") == F.col("Npi_Prov_Prscrb_Lkup.NTNL_PROV_ID")) &
            (F.col("ClmHdr.NPI_PRSCRB_PROV_ID") == F.col("Npi_Prov_Prscrb_Lkup.PROV_ID"))
        ),
        "left"
    )
)

df_xfm_PRSCRB = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmHdr.CLM_ID"), F.lit(";"), F.lit("PRSCRB")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("ClmHdr.CLM_ID").alias("CLM_ID"),
    F.lit("PRSCRB").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("Prov_Prscrb_Lkup.PROV_ID").isNotNull(), F.col("Prov_Prscrb_Lkup.PROV_ID"))
     .when(F.col("Npi_Prov_Prscrb_Lkup.PROV_ID").isNotNull(), F.col("Npi_Prov_Prscrb_Lkup.PROV_ID"))
     .otherwise("UNK").alias("PROV_ID"),
    F.when(F.col("Prov_Prscrb_Lkup.TAX_ID").isNotNull(), F.col("Prov_Prscrb_Lkup.TAX_ID"))
     .when(F.col("Npi_Prov_Prscrb_Lkup.TAX_ID").isNotNull(), F.col("Npi_Prov_Prscrb_Lkup.TAX_ID"))
     .otherwise(None).alias("TAX_ID"),
    F.when(F.col("ClmHdr.NPI_PRSCRB_PROV_ID").isNotNull(),
           F.when(F.col("Npi_Prov_Prscrb_Lkup.NTNL_PROV_ID").isNotNull(), F.col("Npi_Prov_Prscrb_Lkup.NTNL_PROV_ID"))
            .otherwise("UNK")
          ).otherwise("NA").alias("NTNL_PROV_ID")
)

df_xfm_SVC = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmHdr.CLM_ID"), F.lit(";"), F.lit("SVC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("ClmHdr.CLM_ID").alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("Prov_Svc_Lkup.PROV_ID").isNotNull(), F.col("Prov_Svc_Lkup.PROV_ID"))
     .otherwise("UNK").alias("PROV_ID"),
    F.when(F.col("Prov_Svc_Lkup.TAX_ID").isNotNull(), F.col("Prov_Svc_Lkup.TAX_ID"))
     .otherwise(None).alias("TAX_ID"),
    F.when(F.col("ClmHdr.NPI_REND_PROV_ID").isNotNull(),
           F.when(F.col("Prov_Svc_Lkup.NTNL_PROV_ID").isNotNull(), F.col("Prov_Svc_Lkup.NTNL_PROV_ID"))
            .otherwise("UNK")
          ).otherwise("NA").alias("NTNL_PROV_ID")
)

common_cols_link_collector = [
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
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
]

df_Link_Collector = df_xfm_PRSCRB.select(common_cols_link_collector).unionByName(
    df_xfm_SVC.select(common_cols_link_collector)
)

df_Xfm_Clm_Prov = df_Link_Collector

df_Snapshot_SnapShot = df_Xfm_Clm_Prov.select(
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").cast(StringType()).alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("PROV_ID").cast(StringType()).alias("PROV_ID")
)

df_Snapshot_AllCol = df_Xfm_Clm_Prov.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.col("TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID")
)

df_Snapshot_xfm = df_Xfm_Clm_Prov.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD")
)

df_Transformer_in = df_Snapshot_SnapShot

df_Transformer = df_Transformer_in.withColumn(
    "svClmProvRole",
    GetFkeyCodes(SrcSysCd, 0, "CLAIM PROVIDER ROLE TYPE", F.col("CLM_PROV_ROLE_TYP_CD"), 'X')
)

df_B_CLM_PROV = df_Transformer.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svClmProvRole").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
)

params_ClmProvPK = {
    "CurrRunCycle": RunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_ClmProvPK_AllCol, df_ClmProvPK_xfm = df_Snapshot_AllCol, df_Snapshot_xfm
df_ClmProvPK_Key = ClmProvPK(df_ClmProvPK_AllCol, df_ClmProvPK_xfm, params_ClmProvPK)

df_BCBSAClmProvExtr = df_ClmProvPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID")
)

write_files(
    df_BCBSAClmProvExtr,
    f"{adls_path}/key/BCBSAClmProvExtr.DrugClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_B_CLM_PROV_final = df_B_CLM_PROV.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD_SK"),
    F.rpad(F.col("PROV_ID"), 12, " ").alias("PROV_ID")
)

write_files(
    df_B_CLM_PROV_final,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)