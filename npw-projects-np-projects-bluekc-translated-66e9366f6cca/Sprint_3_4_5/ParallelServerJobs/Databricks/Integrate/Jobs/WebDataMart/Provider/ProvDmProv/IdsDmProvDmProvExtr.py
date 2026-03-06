# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Product Data Mart Deduct Component extract from IDS to Data Mart.  
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC             
# MAGIC   
# MAGIC Nagesh Bandi              07/03/2013              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl                 Bhoomi Dasari                        9/3/2013
# MAGIC 
# MAGIC Pooja Sunkara            01/23-2014               5114                                  Modified SQL in the EXTRNL_USER  reference lookup to 
# MAGIC                                                                                                                avoid duplicate warnings(Daptiv#374)                               IntegrateWrhsDevl                 Jag Yelavarthi                         2014-01-23

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC REL_IPA_PROV_SK
# MAGIC PROV_BILL_SVC_SK
# MAGIC REL_GRP_PROV_SK
# MAGIC PROV_ENTY_CD_SK
# MAGIC PROV_TYP_CD_SK
# MAGIC PROV_SPEC_CD_SK
# MAGIC PROV_SK
# MAGIC Write PROV Data into a Sequential file for Load Job IdsDmProvDmProvLoad.
# MAGIC Read all the Data from IDS PROV Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmProvDmProvExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# JDBC Config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_PROV_in
extract_query_db2_PROV_in = f"""
SELECT 
PROV_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD ,  
PROV_ID,
PROV_BILL_SVC_SK,
REL_GRP_PROV_SK,
REL_IPA_PROV_SK,
PROV_ENTY_CD_SK, 
PROV_SPEC_CD_SK,
PROV_TYP_CD_SK,
NTNL_PROV_ID,
PROV_NM,
TAX_ID
FROM {IDSOwner}.PROV PROV
LEFT JOIN {IDSOwner}.CD_MPPNG CD 
  ON PROV.SRC_SYS_CD_SK = CD.CD_MPPNG_SK 
WHERE PROV_SK < 0 OR PROV_SK > 1
"""
df_db2_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_in)
    .load()
)

# Stage: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# Stage: db2_PROV_BILL_SVC_in
extract_query_db2_PROV_BILL_SVC_in = f"""
SELECT 
PROV_BILL_SVC_SK,
PROV_BILL_SVC_ID,
PROV_BILL_SVC_NM
FROM {IDSOwner}.PROV_BILL_SVC
"""
df_db2_PROV_BILL_SVC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_BILL_SVC_in)
    .load()
)

# Stage: db2_PROV_TYP_CD_in
extract_query_db2_PROV_TYP_CD_in = f"""
SELECT 
PROV_TYP_CD_SK,
PROV_TYP_CD,
PROV_TYP_NM
FROM {IDSOwner}.PROV_TYP_CD
"""
df_db2_PROV_TYP_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_TYP_CD_in)
    .load()
)

# Stage: db2_PROV_SPEC_CD_in
extract_query_db2_PROV_SPEC_CD_in = f"""
SELECT 
PROV_SPEC_CD_SK,
PROV_SPEC_CD,
PROV_SPEC_NM
FROM {IDSOwner}.PROV_SPEC_CD
"""
df_db2_PROV_SPEC_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_SPEC_CD_in)
    .load()
)

# Stage: db2_EXTRNL_USER_in
extract_query_db2_EXTRNL_USER_in = f"""
SELECT 
PROV_SK,
EXTRNL_USER_ID
FROM {IDSOwner}.EXTRNL_USER
WHERE EXTRNL_USER_CNSTTNT_TYP_CD IN ('PROVIDER','PROVGRP','PROVIPA')
  AND ( PROV_SK < 0 OR PROV_SK > 1)
"""
df_db2_EXTRNL_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EXTRNL_USER_in)
    .load()
)

# Stage: db2_Rel_PROV_in
extract_query_db2_Rel_PROV_in = f"""
SELECT
PROV_SK,
PROV_ID,
PROV_NM
FROM {IDSOwner}.PROV
"""
df_db2_Rel_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rel_PROV_in)
    .load()
)

# Stage: cpy_Rel_PROV
df_cpy_Rel_PROV_Ref_ProvRelIpaProv_Lkup = df_db2_Rel_PROV_in.select(
    col("PROV_SK"),
    col("PROV_ID"),
    col("PROV_NM")
)

df_cpy_Rel_PROV_Ref_RefProv_Grp_Prov_Lkup = df_db2_Rel_PROV_in.select(
    col("PROV_SK"),
    col("PROV_ID"),
    col("PROV_NM")
)

# Stage: lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_PROV_in.alias("lnk_IdsProvDmProvtExtr_InABC")
    .join(
        df_cpy_Rel_PROV_Ref_ProvRelIpaProv_Lkup.alias("Ref_ProvRelIpaProv_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.REL_IPA_PROV_SK") == col("Ref_ProvRelIpaProv_Lkup.PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_BILL_SVC_in.alias("Ref_Prov_Bill_Svd_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_BILL_SVC_SK") == col("Ref_Prov_Bill_Svd_Lkup.PROV_BILL_SVC_SK"),
        "left"
    )
    .join(
        df_cpy_Rel_PROV_Ref_RefProv_Grp_Prov_Lkup.alias("Ref_RefProv_Grp_Prov_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.REL_GRP_PROV_SK") == col("Ref_RefProv_Grp_Prov_Lkup.PROV_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG_in.alias("Ref_Prov_Enty_Cd_lkp"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_ENTY_CD_SK") == col("Ref_Prov_Enty_Cd_lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_PROV_TYP_CD_in.alias("Ref_Prov_Typ_cd_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_TYP_CD_SK") == col("Ref_Prov_Typ_cd_Lkup.PROV_TYP_CD_SK"),
        "left"
    )
    .join(
        df_db2_PROV_SPEC_CD_in.alias("Ref_Prov_Spec_cd_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_SPEC_CD_SK") == col("Ref_Prov_Spec_cd_Lkup.PROV_SPEC_CD_SK"),
        "left"
    )
    .join(
        df_db2_EXTRNL_USER_in.alias("Ref_Externl_User_Lkup"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_SK") == col("Ref_Externl_User_Lkup.PROV_SK"),
        "left"
    )
    .select(
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_SK").alias("PROV_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_ID").alias("PROV_ID"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_BILL_SVC_SK").alias("PROV_BILL_SVC_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_ENTY_CD_SK").alias("PROV_ENTY_CD_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_TYP_CD_SK").alias("PROV_TYP_CD_SK"),
        col("lnk_IdsProvDmProvtExtr_InABC.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("lnk_IdsProvDmProvtExtr_InABC.PROV_NM").alias("PROV_NM"),
        col("lnk_IdsProvDmProvtExtr_InABC.TAX_ID").alias("TAX_ID"),
        col("Ref_Prov_Enty_Cd_lkp.TRGT_CD").alias("PROV_ENTY_CD"),
        col("Ref_ProvRelIpaProv_Lkup.PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
        col("Ref_ProvRelIpaProv_Lkup.PROV_NM").alias("PROV_REL_IPA_PROV_NM"),
        col("Ref_RefProv_Grp_Prov_Lkup.PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
        col("Ref_RefProv_Grp_Prov_Lkup.PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
        col("Ref_Prov_Bill_Svd_Lkup.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
        col("Ref_Prov_Bill_Svd_Lkup.PROV_BILL_SVC_NM").alias("PROV_BILL_SVC_NM"),
        col("Ref_Prov_Typ_cd_Lkup.PROV_TYP_CD").alias("PROV_TYP_CD"),
        col("Ref_Prov_Typ_cd_Lkup.PROV_TYP_NM").alias("PROV_TYP_NM"),
        col("Ref_Prov_Spec_cd_Lkup.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        col("Ref_Prov_Spec_cd_Lkup.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
        col("Ref_Externl_User_Lkup.EXTRNL_USER_ID").alias("EXTRNL_USER_ID")
    )
)

# Stage: xfrm_BusinessLogic (CTransformerStage)
df_xfrm_BusinessLogic = df_lkp_Codes
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "SRC_SYS_CD",
    col("SRC_SYS_CD")
).withColumn(
    "PROV_ID",
    col("PROV_ID")
).withColumn(
    "PROV_BILL_SVC_NM",
    when(
        trim(
            when(col("PROV_BILL_SVC_NM").isNotNull(), col("PROV_BILL_SVC_NM")).otherwise(lit("NA")))
        == lit("NA"),
        lit(" ")
    ).otherwise(
        when(trim(col("PROV_BILL_SVC_NM")) == lit("UNK"), lit(" ")).otherwise(col("PROV_BILL_SVC_NM"))
    )
).withColumn(
    "PROV_BILL_SVC_ID",
    when(
        (length(trim(col("PROV_BILL_SVC_ID"))) == lit(0)) |
        (trim(col("PROV_BILL_SVC_ID")) == lit("NA")) |
        (trim(col("PROV_BILL_SVC_ID")) == lit("UNK")),
        lit(" ")
    ).otherwise(trim(col("PROV_BILL_SVC_ID")))
).withColumn(
    "PROV_ENTY_CD",
    when(
        (length(trim(col("PROV_ENTY_CD"))) == lit(0)) |
        (trim(col("PROV_ENTY_CD")) == lit("NA")) |
        (trim(col("PROV_ENTY_CD")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_ENTY_CD"))
).withColumn(
    "PROV_NM",
    when(
        length(trim(
            when(col("PROV_NM").isNotNull(), col("PROV_NM")).otherwise(lit(" ")))
        ) == lit(0)),
        lit("")
    ).otherwise(col("PROV_NM"))
).withColumn(
    "PROV_NPI",
    when(
        (length(trim(col("NTNL_PROV_ID"))) == lit(0)) |
        (trim(col("NTNL_PROV_ID")) == lit("NA")) |
        (trim(col("NTNL_PROV_ID")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("NTNL_PROV_ID"))
).withColumn(
    "PROV_REL_GRP_PROV_ID",
    when(
        (length(trim(col("PROV_REL_GRP_PROV_ID"))) == lit(0)) |
        (trim(col("PROV_REL_GRP_PROV_ID")) == lit("NA")) |
        (trim(col("PROV_REL_GRP_PROV_ID")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_REL_GRP_PROV_ID"))
).withColumn(
    "PROV_REL_GRP_PROV_NM",
    when(
        trim(
            when(col("PROV_REL_GRP_PROV_NM").isNotNull(), col("PROV_REL_GRP_PROV_NM")).otherwise(lit("NA")))
        == lit("NA"),
        lit(" ")
    ).otherwise(
        when(trim(col("PROV_REL_GRP_PROV_NM")) == lit("UNK"), lit(" ")).otherwise(col("PROV_REL_GRP_PROV_NM"))
    )
).withColumn(
    "PROV_REL_IPA_PROV_ID",
    when(
        (length(trim(col("PROV_REL_IPA_PROV_ID"))) == lit(0)) |
        (trim(col("PROV_REL_IPA_PROV_ID")) == lit("NA")) |
        (trim(col("PROV_REL_IPA_PROV_ID")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_REL_IPA_PROV_ID"))
).withColumn(
    "PROV_REL_IPA_PROV_NM",
    when(
        trim(
            when(col("PROV_REL_IPA_PROV_NM").isNotNull(), col("PROV_REL_IPA_PROV_NM")).otherwise(lit("NA")))
        == lit("NA"),
        lit(" ")
    ).otherwise(
        when(trim(col("PROV_REL_IPA_PROV_NM")) == lit("UNK"), lit(" ")).otherwise(col("PROV_REL_IPA_PROV_NM"))
    )
).withColumn(
    "PROV_SPEC_CD",
    when(
        (length(trim(col("PROV_SPEC_CD"))) == lit(0)) |
        (trim(col("PROV_SPEC_CD")) == lit("NA")) |
        (trim(col("PROV_SPEC_CD")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_SPEC_CD"))
).withColumn(
    "PROV_SPEC_NM",
    when(
        col("PROV_SPEC_NM").isNull(), lit(" ")
    ).otherwise(
        when(trim(col("PROV_SPEC_NM")) == lit("NA"), lit(" "))
        .otherwise(
            when(trim(col("PROV_SPEC_NM")) == lit("UNK"), lit(" "))
            .otherwise(
                when(trim(col("PROV_SPEC_NM")) == lit(""), lit(" "))
                .otherwise(col("PROV_SPEC_NM"))
            )
        )
    )
).withColumn(
    "PROV_TAX_ID",
    when(
        (length(trim(col("TAX_ID"))) == lit(0)) |
        (trim(col("TAX_ID")) == lit("NA")) |
        (trim(col("TAX_ID")) == lit("UNK")),
        lit(" ")
    ).otherwise(trim(col("TAX_ID")))
).withColumn(
    "PROV_TYP_CD",
    when(
        (length(trim(col("PROV_TYP_CD"))) == lit(0)) |
        (trim(col("PROV_TYP_CD")) == lit("NA")) |
        (trim(col("PROV_TYP_CD")) == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_TYP_CD"))
).withColumn(
    "PROV_TYP_NM",
    when(
        trim(
            when(col("PROV_TYP_NM").isNotNull(), col("PROV_TYP_NM")).otherwise(lit("NA")))
        == lit("NA"),
        lit(" ")
    ).otherwise(
        when(trim(col("PROV_TYP_NM")) == lit("UNK"), lit(" ")).otherwise(col("PROV_TYP_NM"))
    )
).withColumn(
    "LAST_UPDT_RUN_CYC_NO",
    lit(EDWRunCycle)
).withColumn(
    "EXTRNL_USER_ID",
    when(
        (trim(col("PROV_SK")) == lit("")) | (trim(col("EXTRNL_USER_ID")) == lit("NA")),
        lit(" ")
    ).otherwise(col("EXTRNL_USER_ID"))
)

# Select final columns in correct order
df_enriched = df_xfrm_BusinessLogic.select(
    "SRC_SYS_CD",
    "PROV_ID",
    "PROV_BILL_SVC_NM",
    "PROV_BILL_SVC_ID",
    "PROV_ENTY_CD",
    "PROV_NM",
    "PROV_NPI",
    "PROV_REL_GRP_PROV_ID",
    "PROV_REL_GRP_PROV_NM",
    "PROV_REL_IPA_PROV_ID",
    "PROV_REL_IPA_PROV_NM",
    "PROV_SPEC_CD",
    "PROV_SPEC_NM",
    "PROV_TAX_ID",
    "PROV_TYP_CD",
    "PROV_TYP_NM",
    "LAST_UPDT_RUN_CYC_NO",
    "EXTRNL_USER_ID"
)

# Per the requirement to rpad char/varchar columns; exact lengths unknown → use <...> as placeholder
df_final = df_enriched
df_final = df_final.withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
df_final = df_final.withColumn("PROV_ID", rpad(col("PROV_ID"), <...>, " "))
df_final = df_final.withColumn("PROV_BILL_SVC_NM", rpad(col("PROV_BILL_SVC_NM"), <...>, " "))
df_final = df_final.withColumn("PROV_BILL_SVC_ID", rpad(col("PROV_BILL_SVC_ID"), <...>, " "))
df_final = df_final.withColumn("PROV_ENTY_CD", rpad(col("PROV_ENTY_CD"), <...>, " "))
df_final = df_final.withColumn("PROV_NM", rpad(col("PROV_NM"), <...>, " "))
df_final = df_final.withColumn("PROV_NPI", rpad(col("PROV_NPI"), <...>, " "))
df_final = df_final.withColumn("PROV_REL_GRP_PROV_ID", rpad(col("PROV_REL_GRP_PROV_ID"), <...>, " "))
df_final = df_final.withColumn("PROV_REL_GRP_PROV_NM", rpad(col("PROV_REL_GRP_PROV_NM"), <...>, " "))
df_final = df_final.withColumn("PROV_REL_IPA_PROV_ID", rpad(col("PROV_REL_IPA_PROV_ID"), <...>, " "))
df_final = df_final.withColumn("PROV_REL_IPA_PROV_NM", rpad(col("PROV_REL_IPA_PROV_NM"), <...>, " "))
df_final = df_final.withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), <...>, " "))
df_final = df_final.withColumn("PROV_SPEC_NM", rpad(col("PROV_SPEC_NM"), <...>, " "))
df_final = df_final.withColumn("PROV_TAX_ID", rpad(col("PROV_TAX_ID"), <...>, " "))
df_final = df_final.withColumn("PROV_TYP_CD", rpad(col("PROV_TYP_CD"), <...>, " "))
df_final = df_final.withColumn("PROV_TYP_NM", rpad(col("PROV_TYP_NM"), <...>, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_NO", rpad(col("LAST_UPDT_RUN_CYC_NO"), <...>, " "))
df_final = df_final.withColumn("EXTRNL_USER_ID", rpad(col("EXTRNL_USER_ID"), <...>, " "))

# Stage: seq_PROV_DM_PROV_csv_load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/PROV_DM_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)