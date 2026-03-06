# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsEdwProductDsgnSumFSeq        
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is an extract from Product component table into the PROD_DSGN_SUM_F table 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                Project/Altiris/ 
# MAGIC Developer                        Date               User Story #                   Change Description                                                                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    --------------------------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------            
# MAGIC Anitha Perumal                2019-03-05     5832-Spira Care              Originally Programmed                                                                              EnterpriseDev1             Kalyan Neelam            2019-03-12
# MAGIC Anitha Perumal                2019-03-26     5832-Spira Care              Replaced SPIRA_BNF_ID from N to NA                                                  EnterpriseDev1             Kalyan Neelam            2019-03-26
# MAGIC                                                                                                       in the upp transformer
# MAGIC Goutham Kalidnidi            2019-04-15     103729                           Code fixed to set correct Termination                                                        EnterpriseDev1             Hugh Sisson                2019-04-24
# MAGIC                                                                5832-Spira Care              Dates (PROD_CMPNT_TERM_DT_SK)
# MAGIC                                                                                                       and Spira Benefit ID (SPIRA_BNF_ID) 
# MAGIC Goutham Kalidnidi            2019-05-08     Bug 111518                      Code fixed to set correct Termination                                                         EnterpriseDev1             Jaideep Mankala     05/09/2019           
# MAGIC                                                                5832-Spira Care              Dates (PROD_CMPNT_TERM_DT_SK)
# MAGIC                                                                                                       for indicators set to future eff dates
# MAGIC Goutham Kalidnidi            2019-06-10     Bug 116896                     Spira Care Fix for Future Reporting                                                                                              Kalyan Neelam            2019-06-13     
# MAGIC                                                                5832-Spira Care              
# MAGIC Goutham Kalidindi            2019-07-01    Task - 128433                Fix for Trn dates which are incorrect                                                             EnterpriseDev1        Jaideep Mankala         07/01/2019
# MAGIC                                                                5832-Spira Care             and filx the files SPIRA_BNF_ID with
# MAGIC                                                                                                      correct values
# MAGIC Jaideep Mankala           2019-10-24       ProdAbort                      Added Trim function to remove extra spaces on PROD_ID field                  EnterpriseDev2          Hugh Sisson                2019-10-24
# MAGIC 
# MAGIC 
# MAGIC Gioutham Kalidindi         2019-10-28      US- 171093                   Deactivate Spira Indicators                                                                           EnterpriseDev2         Jaideep Mankala        10/30/2019

# MAGIC This file holds the default inserts for all prod_id with 'N' for Spira and 'NA' for UPP that is created in job IdsEdwProdDsgnSumFExtr
# MAGIC Spira extarct for all prod_id
# MAGIC Job:
# MAGIC IdsEdwProdDsgnSumFXfm
# MAGIC Table:
# MAGIC  PROD_DSGN_SUM_F      
# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC UPP extarct for all prod_id - Deactiating this Query NOT to fect UPP IND
# MAGIC Drops duplicates for products with same eff date for spira and upp records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, when, trim as pyspark_trim, rpad, to_date, date_sub
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>  # (No shared containers referenced; placeholder if needed)
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# COMMAND ----------
# Read from sf_default_ins_prod_id (PxSequentialFile)
schema_sf_default_ins_prod_id = StructType([
    StructField("PROD_SK", IntegerType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("PROD_CMPNT_EFF_DT_SK", StringType(), nullable=False),
    StructField("PROD_CMPNT_TERM_DT_SK", StringType(), nullable=False),
    StructField("SPIRA_BNF_ID", StringType(), nullable=False),
    StructField("UPP_XREF_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=True)
])
file_path_sf_default_ins_prod_id = f"{adls_path}/verified/PROD_DSGN_SUM_F_Extr.dat"
df_sf_default_ins_prod_id = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_sf_default_ins_prod_id)
    .load(file_path_sf_default_ins_prod_id)
)

# Prepare output link (lnk_default_ins) columns for funnel input later
df_sf_default_ins_prod_id_lnk_default_ins = df_sf_default_ins_prod_id.select(
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# COMMAND ----------
# Read from db2_ProdCmpnt_Spira (DB2ConnectorPX, Database=IDS)
extract_query_db2_ProdCmpnt_Spira = """
SELECT DISTINCT
TRIM(PROD_ID) AS PROD_ID,
PROD_SK,
SRC_SYS_CD_SK,
MIN(PROD_CMPNT_EFF_DT_SK) AS PROD_CMPNT_EFF_DT_SK,
MAX(PROD_CMPNT_TERM_DT_SK) AS PROD_CMPNT_TERM_DT_SK,
SRC_SYS_CD,
PROD_CMPNT_TYP_CD_SK,
SPIRA_BNF_ID,
PROD_CMPNT_TYP_CD,
UPP_XREF_ID
FROM 
(
    SELECT DISTINCT
    PROD_CMPNT.PROD_ID,
    PROD_CMPNT.PROD_SK,
    PROD_CMPNT.SRC_SYS_CD_SK,
    PROD_CMPNT.PROD_CMPNT_EFF_DT_SK,
    PROD_CMPNT.PROD_CMPNT_TERM_DT_SK,
    COALESCE(CD1.TRGT_CD,'UNK') AS SRC_SYS_CD,
    CM1.CD_MPPNG_SK AS PROD_CMPNT_TYP_CD_SK,
    CM2.TRGT_CD AS SPIRA_BNF_ID,
    CM1.TRGT_CD AS PROD_CMPNT_TYP_CD,
    'NA' AS UPP_XREF_ID
    FROM   """ + f"{IDSOwner}" + """.PROD PROD
    JOIN   """ + f"{IDSOwner}" + """.PROD_CMPNT PROD_CMPNT  ON PROD.PROD_ID=PROD_CMPNT.PROD_ID
    JOIN   """ + f"{IDSOwner}" + """.CD_MPPNG  CD1         ON PROD_CMPNT.SRC_SYS_CD_SK= CD1.CD_MPPNG_SK
    JOIN   """ + f"{IDSOwner}" + """.BNF_SUM_DTL BNF       ON BNF.PROD_CMPNT_PFX_ID = PROD_CMPNT.PROD_CMPNT_PFX_ID
    JOIN   """ + f"{IDSOwner}" + """.CD_MPPNG CM1          ON CM1.CD_MPPNG_SK = PROD_CMPNT.PROD_CMPNT_TYP_CD_SK
    JOIN   """ + f"{IDSOwner}" + """.CD_MPPNG CM2          ON CM2.CD_MPPNG_SK = BNF.BNF_SUM_DTL_TYP_CD_SK
    WHERE  CM1.TRGT_CD = 'BSBS'
    AND    CM2.TRGT_CD = 'CLNC'
    AND    PROD_CMPNT.PROD_CMPNT_SK NOT IN  (0,1)
    ORDER BY  COALESCE(CD1.TRGT_CD,'UNK'), PROD_CMPNT.PROD_ID, PROD_CMPNT.PROD_CMPNT_EFF_DT_SK
)
GROUP BY
PROD_ID,
PROD_SK,
SRC_SYS_CD_SK,
SRC_SYS_CD,
PROD_CMPNT_EFF_DT_SK,
PROD_CMPNT_TYP_CD_SK,
SPIRA_BNF_ID,
PROD_CMPNT_TYP_CD,
UPP_XREF_ID
"""
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_ProdCmpnt_Spira = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ProdCmpnt_Spira)
    .load()
)

# COMMAND ----------
# cpy_spira (PxCopy): produce two outputs from df_db2_ProdCmpnt_Spira

# Output link "lnk_cpy_spira" → Jn1_LO_prod_id
df_cpy_spira_lnk_cpy_spira = df_db2_ProdCmpnt_Spira.select(
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("SPIRA_PROD_CMPNT_EFF_DT_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("SPIRA_PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID_SPIRA"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID_SPIRA"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# Output link "lnk_spira_left" → Jn_LO_prod_id
df_cpy_spira_lnk_spira_left = df_db2_ProdCmpnt_Spira.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("PROD_CMPNT_TYP_CD_SK").alias("PROD_CMPNT_TYP_CD_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("PROD_CMPNT_TYP_CD").alias("PROD_CMPNT_TYP_CD"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# COMMAND ----------
# Read from db2_ProdCmpnt_Upp (DB2ConnectorPX, Database=IDS)
extract_query_db2_ProdCmpnt_Upp = """
SELECT DISTINCT
TRIM(PROD_ID) AS PROD_ID,
PROD_SK,
SRC_SYS_CD_SK,
MIN(PROD_CMPNT_EFF_DT_SK) AS PROD_CMPNT_EFF_DT_SK,
MAX(PROD_CMPNT_TERM_DT_SK) AS PROD_CMPNT_TERM_DT_SK,
SRC_SYS_CD,
PROD_CMPNT_TYP_CD_SK,
SPIRA_BNF_ID,
PROD_CMPNT_TYP_CD,
UPP_XREF_ID
FROM
(
    SELECT DISTINCT
    TRIM(PROD_CMPNT.PROD_ID) AS PROD_ID,
    PROD_CMPNT.PROD_SK,
    PROD_CMPNT.SRC_SYS_CD_SK,
    PROD_CMPNT.PROD_CMPNT_PFX_ID AS UPP_XREF_ID,
    PROD_CMPNT.PROD_CMPNT_EFF_DT_SK,
    PROD_CMPNT.PROD_CMPNT_TERM_DT_SK,
    COALESCE(CD1.TRGT_CD,'UNK') AS SRC_SYS_CD,
    CM1.CD_MPPNG_SK AS PROD_CMPNT_TYP_CD_SK,
    CM1.TRGT_CD AS PROD_CMPNT_TYP_CD,
    'NA' AS SPIRA_BNF_ID
    FROM """ + f"{IDSOwner}" + """.PROD PROD
    JOIN """ + f"{IDSOwner}" + """.PROD_CMPNT PROD_CMPNT ON PROD.PROD_ID=PROD_CMPNT.PROD_ID
    JOIN """ + f"{IDSOwner}" + """.CD_MPPNG  CD1         ON PROD_CMPNT.SRC_SYS_CD_SK= CD1.CD_MPPNG_SK
    JOIN """ + f"{IDSOwner}" + """.CD_MPPNG CM1          ON CM1.CD_MPPNG_SK = PROD_CMPNT.PROD_CMPNT_TYP_CD_SK
    WHERE CM1.TRGT_CD = 'UPP'
    AND PROD_CMPNT.PROD_CMPNT_SK NOT IN  (0,1)
    AND 1=0
    ORDER BY COALESCE(CD1.TRGT_CD,'UNK'), PROD_ID, PROD_CMPNT.PROD_CMPNT_EFF_DT_SK
)
GROUP BY
PROD_ID,
PROD_SK,
SRC_SYS_CD_SK,
SRC_SYS_CD,
PROD_CMPNT_EFF_DT_SK,
PROD_CMPNT_TYP_CD_SK,
SPIRA_BNF_ID,
PROD_CMPNT_TYP_CD,
UPP_XREF_ID
"""
df_db2_ProdCmpnt_Upp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ProdCmpnt_Upp)
    .load()
)

# COMMAND ----------
# cpy_upp (PxCopy): produce two outputs from df_db2_ProdCmpnt_Upp

# Output link "lnk_cpy_upp" → Jn_LO_prod_id
df_cpy_upp_lnk_cpy_upp = df_db2_ProdCmpnt_Upp.select(
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("UPP_PROD_CMPNT_EFF_DT_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("UPP_PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID_UPP"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID_UPP"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# Output link "lnk_upp_left" → Jn1_LO_prod_id
df_cpy_upp_lnk_upp_left = df_db2_ProdCmpnt_Upp.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("PROD_CMPNT_TYP_CD_SK").alias("PROD_CMPNT_TYP_CD_SK"),
    col("PROD_CMPNT_TYP_CD").alias("PROD_CMPNT_TYP_CD"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# COMMAND ----------
# Jn_LO_prod_id (PxJoin) left outer join on (PROD_ID, SRC_SYS_CD)
df_jn_lo_prod_id = (
    df_cpy_spira_lnk_spira_left.alias("lnk_spira_left")
    .join(
        df_cpy_upp_lnk_cpy_upp.alias("lnk_cpy_upp"),
        (col("lnk_spira_left.PROD_ID") == col("lnk_cpy_upp.PROD_ID"))
        & (col("lnk_spira_left.SRC_SYS_CD") == col("lnk_cpy_upp.SRC_SYS_CD")),
        "left"
    )
)

df_jn_lo_prod_id_select = df_jn_lo_prod_id.select(
    col("lnk_spira_left.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_spira_left.PROD_ID").alias("PROD_ID"),
    col("lnk_spira_left.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("lnk_spira_left.PROD_SK").alias("PROD_SK"),
    col("lnk_spira_left.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("lnk_spira_left.PROD_CMPNT_TYP_CD_SK").alias("PROD_CMPNT_TYP_CD_SK"),
    col("lnk_spira_left.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("lnk_spira_left.UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("lnk_spira_left.PROD_CMPNT_TYP_CD").alias("PROD_CMPNT_TYP_CD"),
    col("lnk_spira_left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_cpy_upp.UPP_PROD_CMPNT_EFF_DT_SK").alias("UPP_PROD_CMPNT_EFF_DT_SK"),
    col("lnk_cpy_upp.UPP_PROD_CMPNT_TERM_DT_SK").alias("UPP_PROD_CMPNT_TERM_DT_SK"),
    col("lnk_cpy_upp.SPIRA_BNF_ID_UPP").alias("SPIRA_BNF_ID_UPP"),
    col("lnk_cpy_upp.UPP_XREF_ID_UPP").alias("UPP_XREF_ID_UPP")
)

# COMMAND ----------
# xfm_spira_term_dt (CTransformerStage)
df_xfm_spira_term_dt_stagevar = (
    df_jn_lo_prod_id_select
    # svUppXrefId
    .withColumn(
        "svUppXrefId",
        when(
            pyspark_trim(
                when(col("UPP_PROD_CMPNT_EFF_DT_SK").isNotNull(), col("UPP_PROD_CMPNT_EFF_DT_SK")).otherwise("")
            ).notEqual(""),
            when(
                (to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") >= to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"))
                & (to_date(col("PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd") == to_date(col("UPP_PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd")),
                col("UPP_XREF_ID_UPP")
            ).otherwise("NA")
        ).otherwise("NA")
    )
    # svSpiraProdCmpntTermDt
    .withColumn(
        "svSpiraProdCmpntTermDt",
        when(
            pyspark_trim(
                when(col("UPP_PROD_CMPNT_EFF_DT_SK").isNotNull(), col("UPP_PROD_CMPNT_EFF_DT_SK")).otherwise("")
            ).notEqual(""),
            when(
                (to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") > to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"))
                & (to_date(col("UPP_PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd") == to_date(col("PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd")),
                date_sub(to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"), 1)
            ).otherwise(
                when(
                    (to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") > to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"))
                    & (to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") < to_date(col("PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd")),
                    date_sub(to_date(col("UPP_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"), 1)
                ).otherwise(col("PROD_CMPNT_TERM_DT_SK"))
            )
        ).otherwise(col("PROD_CMPNT_TERM_DT_SK"))
    )
)

df_xfm_spira_term_dt = df_xfm_spira_term_dt_stagevar

# Output link "lnk_spira_ins" → Fnl_prod_id
df_spira_ins = df_xfm_spira_term_dt.select(
    col("PROD_SK").alias("PROD_SK"),
    pyspark_trim(col("PROD_ID")).alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("svSpiraProdCmpntTermDt").alias("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("svUppXrefId").alias("UPP_XREF_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# COMMAND ----------
# Jn1_LO_prod_id (PxJoin) left outer join on (PROD_ID, SRC_SYS_CD)
df_jn1_lo_prod_id = (
    df_cpy_upp_lnk_upp_left.alias("lnk_upp_left")
    .join(
        df_cpy_spira_lnk_cpy_spira.alias("lnk_cpy_spira"),
        (col("lnk_upp_left.PROD_ID") == col("lnk_cpy_spira.PROD_ID"))
        & (col("lnk_upp_left.SRC_SYS_CD") == col("lnk_cpy_spira.SRC_SYS_CD")),
        "left"
    )
)

df_jn1_lo_prod_id_select = df_jn1_lo_prod_id.select(
    col("lnk_upp_left.PROD_SK").alias("PROD_SK"),
    col("lnk_upp_left.PROD_ID").alias("PROD_ID"),
    col("lnk_upp_left.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_upp_left.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("lnk_upp_left.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("lnk_upp_left.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("lnk_upp_left.UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("lnk_upp_left.PROD_CMPNT_TYP_CD_SK").alias("PROD_CMPNT_TYP_CD_SK"),
    col("lnk_upp_left.PROD_CMPNT_TYP_CD").alias("PROD_CMPNT_TYP_CD"),
    col("lnk_upp_left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_cpy_spira.SPIRA_PROD_CMPNT_EFF_DT_SK").alias("SPIRA_PROD_CMPNT_EFF_DT_SK"),
    col("lnk_cpy_spira.SPIRA_PROD_CMPNT_TERM_DT_SK").alias("SPIRA_PROD_CMPNT_TERM_DT_SK"),
    col("lnk_cpy_spira.SPIRA_BNF_ID_SPIRA").alias("SPIRA_BNF_ID_SPIRA"),
    col("lnk_cpy_spira.UPP_XREF_ID_SPIRA").alias("UPP_XREF_ID_SPIRA")
)

# COMMAND ----------
# xfm_upp_term_dt (CTransformerStage)
df_xfm_upp_term_dt_stagevar = (
    df_jn1_lo_prod_id_select
    # svSpiraBnfId
    .withColumn(
        "svSpiraBnfId",
        when(
            pyspark_trim(
                when(col("SPIRA_PROD_CMPNT_EFF_DT_SK").isNotNull(), col("SPIRA_PROD_CMPNT_EFF_DT_SK")).otherwise("")
            ).notEqual(""),
            when(
                (to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") >= to_date(col("SPIRA_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"))
                & (to_date(col("PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd") == to_date(col("SPIRA_PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd")),
                col("SPIRA_BNF_ID_SPIRA")
            ).otherwise(
                when(
                    (to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") < to_date(col("SPIRA_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd")),
                    col("SPIRA_BNF_ID_SPIRA")
                ).otherwise("NA")
            )
        ).otherwise("NA")
    )
    # svUppProdCmpntTermDt
    .withColumn(
        "svUppProdCmpntTermDt",
        when(
            pyspark_trim(
                when(col("SPIRA_PROD_CMPNT_EFF_DT_SK").isNotNull(), col("SPIRA_PROD_CMPNT_EFF_DT_SK")).otherwise("")
            ).notEqual(""),
            when(
                (to_date(col("SPIRA_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd") > to_date(col("PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"))
                & (to_date(col("SPIRA_PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd") == to_date(col("PROD_CMPNT_TERM_DT_SK"), "yyyy-MM-dd")),
                date_sub(to_date(col("SPIRA_PROD_CMPNT_EFF_DT_SK"), "yyyy-MM-dd"), 1)
            ).otherwise(col("PROD_CMPNT_TERM_DT_SK"))
        ).otherwise(col("PROD_CMPNT_TERM_DT_SK"))
    )
)

df_xfm_upp_term_dt = df_xfm_upp_term_dt_stagevar

# Output link "lnk_upp_ins" → Fnl_prod_id
df_upp_ins = df_xfm_upp_term_dt.select(
    col("PROD_SK").alias("PROD_SK"),
    pyspark_trim(col("PROD_ID")).alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("svUppProdCmpntTermDt").alias("PROD_CMPNT_TERM_DT_SK"),
    col("svSpiraBnfId").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# COMMAND ----------
# Fnl_prod_id (PxFunnel) → funnel these three inputs: lnk_default_ins, lnk_spira_ins, lnk_upp_ins
# They share same column structure

df_fnl_prod_id = df_sf_default_ins_prod_id_lnk_default_ins.select(
    "PROD_SK",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK",
    "PROD_CMPNT_TERM_DT_SK",
    "SPIRA_BNF_ID",
    "UPP_XREF_ID",
    "SRC_SYS_CD"
).unionByName(
    df_spira_ins.select(
        "PROD_SK",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "SPIRA_BNF_ID",
        "UPP_XREF_ID",
        "SRC_SYS_CD"
    )
).unionByName(
    df_upp_ins.select(
        "PROD_SK",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "SPIRA_BNF_ID",
        "UPP_XREF_ID",
        "SRC_SYS_CD"
    )
)

# COMMAND ----------
# srt_prod_id (PxSort) by PROD_ID, PROD_CMPNT_EFF_DT_SK, PROD_CMPNT_TERM_DT_SK, SPIRA_BNF_ID
df_srt_prod_id = df_fnl_prod_id.orderBy(
    col("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID")
)

# COMMAND ----------
# rmdup_prod_id (PxRemDup) → use dedup_sort helper w/ partition_cols=["PROD_ID","PROD_CMPNT_EFF_DT_SK"], 
# sorted by [("PROD_ID","A"),("PROD_CMPNT_EFF_DT_SK","A"),("PROD_CMPNT_TERM_DT_SK","A"),("SPIRA_BNF_ID","A")]
df_rmdup_prod_id = dedup_sort(
    df_srt_prod_id,
    ["PROD_ID", "PROD_CMPNT_EFF_DT_SK"],
    [("PROD_ID", "A"), ("PROD_CMPNT_EFF_DT_SK", "A"), ("PROD_CMPNT_TERM_DT_SK", "A"), ("SPIRA_BNF_ID", "A")]
)

# COMMAND ----------
# xfm_run_cyc (CTransformerStage)

df_xfm_run_cyc = df_rmdup_prod_id.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate)
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle)
)

# COMMAND ----------
# seq_ProdDsgnSumFPKey (PxSequentialFile) - write file "verified/PROD_DSGN_SUM_F_Pkey.dat"

# Apply rpad if columns have SqlType=char or varchar with length
df_seq_ProdDsgnSumFPKey = df_xfm_run_cyc.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    rpad(col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK").alias("PROD_SK"),
    rpad(col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("UPP_XREF_ID").alias("UPP_XREF_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

file_path_seq_ProdDsgnSumFPKey = f"{adls_path}/verified/PROD_DSGN_SUM_F_Pkey.dat"
write_files(
    df_seq_ProdDsgnSumFPKey,
    file_path_seq_ProdDsgnSumFPKey,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)