# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ralph Tucker 11/17/2004 -   Originally Programmed
# MAGIC Brent Leland   08/30/2005 -   Changed column output order to match table.
# MAGIC BJ Luce          11/21/2005  -   add DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK, default to NA and 1
# MAGIC BJ Luce          1/17/2006        pull DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK from IDS 
# MAGIC Brent Leland   04/04/2006      Changed parameters to environment parameters
# MAGIC                                                               Removed trim() off SK values.
# MAGIC Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC 
# MAGIC Rama Kamjula  10/09/2013    Rewritten from Server to parallel version                                                                                                  EnterpriseWrhsDevl       Jag Yelavarthi         2013-12-22

# MAGIC JobName: IdsClmFactCrteHash
# MAGIC Job creates datasets for CLM_F  Extr job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter setup
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

# Database configurations
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

# --------------------------------------------------------------------------------
# Stage: db2_RVNU (DB2ConnectorPX)
df_db2_RVNU = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        """SELECT 

LN.CLM_SK,
max(RVNU_HIER_NO)  RVNU_HIER_NO

FROM #$IDSOwner#.W_EDW_ETL_DRVR DRVR, #$IDSOwner#.CLM_LN LN, #$IDSOwner#.RVNU_CD RVNU, #$IDSOwner#.W_RVNU_HIER HIER 

WHERE LN.CLM_ID = DRVR.CLM_ID
and LN.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK

and LN.CLM_LN_RVNU_CD_SK = RVNU.RVNU_CD_SK
and RVNU.RVNU_CD = HIER.RVNU_CD

group by LN.CLM_SK;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: odbc_Exp_sub_cat_rvnu_hier (ODBCConnectorPX)
df_odbc_Exp_sub_cat_rvnu_hier = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"""SELECT 

RVNU_HIER_NO, 
EXP_SUB_CAT_CD 

FROM #$UWSOwner#.EXP_SUB_CAT_RVNU_HIER;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_RVNU (PxLookup)
df_lkp_RVNU = (
    df_db2_RVNU.alias("lnk_RVNU")
    .join(
        df_odbc_Exp_sub_cat_rvnu_hier.alias("lnk_exp_sub_cat_rvnu_hier"),
        how="inner"
    )
    .select(
        col("lnk_RVNU.CLM_SK").alias("CLM_SK"),
        col("lnk_exp_sub_cat_rvnu_hier.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_RVNU (CTransformerStage)
df_xfm_RVNU = df_lkp_RVNU.select(
    col("CLM_SK"),
    col("EXP_SUB_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: ds_clm_fact_exp_clm_rvnu_subcat (PxDataSet)
df_clm_fact_exp_clm_rvnu_subcat = df_xfm_RVNU.select(
    rpad(col("CLM_SK"), <...>, " ").alias("CLM_SK"),
    rpad(col("EXP_SUB_CAT_CD"), <...>, " ").alias("EXP_SUB_CAT_CD")
)
write_files(
    df_clm_fact_exp_clm_rvnu_subcat,
    f"{adls_path}/ds/clm_fact_exp_clm_rvnu_subcat.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: db2_DNTL_CAT (DB2ConnectorPX)
df_db2_DNTL_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 

CLM.CLM_SK,
max(DNTL_HIER_NO)  DNTL_HIER_NO

FROM #$IDSOwner#.CLM CLM,#$IDSOwner#.W_EDW_ETL_DRVR DRVR, #$IDSOwner#.DNTL_CLM_LN LN, #$IDSOwner#.CD_MPPNG NG1,
            #$IDSOwner#.W_DNTL_CAT_HIER HIER 

WHERE CLM.CLM_ID=DRVR.CLM_ID
and CLM.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
and LN.CLM_ID=DRVR.CLM_ID
AND LN.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
and LN.DNTL_CLM_LN_DNTL_CAT_CD_SK = NG1.CD_MPPNG_SK
and NG1.TRGT_CD = HIER.DNTL_CAT_CD

group by CLM.CLM_SK;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: odbc_Exp_sub_cat_dntl_hier (ODBCConnectorPX)
df_odbc_Exp_sub_cat_dntl_hier = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"""SELECT 

DNTL_HIER_NO, 
EXP_SUB_CAT_CD 

FROM #$UWSOwner#.EXP_SUB_CAT_DNTL_HIER;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_DNTL (PxLookup)
df_lkp_DNTL = (
    df_db2_DNTL_CAT.alias("lnk_DNTL_CAT")
    .join(
        df_odbc_Exp_sub_cat_dntl_hier.alias("lnk_exp_sub_cat_dntl_hier"),
        how="inner"
    )
    .select(
        col("lnk_DNTL_CAT.CLM_SK").alias("CLM_SK"),
        col("lnk_exp_sub_cat_dntl_hier.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_DNTL_CAT (CTransformerStage)
df_xfm_DNTL_CAT = df_lkp_DNTL.select(
    col("CLM_SK"),
    col("EXP_SUB_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: ds_clm_fact_exp_clm_dntl_cat_subcat (PxDataSet)
df_clm_fact_exp_clm_dntl_cat_subcat = df_xfm_DNTL_CAT.select(
    rpad(col("CLM_SK"), <...>, " ").alias("CLM_SK"),
    rpad(col("EXP_SUB_CAT_CD"), <...>, " ").alias("EXP_SUB_CAT_CD")
)
write_files(
    df_clm_fact_exp_clm_dntl_cat_subcat,
    f"{adls_path}/ds/clm_fact_exp_clm_dntl_cat_subcat.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: odbc_drug_tier_exp_sub_cat (ODBCConnectorPX)
df_odbc_drug_tier_exp_sub_cat = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"""SELECT 

DRUG_CLM_TIER_CD, 
EXP_SUB_CAT_CD, 
USER_ID, 
LAST_UPDT_DT_SK 

FROM #$UWSOwner#.DRUG_TIER_EXP_SUB_CAT;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_clm_fact_exp_drug_tier_subcat (PxDataSet)
df_clm_fact_exp_drug_tier_subcat = df_odbc_drug_tier_exp_sub_cat.select(
    rpad(col("DRUG_CLM_TIER_CD"), <...>, " ").alias("DRUG_CLM_TIER_CD"),
    rpad(col("EXP_SUB_CAT_CD"), <...>, " ").alias("EXP_SUB_CAT_CD"),
    rpad(col("USER_ID"), <...>, " ").alias("USER_ID"),
    rpad(col("LAST_UPDT_DT_SK"), <...>, " ").alias("LAST_UPDT_DT_SK")
)
write_files(
    df_clm_fact_exp_drug_tier_subcat,
    f"{adls_path}/ds/clm_fact_exp_drug_tier_subcat.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: odbc_prov_spec_exp_ovrd (ODBCConnectorPX)
df_odbc_prov_spec_exp_ovrd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"""SELECT 

PROV_ID, 
PROV_SPEC_CD 

FROM #$UWSOwner#.PROV_SPEC_EXP_OVRD;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: odbc_PROV_SPEC (ODBCConnectorPX)
df_odbc_PROV_SPEC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"""SELECT 

PROV_SPEC_CD, 
EXP_SUB_CAT_CD 

FROM #$UWSOwner#.PROV_SPEC;"""
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Copy_315 (PxCopy)
df_copy_315_out1 = df_odbc_PROV_SPEC.select(
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
)
df_copy_315_out2 = df_odbc_PROV_SPEC.select(
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: ds_clm_fact_exp_spec_subcat (PxDataSet)
df_clm_fact_exp_spec_subcat = df_copy_315_out1.select(
    rpad(col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    rpad(col("EXP_SUB_CAT_CD"), <...>, " ").alias("EXP_SUB_CAT_CD")
)
write_files(
    df_clm_fact_exp_spec_subcat,
    f"{adls_path}/ds/clm_fact_exp_spec_subcat.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: lkp_PROV_SPEC (PxLookup)
df_lkp_PROV_SPEC = (
    df_odbc_prov_spec_exp_ovrd.alias("lnk_prov_spec_exp_ovrd")
    .join(
        df_copy_315_out2.alias("lnk_PROV_SPEC"),
        (col("lnk_prov_spec_exp_ovrd.PROV_SPEC_CD") == col("lnk_PROV_SPEC.PROV_SPEC_CD")),
        "inner"
    )
    .select(
        col("lnk_prov_spec_exp_ovrd.PROV_ID").alias("PROV_ID"),
        col("lnk_PROV_SPEC.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: ds_clm_fact_exp_prov_ovrd (PxDataSet)
df_clm_fact_exp_prov_ovrd = df_lkp_PROV_SPEC.select(
    rpad(col("PROV_ID"), <...>, " ").alias("PROV_ID"),
    rpad(col("EXP_SUB_CAT_CD"), <...>, " ").alias("EXP_SUB_CAT_CD")
)
write_files(
    df_clm_fact_exp_prov_ovrd,
    f"{adls_path}/ds/clm_fact_exp_prov_ovrd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)