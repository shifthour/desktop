# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmFnlDisp
# MAGIC CALLED BY: 
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-15\(9)5628 WORK_COMPNSTN_CLM \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam          2017-02-27            
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file #Env#1.Facets_Ids_WorkCompClm.dat to be send to IDS WORK_COMPNSTN_CLM table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad, to_date
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')
DrvrTable = get_widget_value('DrvrTable','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = f"""
SELECT DISTINCT
    CMC_CDML_CL_LINE.CLCL_ID,
    CMC_CDML_CL_LINE.CDML_CHG_AMT,
    CMC_CDML_CL_LINE.CDML_CONSIDER_CHG,
    CMC_CDML_CL_LINE.CDML_ALLOW,
    CMC_CDML_CL_LINE.CDML_DED_AMT,
    CMC_CDML_CL_LINE.CDML_COPAY_AMT,
    CMC_CDML_CL_LINE.CDML_COINS_AMT,
    CMC_CDML_CL_LINE.CDML_DISALL_AMT,
    CMC_CDML_CL_LINE.CDML_SB_PYMT_AMT,
    CMC_CDML_CL_LINE.CDML_PR_PYMT_AMT,
    CMC_CDML_CL_LINE.CDML_SEQ_NO,
    CMC_CDOR_LI_OVR.EXCD_ID,
    CMC_CLCL_CLAIM.CLCL_PAID_DT,
    CMC_CDOR_LI_OVR.CDOR_OR_AMT,
    CMC_CDML_CL_LINE.CDML_DISALL_EXCD,
    CMC_CLCL_CLAIM.CLCL_CUR_STS,
    CMC_CLCL_CLAIM.CLCL_EOB_EXCD_ID
FROM {FacetsOwner}.CMC_CDML_CL_LINE CMC_CDML_CL_LINE
INNER JOIN tempdb..{DrvrTable} Drvr 
    ON Drvr.CLM_ID = CMC_CDML_CL_LINE.CLCL_ID
INNER JOIN {FacetsOwner}.CMC_CLCL_CLAIM CMC_CLCL_CLAIM 
    ON CMC_CLCL_CLAIM.CLCL_ID = CMC_CDML_CL_LINE.CLCL_ID
LEFT OUTER JOIN {FacetsOwner}.CMC_CDMD_LI_DISALL CMC_CDMD_LI_DISALL 
    ON CMC_CDMD_LI_DISALL.CLCL_ID=CMC_CDML_CL_LINE.CLCL_ID
    AND CMC_CDMD_LI_DISALL.CDML_SEQ_NO=CMC_CDML_CL_LINE.CDML_SEQ_NO
LEFT OUTER JOIN {FacetsOwner}.CMC_CDOR_LI_OVR CMC_CDOR_LI_OVR 
    ON CMC_CDOR_LI_OVR.CLCL_ID=CMC_CDML_CL_LINE.CLCL_ID
    AND CMC_CDOR_LI_OVR.CDML_SEQ_NO=CMC_CDML_CL_LINE.CDML_SEQ_NO
    AND CMC_CDOR_LI_OVR.CDOR_OR_ID='AX'
WHERE CMC_CLCL_CLAIM.CLCL_CL_TYPE = 'M'
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

df_xfm_trim = (
    df_FacetsDB_Input
    .withColumn(
        "svCloseDelete",
        when(
            (trim(col("CLCL_CUR_STS")) == "14")
            | (trim(col("CLCL_CUR_STS")) == "18")
            | (trim(col("CLCL_CUR_STS")) == "93")
            | (trim(col("CLCL_CUR_STS")) == "97")
            | (trim(col("CLCL_CUR_STS")) == "99"),
            lit("CLSDDEL")
        ).otherwise(lit(""))
    )
    .withColumn(
        "svNotApplicable",
        when(
            ~(
                (col("CLCL_CUR_STS") == "02")
                | (col("CLCL_CUR_STS") == "14")
                | (col("CLCL_CUR_STS") == "18")
                | (col("CLCL_CUR_STS") == "91")
                | (col("CLCL_CUR_STS") == "93")
                | (col("CLCL_CUR_STS") == "97")
                | (col("CLCL_CUR_STS") == "99")
            ),
            lit("NA")
        ).otherwise(lit(""))
    )
    .withColumn(
        "svAccepted",
        when(
            ((col("CLCL_CUR_STS") == "02") | (col("CLCL_CUR_STS") == "91"))
            & ((col("CDML_PR_PYMT_AMT") != 0) | (col("CDML_SB_PYMT_AMT") != 0)),
            lit("Accepted")
        ).otherwise(lit(""))
    )
)

df_Lnk_Acptd = (
    df_xfm_trim
    .filter(
        (trim(col("svNotApplicable")) == "")
        & (trim(col("svCloseDelete")) == "")
        & (trim(col("svAccepted")) != "")
    )
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        lit("ACPTD").alias("FINL_DISP_CD"),
        lit(1).alias("PRIORITY")
    )
)

df_Lnk_ExcdLkp = (
    df_xfm_trim
    .filter(
        ((col("CLCL_CUR_STS") == "02") | (col("CLCL_CUR_STS") == "91"))
        & (trim(col("svAccepted")) == "")
    )
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        when(col("EXCD_ID").isNull(), lit("")).otherwise(col("EXCD_ID")).alias("EXCD_ID"),
        to_date(col("CLCL_PAID_DT")).alias("CLCL_PAID_DT"),
        col("CDML_CHG_AMT").alias("CDML_CHG_AMT"),
        col("CDML_CONSIDER_CHG").alias("CDML_CONSIDER_CHG"),
        col("CDML_ALLOW").alias("CDML_ALLOW"),
        col("CDML_DED_AMT").alias("CDML_DED_AMT"),
        col("CDML_COPAY_AMT").alias("CDML_COPAY_AMT"),
        col("CDML_COINS_AMT").alias("CDML_COINS_AMT"),
        col("CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
        col("CDML_SB_PYMT_AMT").alias("CDML_SB_PYMT_AMT"),
        col("CDML_PR_PYMT_AMT").alias("CDML_PR_PYMT_AMT"),
        when(col("CDOR_OR_AMT").isNull(), lit(0.0)).otherwise(col("CDOR_OR_AMT")).alias("CDOR_OR_AMT"),
        col("CDML_DISALL_EXCD").alias("CDML_DISALL_EXCD"),
        col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
        col("CLCL_EOB_EXCD_ID").alias("CLCL_EOB_EXCD_ID")
    )
)

df_Lnk_CloseNA = (
    df_xfm_trim
    .filter(
        (trim(col("svCloseDelete")) != "") | (trim(col("svNotApplicable")) != "")
    )
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        when(trim(col("svNotApplicable")) != "", lit("NA")).otherwise(lit("CLSDDEL")).alias("FINL_DISP_CD"),
        lit(6).alias("PRIORITY")
    )
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_excd = f"""
SELECT 
    DSALW_EXCD.EXCD_ID,
    DATE(DSALW_EXCD.EFF_DT_SK) AS EFF_DT_SK,
    DSALW_EXCD.BYPS_IN,
    CD_MPPNG.SRC_CD AS EXCD_RESP_CD,
    DATE(DSALW_EXCD.TERM_DT_SK) AS TERM_DT_SK
FROM {IDSOwner}.DSALW_EXCD DSALW_EXCD
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG 
    ON CD_MPPNG.CD_MPPNG_SK=DSALW_EXCD.EXCD_RESP_CD_SK
"""
df_EXCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_excd)
    .load()
)

df_Lookup_91 = (
    df_Lnk_ExcdLkp.alias("Lnk_ExcdLkp")
    .join(
        df_EXCD.alias("Lnk_Excd"),
        col("Lnk_ExcdLkp.EXCD_ID") == col("Lnk_Excd.EXCD_ID"),
        "left"
    )
    .select(
        col("Lnk_ExcdLkp.CLCL_ID").alias("CLCL_ID"),
        col("Lnk_ExcdLkp.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        col("Lnk_ExcdLkp.EXCD_ID").alias("EXCD_ID"),
        col("Lnk_Excd.BYPS_IN").alias("BYPS_IN"),
        col("Lnk_Excd.EXCD_RESP_CD").alias("EXCD_RESP_CD"),
        col("Lnk_ExcdLkp.CDML_CHG_AMT").alias("CDML_CHG_AMT"),
        col("Lnk_ExcdLkp.CDML_CONSIDER_CHG").alias("CDML_CONSIDER_CHG"),
        col("Lnk_ExcdLkp.CDML_ALLOW").alias("CDML_ALLOW"),
        col("Lnk_ExcdLkp.CDML_DED_AMT").alias("CDML_DED_AMT"),
        col("Lnk_ExcdLkp.CDML_COPAY_AMT").alias("CDML_COPAY_AMT"),
        col("Lnk_ExcdLkp.CDML_COINS_AMT").alias("CDML_COINS_AMT"),
        col("Lnk_ExcdLkp.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
        col("Lnk_ExcdLkp.CDML_SB_PYMT_AMT").alias("CDML_SB_PYMT_AMT"),
        col("Lnk_ExcdLkp.CDML_PR_PYMT_AMT").alias("CDML_PR_PYMT_AMT"),
        col("Lnk_ExcdLkp.CDOR_OR_AMT").alias("CDOR_OR_AMT"),
        col("Lnk_ExcdLkp.CDML_DISALL_EXCD").alias("CDML_DISALL_EXCD"),
        col("Lnk_ExcdLkp.CLCL_CUR_STS").alias("CLCL_CUR_STS"),
        col("Lnk_ExcdLkp.CLCL_EOB_EXCD_ID").alias("CLCL_EOB_EXCD_ID"),
        col("Lnk_ExcdLkp.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
        col("Lnk_Excd.EFF_DT_SK").alias("EFF_DT_SK"),
        col("Lnk_Excd.TERM_DT_SK").alias("TERM_DT_SK")
    )
)

df_Remove_Duplicates_94 = dedup_sort(
    df_Lookup_91,
    ["CLCL_ID", "CDML_SEQ_NO", "EXCD_ID", "BYPS_IN", "EXCD_RESP_CD"],
    [("CLCL_ID", "A"), ("CDML_SEQ_NO", "A"), ("EXCD_ID", "A"), ("BYPS_IN", "A"), ("EXCD_RESP_CD", "A")]
)

df_Lnk_Susp = (
    df_Remove_Duplicates_94
    .filter((col("EXCD_RESP_CD") == "N"))
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        lit("SUSP").alias("FINL_DISP_CD"),
        lit(2).alias("PRIORITY")
    )
)

df_Others = df_Remove_Duplicates_94.select(
    col("CLCL_ID").alias("CLCL_ID"),
    col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    col("EXCD_ID").alias("EXCD_ID"),
    col("BYPS_IN").alias("BYPS_IN"),
    col("EXCD_RESP_CD").alias("EXCD_RESP_CD"),
    col("CDML_CHG_AMT").alias("CDML_CHG_AMT"),
    col("CDML_CONSIDER_CHG").alias("CDML_CONSIDER_CHG"),
    col("CDML_ALLOW").alias("CDML_ALLOW"),
    col("CDML_DED_AMT").alias("CDML_DED_AMT"),
    col("CDML_COPAY_AMT").alias("CDML_COPAY_AMT"),
    col("CDML_COINS_AMT").alias("CDML_COINS_AMT"),
    col("CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    col("CDML_SB_PYMT_AMT").alias("CDML_SB_PYMT_AMT"),
    col("CDML_PR_PYMT_AMT").alias("CDML_PR_PYMT_AMT"),
    col("CDOR_OR_AMT").alias("CDOR_OR_AMT"),
    col("CDML_DISALL_EXCD").alias("CDML_DISALL_EXCD"),
    col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    col("CLCL_EOB_EXCD_ID").alias("CLCL_EOB_EXCD_ID")
)

df_xfm_others = (
    df_Others
    .withColumn(
        "svDenied",
        when(
            (col("CDML_CHG_AMT") == 0) & (col("CDML_CONSIDER_CHG") == 0),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "svNonPrice",
        when(
            col("CDML_CONSIDER_CHG") >= (
                col("CDML_DISALL_AMT")
                - when(col("EXCD_ID") == "Y08", col("CDOR_OR_AMT")).otherwise(lit(0))
            ),
            when(
                (
                    trim(
                        when(
                            col("CDML_DISALL_EXCD").isNotNull(),
                            col("CDML_DISALL_EXCD")
                        ).otherwise(lit(""))
                    ) == ""
                ) & (col("CDML_CONSIDER_CHG") == 0),
                lit("NONPRICE")
            ).otherwise(lit("DENIEDREJ"))
        ).otherwise(lit("ACPTD"))
    )
)

df_Lnk_Susp2 = (
    df_xfm_others
    .filter(
        (col("CLCL_EOB_EXCD_ID").isin("X99", "Y99", "Z99"))
    )
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        lit("SUSP").alias("FINL_DISP_CD"),
        lit(3).alias("PRIORITY")
    )
)

df_Lnk_Denied = (
    df_xfm_others
    .filter(col("svDenied") == "Y")
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        lit("DENIEDREJ").alias("FINL_DISP_CD"),
        lit(4).alias("PRIORITY")
    )
)

df_Lnk_NonPrice = (
    df_xfm_others
    .filter(col("svDenied") == "N")
    .select(
        col("CLCL_ID").alias("CLCL_ID"),
        col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        col("svNonPrice").alias("FINL_DISP_CD"),
        lit(5).alias("PRIORITY")
    )
)

common_cols_funnel = ["CLCL_ID","CDML_SEQ_NO","FINL_DISP_CD","PRIORITY"]
df_Fnl_Disp = (
    df_Lnk_CloseNA.select(common_cols_funnel)
    .unionByName(df_Lnk_Acptd.select(common_cols_funnel))
    .unionByName(df_Lnk_Susp.select(common_cols_funnel))
    .unionByName(df_Lnk_Denied.select(common_cols_funnel))
    .unionByName(df_Lnk_Susp2.select(common_cols_funnel))
    .unionByName(df_Lnk_NonPrice.select(common_cols_funnel))
)

df_RmvDups_Final = dedup_sort(
    df_Fnl_Disp,
    ["CLCL_ID"],
    [("CLCL_ID", "A"), ("PRIORITY", "A")]
)

df_Lnk_Todataset = df_RmvDups_Final.select(
    col("CLCL_ID").alias("CLCL_ID"),
    col("FINL_DISP_CD").alias("FINL_DISP_CD")
)

df_Ds_FinalDisp = df_Lnk_Todataset.select(
    rpad(col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    col("FINL_DISP_CD").alias("FINL_DISP_CD")
)

write_files(
    df_Ds_FinalDisp,
    "FinalDispDataset.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)