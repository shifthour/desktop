# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwClmInvtryExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extracts from IDS CLM_INVTRY table, groups by the 11-column natural key and counts rows.  
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                                                                         Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                                                                                                Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------                                                                          ----------------------     -------------------   
# MAGIC Hugh Sisson    2008-01-02    3531              Original program                                                                                                                                      Steph Goddard   01/17/2008
# MAGIC 
# MAGIC Pooja Sunkara  2014-02-12   5114             Rewrite in Parallel                                                                                                                                     Bhoomi Dasari     2/24/2014
# MAGIC 
# MAGIC Jag Yelavarthi    2015-10-05   #5407          Modified main extract SQL to perform CODE values denormalization and apply arithmetic operations.     Kalyan Neelam     2015-10-07
# MAGIC                                                                    This is to cover scenarios where multiple SRC_CD values map to same TRGT_CD
# MAGIC                                                                    values in code mapping. Earlier code is applying Arithmetic operations on SK value grouping
# MAGIC                                                                    and then denormalizing CODE values and causing duplicate natural key issues.

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows.
# MAGIC 
# MAGIC Also,Validate the lookup result columns from lookup operations
# MAGIC Apply Lookup operations to get Description/ Name values from CD_MPPNG table and OPS_WORK_UNIT for corresponding SK value
# MAGIC Reference lookup required to find the row with the proper min and max age range for claim age
# MAGIC Code SK lookups
# MAGIC Job name:
# MAGIC IdsEdwClmInvtrySumFExtr
# MAGIC Write CLM_INVTRY_SUM_F Data into a dataset for Pkey job.
# MAGIC Read data from source table CLM_INVTRY and CD_MPPNG table to resolve SK values to CODE values
# MAGIC 
# MAGIC Apply Arithmetic operations and get an INVTRY_CT value for each set of Natural Key row
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_db2_CLM_INVTRY_AGE_CAT_D2_In = """SELECT 
CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_SK,
CLM_INVTRY_AGE_CAT_D.SRC_SYS_CD,
CLM_INVTRY_AGE_CAT_D.CLM_MIN_AGE,
CLM_INVTRY_AGE_CAT_D.CLM_MAX_AGE,
CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_DESC 

FROM 
""" + f"{EDWOwner}" + """.CLM_INVTRY_AGE_CAT_D  CLM_INVTRY_AGE_CAT_D where CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_SK not in (0,1)"""
df_db2_CLM_INVTRY_AGE_CAT_D2_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_INVTRY_AGE_CAT_D2_In)
    .load()
)

extract_query_db2_CLM_INVTRY_AGE_CAT_D1_In = """SELECT 
CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_SK,
CLM_INVTRY_AGE_CAT_D.SRC_SYS_CD,
CLM_INVTRY_AGE_CAT_D.CLM_MIN_AGE,
CLM_INVTRY_AGE_CAT_D.CLM_MAX_AGE,
CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_DESC 

FROM 
""" + f"{EDWOwner}" + """.CLM_INVTRY_AGE_CAT_D  CLM_INVTRY_AGE_CAT_D where CLM_INVTRY_AGE_CAT_D.CLM_INVTRY_AGE_CAT_SK not in (0,1)"""
df_db2_CLM_INVTRY_AGE_CAT_D1_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_INVTRY_AGE_CAT_D1_In)
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_OPS_WORK_UNIT_In = """SELECT 
OPS_WORK_UNIT.OPS_WORK_UNIT_SK,
OPS_WORK_UNIT.OPS_WORK_UNIT_ID,
OPS_WORK_UNIT.OPS_WORK_UNIT_DESC 

FROM 
""" + f"{IDSOwner}" + """.OPS_WORK_UNIT  OPS_WORK_UNIT"""
df_db2_OPS_WORK_UNIT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_OPS_WORK_UNIT_In)
    .load()
)

extract_query_db2_CLM_INVTRY_In = """SELECT 

S.SRC_SYS_CD,
S.OPS_WORK_UNIT_ID,
S.CLM_INVTRY_SUBTYP_CD,
S.CLM_INVTRY_PEND_CAT_CD,
S.CLM_STTUS_CHG_RSN_CD,
S.INPT_DT_SK,
S.RCVD_DT_SK,
S.STTUS_DT_SK,
S.EXTR_DT_SK,
S.IN_HSE_CLM_AGE,
S.IN_LOC_CLM_AGE,
S.CLM_INVTRY_SRC_SYS_CD,
MAX(S.CRT_RUN_CYC_EXCTN_SK) as CRT_RUN_CYC_EXCTN_SK ,
MAX(S.LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK,
MIN(S.CLM_INVTRY_PEND_CAT_CD_SK) as CLM_INVTRY_PEND_CAT_CD_SK,
MIN(S.CLM_SUBTYP_CD_SK) as CLM_SUBTYP_CD_SK,
MIN(S.CLM_STTUS_CHG_RSN_CD_SK) as CLM_STTUS_CHG_RSN_CD_SK,
MIN(S.OPS_WORK_UNIT_SK) as OPS_WORK_UNIT_SK,
SUM(S.INVTRY_CT) as INVTRY_CT


FROM 
            ( SELECT 
            COALESCE(CD_MPPNG_1.TRGT_CD,'UNK') AS SRC_SYS_CD,
            CLM_INVTRY.OPS_WORK_UNIT_SK,
            COALESCE(OPS_WORK_UNIT.OPS_WORK_UNIT_ID,'UNK') AS OPS_WORK_UNIT_ID,
            CLM_INVTRY.CLM_SUBTYP_CD_SK,
            COALESCE(CD_MPPNG_2.TRGT_CD,'UNK') as CLM_INVTRY_SUBTYP_CD,
            CLM_INVTRY.CLM_INVTRY_PEND_CAT_CD_SK,
            COALESCE(CD_MPPNG_3.TRGT_CD,'UNK') as CLM_INVTRY_PEND_CAT_CD,
            CLM_INVTRY.CLM_STTUS_CHG_RSN_CD_SK,
            COALESCE(CD_MPPNG_4.TRGT_CD,'UNK') as CLM_STTUS_CHG_RSN_CD,
            CLM_INVTRY.INPT_DT_SK,
            CLM_INVTRY.RCVD_DT_SK,
            CLM_INVTRY.STTUS_DT_SK,
            CLM_INVTRY.EXTR_DT_SK,
            ( DAYS( CLM_INVTRY.EXTR_DT_SK ) - DAYS( CLM_INVTRY.INPT_DT_SK ) ) AS IN_HSE_CLM_AGE,
            ( DAYS( CLM_INVTRY.EXTR_DT_SK ) - DAYS( CLM_INVTRY.STTUS_DT_SK ) ) AS IN_LOC_CLM_AGE,
            'UWS' as CLM_INVTRY_SRC_SYS_CD,
            CLM_INVTRY.CRT_RUN_CYC_EXCTN_SK,
            CLM_INVTRY.LAST_UPDT_RUN_CYC_EXCTN_SK,
            CLM_INVTRY.INVTRY_CT

            FROM  

           """ + f"{IDSOwner}" + """.CLM_INVTRY  CLM_INVTRY LEFT OUTER JOIN
           """ + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG_1 ON CLM_INVTRY.SRC_SYS_CD_SK=CD_MPPNG_1.CD_MPPNG_SK 
           LEFT OUTER JOIN
           """ + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG_2 ON CLM_INVTRY.CLM_SUBTYP_CD_SK=CD_MPPNG_2.CD_MPPNG_SK
           LEFT OUTER JOIN
           """ + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG_3 ON CLM_INVTRY.CLM_INVTRY_PEND_CAT_CD_SK=CD_MPPNG_3.CD_MPPNG_SK
           LEFT OUTER JOIN
           """ + f"{IDSOwner}" + """.OPS_WORK_UNIT OPS_WORK_UNIT ON CLM_INVTRY.OPS_WORK_UNIT_SK=OPS_WORK_UNIT.OPS_WORK_UNIT_SK
           LEFT OUTER JOIN
           """ + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG_4 ON CLM_INVTRY.CLM_STTUS_CHG_RSN_CD_SK=CD_MPPNG_4.CD_MPPNG_SK

                      )  AS S

GROUP BY
S.SRC_SYS_CD,
S.OPS_WORK_UNIT_ID,
S.CLM_INVTRY_SUBTYP_CD,
S.CLM_INVTRY_PEND_CAT_CD,
S.CLM_STTUS_CHG_RSN_CD,
S.INPT_DT_SK,
S.RCVD_DT_SK,
S.STTUS_DT_SK,
S.EXTR_DT_SK,
S.IN_HSE_CLM_AGE,
S.IN_LOC_CLM_AGE,
S.CLM_INVTRY_SRC_SYS_CD
"""
df_db2_CLM_INVTRY_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_INVTRY_In)
    .load()
)

extract_query_db2_CD_MPPNG_In = """SELECT 

CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 

FROM

""" + f"{IDSOwner}" + """.CD_MPPNG"""
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_In)
    .load()
)

df_copy_CdMppng_1 = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_copy_CdMppng_2 = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_copy_CdMppng_3 = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cdMppng_Lkup = (
    df_db2_CLM_INVTRY_In.alias("Ink_IdsEdwClmInvtrySumFExtr_inABC")
    .join(
        df_copy_CdMppng_1.alias("ref_ClmInvtryPendCat"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_INVTRY_PEND_CAT_CD_SK") == F.col("ref_ClmInvtryPendCat.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_copy_CdMppng_2.alias("ref_ClmSttusChg"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_STTUS_CHG_RSN_CD_SK") == F.col("ref_ClmSttusChg.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_copy_CdMppng_3.alias("ref_ClmInvtrySubType"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_SUBTYP_CD_SK") == F.col("ref_ClmInvtrySubType.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_OPS_WORK_UNIT_In.alias("ref_OpsWorkUnitSk_In"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.OPS_WORK_UNIT_SK") == F.col("ref_OpsWorkUnitSk_In.OPS_WORK_UNIT_SK"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.INPT_DT_SK").alias("INPT_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.STTUS_DT_SK").alias("STTUS_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.EXTR_DT_SK").alias("EXTR_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.IN_HSE_CLM_AGE").alias("IN_HSE_CLM_AGE"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.IN_LOC_CLM_AGE").alias("IN_LOC_CLM_AGE"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_INVTRY_SRC_SYS_CD").alias("CLM_INVTRY_SRC_SYS_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.INVTRY_CT").alias("INVTRY_CT"),
        F.col("ref_ClmInvtryPendCat.TRGT_CD_NM").alias("CLM_INVTRY_PEND_CAT_NM"),
        F.col("ref_ClmSttusChg.TRGT_CD_NM").alias("CLM_STTUS_CHG_RSN_NM"),
        F.col("ref_ClmInvtrySubType.TRGT_CD_NM").alias("CLM_INVTRY_SUBTYP_NM"),
        F.col("ref_OpsWorkUnitSk_In.OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_INVTRY_PEND_CAT_CD_SK").alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_inABC.CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK")
    )
)

df_LocAge_Lkup = (
    df_cdMppng_Lkup.alias("Ink_IdsEdwClmInvtrySumFExtr_Lkup")
    .join(
        df_db2_CLM_INVTRY_AGE_CAT_D1_In.alias("ref_LocClmInvtryAgeCatD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_SRC_SYS_CD") == F.col("ref_LocClmInvtryAgeCatD.SRC_SYS_CD"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.INPT_DT_SK").alias("INPT_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.STTUS_DT_SK").alias("STTUS_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.EXTR_DT_SK").alias("EXTR_DT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.IN_HSE_CLM_AGE").alias("IN_HSE_CLM_AGE"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.IN_LOC_CLM_AGE").alias("IN_LOC_CLM_AGE"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_SRC_SYS_CD").alias("CLM_INVTRY_SRC_SYS_CD"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.INVTRY_CT").alias("INVTRY_CT"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_SUBTYP_NM").alias("CLM_INVTRY_SUBTYP_NM"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_PEND_CAT_NM").alias("CLM_INVTRY_PEND_CAT_NM"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_STTUS_CHG_RSN_NM").alias("CLM_STTUS_CHG_RSN_NM"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC"),
        F.col("ref_LocClmInvtryAgeCatD.CLM_INVTRY_AGE_CAT_SK").alias("IN_LOC_CLM_INVTRY_AGE_CAT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_INVTRY_PEND_CAT_CD_SK").alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        F.col("Ink_IdsEdwClmInvtrySumFExtr_Lkup.CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK")
    )
)

df_InHseAge_Lkup = (
    df_LocAge_Lkup.alias("lnk_InHse_Lkup")
    .join(
        df_db2_CLM_INVTRY_AGE_CAT_D2_In.alias("ref_HseAndLocAge"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_SRC_SYS_CD") == F.col("ref_HseAndLocAge.SRC_SYS_CD"),
        "left"
    )
    .select(
        F.col("lnk_InHse_Lkup.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
        F.col("lnk_InHse_Lkup.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_PEND_CAT_CD_SK").alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        F.col("lnk_InHse_Lkup.CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.col("lnk_InHse_Lkup.INPT_DT_SK").alias("INPT_DT_SK"),
        F.col("lnk_InHse_Lkup.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("lnk_InHse_Lkup.STTUS_DT_SK").alias("STTUS_DT_SK"),
        F.col("lnk_InHse_Lkup.EXTR_DT_SK").alias("EXTR_DT_SK"),
        F.col("lnk_InHse_Lkup.IN_HSE_CLM_AGE").alias("IN_HSE_CLM_AGE"),
        F.col("lnk_InHse_Lkup.IN_LOC_CLM_AGE").alias("IN_LOC_CLM_AGE"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_SRC_SYS_CD").alias("CLM_INVTRY_SRC_SYS_CD"),
        F.col("lnk_InHse_Lkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_InHse_Lkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_InHse_Lkup.INVTRY_CT").alias("INVTRY_CT"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_PEND_CAT_NM").alias("CLM_INVTRY_PEND_CAT_NM"),
        F.col("lnk_InHse_Lkup.CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        F.col("lnk_InHse_Lkup.CLM_STTUS_CHG_RSN_NM").alias("CLM_STTUS_CHG_RSN_NM"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
        F.col("lnk_InHse_Lkup.CLM_INVTRY_SUBTYP_NM").alias("CLM_INVTRY_SUBTYP_NM"),
        F.col("lnk_InHse_Lkup.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        F.col("lnk_InHse_Lkup.OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC"),
        F.col("lnk_InHse_Lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_InHse_Lkup.IN_LOC_CLM_INVTRY_AGE_CAT_SK").alias("IN_LOC_CLM_INVTRY_AGE_CAT_SK"),
        F.col("ref_HseAndLocAge.CLM_INVTRY_AGE_CAT_SK").alias("IN_HSE_CLM_INVTRY_AGE_CAT_SK")
    )
)

df_enriched = df_InHseAge_Lkup

df_main_pre = df_enriched.filter((F.col("SRC_SYS_CD") != "NA") & (F.col("SRC_SYS_CD") != "UNK"))
df_main = df_main_pre
df_main = df_main.withColumn("CLM_INVTRY_SUM_SK", F.lit(0))
df_main = df_main.withColumn("SRC_SYS_CD", F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")))
df_main = df_main.withColumn("OPS_WORK_UNIT_ID", F.when(F.col("OPS_WORK_UNIT_ID").isNull(), F.lit("UNK")).otherwise(F.col("OPS_WORK_UNIT_ID")))
df_main = df_main.withColumn("CLM_INVTRY_SUBTYP_CD", F.when(F.col("CLM_INVTRY_SUBTYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("CLM_INVTRY_SUBTYP_CD")))
df_main = df_main.withColumn("CLM_INVTRY_PEND_CAT_CD", F.when(F.col("CLM_INVTRY_PEND_CAT_CD").isNull(), F.lit("UNK")).otherwise(F.col("CLM_INVTRY_PEND_CAT_CD")))
df_main = df_main.withColumn("CLM_STTUS_CHG_RSN_CD", F.when(F.col("CLM_STTUS_CHG_RSN_CD").isNull(), F.lit("UNK")).otherwise(F.col("CLM_STTUS_CHG_RSN_CD")))
df_main = df_main.withColumn("INPT_DT_SK", F.col("INPT_DT_SK"))
df_main = df_main.withColumn("RCVD_DT_SK", F.col("RCVD_DT_SK"))
df_main = df_main.withColumn("LAST_UPDT_DT_SK", F.col("STTUS_DT_SK"))
df_main = df_main.withColumn("EXTR_DT_SK", F.col("EXTR_DT_SK"))
df_main = df_main.withColumn("IN_HSE_CLM_AGE", F.col("IN_HSE_CLM_AGE"))
df_main = df_main.withColumn("IN_LOC_CLM_AGE", F.col("IN_LOC_CLM_AGE"))
df_main = df_main.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
df_main = df_main.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
df_main = df_main.withColumn("IN_LOC_CLM_INVTRY_AGE_CAT_SK", F.when(F.col("IN_LOC_CLM_INVTRY_AGE_CAT_SK").isNull(), F.lit(0)).otherwise(F.col("IN_LOC_CLM_INVTRY_AGE_CAT_SK")))
df_main = df_main.withColumn("IN_HSE_CLM_INVTRY_AGE_CAT_SK", F.when(F.col("IN_HSE_CLM_INVTRY_AGE_CAT_SK").isNull(), F.lit(0)).otherwise(F.col("IN_HSE_CLM_INVTRY_AGE_CAT_SK")))
df_main = df_main.withColumn("OPS_WORK_UNIT_SK", F.col("OPS_WORK_UNIT_SK"))
df_main = df_main.withColumn("EXTR_DT", FORMAT_DATE_EE(F.col("EXTR_DT_SK"), "DATE", "DATE", "DB2TIMESTAMP").substr(1,10))
df_main = df_main.withColumn("INPT_DT", FORMAT_DATE_EE(F.col("INPT_DT_SK"), "DATE", "DATE", "DB2TIMESTAMP").substr(1,10))
df_main = df_main.withColumn("LAST_UPDT_DT", FORMAT_DATE_EE(F.col("STTUS_DT_SK"), "DATE", "DATE", "DB2TIMESTAMP").substr(1,10))
df_main = df_main.withColumn("RCVD_DT", FORMAT_DATE_EE(F.col("RCVD_DT_SK"), "DATE", "DATE", "DB2TIMESTAMP").substr(1,10))
df_main = df_main.withColumn("INVTRY_CT", F.col("INVTRY_CT"))
df_main = df_main.withColumn("CLM_INVTRY_PEND_CAT_NM", F.when(F.col("CLM_INVTRY_PEND_CAT_NM").isNull(), F.lit("UNK")).otherwise(F.col("CLM_INVTRY_PEND_CAT_NM")))
df_main = df_main.withColumn("CLM_INVTRY_SUBTYP_NM", F.when(F.col("CLM_INVTRY_SUBTYP_NM").isNull(), F.lit("UNK")).otherwise(F.col("CLM_INVTRY_SUBTYP_NM")))
df_main = df_main.withColumn("CLM_STTUS_CHG_RSN_NM", F.when(F.col("CLM_STTUS_CHG_RSN_NM").isNull(), F.lit("UNK")).otherwise(F.col("CLM_STTUS_CHG_RSN_NM")))
df_main = df_main.withColumn("OPS_WORK_UNIT_DESC", F.when(F.col("OPS_WORK_UNIT_DESC").isNull(), F.lit("UNK")).otherwise(F.col("OPS_WORK_UNIT_DESC")))
df_main = df_main.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
df_main = df_main.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
df_main = df_main.withColumn("CLM_INVTRY_PEND_CAT_CD_SK", F.col("CLM_INVTRY_PEND_CAT_CD_SK"))
df_main = df_main.withColumn("CLM_INVTRY_SUBTYP_CD_SK", F.col("CLM_SUBTYP_CD_SK"))
df_main = df_main.withColumn("CLM_STTUS_CHG_RSN_CD_SK", F.col("CLM_STTUS_CHG_RSN_CD_SK"))

df_unk_pre = df_enriched.limit(1)
df_unk = df_unk_pre
df_unk = df_unk.withColumn("CLM_INVTRY_SUM_SK", F.lit(0))
df_unk = df_unk.withColumn("SRC_SYS_CD", F.lit("UNK"))
df_unk = df_unk.withColumn("OPS_WORK_UNIT_ID", F.lit("UNK"))
df_unk = df_unk.withColumn("CLM_INVTRY_SUBTYP_CD", F.lit("UNK"))
df_unk = df_unk.withColumn("CLM_INVTRY_PEND_CAT_CD", F.lit("UNK"))
df_unk = df_unk.withColumn("CLM_STTUS_CHG_RSN_CD", F.lit("UNK"))
df_unk = df_unk.withColumn("INPT_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("RCVD_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("LAST_UPDT_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("EXTR_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("IN_HSE_CLM_AGE", F.lit(0))
df_unk = df_unk.withColumn("IN_LOC_CLM_AGE", F.lit(0))
df_unk = df_unk.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("IN_LOC_CLM_INVTRY_AGE_CAT_SK", F.lit(0))
df_unk = df_unk.withColumn("IN_HSE_CLM_INVTRY_AGE_CAT_SK", F.lit(0))
df_unk = df_unk.withColumn("OPS_WORK_UNIT_SK", F.lit(0))
df_unk = df_unk.withColumn("EXTR_DT", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("INPT_DT", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("LAST_UPDT_DT", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("RCVD_DT", F.lit("1753-01-01"))
df_unk = df_unk.withColumn("INVTRY_CT", F.lit(0))
df_unk = df_unk.withColumn("CLM_INVTRY_PEND_CAT_NM", F.lit("UNK"))
df_unk = df_unk.withColumn("CLM_INVTRY_SUBTYP_NM", F.lit("UNK"))
df_unk = df_unk.withColumn("CLM_STTUS_CHG_RSN_NM", F.lit("UNK"))
df_unk = df_unk.withColumn("OPS_WORK_UNIT_DESC", F.lit(None))
df_unk = df_unk.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
df_unk = df_unk.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
df_unk = df_unk.withColumn("CLM_INVTRY_PEND_CAT_CD_SK", F.lit(0))
df_unk = df_unk.withColumn("CLM_INVTRY_SUBTYP_CD_SK", F.lit(0))
df_unk = df_unk.withColumn("CLM_STTUS_CHG_RSN_CD_SK", F.lit(0))

df_na_pre = df_enriched.limit(1)
df_na = df_na_pre
df_na = df_na.withColumn("CLM_INVTRY_SUM_SK", F.lit(1))
df_na = df_na.withColumn("SRC_SYS_CD", F.lit("NA"))
df_na = df_na.withColumn("OPS_WORK_UNIT_ID", F.lit("NA"))
df_na = df_na.withColumn("CLM_INVTRY_SUBTYP_CD", F.lit("NA"))
df_na = df_na.withColumn("CLM_INVTRY_PEND_CAT_CD", F.lit("NA"))
df_na = df_na.withColumn("CLM_STTUS_CHG_RSN_CD", F.lit("NA"))
df_na = df_na.withColumn("INPT_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("RCVD_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("LAST_UPDT_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("EXTR_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("IN_HSE_CLM_AGE", F.lit(0))
df_na = df_na.withColumn("IN_LOC_CLM_AGE", F.lit(0))
df_na = df_na.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
df_na = df_na.withColumn("IN_LOC_CLM_INVTRY_AGE_CAT_SK", F.lit(1))
df_na = df_na.withColumn("IN_HSE_CLM_INVTRY_AGE_CAT_SK", F.lit(1))
df_na = df_na.withColumn("OPS_WORK_UNIT_SK", F.lit(1))
df_na = df_na.withColumn("EXTR_DT", F.lit("1753-01-01"))
df_na = df_na.withColumn("INPT_DT", F.lit("1753-01-01"))
df_na = df_na.withColumn("LAST_UPDT_DT", F.lit("1753-01-01"))
df_na = df_na.withColumn("RCVD_DT", F.lit("1753-01-01"))
df_na = df_na.withColumn("INVTRY_CT", F.lit(0))
df_na = df_na.withColumn("CLM_INVTRY_PEND_CAT_NM", F.lit("NA"))
df_na = df_na.withColumn("CLM_INVTRY_SUBTYP_NM", F.lit("NA"))
df_na = df_na.withColumn("CLM_STTUS_CHG_RSN_NM", F.lit("NA"))
df_na = df_na.withColumn("OPS_WORK_UNIT_DESC", F.lit(None))
df_na = df_na.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
df_na = df_na.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
df_na = df_na.withColumn("CLM_INVTRY_PEND_CAT_CD_SK", F.lit(1))
df_na = df_na.withColumn("CLM_INVTRY_SUBTYP_CD_SK", F.lit(1))
df_na = df_na.withColumn("CLM_STTUS_CHG_RSN_CD_SK", F.lit(1))

df_fnl_dataLinks = df_main.unionByName(df_unk).unionByName(df_na)

final_cols = [
    "CLM_INVTRY_SUM_SK",
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "CLM_INVTRY_PEND_CAT_CD",
    "CLM_STTUS_CHG_RSN_CD",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "LAST_UPDT_DT_SK",
    "EXTR_DT_SK",
    "IN_HSE_CLM_AGE",
    "IN_LOC_CLM_AGE",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "IN_LOC_CLM_INVTRY_AGE_CAT_SK",
    "IN_HSE_CLM_INVTRY_AGE_CAT_SK",
    "OPS_WORK_UNIT_SK",
    "EXTR_DT",
    "INPT_DT",
    "LAST_UPDT_DT",
    "RCVD_DT",
    "INVTRY_CT",
    "CLM_INVTRY_PEND_CAT_NM",
    "CLM_INVTRY_SUBTYP_NM",
    "CLM_STTUS_CHG_RSN_NM",
    "OPS_WORK_UNIT_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_INVTRY_PEND_CAT_CD_SK",
    "CLM_INVTRY_SUBTYP_CD_SK",
    "CLM_STTUS_CHG_RSN_CD_SK"
]
df_final_select = df_fnl_dataLinks.select(final_cols)

df_final_select = df_final_select.withColumn("INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " "))
df_final_select = df_final_select.withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
df_final_select = df_final_select.withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
df_final_select = df_final_select.withColumn("EXTR_DT_SK", F.rpad(F.col("EXTR_DT_SK"), 10, " "))
df_final_select = df_final_select.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final_select = df_final_select.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))

write_files(
    df_final_select,
    "CLM_INVTRY_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)