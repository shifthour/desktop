# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : EyeMedClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  EyeMed Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                           Date                 Project/Altiris #          Change Description                                                         Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                    --------------------         ------------------------          -----------------------------------------------------------------------                 --------------------------------         -------------------------------   ----------------------------      
# MAGIC Madhavan B                  2018-03-14              5744                        Original Programming                                                       IntegrateDev2                  Kalyan Neelam          2018-04-09


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdErr = get_widget_value('SrcSysCdErr','')
RunID = get_widget_value('RunID','')
LandFileNm = get_widget_value('LandFileNm','')
ErrUpdFileNm = get_widget_value('ErrUpdFileNm','')
ErrRptFileNm = get_widget_value('ErrRptFileNm','')

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)

extract_query_IDS_P_CLM_ERR = (
    f"SELECT "
    f"CLM_SK, "
    f"CLM_ID, "
    f"SRC_SYS_CD, "
    f"CLM_TYP_CD, "
    f"CLM_SUBTYP_CD, "
    f"CLM_SVC_STRT_DT_SK, "
    f"SRC_SYS_GRP_PFX, "
    f"SRC_SYS_GRP_ID, "
    f"SRC_SYS_GRP_SFX, "
    f"SUB_SSN, "
    f"PATN_LAST_NM, "
    f"PATN_FIRST_NM, "
    f"PATN_GNDR_CD, "
    f"PATN_BRTH_DT_SK, "
    f"FEP_MBR_ID, "
    f"SUB_FIRST_NM, "
    f"SUB_LAST_NM, "
    f"SRC_SYS_SUB_ID, "
    f"SRC_SYS_MBR_SFX_NO, "
    f"GRP_ID, "
    f"FILE_DT_SK, "
    f"FEP_MBR_ID AS MBR_ID, "
    f"PATN_SSN "
    f"FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC "
    f"WHERE SRC_SYS_CD = '{SrcSysCdErr}'"
)
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS_P_CLM_ERR)
    .load()
)

df_hf_eyemed_ids_p_clm_err = df_IDS_P_CLM_ERR.dropDuplicates(["CLM_SK"])

extract_query_MBR = (
    f"SELECT "
    f"MBR.MBR_SK, "
    f"CAST((TRIM(SUB.SUB_ID) || TRIM(MBR.MBR_SFX_NO)) AS VARCHAR(20)) AS MBR_ID, "
    f"MBR.MBR_UNIQ_KEY, "
    f"GRP.GRP_ID, "
    f"SUB.SUB_ID, "
    f"MBR.MBR_SFX_NO, "
    f"SUB.SUB_UNIQ_KEY "
    f"FROM {IDSOwner}.MBR AS MBR "
    f"INNER JOIN {IDSOwner}.SUB AS SUB ON MBR.SUB_SK = SUB.SUB_SK "
    f"INNER JOIN {IDSOwner}.GRP AS GRP ON SUB.GRP_SK = GRP.GRP_SK"
)
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_MBR)
    .load()
)

df_Transformer_163 = df_MBR.select(
    col("MBR_ID").alias("MBR_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

df_hf_eyemed_medclm_mbr_lkup = df_Transformer_163.dropDuplicates(["MBR_ID", "GRP_ID"])

df_merged_MbrMtch_Lkup = (
    df_hf_eyemed_ids_p_clm_err.alias("Lnk_ErrClm")
    .join(
        df_hf_eyemed_medclm_mbr_lkup.alias("Mbr_Lkup"),
        (
            (col("Lnk_ErrClm.MBR_ID") == col("Mbr_Lkup.MBR_ID"))
            & (col("Lnk_ErrClm.GRP_ID") == col("Mbr_Lkup.GRP_ID"))
        ),
        "left"
    )
    .withColumn(
        "svMbrUniqKey",
        when(col("Mbr_Lkup.MBR_UNIQ_KEY").isNull(), lit(0)).otherwise(col("Mbr_Lkup.MBR_UNIQ_KEY"))
    )
    .withColumn(
        "svGrpId",
        when(col("Mbr_Lkup.GRP_ID").isNull(), lit("UNK")).otherwise(col("Mbr_Lkup.GRP_ID"))
    )
    .withColumn(
        "svSubId",
        when(col("Mbr_Lkup.SUB_ID").isNull(), lit("0")).otherwise(col("Mbr_Lkup.SUB_ID"))
    )
    .withColumn(
        "svMbrSfxNo",
        when(col("Mbr_Lkup.MBR_SFX_NO").isNull(), lit(0)).otherwise(col("Mbr_Lkup.MBR_SFX_NO"))
    )
    .withColumn(
        "svSubUniqKey",
        when(col("Mbr_Lkup.SUB_UNIQ_KEY").isNull(), lit(0)).otherwise(col("Mbr_Lkup.SUB_UNIQ_KEY"))
    )
)

df_Lnk_WDrugEnr = df_merged_MbrMtch_Lkup.filter(
    col("Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()
).select(
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    col("Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Lnk_ErrClmLand = df_merged_MbrMtch_Lkup.filter(
    col("Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()
).select(
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    col("svGrpId").alias("GRP_ID"),
    col("svSubId").alias("SUB_ID"),
    col("svMbrSfxNo").alias("MBR_SFX_NO"),
    col("svSubUniqKey").alias("SUB_UNIQ_KEY")
)

df_ErrorClm = df_merged_MbrMtch_Lkup.select(
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("Lnk_ErrClm.FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("Lnk_ErrClm.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("Lnk_ErrClm.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("Lnk_ErrClm.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("Lnk_ErrClm.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    col("Lnk_ErrClm.GRP_ID").alias("GRP_ID"),
    col("Lnk_ErrClm.FILE_DT_SK").alias("FILE_DT_SK"),
    col("Lnk_ErrClm.PATN_SSN").alias("PATN_SSN")
)

df_Lnk_ErrClmLand_final = df_Lnk_ErrClmLand.select(
    col("CLM_SK"),
    col("CLM_ID"),
    col("SRC_SYS_CD"),
    col("CLM_TYP_CD"),
    col("CLM_SUBTYP_CD"),
    rpad(col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_PFX"),
    col("SRC_SYS_GRP_ID"),
    col("SRC_SYS_GRP_SFX"),
    col("SUB_SSN"),
    col("PATN_LAST_NM"),
    col("PATN_FIRST_NM"),
    col("PATN_GNDR_CD"),
    rpad(col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    col("MBR_UNIQ_KEY"),
    col("GRP_ID"),
    col("SUB_ID"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    col("SUB_UNIQ_KEY")
)
write_files(
    df_Lnk_ErrClmLand_final,
    f"{adls_path}/verified/{LandFileNm}.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Lnk_WDrugEnr_final = df_Lnk_WDrugEnr.select(
    col("CLM_ID"),
    rpad(col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    col("MBR_UNIQ_KEY")
)
write_files(
    df_Lnk_WDrugEnr_final,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_hf_eyemed_errmbrclm_errlkup = df_ErrorClm.dropDuplicates(["CLM_ID"])

df_SCErrFile = df_hf_eyemed_errmbrclm_errlkup

df_ErrFileUpdate = df_SCErrFile.select(
    col("CLM_SK"),
    col("CLM_ID"),
    col("SRC_SYS_CD"),
    col("CLM_TYP_CD"),
    col("CLM_SUBTYP_CD"),
    rpad(col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_PFX"),
    col("SRC_SYS_GRP_ID"),
    col("SRC_SYS_GRP_SFX"),
    col("SUB_SSN"),
    col("PATN_LAST_NM"),
    col("PATN_FIRST_NM"),
    col("PATN_GNDR_CD"),
    rpad(col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    lit("MBRNOTFOUND").alias("ERR_CD"),
    lit("Member Not Found").alias("ERR_DESC"),
    col("FEP_MBR_ID"),
    col("SUB_FIRST_NM"),
    col("SUB_LAST_NM"),
    col("SRC_SYS_SUB_ID"),
    rpad(col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    col("GRP_ID"),
    rpad(col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    col("PATN_SSN")
)
write_files(
    df_ErrFileUpdate,
    f"{adls_path}/load/{ErrUpdFileNm}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ErrFileReport = df_SCErrFile.select(
    col("CLM_ID"),
    col("SRC_SYS_CD"),
    col("CLM_TYP_CD"),
    col("CLM_SUBTYP_CD"),
    rpad(col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_PFX"),
    col("SRC_SYS_GRP_ID"),
    col("SRC_SYS_GRP_SFX"),
    col("SUB_SSN"),
    col("PATN_LAST_NM"),
    col("PATN_FIRST_NM"),
    col("PATN_GNDR_CD"),
    rpad(col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    lit("MBRNOTFOUND").alias("ERR_CD"),
    lit("Member Not Found").alias("ERR_DESC"),
    col("FEP_MBR_ID"),
    col("SUB_FIRST_NM"),
    col("SUB_LAST_NM"),
    col("SRC_SYS_SUB_ID"),
    rpad(col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    col("GRP_ID"),
    rpad(col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    col("PATN_SSN")
)
write_files(
    df_ErrFileReport,
    f"{adls_path_publish}/external/{ErrRptFileNm}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)