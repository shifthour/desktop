# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCAFEPClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCA FEP Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Sudhir Bomshetty                  2017-10-11              5781                        Original Programming                                                       IntegrateDev2                      Kalyan Neelam          2017-10-20


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCdErr = get_widget_value("SrcSysCdErr","")
RunID = get_widget_value("RunID","")
LandFileNm = get_widget_value("LandFileNm","")
ErrUpdFileNm = get_widget_value("ErrUpdFileNm","")
ErrRptFileNm = get_widget_value("ErrRptFileNm","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sql_IDS_P_CLM_ERR = f"""SELECT
CLM_SK,
CLM_ID,
SRC_SYS_CD,
CLM_TYP_CD,
CLM_SUBTYP_CD,
CLM_SVC_STRT_DT_SK,
SRC_SYS_GRP_PFX,
SRC_SYS_GRP_ID,
SRC_SYS_GRP_SFX,
SUB_SSN,
PATN_LAST_NM,
PATN_FIRST_NM,
PATN_GNDR_CD,
PATN_BRTH_DT_SK,
FEP_MBR_ID,
SUB_FIRST_NM,
SUB_LAST_NM,
SRC_SYS_SUB_ID,
SRC_SYS_MBR_SFX_NO,
GRP_ID,
FILE_DT_SK
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE SRC_SYS_CD = '{SrcSysCdErr}'"""

df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_IDS_P_CLM_ERR)
    .load()
)

df_hf_bcafep_ids_p_clm_err = dedup_sort(df_IDS_P_CLM_ERR, ["CLM_SK"], [])

sql_MBR = f"""SELECT DISTINCT
FEP.MBR_SK,
FEP.FEP_MBR_ID,
MBR.MBR_UNIQ_KEY,
GRP.GRP_ID,
SUB.SUB_ID,
MBR.MBR_SFX_NO,
SUB.SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
(
  SELECT
    A.MBR_SK,
    A.FEP_MBR_ID
  FROM
    {IDSOwner}.BCBSA_FEP_MBR_ENR A,
    (
      SELECT
        B.FEP_MBR_ID,
        MAX(B.MBR_UNIQ_KEY) AS MBR_UNIQ_KEY
      FROM
        {IDSOwner}.BCBSA_FEP_MBR_ENR B
      GROUP BY
        B.FEP_MBR_ID
    ) C
  WHERE
    A.MBR_UNIQ_KEY = C.MBR_UNIQ_KEY
    AND A.FEP_MBR_ID = C.FEP_MBR_ID
) FEP
WHERE
MBR.MBR_SK = FEP.MBR_SK
AND MBR.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
"""

df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_MBR)
    .load()
)

df_Transformer_163 = df_MBR.select(
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

df_hf_bcafepmedclm_mbr_lkup = dedup_sort(df_Transformer_163, ["FEP_MBR_ID"], [])

df_Trn_MbrMtch_Lkup = df_hf_bcafep_ids_p_clm_err.alias("Lnk_ErrClm").join(
    df_hf_bcafepmedclm_mbr_lkup.alias("Mbr_Lkup"),
    F.col("Lnk_ErrClm.FEP_MBR_ID") == F.col("Mbr_Lkup.FEP_MBR_ID"),
    "left"
)

df_Trn_MbrMtch_Lkup = (
    df_Trn_MbrMtch_Lkup
    .withColumn(
        "svMbrUniqKey",
        F.when(F.col("Mbr_Lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Mbr_Lkup.MBR_UNIQ_KEY"))
    )
    .withColumn(
        "svGrpId",
        F.when(F.col("Mbr_Lkup.GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("Mbr_Lkup.GRP_ID"))
    )
    .withColumn(
        "svSubId",
        F.when(F.col("Mbr_Lkup.SUB_ID").isNull(), F.lit("0")).otherwise(F.col("Mbr_Lkup.SUB_ID"))
    )
    .withColumn(
        "svMbrSfxNo",
        F.when(F.col("Mbr_Lkup.MBR_SFX_NO").isNull(), F.lit(0)).otherwise(F.col("Mbr_Lkup.MBR_SFX_NO"))
    )
    .withColumn(
        "svSubUniqKey",
        F.when(F.col("Mbr_Lkup.SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Mbr_Lkup.SUB_UNIQ_KEY"))
    )
)

df_Lnk_WDrugEnr = df_Trn_MbrMtch_Lkup.filter(
    F.col("Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()
).select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.rpad(F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    F.col("Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Lnk_ErrClmLand = df_Trn_MbrMtch_Lkup.filter(
    F.col("Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()
).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.rpad(F.col("Lnk_ErrClm.PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    F.col("svGrpId").alias("GRP_ID"),
    F.col("svSubId").alias("SUB_ID"),
    F.rpad(F.col("svMbrSfxNo"), 2, " ").alias("MBR_SFX_NO"),
    F.col("svSubUniqKey").alias("SUB_UNIQ_KEY")
)

df_ErrorClm = df_Trn_MbrMtch_Lkup.select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.rpad(F.col("Lnk_ErrClm.PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.col("Lnk_ErrClm.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_ErrClm.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_ErrClm.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_ErrClm.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("Lnk_ErrClm.SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("Lnk_ErrClm.GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("Lnk_ErrClm.FILE_DT_SK"), 10, " ").alias("FILE_DT_SK")
)

write_files(
    df_Lnk_ErrClmLand,
    f"{adls_path}/verified/{LandFileNm}.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Lnk_WDrugEnr,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_hf_bcafep_errmbrclm_errlkup = dedup_sort(df_ErrorClm, ["CLM_ID"], [])

df_SCErrFile = df_hf_bcafep_errmbrclm_errlkup.withColumn("ERR_CD", F.lit("MBRNOTFOUND")).withColumn("ERR_DESC", F.lit("Member Not Found"))

df_ErrFileUpdate = df_SCErrFile.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK")
)

df_ErrFileReport = df_SCErrFile.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK")
)

write_files(
    df_ErrFileUpdate,
    f"{adls_path}/load/{ErrUpdFileNm}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_ErrFileReport,
    f"{adls_path_publish}/external/{ErrRptFileNm}",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)