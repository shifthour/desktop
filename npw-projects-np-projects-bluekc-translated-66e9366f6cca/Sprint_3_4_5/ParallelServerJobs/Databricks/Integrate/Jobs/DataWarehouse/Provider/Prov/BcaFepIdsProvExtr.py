# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                  Extracts data from FEP Provider files from BCA and loads into a DataSets
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                           Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                    ----------------------------------              ---------------------------------       -------------------------
# MAGIC Sudhir Bomshetty             2017-08-16            5781 HEDIS                     Initial Programming                                                                    IntegrateDev2                       Kalyan Neelam               2017-09-25
# MAGIC 
# MAGIC Sudhir Bomshetty             2018-03-26            5781 HEDIS                  Changing datatype to Varchar for PROV_TEL_NO,                    IntegrateDev2                       Jaideep Mankala            04/02/2018
# MAGIC                                                                                                             PROV_FAX_NO in PROV_FEP_CLM Sq File st
# MAGIC Karthik Chintalapani         2019-01-10            5884 HEDIS                 Added a new field MISC_FLD at the end of the 
# MAGIC                                                                                                            FEP_MED_CLM file source stage                                                 IntegrateDev1                       Kalyan Neelam               2020-01-15

# MAGIC Job name: BcaFepIdsProvExtr
# MAGIC Remove Duplicate Prov Ids
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import (
    col,
    substring,
    length,
    when,
    lit,
    regexp_replace,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd', 'BCA')
RunID = get_widget_value('RunID', '100')
FEP_ProvFile = get_widget_value('FEP_ProvFile', 'BCBSA.FEP.Provider.2019-11-02.dat')
FEP_MedClmFile = get_widget_value('FEP_MedClmFile', 'BCBSA.FEP.MedClaims.2019-11-02.dat')

# ---------------------------------------------------------------------
# Stage: PROV_FEP_CLM (PxSequentialFile)
# ---------------------------------------------------------------------
schema_PROV_FEP_CLM = StructType([
    StructField("PROV_ID", StringType(), False),
    StructField("OFC_CD", StringType(), True),
    StructField("LOC", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_MIDINIT", StringType(), True),
    StructField("PROV_NM_SFX", StringType(), True),
    StructField("PROV_ADDR_LN_1", StringType(), True),
    StructField("PROV_ADDR_LN_2", StringType(), True),
    StructField("PROV_CITY_NM", StringType(), True),
    StructField("PROV_CNTY_CD", StringType(), True),
    StructField("PROV_ST_CD", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_TEL_NO", StringType(), True),
    StructField("PROV_EMAIL_ADDR", StringType(), True),
    StructField("PROV_FAX_NO", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), True),
    StructField("CSTM_PROV_TYP", StringType(), True),
    StructField("PCP_FLAG", StringType(), True),
    StructField("PRSCRB_PROV_FLAG", StringType(), True),
    StructField("PROV_ORIG_PLN_CD", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), True),
    StructField("ALT_FLD_1", StringType(), True),
    StructField("ALT_FLD_2", StringType(), True),
    StructField("ALT_FLD_3", StringType(), True),
    StructField("CSTM_FLD_1", StringType(), True),
    StructField("CSTM_FLD_2", StringType(), True),
    StructField("OFC_MGR_LAST_NM", StringType(), True),
    StructField("OFC_MGR_FIRST_NM", StringType(), True),
    StructField("OFC_MGR_TTL", StringType(), True),
    StructField("OFC_MGR_EMAIL", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("SRC_SYS", StringType(), True)
])

df_PROV_FEP_CLM = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .schema(schema_PROV_FEP_CLM)
    .load(f"{adls_path_raw}/landing/{FEP_ProvFile}")
)

# ---------------------------------------------------------------------
# Stage: Transformer_Prov (CTransformerStage)
# ---------------------------------------------------------------------
# Stage Variables:
# SvProvId = trim(Extr_prov.PROV_ID[5, len(Extr_prov.PROV_ID) - 4])
# SvValidProvId = If (len(trim(Ereplace(SvProvId, '0', ' '))) = 0 OR IsNull(Extr_prov.PROV_ID)) Then 0 Else
#                 If (len(trim(Convert('`*:. \\\\', '######', SvProvId), '#', "A")) <> len(trim(SvProvId))) Then 0 Else 1

# Build columns for transformation:
df_Transformer_Prov_tmp = (
    df_PROV_FEP_CLM.withColumn(
        "SvProvId",
        trim(substring(col("PROV_ID"), 5, length(col("PROV_ID")) - 4))
    )
)

df_Transformer_Prov_tmp = df_Transformer_Prov_tmp.withColumn(
    "SvValidProvId",
    when(
        col("PROV_ID").isNull() |
        (length(regexp_replace(col("SvProvId"), "0", " ")) == 0),
        lit(0)
    ).when(
        length(
            # Convert('`*:. \\\\', '######', SvProvId) => replace each of `,*,:,.,' '\,\\ with '#'
            # then remove leading/trailing '#' using a regexp
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(col("SvProvId"), "`", "#"),
                            "\\*", "#"
                        ),
                        ":",
                        "#"
                    ),
                    "\\.",
                    "#"
                ),
                "\\\\",
                "#"
            ).replace("#", "#")  # No-op to maintain chain structure
            .pipe(lambda c: regexp_replace(c, "^#+|#+$", ""))  # remove leading/trailing '#'
        ) != length(trim(col("SvProvId"))),
        lit(0)
    ).otherwise(lit(1))
)

df_Transformer_Prov = (
    df_Transformer_Prov_tmp
    .filter("SvValidProvId = 1")
    .select(
        col("PROV_ID").alias("PROV_ID"),
        col("OFC_CD").alias("OFC_CD"),
        col("LOC").alias("LOC"),
        col("PROV_LAST_NM").alias("PROV_LAST_NM"),
        col("PROV_FIRST_NM").alias("PROV_FIRST_NM"),
        col("PROV_MIDINIT").alias("PROV_MIDINIT"),
        col("PROV_NM_SFX").alias("PROV_NM_SFX"),
        col("PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
        col("PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
        col("PROV_CITY_NM").alias("PROV_CITY_NM"),
        col("PROV_CNTY_CD").alias("PROV_CNTY_CD"),
        col("PROV_ST_CD").alias("PROV_ST_CD"),
        col("PROV_ZIP").alias("PROV_ZIP"),
        col("PROV_TEL_NO").alias("PROV_TEL_NO"),
        col("PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
        col("PROV_FAX_NO").alias("PROV_FAX_NO"),
        col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
        col("CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
        col("PCP_FLAG").alias("PCP_FLAG"),
        col("PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
        col("PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
        col("PROV_TAX_ID").alias("PROV_TAX_ID"),
        col("ALT_FLD_1").alias("ALT_FLD_1"),
        col("ALT_FLD_2").alias("ALT_FLD_2"),
        col("ALT_FLD_3").alias("ALT_FLD_3"),
        col("CSTM_FLD_1").alias("CSTM_FLD_1"),
        col("CSTM_FLD_2").alias("CSTM_FLD_2"),
        col("OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
        col("OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
        col("OFC_MGR_TTL").alias("OFC_MGR_TTL"),
        col("OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
        col("RUN_DT").alias("RUN_DT"),
        col("SRC_SYS").alias("SRC_SYS")
    )
)

# ---------------------------------------------------------------------
# Stage: FEP_MED_CLM (PxSequentialFile)
# ---------------------------------------------------------------------
schema_FEP_MED_CLM = StructType([
    StructField("SRC_SYS", StringType(), False),
    StructField("RCRD_ID", StringType(), False),
    StructField("CLM_LN_NO", StringType(), False),
    StructField("ADJ_NO", StringType(), True),
    StructField("PERFORMING_PROV_ID", StringType(), True),
    StructField("MISC_FLD", StringType(), True)
])

df_FEP_MED_CLM = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .schema(schema_FEP_MED_CLM)
    .load(f"{adls_path_raw}/landing/{FEP_MedClmFile}")
)

# ---------------------------------------------------------------------
# Stage: Xfrm_ClmExtr (CTransformerStage)
# ---------------------------------------------------------------------
# svNonFacetsClm = If (trim(Extr_Clm.RCRD_ID[1,3]) = 240 Or trim(Extr_Clm.RCRD_ID[1,3]) = 740) Then 1 Else 2
# Constraint: svNonFacetsClm = 2
df_Xfrm_ClmExtr_tmp = df_FEP_MED_CLM.withColumn(
    "svNonFacetsClm",
    when(
        substring(col("RCRD_ID"), 1, 3).isin("240", "740"),
        lit(1)
    ).otherwise(lit(2))
)

df_Xfrm_ClmExtr = (
    df_Xfrm_ClmExtr_tmp
    .filter("svNonFacetsClm = 2")
    .select(
        when(col("PERFORMING_PROV_ID").isNull(), lit("")).otherwise(col("PERFORMING_PROV_ID")).alias("PROV_ID"),
        col("RCRD_ID").alias("RCRD_ID")
    )
)

# ---------------------------------------------------------------------
# Stage: RemDup_Prov (PxRemDup)
# Filter => retain first, key = PROV_ID
# ---------------------------------------------------------------------
df_RemDup_Prov = dedup_sort(
    df_Xfrm_ClmExtr,
    partition_cols=["PROV_ID"],
    sort_cols=[]
)

# ---------------------------------------------------------------------
# Stage: Join (PxJoin)
# operator= innerjoin, key= PROV_ID
# ---------------------------------------------------------------------
df_Join = (
    df_Transformer_Prov.alias("Prov")
    .join(
        df_RemDup_Prov.alias("Dedup"),
        on=["PROV_ID"],
        how="inner"
    )
    .select(
        col("Prov.PROV_ID").alias("PROV_ID"),
        col("Prov.OFC_CD").alias("OFC_CD"),
        col("Prov.LOC").alias("LOC"),
        col("Prov.PROV_LAST_NM").alias("PROV_LAST_NM"),
        col("Prov.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
        col("Prov.PROV_MIDINIT").alias("PROV_MIDINIT"),
        col("Prov.PROV_NM_SFX").alias("PROV_NM_SFX"),
        col("Prov.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
        col("Prov.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
        col("Prov.PROV_CITY_NM").alias("PROV_CITY_NM"),
        col("Prov.PROV_CNTY_CD").alias("PROV_CNTY_CD"),
        col("Prov.PROV_ST_CD").alias("PROV_ST_CD"),
        col("Prov.PROV_ZIP").alias("PROV_ZIP"),
        col("Prov.PROV_TEL_NO").alias("PROV_TEL_NO"),
        col("Prov.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
        col("Prov.PROV_FAX_NO").alias("PROV_FAX_NO"),
        col("Prov.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
        col("Prov.CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
        col("Prov.PCP_FLAG").alias("PCP_FLAG"),
        col("Prov.PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
        col("Prov.PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
        col("Prov.PROV_TAX_ID").alias("PROV_TAX_ID"),
        col("Prov.ALT_FLD_1").alias("ALT_FLD_1"),
        col("Prov.ALT_FLD_2").alias("ALT_FLD_2"),
        col("Prov.ALT_FLD_3").alias("ALT_FLD_3"),
        col("Prov.CSTM_FLD_1").alias("CSTM_FLD_1"),
        col("Prov.CSTM_FLD_2").alias("CSTM_FLD_2"),
        col("Prov.OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
        col("Prov.OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
        col("Prov.OFC_MGR_TTL").alias("OFC_MGR_TTL"),
        col("Prov.OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
        col("Prov.RUN_DT").alias("RUN_DT"),
        col("Prov.SRC_SYS").alias("SRC_SYS")
    )
)

# ---------------------------------------------------------------------
# Stage: ds_PROV_FEP_CLM (PxDataSet)
# Write to parquet (remove .ds, add .parquet)
# ---------------------------------------------------------------------
# RPad for columns with known char length
df_ds_PROV_FEP_CLM = df_Join.select(
    col("PROV_ID"),
    rpad(col("OFC_CD"), 1, " ").alias("OFC_CD"),
    col("LOC"),
    col("PROV_LAST_NM"),
    col("PROV_FIRST_NM"),
    col("PROV_MIDINIT"),
    col("PROV_NM_SFX"),
    col("PROV_ADDR_LN_1"),
    col("PROV_ADDR_LN_2"),
    col("PROV_CITY_NM"),
    col("PROV_CNTY_CD"),
    rpad(col("PROV_ST_CD"), 2, " ").alias("PROV_ST_CD"),
    col("PROV_ZIP"),
    col("PROV_TEL_NO"),
    col("PROV_EMAIL_ADDR"),
    col("PROV_FAX_NO"),
    col("PROV_NTNL_PROV_ID"),
    col("CSTM_PROV_TYP"),
    rpad(col("PCP_FLAG"), 1, " ").alias("PCP_FLAG"),
    rpad(col("PRSCRB_PROV_FLAG"), 1, " ").alias("PRSCRB_PROV_FLAG"),
    col("PROV_ORIG_PLN_CD"),
    col("PROV_TAX_ID"),
    col("ALT_FLD_1"),
    col("ALT_FLD_2"),
    col("ALT_FLD_3"),
    col("CSTM_FLD_1"),
    col("CSTM_FLD_2"),
    col("OFC_MGR_LAST_NM"),
    col("OFC_MGR_FIRST_NM"),
    col("OFC_MGR_TTL"),
    col("OFC_MGR_EMAIL"),
    col("RUN_DT"),
    col("SRC_SYS")
)

write_files(
    df_ds_PROV_FEP_CLM,
    f"{adls_path}/ds/FEP_PROV.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)