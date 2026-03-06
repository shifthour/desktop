# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedIdsProvXfrm
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                       Date                     Project/Altiris #                       Change Description                                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Madhavan B                            2018-03-19                           5744                               Initial Programming                                                                  IntegrateDev2            Kalyan Neelam              2018-04-04
# MAGIC 
# MAGIC Saikiran Subbagari              2019-01-15                           5887                            Added the TXNMY_CD column in ds_PROV_Xfm           IntegrateDev1                          Kalyan Neelam              2019-02-11
# MAGIC 
# MAGIC Goutham K                          2021-05-15                    US-366403            New Provider file Change to include Loc and Svc loc id                  IntegrateDev1                      Jeyaprasanna                2021-05-24

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: EyeMedIdsProvXfrm
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunID = get_widget_value("RunID","")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_K_CMN_PRCT_Lkp = (
    "SELECT\n"
    "CMN_PRCT_SK,\n"
    "SRC_SYS_CD,\n"
    "CMN_PRCT_ID\n"
    "FROM " + IDSOwner + ".K_CMN_PRCT"
)
df_db2_K_CMN_PRCT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_CMN_PRCT_Lkp)
    .load()
)

extract_query_db2_CMN_PRCT = (
    "SELECT\n"
    "NTNL_PROV_ID,\n"
    "MIN(CMN_PRCT_SK) AS CMN_PRCT_SK\n"
    "FROM " + IDSOwner + ".CMN_PRCT\n"
    "GROUP BY NTNL_PROV_ID"
)
df_db2_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CMN_PRCT)
    .load()
)

df_ds_PROV_EYEMED_CLM = spark.read.parquet(
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet"
)

df_xfrm_BusinessLogic = (
    df_ds_PROV_EYEMED_CLM
    .select(
        F.col("PROV_ID").alias("PROV_ID"),
        F.col("PROV_NPI").alias("PROV_NPI"),
        F.col("TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
        F.col("PROV_FIRST_NM").alias("PROV_FIRST_NM"),
        F.col("PROV_LAST_NM").alias("PROV_LAST_NM"),
        F.col("BUS_NM").alias("BUS_NM"),
        F.col("PROV_ADDR").alias("PROV_ADDR"),
        F.col("PROV_ADDR_2").alias("PROV_ADDR_2"),
        F.col("PROV_CITY").alias("PROV_CITY"),
        F.col("PROV_ST").alias("PROV_ST"),
        F.col("PROV_ZIP").alias("PROV_ZIP"),
        F.col("PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
        F.col("PROF_DSGTN").alias("PROF_DSGTN"),
        F.col("TAX_ENTY_ID").alias("TAX_ENTY_ID"),
        F.col("TXNMY_CD").alias("TXNMY_CD"),
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    )
    .withColumn("FCTS_SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("CMN_PRCT", F.lit(None).cast(T.StringType()))
    .withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None).cast(T.StringType()))
)

df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_fltr_Cd_MppngData = df_ds_CD_MPPNG_Data.filter(
    "SRC_SYS_CD='EYEMED' AND SRC_CLCTN_CD='EYEMED' "
    "AND TRGT_CLCTN_CD='IDS' "
    "AND SRC_DOMAIN_NM='PROVIDER ENTITY' "
    "AND TRGT_DOMAIN_NM='PROVIDER ENTITY'"
)

df_fltr_Cd_MppngData_out = df_fltr_Cd_MppngData.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_db2_CMN_PRCT_2 = (
    df_db2_CMN_PRCT
    .withColumn("SRC_SYS_CD", F.lit(None).cast(T.StringType()))
    .withColumn("CMN_PRCT_ID", F.lit(None).cast(T.StringType()))
    .withColumn("NTNL_PROV_ID", F.col("NTNL_PROV_ID").cast(T.StringType()))
    .withColumn("CMN_PRCT_SK", F.col("CMN_PRCT_SK").cast(T.StringType()))
)

df_join_codes_1 = (
    df_xfrm_BusinessLogic.alias("p")
    .join(
        df_db2_CMN_PRCT_2.alias("c"),
        (
            (F.col("p.CMN_PRCT_SK") == F.col("c.CMN_PRCT_SK"))
            & (F.col("p.FCTS_SRC_SYS_CD") == F.col("c.SRC_SYS_CD"))
            & (F.col("p.CMN_PRCT") == F.col("c.CMN_PRCT_ID"))
            & (F.col("p.PROV_NPI") == F.col("c.NTNL_PROV_ID"))
        ),
        "left"
    )
)

df_join_codes_2 = (
    df_join_codes_1.join(
        df_fltr_Cd_MppngData_out.alias("r"),
        (
            (F.col("p.PROF_DSGTN") == F.col("r.SRC_CD"))
            & (F.col("p.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == F.col("r.CD_MPPNG_SK"))
        ),
        "left"
    )
)

df_lkup_Codes = df_join_codes_2.select(
    F.col("p.PROV_ID").alias("PROV_ID"),
    F.col("p.PROV_NPI").alias("PROV_NPI"),
    F.col("p.TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
    F.col("p.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("p.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("p.BUS_NM").alias("BUS_NM"),
    F.col("p.PROV_ADDR").alias("PROV_ADDR"),
    F.col("p.PROV_ADDR_2").alias("PROV_ADDR_2"),
    F.col("p.PROV_CITY").alias("PROV_CITY"),
    F.col("p.PROV_ST").alias("PROV_ST"),
    F.col("p.PROV_ZIP").alias("PROV_ZIP"),
    F.col("p.PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
    F.col("p.PROF_DSGTN").alias("PROF_DSGTN"),
    F.col("p.TAX_ENTY_ID").alias("TAX_ENTY_ID"),
    F.col("p.TXNMY_CD").alias("TXNMY_CD"),
    F.col("p.FCTS_SRC_SYS_CD").alias("FCTS_SRC_SYS_CD"),
    F.col("p.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("c.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("r.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("p.CMN_PRCT").alias("CMN_PRCT")
)

df_db2_K_CMN_PRCT_Lkp_2 = (
    df_db2_K_CMN_PRCT_Lkp
    .withColumn("NTNL_PROV_ID", F.lit(None).cast(T.StringType()))
    .withColumn("CMN_PRCT_SK", F.col("CMN_PRCT_SK").cast(T.StringType()))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD").cast(T.StringType()))
    .withColumn("CMN_PRCT_ID", F.col("CMN_PRCT_ID").cast(T.StringType()))
)

df_lkup_fkey_1 = (
    df_lkup_Codes.alias("s")
    .join(
        df_db2_K_CMN_PRCT_Lkp_2.alias("k"),
        (
            (F.col("s.CMN_PRCT_SK") == F.col("k.CMN_PRCT_SK"))
            & (F.col("s.FCTS_SRC_SYS_CD") == F.col("k.SRC_SYS_CD"))
            & (F.col("s.CMN_PRCT") == F.col("k.CMN_PRCT_ID"))
            & (F.col("s.PROV_NPI") == F.col("k.NTNL_PROV_ID"))
        ),
        "left"
    )
)

df_lkup_Fkey = df_lkup_fkey_1.select(
    F.col("s.PROV_ID").alias("PROV_ID"),
    F.col("s.PROV_NPI").alias("PROV_NPI"),
    F.col("s.TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
    F.col("s.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("s.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("s.BUS_NM").alias("BUS_NM"),
    F.col("s.PROV_ADDR").alias("PROV_ADDR"),
    F.col("s.PROV_ADDR_2").alias("PROV_ADDR_2"),
    F.col("s.PROV_CITY").alias("PROV_CITY"),
    F.col("s.PROV_ST").alias("PROV_ST"),
    F.col("s.PROV_ZIP").alias("PROV_ZIP"),
    F.col("s.PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
    F.col("s.PROF_DSGTN").alias("PROF_DSGTN"),
    F.col("s.TAX_ENTY_ID").alias("TAX_ENTY_ID"),
    F.col("s.TXNMY_CD").alias("TXNMY_CD"),
    F.col("s.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("s.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("s.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("k.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("s.FCTS_SRC_SYS_CD").alias("FCTS_SRC_SYS_CD")
)

df_lkup_Fkey = (
    df_lkup_Fkey
    .withColumn("SrcSysCd", F.lit(SrcSysCd))
    .withColumn("RunIDTimeStamp", F.lit(RunIDTimeStamp))
    .withColumn("SrcSysCdSk", F.lit(SrcSysCdSk))
)

df_xfrm_BusinessRules = (
    df_lkup_Fkey
    .withColumn(
        "svProvFirstNm",
        UpCase(trim(
            F.when(F.col("PROV_FIRST_NM").isNotNull(), F.col("PROV_FIRST_NM")).otherwise(F.lit(""))
        ))
    )
    .withColumn(
        "svProvLastNm",
        UpCase(trim(
            F.when(F.col("PROV_LAST_NM").isNotNull(), F.col("PROV_LAST_NM")).otherwise(F.lit(""))
        ))
    )
    .withColumn(
        "svBusNm",
        UpCase(trim(
            F.when(F.col("BUS_NM").isNotNull(), F.col("BUS_NM")).otherwise(F.lit(""))
        ))
    )
    .withColumn(
        "svProvNm",
        F.expr("svProvFirstNm || ' ' || svProvLastNm || ',' || svBusNm")
    )
)

df_lnk_EyeMedProv_Out = df_xfrm_BusinessRules.select(
    (
        F.col("PROV_ID").cast(T.StringType())
        .alias("unused_for_expr")
    ),  # dummy so we can do the expression next line
    F.expr("PROV_ID || ';' || SrcSysCd").alias("PRI_NAT_KEY_STRING"),
    F.col("RunIDTimeStamp").alias("FIRST_RECYC_TS"),
    F.lit(0).alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.when(F.col("CMN_PRCT_ID").isNull(), F.lit("")).otherwise(F.col("CMN_PRCT_ID")).alias("CMN_PRCT"),
    F.lit("NA").alias("REL_GRP_PROV"),
    F.lit("NA").alias("REL_IPA_PROV"),
    F.lit("NA").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.lit("NA").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.lit("NA").alias("PROV_CLM_PAYMT_METH_CD"),
    F.lit("PRCTR").alias("PROV_ENTY_CD"),
    F.lit("NA").alias("PROV_FCLTY_TYP_CD"),
    F.lit("NA").alias("PROV_PRCTC_TYP_CD"),
    F.lit("NA").alias("PROV_SVC_CAT_CD"),
    F.when(
        trim(F.col("PROF_DSGTN"), ".", "A") == F.lit("OPT"), F.lit("0084")
    ).when(
        trim(F.col("PROF_DSGTN"), ".", "A") == F.lit("OD"), F.lit("0083")
    ).when(
        trim(F.col("PROF_DSGTN"), ".", "A") == F.lit("MD"), F.lit("0018")
    ).when(
        trim(F.col("PROF_DSGTN"), ".", "A") == F.lit("DO"), F.lit("0018")
    ).otherwise(F.lit("UNK")).alias("PROV_SPEC_CD"),
    F.lit("NA").alias("PROV_STTUS_CD"),
    F.lit("NA").alias("PROV_TERM_RSN_CD"),
    F.lit("NA").alias("PROV_TYP_CD"),
    F.lit("2199-12-31").alias("TERM_DT"),
    F.lit("2199-12-31").alias("PAYMT_HOLD_DT"),
    F.lit("NA").alias("CLRNGHOUSE_ID"),
    F.lit("NA").alias("EDI_DEST_ID"),
    F.lit("").alias("EDI_DEST_QUAL"),
    F.when(
        F.col("PROV_NPI").isNull() | (trim(F.col("PROV_NPI")) == F.lit("")), F.lit("NA")
    ).otherwise(F.col("PROV_NPI")).alias("NTNL_PROV_ID"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.when(
        F.expr("right(trim(svProvNm), 1)") == F.lit(","), trim(F.expr("convert(',', '', svProvNm)")))
    .when(
        F.expr("left(trim(svProvNm), 1)") == F.lit(","), trim(F.expr("convert(',', '', svProvNm)")))
    .when(
        F.col("svProvLastNm") == F.lit(""),
        F.concat_ws(",", trim(F.expr("field(svProvNm, ',', 1)")), trim(F.expr("field(svProvNm, ',', 2)")))
    )
    .otherwise(trim(F.col("svProvNm")))
    .alias("PROV_NM"),
    F.when(
        (F.col("TAX_ENTY_ID").isNull()) | (trim(F.col("TAX_ENTY_ID")) == F.lit("")), F.lit("NA")
    ).otherwise(F.col("TAX_ENTY_ID")).alias("TAX_ID"),
    trim(F.col("TXNMY_CD")).alias("TXNMY_CD")
)

df_lnk_EyeMedProvBProv_Out = df_xfrm_BusinessRules.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.when(
        (F.col("PROV_NPI").isNull()) | (trim(F.col("PROV_NPI")) == F.lit("")),
        F.lit("1")
    ).otherwise(
        F.when(F.col("CMN_PRCT_SK").isNull(), F.lit("0")).otherwise(F.col("CMN_PRCT_SK"))
    ).alias("CMN_PRCT_SK"),
    F.lit("1").alias("REL_GRP_PROV_SK"),
    F.lit("1").alias("REL_IPA_PROV_SK"),
    F.when(
        F.col("CD_MPPNG_SK").isNull(), F.lit("0")
    ).otherwise(F.col("CD_MPPNG_SK")).alias("PROV_ENTY_CD_SK")
)

df_lnk_EyeMedProv_Out_final = (
    df_lnk_EyeMedProv_Out.select(
        F.col("PRI_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("PROV_SK"),
        F.col("PROV_ID"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CMN_PRCT"),
        F.col("REL_GRP_PROV"),
        F.col("REL_IPA_PROV"),
        F.col("PROV_CAP_PAYMT_EFT_METH_CD"),
        F.col("PROV_CLM_PAYMT_EFT_METH_CD"),
        F.col("PROV_CLM_PAYMT_METH_CD"),
        F.col("PROV_ENTY_CD"),
        F.col("PROV_FCLTY_TYP_CD"),
        F.col("PROV_PRCTC_TYP_CD"),
        F.col("PROV_SVC_CAT_CD"),
        F.col("PROV_SPEC_CD"),
        F.col("PROV_STTUS_CD"),
        F.col("PROV_TERM_RSN_CD"),
        F.col("PROV_TYP_CD"),
        F.rpad(F.col("TERM_DT"), 10, " ").alias("TERM_DT"),
        F.rpad(F.col("PAYMT_HOLD_DT"), 10, " ").alias("PAYMT_HOLD_DT"),
        F.col("CLRNGHOUSE_ID"),
        F.col("EDI_DEST_ID"),
        F.col("EDI_DEST_QUAL"),
        F.col("NTNL_PROV_ID"),
        F.col("PROV_ADDR_ID"),
        F.col("PROV_NM"),
        F.col("TAX_ID"),
        F.col("TXNMY_CD"),
    )
)

write_files(
    df_lnk_EyeMedProv_Out_final,
    f"{adls_path}/ds/PROV.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_lnk_EyeMedProvBProv_Out_final = df_lnk_EyeMedProvBProv_Out.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("CMN_PRCT_SK"),
    F.col("REL_GRP_PROV_SK"),
    F.col("REL_IPA_PROV_SK"),
    F.col("PROV_ENTY_CD_SK")
)

write_files(
    df_lnk_EyeMedProvBProv_Out_final,
    f"{adls_path}/load/B_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)