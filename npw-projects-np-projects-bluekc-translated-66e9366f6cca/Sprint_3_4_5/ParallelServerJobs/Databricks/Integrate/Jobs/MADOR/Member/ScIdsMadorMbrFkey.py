# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839- MADOR                 Originally Programmed                            IntegrateDev2             Kalyan Neelam            2018-06-14
# MAGIC Abhiram Dasarathy	         2019-12-12	F-114877		Changed MBR_ID and GRP_ID to	          IntegrateDev2             Jaideep Mankala        12/13/2019
# MAGIC 						to MBR_ID_1 and GRP_ID_1

# MAGIC Look up with CD_MPPNG table for foreign key values.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('$IDSOwner', '$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT GRP_ID as GRP_ID_1, GRP_SK FROM {IDSOwner}.GRP"
    )
    .load()
)

df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT SUB.SUB_ID || MBR.MBR_SFX_NO as MBR_ID_1,
                   SUB.SUB_ID as SUB_ID_1,
                   MBR.MBR_SK,
                   MBR.SUB_SK
              FROM {IDSOwner}.MBR MBR,
                   {IDSOwner}.SUB SUB
             WHERE MBR.SUB_SK = SUB.SUB_SK"""
    )
    .load()
)

df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT SRC_DRVD_LKUP_VAL,
                   CD_MPPNG_SK
              FROM {IDSOwner}.CD_MPPNG CD_MPPNG
             WHERE SRC_SYS_CD = 'IDS'
               AND SRC_CLCTN_CD = 'IDS'
               AND SRC_DOMAIN_NM = 'STATE'
               AND TRGT_CLCTN_CD = 'IDS'
               AND TRGT_DOMAIN_NM = 'STATE'"""
    )
    .load()
)

df_CD_MPPNG2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT SRC_DRVD_LKUP_VAL,
                   CD_MPPNG_SK,
                   SRC_CLCTN_CD,
                   SRC_SYS_CD
              FROM {IDSOwner}.CD_MPPNG CD_MPPNG
             WHERE SRC_DOMAIN_NM = 'MEMBER RELATIONSHIP'
               AND TRGT_CLCTN_CD = 'IDS'
               AND TRGT_DOMAIN_NM = 'MEMBER RELATIONSHIP'"""
    )
    .load()
)

schema_seq_MBR_MA_DOR_Pkey = StructType([
    StructField("MBR_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("TAX_YR", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("AS_OF_DTM", TimestampType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("SUB_MA_DOR_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("MBR_RELSHP_CD_SK", IntegerType(), False),
    StructField("MBR_RELSHP_TYPE_NO", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), False),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), False),
    StructField("MBR_MAIL_ADDR_LN_1", StringType(), False),
    StructField("MBR_MAIL_ADDR_LN_2", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_3", StringType(), True),
    StructField("MBR_MAIL_ADDR_CITY_NM", StringType(), False),
    StructField("MBR_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("MBR_MAIL_ADDR_ZIP_CD_5", StringType(), False),
    StructField("MBR_MAIL_ADDR_ZIP_CD_4", StringType(), True),
    StructField("JAN_COV_IN", StringType(), False),
    StructField("FEB_COV_IN", StringType(), False),
    StructField("MAR_COV_IN", StringType(), False),
    StructField("APR_COV_IN", StringType(), False),
    StructField("MAY_COV_IN", StringType(), False),
    StructField("JUN_COV_IN", StringType(), False),
    StructField("JUL_COV_IN", StringType(), False),
    StructField("AUG_COV_IN", StringType(), False),
    StructField("SEP_COV_IN", StringType(), False),
    StructField("OCT_COV_IN", StringType(), False),
    StructField("NOV_COV_IN", StringType(), False),
    StructField("DEC_COV_IN", StringType(), False),
    StructField("MBR_BRTH_DT", DateType(), False),
    StructField("MBR_SFX_NO", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_MA_DOR_SK", IntegerType(), False)
])

df_seq_MBR_MA_DOR_Pkey = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBR_MA_DOR_Pkey)
    .csv(f"{adls_path}/key/MBR_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat")
)

df_LkupFkey_joined = (
    df_seq_MBR_MA_DOR_Pkey.alias("Pkey_Out")
    .join(
        df_CD_MPPNG.alias("lkup"),
        F.col("Pkey_Out.MBR_MAIL_ADDR_ST_CD") == F.col("lkup.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .join(
        df_CD_MPPNG2.alias("lkup2"),
        (
            (F.col("Pkey_Out.MBR_RELSHP_TYPE_NO") == F.col("lkup2.SRC_DRVD_LKUP_VAL"))
            & (F.col("Pkey_Out.SRC_SYS_CD") == F.col("lkup2.SRC_SYS_CD"))
        ),
        "left"
    )
)

df_LkupFkey = df_LkupFkey_joined.select(
    F.col("Pkey_Out.MBR_ID").alias("MBR_ID"),
    F.col("Pkey_Out.SUB_ID").alias("SUB_ID"),
    F.col("Pkey_Out.GRP_ID").alias("GRP_ID"),
    F.col("Pkey_Out.TAX_YR").alias("TAX_YR"),
    F.col("Pkey_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Pkey_Out.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("Pkey_Out.GRP_SK").alias("GRP_SK"),
    F.col("Pkey_Out.MBR_SK").alias("MBR_SK"),
    F.col("Pkey_Out.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("Pkey_Out.SUB_SK").alias("SUB_SK"),
    F.col("Pkey_Out.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("Pkey_Out.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Pkey_Out.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("Pkey_Out.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("Pkey_Out.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("Pkey_Out.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("Pkey_Out.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("Pkey_Out.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("Pkey_Out.APR_COV_IN").alias("APR_COV_IN"),
    F.col("Pkey_Out.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("Pkey_Out.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("Pkey_Out.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("Pkey_Out.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("Pkey_Out.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("Pkey_Out.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("Pkey_Out.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("Pkey_Out.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("Pkey_Out.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("Pkey_Out.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("Pkey_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Pkey_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Pkey_Out.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    F.col("lkup.CD_MPPNG_SK").alias("STATE_CD_SK"),
    F.col("Pkey_Out.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("lkup2.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_LkupMbr_joined = (
    df_LkupFkey.alias("LkupMbr")
    .join(
        df_MBR.alias("lkup_MBR"),
        (
            (F.col("LkupMbr.MBR_ID") == F.col("lkup_MBR.MBR_ID_1"))
            & (F.col("LkupMbr.SUB_ID") == F.col("lkup_MBR.SUB_ID_1"))
        ),
        "left"
    )
)

df_LkupMbr = df_LkupMbr_joined.select(
    F.col("LkupMbr.MBR_ID").alias("MBR_ID"),
    F.col("LkupMbr.SUB_ID").alias("SUB_ID"),
    F.col("LkupMbr.GRP_ID").alias("GRP_ID"),
    F.col("LkupMbr.TAX_YR").alias("TAX_YR"),
    F.col("LkupMbr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkupMbr.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("LkupMbr.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("LkupMbr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("LkupMbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("LkupMbr.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("LkupMbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("LkupMbr.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("LkupMbr.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("LkupMbr.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("LkupMbr.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("LkupMbr.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("LkupMbr.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("LkupMbr.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("LkupMbr.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("LkupMbr.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("LkupMbr.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("LkupMbr.APR_COV_IN").alias("APR_COV_IN"),
    F.col("LkupMbr.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("LkupMbr.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("LkupMbr.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("LkupMbr.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("LkupMbr.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("LkupMbr.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("LkupMbr.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("LkupMbr.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("LkupMbr.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("LkupMbr.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("LkupMbr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LkupMbr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LkupMbr.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    F.col("LkupMbr.STATE_CD_SK").alias("STATE_CD_SK"),
    F.col("LkupMbr.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("lkup_MBR.MBR_SK").alias("MBR_SK"),
    F.col("lkup_MBR.SUB_SK").alias("SUB_SK"),
    F.col("LkupMbr.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_LkupGrp_joined = (
    df_LkupMbr.alias("LkupGrp")
    .join(
        df_GRP.alias("lkup_GRP"),
        F.col("LkupGrp.GRP_ID") == F.col("lkup_GRP.GRP_ID_1"),
        "left"
    )
)

df_LkupGrp = df_LkupGrp_joined.select(
    F.col("LkupGrp.MBR_ID").alias("MBR_ID"),
    F.col("LkupGrp.SUB_ID").alias("SUB_ID"),
    F.col("LkupGrp.GRP_ID").alias("GRP_ID"),
    F.col("LkupGrp.TAX_YR").alias("TAX_YR"),
    F.col("LkupGrp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkupGrp.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("LkupGrp.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("LkupGrp.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("LkupGrp.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("LkupGrp.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("LkupGrp.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("LkupGrp.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("LkupGrp.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("LkupGrp.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("LkupGrp.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("LkupGrp.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("LkupGrp.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("LkupGrp.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("LkupGrp.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("LkupGrp.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("LkupGrp.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("LkupGrp.APR_COV_IN").alias("APR_COV_IN"),
    F.col("LkupGrp.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("LkupGrp.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("LkupGrp.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("LkupGrp.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("LkupGrp.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("LkupGrp.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("LkupGrp.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("LkupGrp.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("LkupGrp.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("LkupGrp.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("LkupGrp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LkupGrp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LkupGrp.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    F.col("LkupGrp.STATE_CD_SK").alias("STATE_CD_SK"),
    F.col("LkupGrp.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("LkupGrp.MBR_SK").alias("MBR_SK"),
    F.col("LkupGrp.SUB_SK").alias("SUB_SK"),
    F.col("lkup_GRP.GRP_SK").alias("GRP_SK"),
    F.col("LkupGrp.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_xfm_check_tmp = (
    df_LkupGrp
    .withColumn(
        "SvFKeyLkpCheckStCd",
        F.when(
            F.trim(F.coalesce(F.col("STATE_CD_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckRnCd",
        F.when(
            F.trim(F.coalesce(F.col("MBR_RELSHP_CD_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckGrpSk",
        F.when(
            F.trim(F.coalesce(F.col("GRP_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckMbrSk",
        F.when(
            (
                F.trim(F.coalesce(F.col("MBR_SK").cast(StringType()), F.lit("0"))) == F.lit("0")
            ) | (
                F.trim(F.coalesce(F.col("SUB_SK").cast(StringType()), F.lit("0"))) == F.lit("0")
            ),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_xfm_check = (
    df_xfm_check_tmp
    .withColumn(
        "GRP_SK",
        F.when(
            F.trim(F.coalesce(F.col("GRP_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit(1).cast(IntegerType())
        ).otherwise(F.col("GRP_SK"))
    )
    .withColumn(
        "MBR_SK",
        F.when(
            F.trim(F.coalesce(F.col("MBR_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit(1).cast(IntegerType())
        ).otherwise(F.col("MBR_SK"))
    )
    .withColumn(
        "SUB_SK",
        F.when(
            F.trim(F.coalesce(F.col("SUB_SK").cast(StringType()), F.lit("0"))) == F.lit("0"),
            F.lit(1).cast(IntegerType())
        ).otherwise(F.col("SUB_SK"))
    )
    .withColumn(
        "MBR_RELSHP_CD_SK",
        F.when(
            (F.col("SRC_SYS_CD") == F.lit("FACETS")) & (F.col("MBR_RELSHP_CD_SK") == F.lit(99)),
            F.col("CD_MPPNG_SK")
        ).when(
            (F.col("SRC_SYS_CD") == F.lit("FACETS")) & (F.col("MBR_RELSHP_CD_SK") != F.lit(99)),
            F.col("MBR_RELSHP_CD_SK")
        ).when(
            F.trim(F.coalesce(F.col("CD_MPPNG_SK").cast(StringType()), F.lit(""))) == F.lit(""),
            F.lit(1).cast(IntegerType())
        ).otherwise(F.col("CD_MPPNG_SK"))
    )
    .withColumn(
        "MBR_MAIL_ADDR_ST_CD_SK",
        F.when(
            F.trim(F.coalesce(F.col("STATE_CD_SK").cast(StringType()), F.lit(""))) == F.lit(""),
            F.lit(1862).cast(IntegerType())
        ).otherwise(F.col("STATE_CD_SK"))
    )
)

df_xfm_check_final = df_xfm_check.select(
    F.col("MBR_MA_DOR_SK"),
    F.col("MBR_ID"),
    F.col("SUB_ID"),
    F.col("GRP_ID"),
    F.col("TAX_YR"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AS_OF_DTM"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("SUB_MA_DOR_SK"),
    F.col("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_MAIL_ADDR_ST_CD_SK"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("JAN_COV_IN"),
    F.col("FEB_COV_IN"),
    F.col("MAR_COV_IN"),
    F.col("APR_COV_IN"),
    F.col("MAY_COV_IN"),
    F.col("JUN_COV_IN"),
    F.col("JUL_COV_IN"),
    F.col("AUG_COV_IN"),
    F.col("SEP_COV_IN"),
    F.col("OCT_COV_IN"),
    F.col("NOV_COV_IN"),
    F.col("DEC_COV_IN"),
    F.col("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO"),
    F.col("SvFKeyLkpCheckStCd"),
    F.col("SvFKeyLkpCheckRnCd"),
    F.col("SvFKeyLkpCheckGrpSk"),
    F.col("SvFKeyLkpCheckMbrSk")
)

df_Lnk_Main = df_xfm_check_final

window_rn = F.row_number().over(F.orderBy(F.lit(1)))
df_xfm_check_rn = df_xfm_check_final.withColumn("row_num", window_rn)

df_NA_tmp = df_xfm_check_rn.filter(F.col("row_num") == 1)
df_UNK_tmp = df_xfm_check_rn.filter(F.col("row_num") == 1)

df_NA = df_NA_tmp.select(
    F.lit(1).alias("MBR_MA_DOR_SK"),
    F.lit("NA").alias("MBR_ID"),
    F.lit("NA").alias("SUB_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("TAX_YR"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    current_timestamp().alias("AS_OF_DTM"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("SUB_MA_DOR_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("MBR_RELSHP_CD_SK"),
    F.lit("NA").alias("MBR_FIRST_NM"),
    F.lit("N").alias("MBR_MIDINIT"),
    F.lit("NA").alias("MBR_LAST_NM"),
    F.lit("NA").alias("MBR_MAIL_ADDR_LN_1"),
    F.lit("NA").alias("MBR_MAIL_ADDR_LN_2"),
    F.lit("NA").alias("MBR_MAIL_ADDR_LN_3"),
    F.lit("NA").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.lit(1).alias("MBR_MAIL_ADDR_ST_CD_SK"),
    F.lit("NA").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.lit("NA").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.lit("N").alias("JAN_COV_IN"),
    F.lit("N").alias("FEB_COV_IN"),
    F.lit("N").alias("MAR_COV_IN"),
    F.lit("N").alias("APR_COV_IN"),
    F.lit("N").alias("MAY_COV_IN"),
    F.lit("N").alias("JUN_COV_IN"),
    F.lit("N").alias("JUL_COV_IN"),
    F.lit("N").alias("AUG_COV_IN"),
    F.lit("N").alias("SEP_COV_IN"),
    F.lit("N").alias("OCT_COV_IN"),
    F.lit("N").alias("NOV_COV_IN"),
    F.lit("N").alias("DEC_COV_IN"),
    current_date().alias("MBR_BRTH_DT"),
    F.lit("NA").alias("MBR_SFX_NO")
)

df_UNK = df_UNK_tmp.select(
    F.lit(0).alias("MBR_MA_DOR_SK"),
    F.lit("UNK").alias("MBR_ID"),
    F.lit("UNK").alias("SUB_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("TAX_YR"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    current_timestamp().alias("AS_OF_DTM"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("SUB_MA_DOR_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("MBR_RELSHP_CD_SK"),
    F.lit("UNK").alias("MBR_FIRST_NM"),
    F.lit("X").alias("MBR_MIDINIT"),
    F.lit("UNK").alias("MBR_LAST_NM"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_LN_1"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_LN_2"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_LN_3"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.lit(0).alias("MBR_MAIL_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.lit("UNK").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.lit("U").alias("JAN_COV_IN"),
    F.lit("U").alias("FEB_COV_IN"),
    F.lit("U").alias("MAR_COV_IN"),
    F.lit("U").alias("APR_COV_IN"),
    F.lit("U").alias("MAY_COV_IN"),
    F.lit("U").alias("JUN_COV_IN"),
    F.lit("U").alias("JUL_COV_IN"),
    F.lit("U").alias("AUG_COV_IN"),
    F.lit("U").alias("SEP_COV_IN"),
    F.lit("U").alias("OCT_COV_IN"),
    F.lit("U").alias("NOV_COV_IN"),
    F.lit("U").alias("DEC_COV_IN"),
    current_date().alias("MBR_BRTH_DT"),
    F.lit("UNK").alias("MBR_SFX_NO")
)

df_Lnk_K_Fkey_Code = df_xfm_check_final.filter(F.col("SvFKeyLkpCheckStCd") == F.lit("Y")).select(
    F.col("MBR_MA_DOR_SK").alias("PRI_SK"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("ScIdsMadorMbrFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_MBR_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD", "MBR_MAIL_ADDR_ST_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Cd2 = df_xfm_check_final.filter(F.col("SvFKeyLkpCheckRnCd") == F.lit("Y")).select(
    F.col("MBR_MA_DOR_SK").alias("PRI_SK"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("ScIdsMadorMbrFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_MBR_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD", "MBR_RELSHP_TYPE_NO").alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Mbr = df_xfm_check_final.filter(F.col("SvFKeyLkpCheckMbrSk") == F.lit("Y")).select(
    F.col("MBR_MA_DOR_SK").alias("PRI_SK"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("ScIdsMadorMbrFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_MBR_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD", "MBR_MAIL_ADDR_ST_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Grp = df_xfm_check_final.filter(F.col("SvFKeyLkpCheckGrpSk") == F.lit("Y")).select(
    F.col("MBR_MA_DOR_SK").alias("PRI_SK"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("ScIdsMadorMbrFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_MBR_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat_ws(":", "MBR_ID", "SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD", "MBR_MAIL_ADDR_ST_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Fnl = df_Lnk_K_Fkey_Code.unionByName(df_Cd2, True).unionByName(df_Mbr, True).unionByName(df_Grp, True)

# Apply rpad (char/varchar). We have no explicit lengths for these error-file columns, so we do minimal pass:
df_Fnl_rpad = df_Fnl.select(
    F.col("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.col("JOB_NM"),
    F.col("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_Fnl_rpad,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.ScIdsMadorMbrFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_Funnel_77 = df_Lnk_Main.select(
    "MBR_MA_DOR_SK",
    "MBR_ID",
    "SUB_ID",
    "GRP_ID",
    "TAX_YR",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AS_OF_DTM",
    "GRP_SK",
    "MBR_SK",
    "SUB_MA_DOR_SK",
    "SUB_SK",
    "MBR_RELSHP_CD_SK",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "MBR_MAIL_ADDR_LN_1",
    "MBR_MAIL_ADDR_LN_2",
    "MBR_MAIL_ADDR_LN_3",
    "MBR_MAIL_ADDR_CITY_NM",
    "MBR_MAIL_ADDR_ST_CD_SK",
    "MBR_MAIL_ADDR_ZIP_CD_5",
    "MBR_MAIL_ADDR_ZIP_CD_4",
    "JAN_COV_IN",
    "FEB_COV_IN",
    "MAR_COV_IN",
    "APR_COV_IN",
    "MAY_COV_IN",
    "JUN_COV_IN",
    "JUL_COV_IN",
    "AUG_COV_IN",
    "SEP_COV_IN",
    "OCT_COV_IN",
    "NOV_COV_IN",
    "DEC_COV_IN",
    "MBR_BRTH_DT",
    "MBR_SFX_NO"
).unionByName(df_NA.select(
    "MBR_MA_DOR_SK",
    "MBR_ID",
    "SUB_ID",
    "GRP_ID",
    "TAX_YR",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AS_OF_DTM",
    "GRP_SK",
    "MBR_SK",
    "SUB_MA_DOR_SK",
    "SUB_SK",
    "MBR_RELSHP_CD_SK",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "MBR_MAIL_ADDR_LN_1",
    "MBR_MAIL_ADDR_LN_2",
    "MBR_MAIL_ADDR_LN_3",
    "MBR_MAIL_ADDR_CITY_NM",
    "MBR_MAIL_ADDR_ST_CD_SK",
    "MBR_MAIL_ADDR_ZIP_CD_5",
    "MBR_MAIL_ADDR_ZIP_CD_4",
    "JAN_COV_IN",
    "FEB_COV_IN",
    "MAR_COV_IN",
    "APR_COV_IN",
    "MAY_COV_IN",
    "JUN_COV_IN",
    "JUL_COV_IN",
    "AUG_COV_IN",
    "SEP_COV_IN",
    "OCT_COV_IN",
    "NOV_COV_IN",
    "DEC_COV_IN",
    "MBR_BRTH_DT",
    "MBR_SFX_NO"
), True).unionByName(df_UNK.select(
    "MBR_MA_DOR_SK",
    "MBR_ID",
    "SUB_ID",
    "GRP_ID",
    "TAX_YR",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AS_OF_DTM",
    "GRP_SK",
    "MBR_SK",
    "SUB_MA_DOR_SK",
    "SUB_SK",
    "MBR_RELSHP_CD_SK",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "MBR_MAIL_ADDR_LN_1",
    "MBR_MAIL_ADDR_LN_2",
    "MBR_MAIL_ADDR_LN_3",
    "MBR_MAIL_ADDR_CITY_NM",
    "MBR_MAIL_ADDR_ST_CD_SK",
    "MBR_MAIL_ADDR_ZIP_CD_5",
    "MBR_MAIL_ADDR_ZIP_CD_4",
    "JAN_COV_IN",
    "FEB_COV_IN",
    "MAR_COV_IN",
    "APR_COV_IN",
    "MAY_COV_IN",
    "JUN_COV_IN",
    "JUL_COV_IN",
    "AUG_COV_IN",
    "SEP_COV_IN",
    "OCT_COV_IN",
    "NOV_COV_IN",
    "DEC_COV_IN",
    "MBR_BRTH_DT",
    "MBR_SFX_NO"
), True)

# Apply rpad for final output (char/varchar columns with known lengths)
df_Funnel_77_rpad = (
    df_Funnel_77
    .withColumn("TAX_YR", F.rpad(F.col("TAX_YR"), 4, " "))
    .withColumn("MBR_MIDINIT", F.rpad(F.col("MBR_MIDINIT"), 1, " "))
    .withColumn("MBR_MAIL_ADDR_ZIP_CD_5", F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " "))
    .withColumn("MBR_MAIL_ADDR_ZIP_CD_4", F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " "))
    .withColumn("JAN_COV_IN", F.rpad(F.col("JAN_COV_IN"), 1, " "))
    .withColumn("FEB_COV_IN", F.rpad(F.col("FEB_COV_IN"), 1, " "))
    .withColumn("MAR_COV_IN", F.rpad(F.col("MAR_COV_IN"), 1, " "))
    .withColumn("APR_COV_IN", F.rpad(F.col("APR_COV_IN"), 1, " "))
    .withColumn("MAY_COV_IN", F.rpad(F.col("MAY_COV_IN"), 1, " "))
    .withColumn("JUN_COV_IN", F.rpad(F.col("JUN_COV_IN"), 1, " "))
    .withColumn("JUL_COV_IN", F.rpad(F.col("JUL_COV_IN"), 1, " "))
    .withColumn("AUG_COV_IN", F.rpad(F.col("AUG_COV_IN"), 1, " "))
    .withColumn("SEP_COV_IN", F.rpad(F.col("SEP_COV_IN"), 1, " "))
    .withColumn("OCT_COV_IN", F.rpad(F.col("OCT_COV_IN"), 1, " "))
    .withColumn("NOV_COV_IN", F.rpad(F.col("NOV_COV_IN"), 1, " "))
    .withColumn("DEC_COV_IN", F.rpad(F.col("DEC_COV_IN"), 1, " "))
)

write_files(
    df_Funnel_77_rpad,
    f"{adls_path}/load/MBR_MA_DOR.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)