# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC   Tao Luo:  Original Programming - 02/22/2006
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tracy Davis             5/1/2009         Web Realignment IAD (3500)             Added new fields:                                                                          devlIDSnew                     Steph Goddard          05/18/2009
# MAGIC                                                                                                                              MBR_COB_CAT_CD,
# MAGIC                                                                                                                              MBR_COB_CAN_NM,
# MAGIC                                                                                                                              MBR_COB_ACTV_COB_IN
# MAGIC Bhoomi D               07/14/2009      3500                                                   Changed SQL to incorporate LastUpdtRunCycle in                        devlIDSnew                      Steph Goddard         07/15/2009
# MAGIC                                                                                                                    Mbr delete process
# MAGIC SAndrew               2009-10-01           4113Mbr360                                    renamed folder, documentation on hash file                                    devlIDSnew                     Steph Goddard        10/17/2009
# MAGIC 
# MAGIC Bhupinder Kaur               2013-08-01       5114                        Create Load File for  Web Access DM Table MBRSH_DM_MBR_COB          IntegrateWrhsDevl            Jag Yelavarthi           2013-10-23

# MAGIC Mbr_Lkp link is kept drop when lkp condition not met under lookup constraints
# MAGIC Write MBRSH_DM_MBR_COB Data into a Sequential file for Load Job IdsDmMbrshDmMbrCobLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC MBR_COB_TYP_CD_SK
# MAGIC MBR_COB_PAYMT_PRTY_CD_SK
# MAGIC MBR_COB_LAST_VER_METH_CD_SK
# MAGIC MBR_COB_OTHR_CAR_ID_CD_SK
# MAGIC MBR_SK
# MAGIC MBR_COB_TERM_RSN_CD_SK
# MAGIC MBR_COB_CAT_CD_SK
# MAGIC Job Name: IdsDmMbrshDmMbrCobExtr
# MAGIC Read from source table MBR_COB from IDS.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, to_timestamp, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_GRP_MBR_ENR_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT DISTINCT MBR_ENR.MBR_SK, GRP.GRP_ID, GRP.GRP_UNIQ_KEY, GRP.GRP_NM "
        "FROM " + IDSOwner + ".GRP GRP, " + IDSOwner + ".MBR_ENR MBR_ENR "
        "WHERE MBR_ENR.GRP_SK = GRP.GRP_SK"
    )
    .load()
)

df_db2_MBR_COB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT MC.SRC_SYS_CD_SK, "
        "MC.MBR_UNIQ_KEY, "
        "MC.MBR_COB_TYP_CD_SK, "
        "MC.MBR_COB_PAYMT_PRTY_CD_SK, "
        "MC.EFF_DT_SK, "
        "MC.MBR_COB_LAST_VER_METH_CD_SK, "
        "MC.MBR_COB_OTHR_CAR_ID_CD_SK, "
        "MC.MBR_COB_TERM_RSN_CD_SK, "
        "MC.LACK_OF_COB_INFO_STRT_DT_SK, "
        "MC.COB_LTR_TRGR_DT_SK, "
        "MC.TERM_DT_SK, "
        "MC.LAST_VERIFIER_TX, "
        "MC.OTHR_CAR_POL_ID, "
        "MC.MBR_COB_CAT_CD_SK, "
        "MC.MBR_SK, "
        "COALESCE(CM.TRGT_CD,'UNK') as SRC_SYS_CD "
        "FROM " + IDSOwner + ".MBR_COB MC "
        "JOIN " + IDSOwner + ".W_MBR_DM_DRVR DRVR ON MC.MBR_UNIQ_KEY = DRVR.KEY_VAL_INT "
        "LEFT OUTER JOIN " + IDSOwner + ".CD_MPPNG CM ON MC.SRC_SYS_CD_SK=CM.CD_MPPNG_SK "
        "WHERE MC.MBR_UNIQ_KEY NOT IN (0,1)"
    )
    .load()
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
        "FROM " + IDSOwner + ".CD_MPPNG"
    )
    .load()
)

# Copy Stage (Cpy_Mppng_Cd) => produce multiple outputs from the same dataframe
df_Ref_MbrCobPaymtPrtyCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobLastVerMethCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobOthrCarIdCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobTypCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobTermRsnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobCatCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Lookup Stage (lkp_Codes)
df_lkp_Codes = (
    df_db2_MBR_COB_in.alias("primary")
    .join(df_db2_GRP_MBR_ENR_Extr.alias("ref_mbr"), col("primary.MBR_SK") == col("ref_mbr.MBR_SK"), "inner")
    .join(df_Ref_MbrCobTypCd_Lkp.alias("Ref_MbrCobTypCd_Lkp"), col("primary.MBR_COB_TYP_CD_SK") == col("Ref_MbrCobTypCd_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Ref_MbrCobLastVerMethCd_Lkp.alias("Ref_MbrCobLastVerMethCd_Lkp"), col("primary.MBR_COB_LAST_VER_METH_CD_SK") == col("Ref_MbrCobLastVerMethCd_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Ref_MbrCobPaymtPrtyCd_Lkp.alias("Ref_MbrCobPaymtPrtyCd_Lkp"), col("primary.MBR_COB_PAYMT_PRTY_CD_SK") == col("Ref_MbrCobPaymtPrtyCd_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Ref_MbrCobOthrCarIdCd_Lkp.alias("Ref_MbrCobOthrCarIdCd_Lkp"), col("primary.MBR_COB_OTHR_CAR_ID_CD_SK") == col("Ref_MbrCobOthrCarIdCd_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Ref_MbrCobTermRsnCd_Lkp.alias("Ref_MbrCobTermRsnCd_Lkp"), col("primary.MBR_COB_TERM_RSN_CD_SK") == col("Ref_MbrCobTermRsnCd_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Ref_MbrCobCatCd_Lkp.alias("Ref_MbrCobCatCd_Lkp"), col("primary.MBR_COB_CAT_CD_SK") == col("Ref_MbrCobCatCd_Lkp.CD_MPPNG_SK"), "left")
    .select(
        col("primary.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("primary.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("Ref_MbrCobTypCd_Lkp.TRGT_CD").alias("MBR_COB_TYP_CD"),
        col("Ref_MbrCobPaymtPrtyCd_Lkp.TRGT_CD").alias("MBR_COB_PAYMT_PRTY_CD"),
        col("primary.EFF_DT_SK").alias("MBR_COB_EFF_DT_SK"),
        col("Ref_MbrCobTypCd_Lkp.TRGT_CD_NM").alias("MBR_COB_TYP_NM"),
        col("Ref_MbrCobLastVerMethCd_Lkp.TRGT_CD").alias("MBR_COB_LAST_VER_METH_CD"),
        col("Ref_MbrCobLastVerMethCd_Lkp.TRGT_CD_NM").alias("MBR_COB_LAST_VER_METH_NM"),
        col("Ref_MbrCobOthrCarIdCd_Lkp.TRGT_CD").alias("MBR_COB_OTHR_CAR_ID_CD"),
        col("Ref_MbrCobOthrCarIdCd_Lkp.TRGT_CD_NM").alias("MBR_COB_OTHR_CAR_ID_NM"),
        col("Ref_MbrCobPaymtPrtyCd_Lkp.TRGT_CD_NM").alias("MBR_COB_PAYMT_PRTY_NM"),
        col("Ref_MbrCobTermRsnCd_Lkp.TRGT_CD").alias("MBR_COB_TERM_RSN_CD"),
        col("Ref_MbrCobTermRsnCd_Lkp.TRGT_CD_NM").alias("MBR_COB_TERM_RSN_NM"),
        col("primary.LACK_OF_COB_INFO_STRT_DT_SK").alias("MBR_COB_LACK_OF_COB_INFO_STRT_DT_SK"),
        col("primary.COB_LTR_TRGR_DT_SK").alias("MBR_COB_LTR_TRGR_DT_SK"),
        col("primary.TERM_DT_SK").alias("MBR_COB_TERM_DT"),
        col("ref_mbr.GRP_ID").alias("GRP_ID"),
        col("ref_mbr.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        col("primary.LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
        col("primary.OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
        col("Ref_MbrCobCatCd_Lkp.TRGT_CD").alias("MBR_COB_CAT_CD"),
        col("Ref_MbrCobCatCd_Lkp.TRGT_CD_NM").alias("MBR_COB_CAT_NM"),
        col("ref_mbr.MBR_SK").alias("MBR_SK"),
        col("ref_mbr.GRP_NM").alias("GRP_NM")
    )
)

# Transformer Stage (xfm_BusinessLogic)
df_enriched = (
    df_lkp_Codes
    .withColumn("SRC_SYS_CD", when(col("SRC_SYS_CD").isNull() | (col("SRC_SYS_CD") == ''), lit(' ')).otherwise(col("SRC_SYS_CD")))
    .withColumn("MBR_COB_TYP_CD", when(col("MBR_COB_TYP_CD").isNull() | (col("MBR_COB_TYP_CD") == ''), lit(' ')).otherwise(col("MBR_COB_TYP_CD")))
    .withColumn("MBR_COB_PAYMT_PRTY_CD", when(col("MBR_COB_PAYMT_PRTY_CD").isNull() | (col("MBR_COB_PAYMT_PRTY_CD") == ''), lit(' ')).otherwise(col("MBR_COB_PAYMT_PRTY_CD")))
    .withColumn(
        "MBR_COB_EFF_DT",
        when(col("MBR_COB_EFF_DT_SK").isNull() | (col("MBR_COB_EFF_DT_SK") == ''), lit('1753-01-01 00:00:00.000'))
        .otherwise(to_timestamp(col("MBR_COB_EFF_DT_SK"), "yyyy-MM-dd"))
    )
    .withColumn("MBR_COB_TYP_NM", when(col("MBR_COB_TYP_NM").isNull() | (col("MBR_COB_TYP_NM") == ''), lit(' ')).otherwise(col("MBR_COB_TYP_NM")))
    .withColumn("MBR_COB_LAST_VER_METH_CD", when(col("MBR_COB_LAST_VER_METH_CD").isNull() | (col("MBR_COB_LAST_VER_METH_CD") == ''), lit(' ')).otherwise(col("MBR_COB_LAST_VER_METH_CD")))
    .withColumn("MBR_COB_LAST_VER_METH_NM", when(col("MBR_COB_LAST_VER_METH_NM").isNull() | (col("MBR_COB_LAST_VER_METH_NM") == ''), lit(' ')).otherwise(col("MBR_COB_LAST_VER_METH_NM")))
    .withColumn(
        "MBR_COB_OTHR_CAR_ID_CD",
        when(col("MBR_COB_OTHR_CAR_ID_CD").isNull() | (col("MBR_COB_OTHR_CAR_ID_CD") == ''), lit(' '))
        .otherwise(
            when(col("MBR_COB_OTHR_CAR_ID_CD") == 'NA', lit(' '))
            .otherwise(col("MBR_COB_OTHR_CAR_ID_CD"))
        )
    )
    .withColumn(
        "MBR_COB_OTHR_CAR_ID_NM",
        when(col("MBR_COB_OTHR_CAR_ID_NM").isNull() | (col("MBR_COB_OTHR_CAR_ID_NM") == ''), lit(' '))
        .otherwise(
            when(col("MBR_COB_OTHR_CAR_ID_NM") == 'NA', lit(' '))
            .otherwise(col("MBR_COB_OTHR_CAR_ID_NM"))
        )
    )
    .withColumn("MBR_COB_PAYMT_PRTY_NM", when(col("MBR_COB_PAYMT_PRTY_NM").isNull() | (col("MBR_COB_PAYMT_PRTY_NM") == ''), lit(' ')).otherwise(col("MBR_COB_PAYMT_PRTY_NM")))
    .withColumn("MBR_COB_TERM_RSN_CD", when(col("MBR_COB_TERM_RSN_CD").isNull() | (col("MBR_COB_TERM_RSN_CD") == ''), lit(' ')).otherwise(col("MBR_COB_TERM_RSN_CD")))
    .withColumn("MBR_COB_TERM_RSN_NM", when(col("MBR_COB_TERM_RSN_NM").isNull() | (col("MBR_COB_TERM_RSN_NM") == ''), lit(' ')).otherwise(col("MBR_COB_TERM_RSN_NM")))
    .withColumn("MBR_COB_LACK_COB_INFO_STRT_DT", to_timestamp(col("MBR_COB_LACK_OF_COB_INFO_STRT_DT_SK"), "yyyy-MM-dd"))
    .withColumn(
        "MBR_COB_LTR_TRGR_DT",
        when(trim(col("MBR_COB_LTR_TRGR_DT_SK")) == 'UNK', lit(None))
        .otherwise(to_timestamp(col("MBR_COB_LTR_TRGR_DT_SK"), "yyyy-MM-dd"))
    )
    .withColumn("MBR_COB_TERM_DT", to_timestamp(col("MBR_COB_TERM_DT"), "yyyy-MM-dd"))
    .withColumn("GRP_ID", when(col("GRP_ID").isNull() | (col("GRP_ID") == ''), lit(' ')).otherwise(col("GRP_ID")))
    .withColumn("GRP_UNIQ_KEY", when(col("GRP_UNIQ_KEY").isNull() | (col("GRP_UNIQ_KEY") == ''), lit('0')).otherwise(col("GRP_UNIQ_KEY")))
    .withColumn("MBR_COB_LAST_VERIFIER_TX", col("MBR_COB_LAST_VERIFIER_TX"))
    .withColumn("MBR_COB_OTHR_CAR_POL_ID", col("MBR_COB_OTHR_CAR_POL_ID"))
    .withColumn("LAST_UPDT_RUN_CYC_NO", lit(CurrRunCycle))
    .withColumn("MBR_COB_CAT_CD", when(col("MBR_COB_CAT_CD").isNull() | (col("MBR_COB_CAT_CD") == ''), lit(' ')).otherwise(col("MBR_COB_CAT_CD")))
    .withColumn("MBR_COB_CAT_NM", when(col("MBR_COB_CAT_NM").isNull() | (col("MBR_COB_CAT_NM") == ''), lit(' ')).otherwise(col("MBR_COB_CAT_NM")))
    .withColumn(
        "MBR_COB_ACTV_COB_IN",
        when(col("MBR_COB_TYP_CD").isNull() | (col("MBR_COB_TYP_CD") == ''), lit(' '))
        .when(col("MBR_COB_TYP_CD") == 'NOCOB', lit('N'))
        .when(col("MBR_COB_OTHR_CAR_ID_CD") == '24', lit('N'))
        .otherwise(lit('Y'))
    )
)

df_seq_MBRSH_DM_MBR_COB_csv_load = df_enriched.select(
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("MBR_COB_TYP_CD"),
    col("MBR_COB_PAYMT_PRTY_CD"),
    col("MBR_COB_EFF_DT"),
    col("MBR_COB_TYP_NM"),
    col("MBR_COB_LAST_VER_METH_CD"),
    col("MBR_COB_LAST_VER_METH_NM"),
    col("MBR_COB_OTHR_CAR_ID_CD"),
    col("MBR_COB_OTHR_CAR_ID_NM"),
    col("MBR_COB_PAYMT_PRTY_NM"),
    col("MBR_COB_TERM_RSN_CD"),
    col("MBR_COB_TERM_RSN_NM"),
    col("MBR_COB_LACK_COB_INFO_STRT_DT"),
    col("MBR_COB_LTR_TRGR_DT"),
    col("MBR_COB_TERM_DT"),
    col("GRP_ID"),
    col("GRP_UNIQ_KEY"),
    col("MBR_COB_LAST_VERIFIER_TX"),
    col("MBR_COB_OTHR_CAR_POL_ID"),
    col("LAST_UPDT_RUN_CYC_NO"),
    col("MBR_COB_CAT_CD"),
    col("MBR_COB_CAT_NM"),
    rpad(col("MBR_COB_ACTV_COB_IN"), 1, " ").alias("MBR_COB_ACTV_COB_IN")
)

write_files(
    df_seq_MBRSH_DM_MBR_COB_csv_load,
    f"{adls_path}/load/MBRSH_DM_MBR_COB.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)