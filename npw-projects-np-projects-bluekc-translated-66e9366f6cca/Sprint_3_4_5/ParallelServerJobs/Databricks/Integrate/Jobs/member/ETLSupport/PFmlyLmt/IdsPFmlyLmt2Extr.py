# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IdsMbrLmtExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Select Ids MBR_ENR rows for P table creation
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          06/15/2010      4113                      IntegrateNewDevl                                                                       IntegrateNewDevl            Steph Goddard          06/23/2010
# MAGIC Steph Goddard        07/08/2010     4113                      changed to extract current product/member and compare          RebuildIntNewDevl
# MAGIC                                                                                       with prior to only select prior records that have current membership
# MAGIC 
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl           Kalyan Neelam           2013-10-09
# MAGIC 
# MAGIC Manasa Andru          02/14/2014    TFS - 8026            Updated the field LMT_AMT as per the new mapping rule.         IntegrateCurDevl             Kalyan Neelam           2014-02-19
# MAGIC 
# MAGIC            
# MAGIC Karthik Chintalapani   2016-11-14       5634                   Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2            Kalyan Neelam           2016-11-28
# MAGIC 
# MAGIC Shanmugam A \(9) 2017-03-02         5321                  SQL needs to be changed to alias '#CurrYear#'                               IntegrateDev2           Jag Yelavarthi              2017-03-07

# MAGIC Apply business logic
# MAGIC Load File
# MAGIC Extract for current year - extract product data for current year, only extract membership if the member is CURRENT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2014-02-28')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrYear = get_widget_value('CurrYear','2014')
PrevYear = get_widget_value('PrevYear','2013')
YrEndDate = get_widget_value('YrEndDate','2014-12-31')
ProdCmpntTypCdSk = get_widget_value('ProdCmpntTypCdSk','844281128')
ClsPlnProdCatCdMedSk = get_widget_value('ClsPlnProdCatCdMedSk','1949')
ClsPlnProdCatCdDntlSk = get_widget_value('ClsPlnProdCatCdDntlSk','1946')
MbrRelshpCdSk = get_widget_value('MbrRelshpCdSk','1975')

schema_Cd_Mppng = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("SRC_CD_NM", StringType(), True)
])
df_Cd_Mppng = spark.read.schema(schema_Cd_Mppng).parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

schema_PMbrLmtPriorEnroll = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("PROD_ACCUM_ID", StringType(), True),
    StructField("ACCUM_NO", IntegerType(), True),
    StructField("BNF_SUM_DTL_NTWK_TYP_CD_SK", IntegerType(), True),
    StructField("BNF_SUM_DTL_TYP_CD_SK", IntegerType(), True),
    StructField("BLUEKC_DPLY_IN", StringType(), True),
    StructField("MBR_360_DPLY_IN", StringType(), True),
    StructField("EXTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("INTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("LMT_AMT", DecimalType(38,10), True),
    StructField("STOPLOSS_AMT", DecimalType(38,10), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("YR_NO", IntegerType(), True),
    StructField("PLN_YR_EFF_DT", DateType(), True),
    StructField("PLN_YR_END_DT", DateType(), True)
])
df_PMbrLmtPriorEnroll = (
    spark.read.format("csv")
    .option("header","false")
    .option("sep",",")
    .option("quote","\"")
    .schema(schema_PMbrLmtPriorEnroll)
    .load(f"{adls_path_raw}/landing/PFmlyLmtPriorEnroll.dat")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT ENR.SRC_SYS_CD_SK,\n"
    f"       SUB.SUB_UNIQ_KEY,\n"
    f"       PAC.PROD_ACCUM_ID,\n"
    f"       BNF.ACCUM_NO,\n"
    f"       BNF.BNF_SUM_DTL_NTWK_TYP_CD_SK,\n"
    f"       BNF.BNF_SUM_DTL_TYP_CD_SK,\n"
    f"       PAC.BLUEKC_DPLY_IN,\n"
    f"       PAC.MBR_360_DPLY_IN,\n"
    f"       PAC.EXTRNL_DPLY_ACCUM_DESC,\n"
    f"       PAC.INTRNL_DPLY_ACCUM_DESC,\n"
    f"       BNF.LMT_AMT,\n"
    f"       BNF.STOPLOSS_AMT,\n"
    f"       PC.PROD_SK,\n"
    f"       {CurrYear} AS YR_NO,\n"
    f"       CSPD.PLN_BEG_DT_MO_DAY\n"
    f"FROM  {IDSOwner}.P_ACCUM_CTL PAC,\n"
    f"      {IDSOwner}.BNF_SUM_DTL BNF,\n"
    f"      {IDSOwner}.PROD_CMPNT PC,\n"
    f"      {IDSOwner}.MBR_ENR ENR,\n"
    f"      {IDSOwner}.W_MBR_ACCUM ENRDRVR,\n"
    f"      {IDSOwner}.SUB SUB,\n"
    f"      {IDSOwner}.MBR MBR,\n"
    f"      {IDSOwner}.CLS_PLN_DTL CSPD\n"
    f"Where PAC.BNF_SUM_DTL_ACCUM_LVL_CD = 'FMLY'\n"
    f"  AND (PAC.MBR_360_DPLY_IN = 'Y' or PAC.BLUEKC_DPLY_IN = 'Y')\n"
    f"  AND PAC.ACCUM_NO = BNF.ACCUM_NO\n"
    f"  AND (BNF.STOPLOSS_AMT > 0 OR BNF.LMT_AMT > 0)\n"
    f"  AND BNF.PROD_CMPNT_PFX_ID = PC.PROD_CMPNT_PFX_ID\n"
    f"  AND PC.PROD_CMPNT_TYP_CD_SK = {ProdCmpntTypCdSk}\n"
    f"  AND ENR.EFF_DT_SK <= '{CurrDate}'\n"
    f"  AND ENR.PROD_SK = PC.PROD_SK\n"
    f"  AND ENR.ELIG_IN = 'Y'\n"
    f"  AND ENRDRVR.TERM_DT_SK = ENR.TERM_DT_SK\n"
    f"  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK IN ({ClsPlnProdCatCdMedSk},{ClsPlnProdCatCdDntlSk})\n"
    f"  AND ENRDRVR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY\n"
    f"  AND ENRDRVR.AS_OF_DT_SK BETWEEN PC.PROD_CMPNT_EFF_DT_SK AND PC.PROD_CMPNT_TERM_DT_SK\n"
    f"  AND PAC.PROD_ACCUM_ID = ENRDRVR.PROD_ACCUM_ID\n"
    f"  AND MBR.MBR_SK = ENR.MBR_SK\n"
    f"  AND MBR.SUB_SK = SUB.SUB_SK\n"
    f"  AND MBR.MBR_RELSHP_CD_SK = {MbrRelshpCdSk}\n"
    f"  AND CSPD.GRP_SK=ENR.GRP_SK\n"
    f"  AND CSPD.CLS_PLN_SK=ENR.CLS_PLN_SK\n"
    f"  AND CSPD.CLS_SK=ENR.CLS_SK\n"
    f"  AND CSPD.PROD_SK=ENR.PROD_SK\n"
    f"  AND CSPD.EFF_DT_SK<='{CurrDate}'\n"
    f"  AND CSPD.TERM_DT_SK>='{CurrDate}'"
)
df_CurrProd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_XfmCurrEnroll = (
    df_CurrProd
    .withColumn("svFutrYear", F.lit(int(CurrYear)+1))
    .withColumn(
        "svPlnEffDt",
        F.when(
            F.length("PLN_BEG_DT_MO_DAY") == 3,
            F.concat_ws("-",
                F.lit(CurrYear),
                F.concat(F.lit("0"), F.substring("PLN_BEG_DT_MO_DAY",1,1)),
                F.substring("PLN_BEG_DT_MO_DAY",2,2)
            )
        ).otherwise(
            F.concat_ws("-",
                F.lit(CurrYear),
                F.substring("PLN_BEG_DT_MO_DAY",1,2),
                F.substring("PLN_BEG_DT_MO_DAY",3,2)
            )
        )
    )
    .withColumn(
        "svPlnEndDt",
        F.when(
            F.length("PLN_BEG_DT_MO_DAY") == 3,
            F.concat_ws("-",
                F.col("svFutrYear").cast(StringType()),
                F.concat(F.lit("0"), F.substring("PLN_BEG_DT_MO_DAY",1,1)),
                F.substring("PLN_BEG_DT_MO_DAY",2,2)
            )
        ).otherwise(
            F.concat_ws("-",
                F.col("svFutrYear").cast(StringType()),
                F.substring("PLN_BEG_DT_MO_DAY",1,2),
                F.substring("PLN_BEG_DT_MO_DAY",3,2)
            )
        )
    )
    .withColumn(
        "PLN_YR_END_DT",
        FIND.DATE(
            F.col("svPlnEndDt"),
            F.lit("-1"),
            F.lit("D"),
            F.lit("X"),
            F.lit("CCYY-MM-DD")
        )
    )
)

df_CurrEnrollOut = df_XfmCurrEnroll.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD_SK").alias("BNF_SUM_DTL_NTWK_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK"),
    F.col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("LMT_AMT").alias("LMT_AMT"),
    F.col("STOPLOSS_AMT").alias("STOPLOSS_AMT"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("svPlnEffDt").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

write_files(
    df_CurrEnrollOut.select(
        "SRC_SYS_CD_SK",
        "SUB_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "ACCUM_NO",
        "BNF_SUM_DTL_NTWK_TYP_CD_SK",
        "BNF_SUM_DTL_TYP_CD_SK",
        "BLUEKC_DPLY_IN",
        "MBR_360_DPLY_IN",
        "EXTRNL_DPLY_ACCUM_DESC",
        "INTRNL_DPLY_ACCUM_DESC",
        "LMT_AMT",
        "STOPLOSS_AMT",
        "PROD_SK",
        "YR_NO",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT"
    ),
    f"{adls_path_raw}/landing/PFmlyLmtCurrEnroll.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

schema_CurrentRecs = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("PROD_ACCUM_ID", StringType(), True),
    StructField("ACCUM_NO", IntegerType(), True),
    StructField("BNF_SUM_DTL_NTWK_TYP_CD_SK", IntegerType(), True),
    StructField("BNF_SUM_DTL_TYP_CD_SK", IntegerType(), True),
    StructField("BLUEKC_DPLY_IN", StringType(), True),
    StructField("MBR_360_DPLY_IN", StringType(), True),
    StructField("EXTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("INTRNL_DPLY_ACCUM_DESC", StringType(), True),
    StructField("LMT_AMT", DecimalType(38,10), True),
    StructField("STOPLOSS_AMT", DecimalType(38,10), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("YR_NO", IntegerType(), True),
    StructField("PLN_YR_EFF_DT", DateType(), True),
    StructField("PLN_YR_END_DT", DateType(), True)
])
df_CurrentRecs_out = (
    spark.read.format("csv")
    .option("header","false")
    .option("sep",",")
    .option("quote","\"")
    .schema(schema_CurrentRecs)
    .load(f"{adls_path_raw}/landing/PFmlyLmtCurrEnroll.dat")
)

df_Collector_81 = df_CurrentRecs_out.unionByName(df_PMbrLmtPriorEnroll)

df_CurrRecsOut = df_Collector_81.alias("CurrRecsOut")

df_Trans3_temp = (
    df_CurrRecsOut
    .join(
        df_Cd_Mppng.alias("refNtwkTypCd"),
        F.col("CurrRecsOut.BNF_SUM_DTL_NTWK_TYP_CD_SK") == F.col("refNtwkTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cd_Mppng.alias("refDtlTypCd"),
        F.col("CurrRecsOut.BNF_SUM_DTL_TYP_CD_SK") == F.col("refDtlTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cd_Mppng.alias("refSrcSysCd"),
        F.col("CurrRecsOut.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left"
    )
)

df_Trans3_out = df_Trans3_temp.select(
    F.when(F.col("refSrcSysCd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("CurrRecsOut.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CurrRecsOut.YR_NO").alias("YR_NO"),
    F.col("CurrRecsOut.PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("CurrRecsOut.ACCUM_NO").alias("ACCUM_NO"),
    F.when(F.col("refNtwkTypCd.TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("refNtwkTypCd.TRGT_CD")).alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    F.when(F.col("refNtwkTypCd.TRGT_CD_NM").isNull(), F.lit(" ")).otherwise(F.col("refNtwkTypCd.TRGT_CD_NM")).alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.when(F.col("refDtlTypCd.TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("refDtlTypCd.TRGT_CD")).alias("BNF_SUM_DTL_TYP_CD"),
    F.when(F.col("refDtlTypCd.TRGT_CD_NM").isNull(), F.lit(" ")).otherwise(F.col("refDtlTypCd.TRGT_CD_NM")).alias("BNF_SUM_DTL_TYP_NM"),
    F.col("CurrRecsOut.BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("CurrRecsOut.MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("CurrRecsOut.EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("CurrRecsOut.INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("CurrRecsOut.LMT_AMT").alias("LMT_AMT"),
    F.col("CurrRecsOut.STOPLOSS_AMT").alias("STOPLOSS_AMT"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("CurrRecsOut.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("CurrRecsOut.PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_BusinessRules_out = df_Trans3_out.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    F.col("BNF_SUM_DTL_NTWK_TYP_NM").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_SUM_DTL_TYP_NM").alias("BNF_SUM_DTL_TYP_NM"),
    F.when(
        F.col("ACCUM_NO").isin(2,8,9,10),
        F.when(F.col("STOPLOSS_AMT")>0, F.col("STOPLOSS_AMT")).otherwise(F.lit(0))
    ).otherwise(F.lit(0)).alias("LMT_AMT"),
    F.col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_P_FMLY_LMT = (
    df_BusinessRules_out
    .withColumn("BLUEKC_DPLY_IN", F.rpad("BLUEKC_DPLY_IN", 1, " "))
    .withColumn("MBR_360_DPLY_IN", F.rpad("MBR_360_DPLY_IN", 1, " "))
)

write_files(
    df_P_FMLY_LMT.select(
        "SRC_SYS_CD",
        "SUB_UNIQ_KEY",
        "YR_NO",
        "PROD_ACCUM_ID",
        "ACCUM_NO",
        "BNF_SUM_DTL_NTWK_TYP_CD",
        "BNF_SUM_DTL_NTWK_TYP_NM",
        "BNF_SUM_DTL_TYP_CD",
        "BNF_SUM_DTL_TYP_NM",
        "LMT_AMT",
        "BLUEKC_DPLY_IN",
        "MBR_360_DPLY_IN",
        "EXTRNL_DPLY_ACCUM_DESC",
        "INTRNL_DPLY_ACCUM_DESC",
        "LAST_UPDT_RUN_CYC_NO",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT"
    ),
    f"{adls_path}/load/P_FMLY_LMT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)