# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiProdClntCntrExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Std Product Clent Contract to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiProdExtrSeq
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                    Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                                                 -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl       Kalyan Neelam           2013-10-31
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                            Added new field BCBSA_PROD_ID in Trns                  EnterpriseNewDevl       Bhoomi Dasari            6/22/2014
# MAGIC                                                                                                                                            with default spaces     
# MAGIC Praveen Annam                                          2014-07-14           5115 BHI                               FTP stage added to transfer in binary mode               EnterpriseNewDevl       Kalyan Neelam           2014-07-14
# MAGIC Michael Harmon                                          2015-03-23           INC0156427                     Added 'BLUESELECT+' to WHERE clauses                EnterpriseCurDevl
# MAGIC                                                                                                                                             for PROD_SH_NM      
# MAGIC Aishwarya                                                    2016-06-20            5604                                 Added MKTPLC_TYP_CD,ACA_METAL_LVL_IN,       EnterpriseDevl              Jag Yelavarthi            2016-06-22      
# MAGIC                                                                                                                                            TANSITIONAL_HLTH_PLN_IN, 
# MAGIC                                                                                                                                            CNTR_GRP_SIZE_CD
# MAGIC Praneeth Kakarla                                        2019-08-15           US-136668                       Coding from MA Datamart to BCA                                   EnterpriseDev2             Jaideep Mankala       10/09/2019          
# MAGIC                                                                                                                                           (Blue Cross Association) for ProdClnt Extract
# MAGIC Mohan Karnati                                          2020-07-01              US-243135                 Including High Performance n/w  in product short name 
# MAGIC                                                                                                                                                while  extracting in the source query                           EnterpriseDev1           Hugh Sisson               2020-08-18
# MAGIC Mohan Karnati                                           2021-12-05             US 343836                  Added MA Products 'BMADVH','BMADVP' to EDW source          EnterpriseDev2             Jeyaprasanna            2021-04-20
# MAGIC                                                                                                                                      and updated BCBSA_PROD_ID transformation
# MAGIC                                                                                                                                      Removed MA Datamart from Source

# MAGIC Extract Product Client Control data,
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC Inner Join PROD_CLNT on Concat key (created by concat 4 columns) to get max  CLS_PLN_DTL_EFF_DT
# MAGIC Look up on CD_MPPNG for TRGT_CD
# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC Look up on MBR_RCST_CT_F for Calculating Sum on Activity year month
# MAGIC Look up on MBR_RCST_CT_F for Calculating Sum of  on Max Activity year month
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')

jdbc_url_CD_MPPNG, jdbc_props_CD_MPPNG = get_db_config(edw_secret_name)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CD_MPPNG)
    .options(**jdbc_props_CD_MPPNG)
    .option(
        "query",
        "SELECT DISTINCT SRC_CD,TRGT_CD,TRGT_DOMAIN_NM "
        "FROM #$EDWOwner#.CD_MPPNG "
        "WHERE "
        "TRGT_CLCTN_CD='BCA' AND "
        "TRGT_DOMAIN_NM IN ('MARKETPLACE TYPE','ACA METAL LEVEL','CONTRACT GROUP SIZE')"
    )
    .load()
)

df_Fltr_CD_MPPNG_MKT_PLC = df_CD_MPPNG.filter(F.col("TRGT_DOMAIN_NM") == "MARKETPLACE TYPE").select(
    F.col("SRC_CD"),
    F.col("TRGT_CD"),
)
df_Fltr_CD_MPPNG_CNTR_GRP = df_CD_MPPNG.filter(F.col("TRGT_DOMAIN_NM") == "CONTRACT GROUP SIZE").select(
    F.col("SRC_CD"),
    F.col("TRGT_CD"),
)
df_Fltr_CD_MPPNG_ACA_MTL = df_CD_MPPNG.filter(F.col("TRGT_DOMAIN_NM") == "ACA METAL LEVEL").select(
    F.col("SRC_CD"),
    F.col("TRGT_CD"),
)

jdbc_url_SIC_CD, jdbc_props_SIC_CD = get_db_config(edw_secret_name)
df_SIC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_SIC_CD)
    .options(**jdbc_props_SIC_CD)
    .option("query", "SELECT SIC_CD FROM #$EDWOwner#.sic_d")
    .load()
)

df_Copy_Sic_cd_Sic_cd = df_SIC_CD.select(F.col("SIC_CD").alias("SIC_CD"))
df_Copy_Sic_cd_Sic_cd_2 = df_SIC_CD.select(F.col("SIC_CD").alias("SIC_CD"))

jdbc_url_MBR_RCST_CT_F_MAX, jdbc_props_MBR_RCST_CT_F_MAX = get_db_config(edw_secret_name)
df_MBR_RCST_CT_F_MAX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR_RCST_CT_F_MAX)
    .options(**jdbc_props_MBR_RCST_CT_F_MAX)
    .option(
        "query",
        "SELECT M.GRP_SK,\n"
        "   CASE WHEN M.SUM_CNTR_PRSN_CT >= 20 THEN 'Y' ELSE 'N' END AS NTNL_MAX_YR\n"
        "FROM\n"
        "(SELECT grpd.GRP_SK,\n"
        "    grpd.GRP_DP_IN,\n"
        "    grpd.GRP_STTUS_CD,\n"
        "    grpd.GRP_RNWL_DT_SK,\n"
        "    mbrr.ACTVTY_YR_MO_SK, \n"
        "    mbrr.PROD_BILL_CMPNT_COV_CAT_CD, \n"
        "    sum(mbrr.CNTR_PRSN_CT) as SUM_CNTR_PRSN_CT\n"
        "    FROM #$EDWOwner#.MBR_RCST_CT_F mbrr,\n"
        "    #$EDWOwner#.GRP_D grpd,\n"
        "    #$EDWOwner#.MBR_D mbrd\n"
        "    WHERE mbrr.GRP_SK = grpd.GRP_SK\n"
        "    AND  mbrr.MBR_SK = mbrd.MBR_SK\n"
        "    AND MBRR.PROD_BILL_CMPNT_COV_CAT_CD='MED'\n"
        "    AND mbrd.SUB_IN_AREA_IN = 'N'\n"
        "    GROUP BY grpd.GRP_SK,grpd.GRP_DP_IN, grpd.GRP_STTUS_CD,grpd.GRP_RNWL_DT_SK, mbrr.ACTVTY_YR_MO_SK, mbrr.PROD_BILL_CMPNT_COV_CAT_CD, mbrd.SUB_IN_AREA_IN)M,\n"
        "    (SELECT grpd.GRP_SK,MAX(mbrr.ACTVTY_YR_MO_SK) -100  AS MAX_ACTVTY_YR_MO_SK\n"
        "    FROM #$EDWOwner#.MBR_RCST_CT_F mbrr,\n"
        "    #$EDWOwner#.GRP_D grpd,\n"
        "    #$EDWOwner#.MBR_D mbrd\n"
        "    WHERE mbrr.GRP_SK = grpd.GRP_SK\n"
        "    AND  mbrr.MBR_SK = mbrd.MBR_SK\n"
        "    AND MBRR.PROD_BILL_CMPNT_COV_CAT_CD='MED'\n"
        "    AND mbrd.SUB_IN_AREA_IN = 'N'\n"
        " group by grpd.GRP_SK) MAX \n"
        "WHERE MAX.GRP_SK = M.GRP_SK \n"
        "     AND MAX.MAX_ACTVTY_YR_MO_SK = M.ACTVTY_YR_MO_SK\n"
        "     AND M.GRP_RNWL_DT_SK NOT IN ('NA','UNK')"
    )
    .load()
)

jdbc_url_MBR_RCST_CT_F_YRMOO, jdbc_props_MBR_RCST_CT_F_YRMOO = get_db_config(edw_secret_name)
df_MBR_RCST_CT_F_YRMOO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR_RCST_CT_F_YRMOO)
    .options(**jdbc_props_MBR_RCST_CT_F_YRMOO)
    .option(
        "query",
        "Select T.GRP_SK,\n"
        "    CASE WHEN  T.SUM_CNTR_PRSN_CT >= 20 THEN 'Y' ELSE 'N' END AS NTNL_YRMO\n"
        "from\n"
        "(SELECT grpd.GRP_SK,\n"
        "    grpd.GRP_DP_IN,\n"
        "    grpd.GRP_STTUS_CD,\n"
        "    grpd.GRP_RNWL_DT_SK,\n"
        "   to_char((to_date(grpd.GRP_RNWL_DT_SK,'YYYYMMDD') - 1 year),'YYYYMM') as year_GRP_RNWL_DT_SK ,\n"
        "    mbrr.ACTVTY_YR_MO_SK, \n"
        "    mbrr.PROD_BILL_CMPNT_COV_CAT_CD, \n"
        "    sum(mbrr.CNTR_PRSN_CT) as SUM_CNTR_PRSN_CT\n"
        "    FROM #$EDWOwner#.MBR_RCST_CT_F mbrr,\n"
        "    #$EDWOwner#.GRP_D grpd,\n"
        "    #$EDWOwner#.MBR_D mbrd\n"
        "    WHERE mbrr.GRP_SK = grpd.GRP_SK\n"
        "    AND  mbrr.MBR_SK = mbrd.MBR_SK\n"
        "    AND MBRR.PROD_BILL_CMPNT_COV_CAT_CD='MED'\n"
        "    AND grpd.GRP_RNWL_DT_SK NOT IN ('NA','UNK')\n"
        "    AND mbrd.SUB_IN_AREA_IN = 'N'\n"
        "      GROUP BY grpd.GRP_SK,grpd.GRP_DP_IN, grpd.GRP_STTUS_CD,grpd.GRP_RNWL_DT_SK, mbrr.ACTVTY_YR_MO_SK, mbrr.PROD_BILL_CMPNT_COV_CAT_CD, mbrd.SUB_IN_AREA_IN) T\n"
        "where T.year_GRP_RNWL_DT_SK  = T.ACTVTY_YR_MO_SK AND T.GRP_RNWL_DT_SK NOT IN ('NA','UNK')"
    )
    .load()
)

jdbc_url_Prod_Clnt_Cntr, jdbc_props_Prod_Clnt_Cntr = get_db_config(edw_secret_name)
df_Prod_Clnt_Cntr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Prod_Clnt_Cntr)
    .options(**jdbc_props_Prod_Clnt_Cntr)
    .option(
        "query",
        "SELECT DISTINCT\n"
        "  grp.GRP_ID,\n"
        "   grp.GRP_TERM_DT_SK,\n"
        "   grp.PRNT_GRP_ID,\n"
        "   grp.PRNT_GRP_SIC_NACIS_CD,\n"
        "   subgrp.SUBGRP_ID,\n"
        "   prd.EXPRNC_CAT_CD,\n"
        "   prd.PROD_SUBPROD_CD,\n"
        "   clsPln.PROD_ID,\n"
        "   clsPln.ALPHA_PFX_CD,\n"
        "   grp.GRP_SK,\n"
        "   grp.GRP_DP_IN,\n"
        "   grp.GRP_STTUS_CD,\n"
        "   grp.GRP_RNWL_DT_SK,\n"
        "   clspln.CLS_PLN_DTL_EFF_DT_SK,\n"
        "   Left(grp.PRNT_GRP_SIC_NACIS_CD,2) as PRNT_GRP_SIC_NACIS_CD_2,\n"
        "   (clsPln.PROD_ID||grp.PRNT_GRP_ID||grp.GRP_ID||subgrp.SUBGRP_ID) as concat_key,\n"
        "   prd.PROD_SH_NM,\n"
        "   prd.QHP_EXCH_CHAN_CD,\n"
        "   prd.QHP_METAL_LVL_CD,\n"
        "   grp.GRP_MKT_SIZE_CAT_CD,\n"
        "   prd.QHP_ID\n"
        "FROM #$EDWOwner#.CLS_PLN_DTL_I clsPln,\n"
        "     #$EDWOwner#.GRP_D grp,\n"
        "     #$EDWOwner#.PROD_D prd,\n"
        "     #$EDWOwner#.SUBGRP_D subgrp\n"
        "WHERE clsPln.GRP_SK = grp.GRP_SK\n"
        "AND   grp.GRP_SK = subgrp.GRP_SK\n"
        "AND   clsPln.PROD_SK = prd.PROD_SK\n"
        "and   clsPln.CLS_PLN_DTL_TERM_DT_SK >= '2010-01-01'\n"
        "AND   clsPln.GRP_ID NOT IN ('10004000','10023000','10024000','12872000')\n"
        "AND   clsPln.CLS_ID <> 'MHIP'\n"
        "AND   clsPln.CLS_PLN_DTL_PROD_CAT_CD = 'MED'\n"
        "AND   grp.PRNT_GRP_ID <> '650600000'\n"
        "and    Left(subgrp.SUBGRP_ID,2) <> 'HY'\n"
        "AND   prd.PROD_SH_NM IN ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+','HP','BMADVH','BMADVP')\n"
        "order by grp.GRP_SK,concat_key,clspln.CLS_PLN_DTL_EFF_DT_SK"
    )
    .load()
)

jdbc_url_Prod_Clnt_Cntr_Concat_key, jdbc_props_Prod_Clnt_Cntr_Concat_key = get_db_config(edw_secret_name)
df_Prod_Clnt_Cntr_Concat_key = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Prod_Clnt_Cntr_Concat_key)
    .options(**jdbc_props_Prod_Clnt_Cntr_Concat_key)
    .option(
        "query",
        "SELECT\n"
        "  (clsPln.PROD_ID||grp.PRNT_GRP_ID||grp.GRP_ID||subgrp.SUBGRP_ID) as concat_key,\n"
        " Max(clspln.CLS_PLN_DTL_EFF_DT_SK) AS CLS_PLN_DTL_EFF_DT_SK\n"
        "FROM #$EDWOwner#.CLS_PLN_DTL_I clsPln,\n"
        "     #$EDWOwner#.GRP_D grp,\n"
        "     #$EDWOwner#.PROD_D prd,\n"
        "     #$EDWOwner#.SUBGRP_D subgrp\n"
        "WHERE clsPln.GRP_SK = grp.GRP_SK\n"
        "AND   grp.GRP_SK = subgrp.GRP_SK\n"
        "AND   clsPln.PROD_SK = prd.PROD_SK\n"
        "AND   clsPln.GRP_ID NOT IN ('10004000','10023000','10024000')\n"
        "AND   clsPln.CLS_ID <> 'MHIP'\n"
        "AND   clsPln.CLS_PLN_DTL_PROD_CAT_CD = 'MED'\n"
        "AND   grp.PRNT_GRP_ID <> '650600000'\n"
        "and Left(subgrp.SUBGRP_ID,2) <> 'HY'\n"
        "AND   prd.PROD_SH_NM IN ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+','HP','BMADVH','BMADVP')\n"
        "group by  (clsPln.PROD_ID||grp.PRNT_GRP_ID||grp.GRP_ID||subgrp.SUBGRP_ID)\n"
        "Order by concat_key,CLS_PLN_DTL_EFF_DT_SK"
    )
    .load()
)

df_Join = (
    df_Prod_Clnt_Cntr.alias("Join_Src")
    .join(
        df_Prod_Clnt_Cntr_Concat_key.alias("Join_Concat_Key"),
        on=[
            F.col("Join_Src.concat_key") == F.col("Join_Concat_Key.concat_key"),
            F.col("Join_Src.CLS_PLN_DTL_EFF_DT_SK") == F.col("Join_Concat_Key.CLS_PLN_DTL_EFF_DT_SK"),
        ],
        how="inner",
    )
    .select(
        F.col("Join_Src.GRP_SK").alias("GRP_SK"),
        F.col("Join_Src.concat_key").alias("concat_key"),
        F.col("Join_Src.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Join_Src.GRP_ID").alias("GRP_ID"),
        F.col("Join_Src.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Join_Src.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Join_Src.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Join_Src.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Join_Src.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Join_Src.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Join_Src.PROD_ID").alias("PROD_ID"),
        F.col("Join_Src.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Join_Src.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Join_Src.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Join_Src.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Join_Src.CLS_PLN_DTL_EFF_DT_SK").alias("CLS_PLN_DTL_EFF_DT_SK"),
        F.col("Join_Src.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Join_Src.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Join_Src.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Join_Src.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("Join_Src.QHP_ID").alias("QHP_ID"),
    )
)

df_Rm_Dul = dedup_sort(
    df_Join,
    ["concat_key", "CLS_PLN_DTL_EFF_DT_SK", "SUBGRP_ID", "PROD_ID", "PRNT_GRP_ID", "GRP_ID"],
    [],
).select(
    F.col("GRP_SK"),
    F.col("GRP_ID"),
    F.col("GRP_TERM_DT_SK"),
    F.col("PRNT_GRP_ID"),
    F.col("PRNT_GRP_SIC_NACIS_CD"),
    F.col("SUBGRP_ID"),
    F.col("EXPRNC_CAT_CD"),
    F.col("PROD_SUBPROD_CD"),
    F.col("PROD_ID"),
    F.col("ALPHA_PFX_CD"),
    F.col("GRP_DP_IN"),
    F.col("GRP_STTUS_CD"),
    F.col("GRP_RNWL_DT_SK"),
    F.col("CLS_PLN_DTL_EFF_DT_SK"),
    F.col("PRNT_GRP_SIC_NACIS_CD_2"),
    F.col("concat_key"),
    F.col("PROD_SH_NM"),
    F.col("QHP_EXCH_CHAN_CD"),
    F.col("QHP_METAL_LVL_CD"),
    F.col("GRP_MKT_SIZE_CAT_CD"),
    F.col("QHP_ID"),
)

df_Lookup_24 = (
    df_Rm_Dul.alias("Lkup_Src_Yrmo")
    .join(
        df_MBR_RCST_CT_F_YRMOO.alias("Mbr_rcst_Yrmo"),
        F.col("Lkup_Src_Yrmo.GRP_SK") == F.col("Mbr_rcst_Yrmo.GRP_SK"),
        "left",
    )
    .select(
        F.col("Lkup_Src_Yrmo.GRP_SK").alias("GRP_SK"),
        F.col("Lkup_Src_Yrmo.GRP_ID").alias("GRP_ID"),
        F.col("Lkup_Src_Yrmo.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Lkup_Src_Yrmo.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Lkup_Src_Yrmo.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Lkup_Src_Yrmo.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lkup_Src_Yrmo.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Lkup_Src_Yrmo.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Lkup_Src_Yrmo.PROD_ID").alias("PROD_ID"),
        F.col("Lkup_Src_Yrmo.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Lkup_Src_Yrmo.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Lkup_Src_Yrmo.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Lkup_Src_Yrmo.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Lkup_Src_Yrmo.CLS_PLN_DTL_EFF_DT_SK").alias("CLS_PLN_DTL_EFF_DT_SK"),
        F.col("Mbr_rcst_Yrmo.NTNL_YRMO").alias("NTNL_YRMO"),
        F.col("Lkup_Src_Yrmo.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Lkup_Src_Yrmo.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Lkup_Src_Yrmo.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Lkup_Src_Yrmo.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Lkup_Src_Yrmo.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("Lkup_Src_Yrmo.QHP_ID").alias("QHP_ID"),
    )
)

df_Lookup_27 = (
    df_Lookup_24.alias("Lkup_Src_Max")
    .join(
        df_MBR_RCST_CT_F_MAX.alias("Mbr_Rcst_Max"),
        F.col("Lkup_Src_Max.GRP_SK") == F.col("Mbr_Rcst_Max.GRP_SK"),
        "left",
    )
    .select(
        F.col("Lkup_Src_Max.GRP_SK").alias("GRP_SK"),
        F.col("Lkup_Src_Max.GRP_ID").alias("GRP_ID"),
        F.col("Lkup_Src_Max.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Lkup_Src_Max.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Lkup_Src_Max.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Lkup_Src_Max.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lkup_Src_Max.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Lkup_Src_Max.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Lkup_Src_Max.PROD_ID").alias("PROD_ID"),
        F.col("Lkup_Src_Max.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Lkup_Src_Max.NTNL_YRMO").alias("NTNL_YRMO"),
        F.col("Mbr_Rcst_Max.NTNL_MAX_YR").alias("NTNL_MAX_YR"),
        F.col("Lkup_Src_Max.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Lkup_Src_Max.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Lkup_Src_Max.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Lkup_Src_Max.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Lkup_Src_Max.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Lkup_Src_Max.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Lkup_Src_Max.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Lkup_Src_Max.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("Lkup_Src_Max.QHP_ID").alias("QHP_ID"),
    )
)

df_Lookup_49 = (
    df_Lookup_27.alias("Lk_Sic_cd")
    .join(
        df_Copy_Sic_cd_Sic_cd.alias("Sic_cd"),
        F.col("Lk_Sic_cd.PRNT_GRP_SIC_NACIS_CD") == F.col("Sic_cd.SIC_CD"),
        "left",
    )
    .select(
        F.col("Lk_Sic_cd.GRP_SK").alias("GRP_SK"),
        F.col("Sic_cd.SIC_CD").alias("SIC_CD_1"),
        F.col("Lk_Sic_cd.GRP_ID").alias("GRP_ID"),
        F.col("Lk_Sic_cd.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Lk_Sic_cd.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Lk_Sic_cd.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Lk_Sic_cd.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lk_Sic_cd.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Lk_Sic_cd.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Lk_Sic_cd.PROD_ID").alias("PROD_ID"),
        F.col("Lk_Sic_cd.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Lk_Sic_cd.NTNL_YRMO").alias("NTNL_YRMO"),
        F.col("Lk_Sic_cd.NTNL_MAX_YR").alias("NTNL_MAX_YR"),
        F.col("Lk_Sic_cd.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Lk_Sic_cd.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Lk_Sic_cd.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Lk_Sic_cd.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Lk_Sic_cd.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Lk_Sic_cd.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Lk_Sic_cd.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Lk_Sic_cd.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("Lk_Sic_cd.QHP_ID").alias("QHP_ID"),
    )
)

df_Lookup_50 = (
    df_Lookup_49.alias("Lk_Sic_cd_2")
    .join(
        df_Copy_Sic_cd_Sic_cd_2.alias("Sic_Cd_2"),
        F.col("Lk_Sic_cd_2.PRNT_GRP_SIC_NACIS_CD_2") == F.col("Sic_Cd_2.SIC_CD"),
        "left",
    )
    .select(
        F.col("Lk_Sic_cd_2.GRP_SK").alias("GRP_SK"),
        F.col("Lk_Sic_cd_2.SIC_CD_1").alias("SIC_CD_1"),
        F.col("Sic_Cd_2.SIC_CD").alias("SIC_CD_2"),
        F.col("Lk_Sic_cd_2.GRP_ID").alias("GRP_ID"),
        F.col("Lk_Sic_cd_2.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Lk_Sic_cd_2.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Lk_Sic_cd_2.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Lk_Sic_cd_2.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lk_Sic_cd_2.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Lk_Sic_cd_2.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Lk_Sic_cd_2.PROD_ID").alias("PROD_ID"),
        F.col("Lk_Sic_cd_2.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Lk_Sic_cd_2.NTNL_YRMO").alias("NTNL_YRMO"),
        F.col("Lk_Sic_cd_2.NTNL_MAX_YR").alias("NTNL_MAX_YR"),
        F.col("Lk_Sic_cd_2.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Lk_Sic_cd_2.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Lk_Sic_cd_2.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Lk_Sic_cd_2.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Lk_Sic_cd_2.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Lk_Sic_cd_2.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Lk_Sic_cd_2.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Lk_Sic_cd_2.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("Lk_Sic_cd_2.QHP_ID").alias("QHP_ID"),
    )
)

df_Lookup_61_temp = df_Lookup_50.alias("Trns_lkp")
df_Lookup_61_temp = (
    df_Lookup_61_temp
    .join(
        df_Fltr_CD_MPPNG_CNTR_GRP.alias("CNTR_GRP"),
        F.col("Trns_lkp.GRP_MKT_SIZE_CAT_CD") == F.col("CNTR_GRP.SRC_CD"),
        "left",
    )
    .join(
        df_Fltr_CD_MPPNG_ACA_MTL.alias("ACA_MTL"),
        F.col("Trns_lkp.QHP_METAL_LVL_CD") == F.col("ACA_MTL.SRC_CD"),
        "left",
    )
    .join(
        df_Fltr_CD_MPPNG_MKT_PLC.alias("MKT_PLC"),
        F.col("Trns_lkp.QHP_EXCH_CHAN_CD") == F.col("MKT_PLC.SRC_CD"),
        "left",
    )
    .select(
        F.col("Trns_lkp.GRP_SK").alias("GRP_SK"),
        F.col("Trns_lkp.SIC_CD_1").alias("SIC_CD_1"),
        F.col("Trns_lkp.SIC_CD_2").alias("SIC_CD_2"),
        F.col("Trns_lkp.GRP_ID").alias("GRP_ID"),
        F.col("Trns_lkp.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
        F.col("Trns_lkp.PRNT_GRP_ID").alias("PRNT_GRP_ID"),
        F.col("Trns_lkp.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"),
        F.col("Trns_lkp.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Trns_lkp.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("Trns_lkp.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        F.col("Trns_lkp.PROD_ID").alias("PROD_ID"),
        F.col("Trns_lkp.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("Trns_lkp.NTNL_YRMO").alias("NTNL_YRMO"),
        F.col("Trns_lkp.NTNL_MAX_YR").alias("NTNL_MAX_YR"),
        F.col("Trns_lkp.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Trns_lkp.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("Trns_lkp.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("Trns_lkp.PRNT_GRP_SIC_NACIS_CD_2").alias("PRNT_GRP_SIC_NACIS_CD_2"),
        F.col("Trns_lkp.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("Trns_lkp.QHP_EXCH_CHAN_CD").alias("QHP_EXCH_CHAN_CD"),
        F.col("Trns_lkp.QHP_METAL_LVL_CD").alias("QHP_METAL_LVL_CD"),
        F.col("Trns_lkp.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
        F.col("CNTR_GRP.TRGT_CD").alias("TRGT_CD_CNTR_GRP"),
        F.col("ACA_MTL.TRGT_CD").alias("TRGT_CD_ACA_MTL"),
        F.col("MKT_PLC.TRGT_CD").alias("TRGT_CD_MKT_PLC"),
        F.col("Trns_lkp.QHP_ID").alias("QHP_ID"),
    )
)

df_Trns_vars = df_Lookup_61_temp.withColumn(
    "SvSicCd",
    F.when(F.col("SIC_CD_1") == F.col("PRNT_GRP_SIC_NACIS_CD"), F.col("PRNT_GRP_SIC_NACIS_CD")).otherwise(F.lit("9999"))
)

df_Trns = df_Trns_vars.select(
    F.lit("240").alias("BHI_HOME_PLN_ID"),
    F.expr("PadString(PROD_ID, ' ', 15)").alias("HOME_PLN_PROD_ID"),
    F.expr("PadString(PRNT_GRP_ID, ' ', 14)").alias("ACCT"),
    F.expr("PadString(GRP_ID, ' ', 14)").alias("GRP_ID"),
    F.expr("PadString(SUBGRP_ID, ' ', 10)").alias("SUBGRP_ID"),
    F.when(
        F.expr("Len(SvSicCd) = 0 or SvSicCd = 'NA' or SvSicCd = 'UNK' or SvSicCd = 'UNKNAICS' or IsNull(SIC_CD_2)"),
        F.lit("9999"),
    ).otherwise(F.col("SvSicCd")).alias("INDST_GRPNG_SIC_CD"),
    F.lit("NA    ").alias("INDST_GRPNG_NAICS_CD"),
    F.when(
        F.col("GRP_DP_IN") == F.lit("Y"),
        F.lit("N"),
    ).otherwise(
        F.when(
            (F.col("GRP_STTUS_CD") == F.lit("ACTV")) | (F.col("GRP_STTUS_CD") == F.lit("TERM")),
            F.when(F.col("GRP_RNWL_DT_SK") == F.lit("NA"), F.lit("N"))
            .otherwise(
                F.when(F.col("NTNL_YRMO") == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N"))
            ),
        ).otherwise(F.when(F.col("NTNL_MAX_YR") == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N")))
    ).alias("NTNL_ACCT_IN"),
    F.when(
        (F.col("EXPRNC_CAT_CD") == F.lit("ASO"))
        | (F.col("EXPRNC_CAT_CD") == F.lit("CPLS"))
        | (F.col("EXPRNC_CAT_CD") == F.lit("MPLS")),
        F.lit("Y"),
    ).otherwise(F.lit("N")).alias("ASO_CD"),
    F.when(
        F.col("GRP_DP_IN") == F.lit("Y"),
        F.lit("INDIVIDUAL"),
    ).otherwise(F.lit("GROUP     ")).alias("GRP_INDV_CD"),
    F.when(
        (F.col("ALPHA_PFX_CD") == F.lit(" "))
        | F.expr("IsNull(ALPHA_PFX_CD)")
        | (F.col("ALPHA_PFX_CD") == F.lit("NA"))
        | (F.col("ALPHA_PFX_CD") == F.lit("UNK")),
        F.lit("999"),
    ).otherwise(F.col("ALPHA_PFX_CD")).alias("ALPHA_PFX"),
    F.lit("N").alias("CSTM_NTWK_IN"),
    F.when(
        (F.col("PROD_SUBPROD_CD") == F.lit("HRA")) | (F.col("PROD_SUBPROD_CD") == F.lit("HSA")),
        F.expr("PROD_SUBPROD_CD : Str('', (5 - Len(PROD_ID)))"),
    ).otherwise(F.lit("NONE ")).alias("CDHP_OFFRD"),
    F.lit("FCTS ").alias("TRACEABILITY_FLD"),
    F.lit("Y").alias("MBR_ENR_IN"),
    F.when(
        F.col("PROD_SH_NM") == F.lit("PCB"),
        F.when(
            F.col("QHP_EXCH_CHAN_CD") == F.lit("FEDEXCH"),
            F.lit("MOL7"),
        ).otherwise(F.lit("MOL1")),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("PC"),
        F.lit("MOB1"),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("BCARE"),
        F.lit("MO1B"),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("BLUE-ACCESS"),
        F.lit("MOL3"),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("HP"),
        F.lit("MO7E"),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("BMADVP"),
        F.lit("MO3M"),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("BMADVH"),
        F.lit("    "),
    )
    .when(
        F.col("PROD_SH_NM") == F.lit("BLUE-SELECT"),
        F.when(F.col("QHP_EXCH_CHAN_CD") == F.lit("FEDEXCH"), F.lit("MOL5")).otherwise(F.lit("MO3E")),
    )
    .otherwise(F.lit("MOL9"))
    .alias("BCBSA_PROD_ID"),
    F.when(
        (F.col("QHP_EXCH_CHAN_CD") == F.lit("NA")) | (F.col("QHP_EXCH_CHAN_CD") == F.lit("UNK")),
        F.lit("NA "),
    ).otherwise(F.col("TRGT_CD_MKT_PLC")).alias("MKTPLC_TYP_CD"),
    F.when(
        (F.col("QHP_METAL_LVL_CD") == F.lit("NA")) | (F.col("QHP_METAL_LVL_CD") == F.lit("UNK")),
        F.lit("NA"),
    ).otherwise(F.col("TRGT_CD_ACA_MTL")).alias("ACA_METAL_LVL_IN"),
    F.when(
        F.col("GRP_ID") == F.lit("10003000"),
        F.lit("N"),
    )
    .otherwise(
        F.when(
            (F.col("GRP_MKT_SIZE_CAT_CD") == F.lit("DP"))
            | (F.col("GRP_MKT_SIZE_CAT_CD") == F.lit("SMGRP1"))
            | (F.col("GRP_MKT_SIZE_CAT_CD") == F.lit("SMGRP2"))
            | (F.col("GRP_MKT_SIZE_CAT_CD") == F.lit("MDGRP1"))
            | (F.col("GRP_MKT_SIZE_CAT_CD") == F.lit("MDGRP2")),
            F.when(F.col("QHP_ID") != F.lit("NA"), F.lit("N")).otherwise(F.lit("Y")),
        ).otherwise(F.lit("N"))
    )
    .alias("TANSITIONAL_HLTH_PLN_IN"),
    F.when(F.expr("IsNull(TRGT_CD_CNTR_GRP)"), F.lit("NA")).otherwise(F.col("TRGT_CD_CNTR_GRP")).alias("CNTR_GRP_SIZE_CD"),
)

df_Copy = df_Trns

df_Copy_Extr = df_Copy.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("HOME_PLN_PROD_ID"),
    F.col("ACCT"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("INDST_GRPNG_SIC_CD"),
    F.col("INDST_GRPNG_NAICS_CD"),
    F.col("NTNL_ACCT_IN"),
    F.col("ASO_CD"),
    F.col("GRP_INDV_CD"),
    F.col("ALPHA_PFX"),
    F.col("CSTM_NTWK_IN"),
    F.col("CDHP_OFFRD").alias("CDPH_OFFRD"),
    F.col("TRACEABILITY_FLD"),
    F.col("MBR_ENR_IN"),
    F.col("BCBSA_PROD_ID"),
    F.col("MKTPLC_TYP_CD"),
    F.col("ACA_METAL_LVL_IN"),
    F.col("TANSITIONAL_HLTH_PLN_IN"),
    F.col("CNTR_GRP_SIZE_CD"),
)

df_Copy_Count = df_Copy.select(F.col("BHI_HOME_PLN_ID"))

df_std_product_client_contract = df_Copy_Extr.select(
    rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad(F.col("HOME_PLN_PROD_ID"), 15, " ").alias("HOME_PLN_PROD_ID"),
    rpad(F.col("ACCT"), 14, " ").alias("ACCT"),
    rpad(F.col("GRP_ID"), 14, " ").alias("GRP_ID"),
    rpad(F.col("SUBGRP_ID"), 10, " ").alias("SUBGRP_ID"),
    rpad(F.col("INDST_GRPNG_SIC_CD"), 4, " ").alias("INDST_GRPNG_SIC_CD"),
    rpad(F.col("INDST_GRPNG_NAICS_CD"), 6, " ").alias("INDST_GRPNG_NAICS_CD"),
    rpad(F.col("NTNL_ACCT_IN"), 1, " ").alias("NTNL_ACCT_IN"),
    rpad(F.col("ASO_CD"), 1, " ").alias("ASO_CD"),
    rpad(F.col("GRP_INDV_CD"), 10, " ").alias("GRP_INDV_CD"),
    rpad(F.col("ALPHA_PFX"), 3, " ").alias("ALPHA_PFX"),
    rpad(F.col("CSTM_NTWK_IN"), 1, " ").alias("CSTM_NTWK_IN"),
    rpad(F.col("CDPH_OFFRD"), 5, " ").alias("CDPH_OFFRD"),
    rpad(F.col("TRACEABILITY_FLD"), 5, " ").alias("TRACEABILITY_FLD"),
    rpad(F.col("MBR_ENR_IN"), 1, " ").alias("MBR_ENR_IN"),
    rpad(F.col("BCBSA_PROD_ID"), 4, " ").alias("BCBSA_PROD_ID"),
    rpad(F.col("MKTPLC_TYP_CD"), 3, " ").alias("MKTPLC_TYP_CD"),
    rpad(F.col("ACA_METAL_LVL_IN"), 2, " ").alias("ACA_METAL_LVL_IN"),
    rpad(F.col("TANSITIONAL_HLTH_PLN_IN"), 1, " ").alias("TANSITIONAL_HLTH_PLN_IN"),
    rpad(F.col("CNTR_GRP_SIZE_CD"), 2, " ").alias("CNTR_GRP_SIZE_CD"),
)

write_files(
    df_std_product_client_contract,
    f"{adls_path_publish}/external/std_product_client_contract",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

df_Aggregator = df_Copy_Count.groupBy("BHI_HOME_PLN_ID").agg(F.count("*").alias("COUNT"))

df_Aggregator_sel = df_Aggregator.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("COUNT"),
)

df_Trns_cntrl_vars = df_Aggregator_sel.withColumn("svCount", F.col("COUNT"))

df_Trns_cntrl = df_Trns_cntrl_vars.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.expr("PadString('STD_PRODUCT_CLIENT_CONTRACT', ' ', 30)").alias("EXTR_NM"),
    F.expr("Trim(StartDate, '-', 'A')").alias("MIN_CLM_PROCESSED_DT"),
    F.expr("Trim(EndDate, '-', 'A')").alias("MAX_CLM_PRCS_DT"),
    F.expr("Trim(CurrDate, '-', 'A')").alias("SUBMSN_DT"),
    F.expr("Str('0', 10 - LEN(svCount)) : svCount").alias("RCRD_CT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_SUBMT_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_NONCOV_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_ALW_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_PD_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_COB_TPL_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_COINS_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_COPAY_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_DEDCT_AMT"),
    F.when(F.expr("Left('00000000000000', 1) <> '-'"), F.expr("('+' : '00000000000000')")).otherwise(F.lit("00000000000000")).alias("TOT_FFS_EQVLNT_AMT"),
)

df_submission_control = df_Trns_cntrl.select(
    rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad(F.col("EXTR_NM"), 30, " ").alias("EXTR_NM"),
    rpad(F.col("MIN_CLM_PROCESSED_DT"), 8, " ").alias("MIN_CLM_PROCESSED_DT"),
    rpad(F.col("MAX_CLM_PRCS_DT"), 8, " ").alias("MAX_CLM_PRCS_DT"),
    rpad(F.col("SUBMSN_DT"), 8, " ").alias("SUBMSN_DT"),
    rpad(F.col("RCRD_CT"), 10, " ").alias("RCRD_CT"),
    rpad(F.col("TOT_SUBMT_AMT"), 15, " ").alias("TOT_SUBMT_AMT"),
    rpad(F.col("TOT_NONCOV_AMT"), 15, " ").alias("TOT_NONCOV_AMT"),
    rpad(F.col("TOT_ALW_AMT"), 15, " ").alias("TOT_ALW_AMT"),
    rpad(F.col("TOT_PD_AMT"), 15, " ").alias("TOT_PD_AMT"),
    rpad(F.col("TOT_COB_TPL_AMT"), 15, " ").alias("TOT_COB_TPL_AMT"),
    rpad(F.col("TOT_COINS_AMT"), 15, " ").alias("TOT_COINS_AMT"),
    rpad(F.col("TOT_COPAY_AMT"), 15, " ").alias("TOT_COPAY_AMT"),
    rpad(F.col("TOT_DEDCT_AMT"), 15, " ").alias("TOT_DEDCT_AMT"),
    rpad(F.col("TOT_FFS_EQVLNT_AMT"), 15, " ").alias("TOT_FFS_EQVLNT_AMT"),
)

write_files(
    df_submission_control,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)