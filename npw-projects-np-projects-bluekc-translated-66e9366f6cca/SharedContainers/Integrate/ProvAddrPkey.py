# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
DataStage Shared Container: ProvAddrPkey

* VC LOGS *
^1_4 07/27/09 10:47:20 Batch  15184_38855 PROMOTE bckcetl:31540 ids20 dsadm JHH per Ralph
^1_4 07/27/09 10:44:32 Batch  15184_38685 INIT bckcett:31540 testIDSnew dsadm JHHII per Ralph
^1_3 07/27/09 10:11:35 Batch  15184_36722 INIT bckcett:31540 testIDSnew dsadm JHHII
^1_2 07/15/09 16:34:26 Batch  15172_59695 PROMOTE bckcett:31540 testIDSnew u150906 3500-WebRealignProv_Ralph_testIDSnew                      Maddy
^1_2 07/15/09 16:24:47 Batch  15172_59140 INIT bckcett:31540 devlIDSnew u150906 3500WebRealignProv_Ralph_devlIDSnew                Maddy
^1_2 09/29/08 15:06:40 Batch  14883_54404 PROMOTE bckcetl ids20 dsadm bls for sa
^1_2 09/29/08 14:52:18 Batch  14883_53540 INIT bckcett testIDS dsadm bls for sa
^1_1 09/18/08 13:36:30 Batch  14872_48995 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 09/18/08 13:26:42 Batch  14872_48429 INIT bckcett devlIDSnew u03651 steffy
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_2 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_2 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_1 09/21/07 10:21:10 Batch  14509_37273 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 09/21/07 10:19:18 Batch  14509_37160 INIT bckcett testIDS30 dsadm bls for sa
^1_2 09/18/07 16:45:38 Batch  14506_60351 PROMOTE bckcett testIDS30 u10157 IAD 3rd Quarter Provider code moved to test
^1_2 09/18/07 15:05:48 Batch  14506_54384 INIT bckcett devlIDS30 u10157 moving IAD 3rd Quarter Provider changes to test
^1_2 09/14/06 13:22:08 Batch  14137_48134 INIT bckcett devlIDS30 u06640 Ralph
^1_1 09/08/06 12:32:44 Batch  14131_45168 INIT bckcett devlIDS30 u150129 Laurel
^1_1 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent

**************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY


CALLED BY:   FctsProvAddrExtr, ESIProvAddrExtr and NABPProvAddrExtr

DESCRIPTION: Container used for primary keying of the provider address jobs

MODIFICATIONS:
Developer                    Date                                  Change Description                                          Project #                      Development Project          Code Reviewer               Date Reviewed
------------------           ----------------------------        ----------------------------------------------------------------------------          -------------------------           ------------------------------------       ----------------
Parik                    2008-09-15               Added the new field to the container                                  3784(PBM)                   devlIDSnew                       Steph Goddard               09/15/2008
Ralph Tucker      2009-07-02               Changed field attributes for Long/Lat fields                        3500 - Web Realign    devIDSnew                          Steph Goddard              07/15/2009
Kalyan Neelam    2010-12-24               Removed IDS_SK and added PROV_ADDR_SK              4616                           IntegrateNewDevl                Steph Goddard              12/28/2010
                                                             while assigning Primary Key Updated documentation with Medtrak
Brent Leland        8-08-2011                Removed "data element" column values.                           TTR 655                      IntegrateCurDevl
                                                            Corrected PROV_ADDR_GEO_ACES_RTRN_CD_TX
                                                            metadata.

Annotations:
- This container is used in:
  FctsProvAddrExtr
  ESIProvAddrExtr
  MedtrakProvAddrExtr
  BCBSSCProvAddrExtr
- If primary key found, assign surrogate key, otherwise get next key and update dummy table.

"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_ProvAddrPkey(
        df_Transform: DataFrame,
        params: dict
    ) -> DataFrame:
    """
    Executes the ProvAddrPkey shared-container logic.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame representing the 'Transform' input link.
    params : dict
        Dictionary containing runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Resulting DataFrame corresponding to the 'Key' output link.
    """

    # ------------------------- unpack parameters -------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    IDSOwner              = params.get("IDSOwner", "")
    ids_secret_name       = params.get("ids_secret_name", "")
    # --------------------------------------------------------------------

    # ------------------------- read dummy table (lookup) -----------------
    extract_query = """
        SELECT
            SRC_SYS_CD            AS SRC_SYS_CD_lkp,
            PROV_ADDR_ID          AS PROV_ADDR_ID_lkp,
            PROV_ADDR_TYP_CD      AS PROV_ADDR_TYP_CD_lkp,
            PROV_ADDR_EFF_DT      AS PROV_ADDR_EFF_DT_lkp,
            CRT_RUN_CYC_EXCTN_SK  AS CRT_RUN_CYC_EXCTN_SK_lkp,
            PROV_ADDR_SK          AS PROV_ADDR_SK_lkp
        FROM dummy_hf_prov_addr
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # --------------------------------------------------------------------

    # ------------------------- join input with lookup -------------------
    df_join = (
        df_Transform.alias("t")
            .join(
                df_lkup.alias("lk"),
                [
                    F.col("t.SRC_SYS_CD")       == F.col("lk.SRC_SYS_CD_lkp"),
                    F.col("t.PROV_ADDR_ID")     == F.col("lk.PROV_ADDR_ID_lkp"),
                    F.col("t.PROV_ADDR_TYP_CD") == F.col("lk.PROV_ADDR_TYP_CD_lkp"),
                    F.col("t.EFF_DT")           == F.col("lk.PROV_ADDR_EFF_DT_lkp")
                ],
                "left"
            )
    )
    # --------------------------------------------------------------------

    # ------------------------- apply transformations --------------------
    df_enriched = (
        df_join
            .withColumn(
                "svProvAddrSK",
                F.when(
                    F.col("lk.PROV_ADDR_SK_lkp").isNotNull(),
                    F.col("lk.PROV_ADDR_SK_lkp")
                )
            )
            .withColumn(
                "svOrigRunCycle",
                F.when(
                    F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp").isNotNull(),
                    F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp")
                ).otherwise(F.lit(CurrRunCycle))
            )
    )

    # -------------------- surrogate-key generation ----------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svProvAddrSK',
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------------------------

    # ------------------------- prepare 'Key' output ---------------------
    df_key = df_enriched.select(
        F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("t.DISCARD_IN").alias("DISCARD_IN"),
        F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("t.ERR_CT").alias("ERR_CT"),
        F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("svProvAddrSK").alias("PROV_ADDR_SK"),
        F.col("t.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("t.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        F.col("t.EFF_DT").alias("EFF_DT"),
        F.col("svOrigRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("t.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
        F.col("t.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
        F.col("t.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
        F.col("t.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
        F.col("t.HCAP_IN").alias("HCAP_IN"),
        F.col("t.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        F.col("t.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        F.col("t.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        F.col("t.TERM_DT").alias("TERM_DT"),
        F.col("t.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("t.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("t.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("t.CITY_NM").alias("CITY_NM"),
        F.col("t.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
        F.col("t.POSTAL_CD").alias("POSTAL_CD"),
        F.col("t.CNTY_NM").alias("CNTY_NM"),
        F.col("t.PHN_NO").alias("PHN_NO"),
        F.col("t.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("t.FAX_NO").alias("FAX_NO"),
        F.col("t.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("t.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("t.LAT_TX").alias("LAT_TX"),
        F.col("t.LONG_TX").alias("LONG_TX"),
        F.col("t.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
        F.col("t.PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT")
    )
    # --------------------------------------------------------------------

    # ------------------------- prepare rows for dummy-table update -------
    df_updt = (
        df_enriched
            .filter(F.col("lk.PROV_ADDR_SK_lkp").isNull())
            .select(
                F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
                F.col("t.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
                F.col("t.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
                F.col("t.EFF_DT").alias("PROV_ADDR_EFF_DT"),
                F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
                F.col("svProvAddrSK").alias("PROV_ADDR_SK")
            )
    )

    (
        df_updt.write.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_prov_addr")
            .mode("append")
            .save()
    )
    # --------------------------------------------------------------------

    return df_key