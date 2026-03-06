model:
  config:
    origin: datastage
  jobs:
    - name: IdsDrugRvrslExtr
      origin: datastage
      type: dataflow
      folder: "claim/DrugReversal"
      runAfterSubrOnJobSuccess: enabled
      description: "IDS Drug Reversals"
      fullDescription: "COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:     Populate the adjusted claims in CLM, DRUG_CLM, CLM_LN, CLM_REMIT_HIST with data from the original claim\n                              Update original claim with adjusted to clm id, sk, and change to status code A09\n                              Build driver table data for Claim Mart for Argus claims with fill date > 2004-12-31\n \nMODIFICATIONS:\nDeveloper                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       \n------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                  --------------------------------       -------------------------------   ----------------------------       \nBJ Luce                  2005/05                                          originally programmed\nBJ Luce                  2005/08                                          changed for new file format DRUG_CLM for rx deductibles\n                               2005-08-02                                      add recon dt to drug_clm\n                               2005-09-10                                      build driver tables for CLM_MART if ARGUS and fill date > '2004-12-01\nBJ Luce                  2005-09-28                                      add CALC_PD_AMT_IN to clm remit historyt\n\nSGoddard               2005-12-13                                      add calculated payable amount indicator to claim remit history\nBJ Luce                  2005-12-31                                     add reversal for CLM_COB for Medicare Part D staff tracker 1736\n                                                                                      add new columns to DRUG_CLM\n                                                                                      set ACTL_PD_AMT on adjustments to the original ACTL_PD_AMT * 1\nBJ Luce                  2006-01-23                                     add CLM_LN_SVC_LOC_TYP_CD_SK, CLM_LN_SVC_PRICE_RULE_CD_SK \n                                                                                     and NON_PAR_SAV_AMT to claim line adjustments\nSGoddard               2006-03-21                                    add CLM_REMIT_HIST_PAYMTMETH_CD_SK to claim remit history\nBrent Leland           2006-04-25                                    Changed parameters to environment values\n                                                                                     Specified in SQL that FILL_DT_SK column comes from DRUG_CLM.\n                                                                                     Changed name of W_CLM_RVRSLto P_CLM_RVRSL\nBrent Leland          2006-05-08                                     Changed file name and column structure for claim mart files.\nBrent Leland          2006-11-13                                     Set original claim last update run cycle with current cycle.\nBrent Leland          2007-03-16     IAD Prod. Sup.        Checked for null amount fields in claim remit history lookup transform.              devlIDS30                    Steph Goddard     4/5/07\n                                                                                     Input and output columns for clm_remit_hist_hash did not match.\n                                                                                     Added PCA_PD_AMT column to remit history output.  Set to 0.00.\n                                                                                     Added PCA_TYP_CD_SK, REL_PCA_CLM_SK, and REL_BASE_CLM_SK\n                                                                                     columns to claim table output with default surrogate key of NA.\nSandrew                 2008-10-03       3784(PBM)            Added fields ADM_FEE_AMT and DRUG_CLM_BILL_BSS_CD_SK \n                                                                                       to the adj_drug_clm extract to load to DRUG_CLM                                      devlIDSnew                   Steph Goddard       10/28/2008\nTracy Davis            2009-04-09       3784(PBM)            New field AVG_WHLSL_PRICE_AMT                                                            devlIDS                        Steph Goddard        04/14/2009\n\nSAndrew                 2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of file                                                      devlIDSnew                    Steph Goddard        07/06/2009\n                                                                                               ALT_CHRG_IN and ALT_CHRG_PROV_WRTOFF_AMT\nRalph Tucker          2009-11-03      15 - Prod Sup           Added new field: REMIT_SUPRSION_AMT                                              devlIDS                            \nKalyan Neelam        2011-01-18      4616                        Updated the default value to NULL from Space for 3 fields                        IntegrateNewDevl            Steph Goddard       01/18/2011\n                                                                                        (MCAID_RESUB_NO, PATN_ACCT_NO, RFRNG_PROV_TX) in clm Transformer\nKalyan Neelam        2011-02-02      4616                        Added 'MEDTRAK' source check for VNDR_CLM_NO                             IntegrateNewDevl            Steph Goddard        02/15/2011\n                                                                                       in adj_drug_clm SQL.\n                                                                                       Added Balancing snapshots for CLM\nKarthik Chintalaphani  2012-02-21    4784                    Added logic to check if the SRC_SYS_CD ='BCBSSC' to populate           IntegrateCurDevl                 Brent Leland            03-05-2012\n                                                                                      the INPUT_DT_SK and RCVD_DT_SK for adjusted claim.\nKalyan Neelam        2012-10-17      4784                       Added separate query for BCBSSC as a union in orig_clm_hash link          IntegrateWrhsDevl          Bhoomi Dasari         10/20/2012\nKalyan Neelam         2013-03-11      4963                Added a new column UCR_AMT in DRUG_CLM and 3 new columns          IntegrateNewDevl               Bhoomi Dasari         3/15/2013\n                                                                                 VBB_RULE_ID, VBB_EXCD_ID, CLM_LN_VBB_IN in CLM_LN\n                                                                                     Removed Data Elememts in all places\n Raja Gummadi        2013-08-28      5115 BHI           1.  Added 3 new columns to the Drug Clm Extract.                                         IntegrateNewDevl              SAndrew                 2013-09-04\n                                                                                           DRUG_CLM_PRTL_FILL_CD_SK\n                                                                                           DRUG_CLM_PDX_TYP_CD_SK\n                                                                                            INCNTV_FEE_AMT\n                                                                                  2.    Included TTR work to fix reversal amounts for DrugClm Admin Fee, UCR Amount and AWG fields.  they were not being negated.\n\n\nManasa Andru        2014-10-17       TFS - 9580     Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and  ITS_SRCHRG_AMT)      IntegrateCurDevl         Kalyan Neelam           2014-10-27\n                                                                               at the end. For the ClmLn File. \n\nManasa Andru        2017-06-21       Project -5321   Updated the extract SQL in the ids Stage, orig_clm_hash link by aliasing             IntegrateDev1                   Jag Yelavarthi        2017-08-02       \n                                                                                 R.RVRSL_CLM_SK as CLM_SK, R.RVRSL_CLM_ID as CLM_ID, \n                                                                              CLM.CLM_SK as ORIG_CLM_SK and CLM.CLM_ID as ORIG_CLM_ID \n\nHari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,                        IntegrateDev1             Kalyan Neelam           2017-09-06\n                                                                                    NDC_UNIT_CT) at the end\n\nMadhavan B          2018-02-06      5792 \x09                     Changed the datatype of the column                                                              IntegrateDev1             Kalyan Neelam          2018-02-12\n                                                      \x09\x09     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)\n\nMohan Karnati       2019-07-02      ADO-73034          Adding CLM_TXNMY_CD filed and pushing till target stage                            IntegrateDev1             Kalyan Neelam          2019-07-03\n\nRekha Radhakrishna       2020-02-07      6131-PBM replacement      Added 5 AMT fields to reversal logic for DRUG_CLM_PRICE           IntegrateDev2             Kalyan Neelam          2020-02-08   \nRekha Radhakrishna       2020-02-13      6131-PBM replacement      Corrected the order of fields  for DRUG_CLM_PRICE               IntegrateDev2              Jaideep Mankala      03/10/2020 \nRekha Radhakrishna     2020-02-29       6131-PBM Replacement     Added SrcSysCd 'OPTUMRX'  claims to be written \n                                                                                                           to W_WEBDM_ETL_DRUG.* file\n\nSagar Sayam              2020-03-28      6131-PBM replacement      Added BILL_PAYMT_EXCL_IN filed  in CLM                                                IntegrateDev2          \n\nSagar Sayam              2020-03-31      6131-PBM replacement      Added SPEC_DRUG_IN filed in DRUG_CLM                                                IntegrateDev2          \nSri Nannapaneni         2020-07-15      6131 PBM replacement      Added 7 new fields to database unload for drug_clm                                     IntegrateDev2                 \nSri Nannapaneni         2020-08-13      6131 PBM replacement      Added GNRC_PROD_ID  new fields to database unload for drug_clm         IntegrateDev2            Sravya Gorla   12/09/2020     \nRekha radhakrishna   2021-06-19      PBM Phase II Carryover      Added the fields DRUG_TYP_CD_SK, TIER_ID and FRMLRY_SK            IntegrateDev2\x09Abhiram Dasarathy\x092021-06-23\n\nAshok kumar Baskaran 2023-12-22   600693-Plan drug status cd  added new field Plan drug status cd                                                              IntegrateDev2           Jeyaprasanna         2024-01-01\n\nAshok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                 IntegrateDev2           Jeyaprasanna          2024-03-14"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: RunID
          type: string
          description: RunID
        - name: Source
          type: string
          description: "Source System"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\5;hf_drug_orig_clm;hf_drug_orig_clm_ln;hf_drug_orig_clm_remit_hist;hf_drug_orig_clm_cob;hf_drug_orig_drug_clm_price"
        subr: "5;hf_drug_orig_clm;hf_drug_orig_clm_ln;hf_drug_orig_clm_remit_hist;hf_drug_orig_clm_cob;hf_drug_orig_drug_clm_price"
      steps:
        - name: B_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_CLM.#Source#.dat.#RunID#"
          source: "clm.V167S20P7"
          option: 
            updateMode: append
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ClmCob
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_COB.#Source#.dat"
          source: clm_cob
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ClmRemitHist
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_REMIT_HIST.#Source#.dat"
          source: clm_remit_hist
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ClmLn
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_LN.#Source#.dat"
          source: clm_ln
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: W_WEBDM_ETL_DRVR_orig
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.DrugReversalOrig.#RunID#"
          source: "clm.V167S20P6"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: W_WEBDM_ETL_DRVR_adj
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.DrugReversalAdj.#RunID#"
          source: "clm.V167S20P5"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: CLM_REVERSAL_DRUG_ORIG_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_REVERSAL_DRUG_ORIG_CLM.dat"
          source: "clm.V167S20P4"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: CLM_REVERSAL_DRUG_ADJ_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_REVERSAL_DRUG_ADJ_CLM.dat"
          source: "clm.V167S20P3"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: DrugClm
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM.#Source#.dat"
          source: drug_clm
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: drug_clm_price
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM_PRICE.#Source#.dat"
          source: xfm_drug_clm_price
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: clm_cob_hash
          type: map
          source: ""
          alias: orig_clm_cob_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.CLM_SK"
            - name: CLM_COB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.CLM_COB_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_cob_hash.CLM_ID"
            - name: CLM_COB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.CLM_COB_TYP_CD_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_COB_LIAB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_hash.CLM_COB_LIAB_TYP_CD_SK"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.ALW_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.DSALW_AMT"
            - name: MED_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.MED_COINS_AMT"
            - name: MNTL_HLTH_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.MNTL_HLTH_COINS_AMT"
            - name: PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.PD_AMT"
            - name: SANC_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_hash.SANC_AMT"
            - name: COB_CAR_RSN_CD_TX
              type: varchar
              width: 255
              map: "orig_clm_cob_hash.COB_CAR_RSN_CD_TX"
            - name: COB_CAR_RSN_TX
              type: varchar
              width: 255
              map: "orig_clm_cob_hash.COB_CAR_RSN_TX"
        - name: clm_remit_hist_hash
          type: map
          source: ""
          alias: orig_clm_remit_hist_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_remit_hist_hash.CLM_SK"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_remit_hist_hash.CLM_REMIT_HIST_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_remit_hist_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_remit_hist_hash.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_remit_hist_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_remit_hist_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_hash.SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_hash.SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.COB_PD_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.PATN_RESP_AMT"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.PROV_WRTOFF_AMT"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_hash.CALC_ACTL_PD_AMT_IN"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_hash.ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_remit_hist_hash.ALT_CHRG_PROV_WRTOFF_AMT"
        - name: clm_ln_hash
          type: map
          source: ""
          alias: orig_clm_ln_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_SK"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_SEQ_NO"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_hash.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.PROC_CD_SK"
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.SVC_PROV_SK"
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_DSALW_EXCD_SK"
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_EOB_EXCD_SK"
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_FINL_DISP_CD_SK"
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_LOB_CD_SK"
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_POS_CD_SK"
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_PREAUTH_CD_SK"
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_PREAUTH_SRC_CD_SK"
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_PRICE_SRC_CD_SK"
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_RFRL_CD_SK"
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_RVNU_CD_SK"
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_ROOM_PRICE_METH_CD_SK"
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_ROOM_TYP_CD_SK"
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_TOS_CD_SK"
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_UNIT_TYP_CD_SK"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_hash.CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_hash.PRI_LOB_IN"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_ln_hash.SVC_END_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_ln_hash.SVC_STRT_DT_SK"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_ln_hash.ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_ln_hash.UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              map: "orig_clm_ln_hash.PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              map: "orig_clm_ln_hash.RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.LMT_PFX_ID"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "orig_clm_ln_hash.PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: char
              width: 9
              map: "orig_clm_ln_hash.RFRL_ID_TX"
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.SVC_PRICE_RULE_ID"
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              map: "orig_clm_ln_hash.SVC_RULE_TYP_TX"
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_SVC_LOC_TYP_CD_SK"
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_SVC_PRICE_RULE_CD_SK"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.NON_PAR_SAV_AMT"
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.VBB_RULE_SK"
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_hash.VBB_EXCD_SK"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_hash.CLM_LN_VBB_IN"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.ITS_SUPLMT_DSCNT_AMT"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_hash.ITS_SRCHRG_AMT"
            - name: NDC_SK
              type: int
              width: 4
              map: "orig_clm_ln_hash.NDC_SK"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "orig_clm_ln_hash.NDC_DRUG_FORM_CD_SK"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "orig_clm_ln_hash.NDC_UNIT_CT"
        - name: clm_and_drug_clm_hash.orig_clm_lkup
          type: map
          source: ""
          alias: orig_clm_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ORIG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ORIG_CLM_SK"
            - name: ORIG_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ORIG_CLM_ID"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ADJ_TO_CLM_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SUB_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_COB_CD_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_EOB_EXCD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.FIRST_PASS_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_hash.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "orig_clm_hash.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "orig_clm_hash.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "orig_clm_hash.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_hash.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "orig_clm_hash.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "orig_clm_hash.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "orig_clm_hash.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_hash.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "orig_clm_hash.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "orig_clm_hash.SUB_ID"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ALPHA_PFX_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACDNT_CD_SK"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.HOST_IN"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.FILL_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_clm_hash.RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_clm_hash.RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "orig_clm_hash.PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "orig_clm_hash.VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.AVG_WHLSL_PRICE_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              map: "orig_clm_hash.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.PATN_PD_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INCNTV_FEE_AMT"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "orig_clm_hash.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.BILL_PAYMT_EXCL_IN"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.SPEC_DRUG_IN"
        - name: clm_and_drug_clm_hash.orig_drug_clm_lkup
          type: map
          source: ""
          alias: orig_clm_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ORIG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ORIG_CLM_SK"
            - name: ORIG_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ORIG_CLM_ID"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ADJ_TO_CLM_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.SUB_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_COB_CD_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_EOB_EXCD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.FIRST_PASS_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_hash.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "orig_clm_hash.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "orig_clm_hash.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "orig_clm_hash.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_hash.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "orig_clm_hash.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "orig_clm_hash.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "orig_clm_hash.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_hash.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "orig_clm_hash.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "orig_clm_hash.SUB_ID"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.ALPHA_PFX_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.CLM_ACDNT_CD_SK"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.HOST_IN"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_hash.FILL_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_clm_hash.RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_clm_hash.RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "orig_clm_hash.PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "orig_clm_hash.VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "orig_clm_hash.VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.AVG_WHLSL_PRICE_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_hash.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.PATN_PD_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_hash.DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_hash.INCNTV_FEE_AMT"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "orig_clm_hash.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_hash.BILL_PAYMT_EXCL_IN"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: "orig_clm_hash.SPEC_DRUG_IN"
        - name: drug_clm_price_hash
          type: map
          source: ""
          alias: orig_DrugClmPrice_hash
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.CLM_SK"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_CST_SRC_CD_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.GNRC_OVRD_CD_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.PDX_NTWK_QLFR_CD_SK"
            - name: FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_DrugClmPrice_hash.FRMLRY_PROTOCOL_IN"
            - name: RECON_IN
              type: char
              width: 1
              map: "orig_DrugClmPrice_hash.RECON_IN"
            - name: SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
              map: "orig_DrugClmPrice_hash.SPEC_PGM_IN"
            - name: FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_DrugClmPrice_hash.FINL_PLN_EFF_DT_SK"
            - name: ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_DrugClmPrice_hash.ORIG_PD_TRANS_SUBMT_DT_SK"
            - name: PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
              map: "orig_DrugClmPrice_hash.PRORTD_DISPNS_QTY"
            - name: SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.SUBMT_DISPNS_METRIC_QTY"
            - name: AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "orig_DrugClmPrice_hash.AVG_WHLSL_PRICE_UNIT_CST_AMT"
            - name: WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "orig_DrugClmPrice_hash.WHLSL_ACQSTN_CST_UNIT_CST_AMT"
            - name: BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_DISPNS_FEE_AMT"
            - name: BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_INGR_CST_AMT"
            - name: BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_RATE_PCT"
            - name: BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_SLS_TAX_AMT"
            - name: BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_TOT_DUE_AMT"
            - name: BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_TOT_OTHR_AMT"
            - name: CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "orig_DrugClmPrice_hash.CST_TYP_UNIT_CST_AMT"
            - name: INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.INVC_TOT_DUE_AMT"
            - name: SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "orig_DrugClmPrice_hash.SELL_CST_TYP_UNIT_CST_AMT"
            - name: SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.SELL_RATE_PCT"
            - name: SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.SPREAD_DISPNS_FEE_AMT"
            - name: SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.SPREAD_INGR_CST_AMT"
            - name: SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_DrugClmPrice_hash.SPREAD_SLS_TAX_AMT"
            - name: BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_CST_TYP_ID"
            - name: BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.BUY_PRICE_TYP_ID"
            - name: CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.CST_TYP_ID"
            - name: DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.DRUG_TYP_ID"
            - name: FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.FINL_PLN_ID"
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.GNRC_PROD_ID"
            - name: SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.SELL_PRICE_TYP_ID"
            - name: SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_DrugClmPrice_hash.SPEC_PGM_ID"
            - name: BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "orig_DrugClmPrice_hash.BUY_INCNTV_FEE_AMT"
            - name: SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "orig_DrugClmPrice_hash.SELL_INCNTV_FEE_AMT"
            - name: SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "orig_DrugClmPrice_hash.SELL_OTHR_AMT"
            - name: SELL_OTHR_PAYER_AMT
              type: decimal
              width: 13
              precision: 2
              map: "orig_DrugClmPrice_hash.SELL_OTHR_PAYER_AMT"
            - name: SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "orig_DrugClmPrice_hash.SPREAD_INCNTV_FEE_AMT"
            - name: FRMLRY_SK
              type: int
              width: 4
              map: "orig_DrugClmPrice_hash.FRMLRY_SK"
            - name: TIER_ID
              type: varchar
              width: 20
              map: "orig_DrugClmPrice_hash.TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_DrugClmPrice_hash.DRUG_TYP_CD_SK"
        - name: ids
          type: dbReader
          originalDb: db2
        - name: clm_cob.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_clm_cob
              joinFilter: ""
              joinExpression: []
            - name: clm_cob_hash
              alias: orig_clm_cob_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_clm_cob_lkup.CLM_SK"
                toExpr: "adj_clm_cob.CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_clm_cob_lkup.CLM_SK = adj_clm_cob.CLM_SK"
              caseInSensitive: true
          schema: 
            - name: adj_clm_cob_out_CLM_COB_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.CLM_COB_SK"
            - name: adj_clm_cob_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.SRC_SYS_CD_SK"
            - name: adj_clm_cob_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_cob.CLM_ID"
            - name: adj_clm_cob_out_CLM_COB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.CLM_COB_TYP_CD_SK"
            - name: adj_clm_cob_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_cob_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_cob_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob.CLM_SK"
            - name: adj_clm_cob_out_CLM_COB_LIAB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_cob_lkup.CLM_COB_LIAB_TYP_CD_SK"
            - name: adj_clm_cob_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.ALW_AMT * (-1)"
            - name: adj_clm_cob_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.COPAY_AMT * (-1)"
            - name: adj_clm_cob_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.DEDCT_AMT * (-1)"
            - name: adj_clm_cob_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.DSALW_AMT * (-1)"
            - name: adj_clm_cob_out_MED_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.MED_COINS_AMT * (-1)"
            - name: adj_clm_cob_out_MNTL_HLTH_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.MNTL_HLTH_COINS_AMT * (-1)"
            - name: adj_clm_cob_out_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.PD_AMT * (-1)"
            - name: adj_clm_cob_out_SANC_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_cob_lkup.SANC_AMT * (-1)"
            - name: adj_clm_cob_out_COB_CAR_RSN_CD_TX
              type: varchar
              width: 255
              map: "orig_clm_cob_lkup.COB_CAR_RSN_CD_TX"
            - name: adj_clm_cob_out_COB_CAR_RSN_TX
              type: varchar
              width: 255
              map: "orig_clm_cob_lkup.COB_CAR_RSN_TX"
            - name: filter_adj_clm_cob_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_cob_lkup.CLM_ID) = @FALSE) then 1 else 0"
        - name: clm_cob.filter
          type: filter
          source: clm_cob.lookup
          filter: "filter_adj_clm_cob_out = 1"
          schema: 
            - name: adj_clm_cob_out_CLM_COB_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_cob_out_CLM_COB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_CLM_COB_LIAB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_cob_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_MED_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_MNTL_HLTH_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_SANC_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_cob_out_COB_CAR_RSN_CD_TX
              type: varchar
              width: 255
            - name: adj_clm_cob_out_COB_CAR_RSN_TX
              type: varchar
              width: 255
        - name: clm_cob
          type: map
          source: clm_cob.filter
          alias: adj_clm_cob_out
          schema: 
            - name: CLM_COB_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CLM_COB_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CLM_ID"
            - name: CLM_COB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CLM_COB_TYP_CD_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CLM_SK"
            - name: CLM_COB_LIAB_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_CLM_COB_LIAB_TYP_CD_SK"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_ALW_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_DSALW_AMT"
            - name: MED_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_MED_COINS_AMT"
            - name: MNTL_HLTH_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_MNTL_HLTH_COINS_AMT"
            - name: PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_PD_AMT"
            - name: SANC_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_cob_out.adj_clm_cob_out_SANC_AMT"
            - name: COB_CAR_RSN_CD_TX
              type: varchar
              width: 255
              map: "adj_clm_cob_out.adj_clm_cob_out_COB_CAR_RSN_CD_TX"
            - name: COB_CAR_RSN_TX
              type: varchar
              width: 255
              map: "adj_clm_cob_out.adj_clm_cob_out_COB_CAR_RSN_TX"
        - name: clm_remit_hist.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_clm_remit_hist
              joinFilter: ""
              joinExpression: []
            - name: clm_remit_hist_hash
              alias: orig_clm_remit_hist_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_clm_remit_hist_lkup.CLM_SK"
                toExpr: "adj_clm_remit_hist.CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_clm_remit_hist_lkup.CLM_SK = adj_clm_remit_hist.CLM_SK"
              caseInSensitive: true
          schema: 
            - name: adj_clm_remit_hist_out_CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist.CLM_REMIT_HIST_SK"
            - name: adj_clm_remit_hist_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist.SRC_SYS_CD_SK"
            - name: adj_clm_remit_hist_out_CLM_ID
              type: char
              width: 20
              nullable: N
              map: "adj_clm_remit_hist.CLM_ID"
            - name: adj_clm_remit_hist_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_remit_hist_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_remit_hist_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist.CLM_SK"
            - name: adj_clm_remit_hist_out_SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_lkup.SUPRESS_EOB_IN"
            - name: adj_clm_remit_hist_out_SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_remit_hist_lkup.SUPRESS_REMIT_IN"
            - name: adj_clm_remit_hist_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.ACTL_PD_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.ACTL_PD_AMT * (-1)"
            - name: adj_clm_remit_hist_out_COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.COB_PD_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.COB_PD_AMT * (-1)"
            - name: adj_clm_remit_hist_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.COINS_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.COINS_AMT * (-1)"
            - name: adj_clm_remit_hist_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.CNSD_CHRG_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.CNSD_CHRG_AMT * (-1)"
            - name: adj_clm_remit_hist_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.COPAY_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.COPAY_AMT * (-1)"
            - name: adj_clm_remit_hist_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.DEDCT_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.DEDCT_AMT * (-1)"
            - name: adj_clm_remit_hist_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.DSALW_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.DSALW_AMT * (-1)"
            - name: adj_clm_remit_hist_out_ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.ER_COPAY_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.ER_COPAY_AMT * (-1)"
            - name: adj_clm_remit_hist_out_INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.INTRST_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.INTRST_AMT * (-1)"
            - name: adj_clm_remit_hist_out_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.NO_RESP_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.NO_RESP_AMT * (-1)"
            - name: adj_clm_remit_hist_out_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.PATN_RESP_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.PATN_RESP_AMT * (-1)"
            - name: adj_clm_remit_hist_out_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.PROV_WRTOFF_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.PROV_WRTOFF_AMT * (-1)"
            - name: adj_clm_remit_hist_out_CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.CALC_ACTL_PD_AMT_IN) Then 0.00 Else orig_clm_remit_hist_lkup.CALC_ACTL_PD_AMT_IN"
            - name: adj_clm_remit_hist_out_PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: adj_clm_remit_hist_out_ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.ALT_CHRG_IN) Then \"N\" Else orig_clm_remit_hist_lkup.ALT_CHRG_IN"
            - name: adj_clm_remit_hist_out_ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_clm_remit_hist_lkup.ALT_CHRG_PROV_WRTOFF_AMT) Then 0.00 Else orig_clm_remit_hist_lkup.ALT_CHRG_PROV_WRTOFF_AMT * (-1)"
            - name: filter_adj_clm_remit_hist_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_remit_hist_lkup.CLM_ID) = @FALSE) then 1 else 0"
        - name: clm_remit_hist.filter
          type: filter
          source: clm_remit_hist.lookup
          filter: "filter_adj_clm_remit_hist_out = 1"
          schema: 
            - name: adj_clm_remit_hist_out_CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_remit_hist_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_remit_hist_out_CLM_ID
              type: char
              width: 20
              nullable: N
            - name: adj_clm_remit_hist_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_remit_hist_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_remit_hist_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_remit_hist_out_SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_remit_hist_out_SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_remit_hist_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_remit_hist_out_PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_remit_hist_out_ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_remit_hist_out_ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
        - name: clm_remit_hist
          type: map
          source: clm_remit_hist.filter
          alias: adj_clm_remit_hist_out
          schema: 
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CLM_REMIT_HIST_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CLM_SK"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_COB_PD_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_PATN_RESP_AMT"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_PROV_WRTOFF_AMT"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_CALC_ACTL_PD_AMT_IN"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_PCA_PD_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_remit_hist_out.adj_clm_remit_hist_out_ALT_CHRG_PROV_WRTOFF_AMT"
        - name: clm_ln.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_clm_ln
              joinFilter: ""
              joinExpression: []
            - name: clm_ln_hash
              alias: orig_clm_ln_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_clm_ln_lkup.CLM_SK"
                toExpr: "adj_clm_ln.CLM_SK"
                caseInSensitive: true
              - fromExpr: "orig_clm_ln_lkup.CLM_LN_SEQ_NO"
                toExpr: "adj_clm_ln.CLM_LN_SEQ_NO"
                caseInSensitive: true
              - fromExpr: "orig_clm_ln_lkup.CLM_LN_SK"
                toExpr: "adj_clm_ln.CLM_LN_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_clm_ln_lkup.CLM_SK = adj_clm_ln.CLM_SK"
              caseInSensitive: true
            - expression: "orig_clm_ln_lkup.CLM_LN_SEQ_NO = adj_clm_ln.CLM_LN_SEQ_NO"
              caseInSensitive: true
            - expression: "orig_clm_ln_lkup.CLM_LN_SK = adj_clm_ln.CLM_LN_SK"
              caseInSensitive: true
          schema: 
            - name: adj_clm_ln_out_CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln.CLM_LN_SK"
            - name: adj_clm_ln_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln.SRC_SYS_CD_SK"
            - name: adj_clm_ln_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln.CLM_ID"
            - name: adj_clm_ln_out_CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "adj_clm_ln.CLM_LN_SEQ_NO"
            - name: adj_clm_ln_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_ln_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_ln_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln.CLM_SK"
            - name: adj_clm_ln_out_PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.PROC_CD_SK"
            - name: adj_clm_ln_out_SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.SVC_PROV_SK"
            - name: adj_clm_ln_out_CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_DSALW_EXCD_SK"
            - name: adj_clm_ln_out_CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_EOB_EXCD_SK"
            - name: adj_clm_ln_out_CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_FINL_DISP_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_LOB_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_POS_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_PREAUTH_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_PREAUTH_SRC_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_PRICE_SRC_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_RFRL_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_RVNU_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_ROOM_PRICE_METH_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_ROOM_TYP_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_TOS_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_UNIT_TYP_CD_SK"
            - name: adj_clm_ln_out_CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_lkup.CAP_LN_IN"
            - name: adj_clm_ln_out_PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_lkup.PRI_LOB_IN"
            - name: adj_clm_ln_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_ln_lkup.SVC_END_DT_SK"
            - name: adj_clm_ln_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_ln_lkup.SVC_STRT_DT_SK"
            - name: adj_clm_ln_out_AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.AGMNT_PRICE_AMT * (-1)"
            - name: adj_clm_ln_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.ALW_AMT * (-1)"
            - name: adj_clm_ln_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.CHRG_AMT * (-1)"
            - name: adj_clm_ln_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.COINS_AMT * (-1)"
            - name: adj_clm_ln_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.CNSD_CHRG_AMT * (-1)"
            - name: adj_clm_ln_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.COPAY_AMT * (-1)"
            - name: adj_clm_ln_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.DEDCT_AMT * (-1)"
            - name: adj_clm_ln_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.DSALW_AMT * (-1)"
            - name: adj_clm_ln_out_ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.ITS_HOME_DSCNT_AMT * (-1)"
            - name: adj_clm_ln_out_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.NO_RESP_AMT * (-1)"
            - name: adj_clm_ln_out_MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.MBR_LIAB_BSS_AMT * (-1)"
            - name: adj_clm_ln_out_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PATN_RESP_AMT * (-1)"
            - name: adj_clm_ln_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PAYBL_AMT * (-1)"
            - name: adj_clm_ln_out_PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PAYBL_TO_PROV_AMT * (-1)"
            - name: adj_clm_ln_out_PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PAYBL_TO_SUB_AMT * (-1)"
            - name: adj_clm_ln_out_PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PROC_TBL_PRICE_AMT * (-1)"
            - name: adj_clm_ln_out_PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PROFL_PRICE_AMT * (-1)"
            - name: adj_clm_ln_out_PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.PROV_WRT_OFF_AMT * (-1)"
            - name: adj_clm_ln_out_RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.RISK_WTHLD_AMT * (-1)"
            - name: adj_clm_ln_out_SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.SVC_PRICE_AMT * (-1)"
            - name: adj_clm_ln_out_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.SUPLMT_DSCNT_AMT * (-1)"
            - name: adj_clm_ln_out_ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_ln_lkup.ALW_PRICE_UNIT_CT * (-1)"
            - name: adj_clm_ln_out_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_ln_lkup.UNIT_CT * (-1)"
            - name: adj_clm_ln_out_DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_lkup.DEDCT_AMT_ACCUM_ID"
            - name: adj_clm_ln_out_PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "orig_clm_ln_lkup.PREAUTH_SVC_SEQ_NO"
            - name: adj_clm_ln_out_RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "orig_clm_ln_lkup.RFRL_SVC_SEQ_NO"
            - name: adj_clm_ln_out_LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_lkup.LMT_PFX_ID"
            - name: adj_clm_ln_out_PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "orig_clm_ln_lkup.PREAUTH_ID"
            - name: adj_clm_ln_out_PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_lkup.PROD_CMPNT_DEDCT_PFX_ID"
            - name: adj_clm_ln_out_PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_lkup.PROD_CMPNT_SVC_PAYMT_ID"
            - name: adj_clm_ln_out_RFRL_ID_TX
              type: varchar
              width: 20
              map: "orig_clm_ln_lkup.RFRL_ID_TX"
            - name: adj_clm_ln_out_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_ln_lkup.SVC_ID"
            - name: adj_clm_ln_out_SVC_PRICE_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.SVC_PRICE_RULE_ID"
            - name: adj_clm_ln_out_SVC_RULE_TYP_TX
              type: char
              width: 20
              map: "orig_clm_ln_lkup.SVC_RULE_TYP_TX"
            - name: adj_clm_ln_out_CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_SVC_LOC_TYP_CD_SK"
            - name: adj_clm_ln_out_CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_SVC_PRICE_RULE_CD_SK"
            - name: adj_clm_ln_out_NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.NON_PAR_SAV_AMT * (-1)"
            - name: adj_clm_ln_out_VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.VBB_RULE_SK"
            - name: adj_clm_ln_out_VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_ln_lkup.VBB_EXCD_SK"
            - name: adj_clm_ln_out_CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_ln_lkup.CLM_LN_VBB_IN"
            - name: adj_clm_ln_out_ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.ITS_SUPLMT_DSCNT_AMT * (-1)"
            - name: adj_clm_ln_out_ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_ln_lkup.ITS_SRCHRG_AMT * (-1)"
            - name: adj_clm_ln_out_NDC_SK
              type: int
              width: 4
              map: "orig_clm_ln_lkup.NDC_SK"
            - name: adj_clm_ln_out_NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "orig_clm_ln_lkup.NDC_DRUG_FORM_CD_SK"
            - name: adj_clm_ln_out_NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "orig_clm_ln_lkup.NDC_UNIT_CT * (-1)"
            - name: filter_adj_clm_ln_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_ln_lkup.CLM_ID) = @FALSE) then 1 else 0"
        - name: clm_ln.filter
          type: filter
          source: clm_ln.lookup
          filter: "filter_adj_clm_ln_out = 1"
          schema: 
            - name: adj_clm_ln_out_CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: adj_clm_ln_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_PROC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_SVC_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_ln_out_PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_ln_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_clm_ln_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_clm_ln_out_AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: adj_clm_ln_out_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: adj_clm_ln_out_DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: adj_clm_ln_out_RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: adj_clm_ln_out_LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
            - name: adj_clm_ln_out_PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_RFRL_ID_TX
              type: varchar
              width: 20
            - name: adj_clm_ln_out_SVC_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_ln_out_SVC_PRICE_RULE_ID
              type: varchar
              width: 4
              nullable: N
            - name: adj_clm_ln_out_SVC_RULE_TYP_TX
              type: char
              width: 20
            - name: adj_clm_ln_out_CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_VBB_RULE_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_ln_out_CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_ln_out_ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_ln_out_NDC_SK
              type: int
              width: 4
            - name: adj_clm_ln_out_NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
            - name: adj_clm_ln_out_NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
        - name: clm_ln
          type: map
          source: clm_ln.filter
          alias: adj_clm_ln_out
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_SEQ_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_SK"
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROC_CD_SK"
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_PROV_SK"
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_DSALW_EXCD_SK"
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_EOB_EXCD_SK"
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_FINL_DISP_CD_SK"
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_LOB_CD_SK"
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_POS_CD_SK"
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_PREAUTH_CD_SK"
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_PREAUTH_SRC_CD_SK"
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_PRICE_SRC_CD_SK"
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_RFRL_CD_SK"
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_RVNU_CD_SK"
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_ROOM_PRICE_METH_CD_SK"
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_ROOM_TYP_CD_SK"
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_TOS_CD_SK"
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_UNIT_TYP_CD_SK"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PRI_LOB_IN"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_END_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_STRT_DT_SK"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "adj_clm_ln_out.adj_clm_ln_out_PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "adj_clm_ln_out.adj_clm_ln_out_RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_LMT_PFX_ID"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              map: "adj_clm_ln_out.adj_clm_ln_out_RFRL_ID_TX"
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_PRICE_RULE_ID"
            - name: SVC_RULE_TYP_TX
              type: char
              width: 20
              map: "adj_clm_ln_out.adj_clm_ln_out_SVC_RULE_TYP_TX"
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_SVC_LOC_TYP_CD_SK"
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_SVC_PRICE_RULE_CD_SK"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_NON_PAR_SAV_AMT"
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_VBB_RULE_SK"
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_VBB_EXCD_SK"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_CLM_LN_VBB_IN"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_ITS_SUPLMT_DSCNT_AMT"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_ln_out.adj_clm_ln_out_ITS_SRCHRG_AMT"
            - name: NDC_SK
              type: int
              width: 4
              map: "adj_clm_ln_out.adj_clm_ln_out_NDC_SK"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "adj_clm_ln_out.adj_clm_ln_out_NDC_DRUG_FORM_CD_SK"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "adj_clm_ln_out.adj_clm_ln_out_NDC_UNIT_CT"
        - name: drug_clm.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_drug_clm
              joinFilter: ""
              joinExpression: []
            - name: clm_and_drug_clm_hash.orig_drug_clm_lkup
              alias: orig_drug_clm_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_drug_clm_lkup.CLM_SK"
                toExpr: "adj_drug_clm.CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_drug_clm_lkup.CLM_SK = adj_drug_clm.CLM_SK"
              caseInSensitive: true
          schema: 
            - name: adj_drug_clm_out_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.DRUG_CLM_SK"
            - name: adj_drug_clm_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.SRC_SYS_CD_SK"
            - name: adj_drug_clm_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_drug_clm.CLM_ID"
            - name: adj_drug_clm_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_drug_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_drug_clm_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.CLM_SK"
            - name: adj_drug_clm_out_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.NDC_SK"
            - name: adj_drug_clm_out_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.PRSCRB_PROV_DEA_SK"
            - name: adj_drug_clm_out_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_LGL_STTUS_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_TIER_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: adj_drug_clm_out_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.CMPND_IN"
            - name: adj_drug_clm_out_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.FRMLRY_IN"
            - name: adj_drug_clm_out_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.GNRC_DRUG_IN"
            - name: adj_drug_clm_out_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.MAIL_ORDER_IN"
            - name: adj_drug_clm_out_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.MNTN_IN"
            - name: adj_drug_clm_out_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.MAX_ALW_CST_REDC_IN"
            - name: adj_drug_clm_out_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.NON_FRMLRY_DRUG_IN"
            - name: adj_drug_clm_out_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.SNGL_SRC_IN"
            - name: adj_drug_clm_out_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_drug_clm.ADJ_DT_SK"
            - name: adj_drug_clm_out_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_drug_clm_lkup.FILL_DT_SK"
            - name: adj_drug_clm_out_RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_drug_clm.RECON_DT_SK"
            - name: adj_drug_clm_out_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.DISPNS_FEE_AMT * (-1)"
            - name: adj_drug_clm_out_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.HLTH_PLN_EXCL_AMT * (-1)"
            - name: adj_drug_clm_out_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.HLTH_PLN_PD_AMT * (-1)"
            - name: adj_drug_clm_out_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.INGR_CST_ALW_AMT * (-1)"
            - name: adj_drug_clm_out_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.INGR_CST_CHRGD_AMT * (-1)"
            - name: adj_drug_clm_out_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.INGR_SAV_AMT * (-1)"
            - name: adj_drug_clm_out_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.MBR_DEDCT_EXCL_AMT * (-1)"
            - name: adj_drug_clm_out_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.MBR_DIFF_PD_AMT * (-1)"
            - name: adj_drug_clm_out_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.MBR_OOP_AMT * (-1)"
            - name: adj_drug_clm_out_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.MBR_OOP_EXCL_AMT * (-1)"
            - name: adj_drug_clm_out_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.OTHR_SAV_AMT * (-1)"
            - name: adj_drug_clm_out_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_drug_clm_lkup.RX_ALW_QTY * (-1)"
            - name: adj_drug_clm_out_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "orig_drug_clm_lkup.RX_SUBMT_QTY * (-1)"
            - name: adj_drug_clm_out_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_drug_clm_lkup.SLS_TAX_AMT * (-1)"
            - name: adj_drug_clm_out_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.RX_ALW_DAYS_SUPL_QTY * (-1)"
            - name: adj_drug_clm_out_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.RX_ORIG_DAYS_SUPL_QTY * (-1)"
            - name: adj_drug_clm_out_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "orig_drug_clm_lkup.PDX_NTWK_ID"
            - name: adj_drug_clm_out_RX_NO
              type: varchar
              width: 20
              map: "orig_drug_clm_lkup.RX_NO"
            - name: adj_drug_clm_out_RFL_NO
              type: varchar
              width: 20
              map: "orig_drug_clm_lkup.RFL_NO"
            - name: adj_drug_clm_out_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "adj_drug_clm.VNDR_CLM_NO"
            - name: adj_drug_clm_out_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "orig_drug_clm_lkup.VNDR_PREAUTH_ID"
            - name: adj_drug_clm_out_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_PDX_NTWK_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_PRAUTH_CD_SK"
            - name: adj_drug_clm_out_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_lkup.MNDTRY_MAIL_ORDER_IN"
            - name: adj_drug_clm_out_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_lkup.ADM_FEE_AMT) then 0.00 else (orig_drug_clm_lkup.ADM_FEE_AMT * -1)"
            - name: adj_drug_clm_out_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " iF IsNull(orig_drug_clm_lkup.DRUG_CLM_BILL_BSS_CD_SK) then 1 else orig_drug_clm_lkup.DRUG_CLM_BILL_BSS_CD_SK"
            - name: adj_drug_clm_out_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "(orig_drug_clm_lkup.AVG_WHLSL_PRICE_AMT) * -1"
            - name: adj_drug_clm_out_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "(orig_drug_clm_lkup.UCR_AMT) * -1"
            - name: adj_drug_clm_out_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_PRTL_FILL_CD_SK"
            - name: adj_drug_clm_out_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_lkup.DRUG_CLM_PDX_TYP_CD_SK"
            - name: adj_drug_clm_out_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "(orig_drug_clm_lkup.INCNTV_FEE_AMT) * -1"
            - name: adj_drug_clm_out_SPEC_DRUG_IN
              type: char
              width: 1
              map: "adj_drug_clm.SPEC_DRUG_IN"
            - name: adj_drug_clm_out_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.SUBMT_PROD_ID_QLFR_CD_SK"
            - name: adj_drug_clm_out_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm.CNTNGNT_THER_CD_SK"
            - name: adj_drug_clm_out_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "adj_drug_clm.CNTNGNT_THER_SCHD_ID"
            - name: adj_drug_clm_out_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm.MBR_NTWK_DIFF_PD_AMT"
            - name: adj_drug_clm_out_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm.MBR_SLS_TAX_AMT"
            - name: adj_drug_clm_out_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm.MBR_PRCS_FEE_AMT"
            - name: adj_drug_clm_out_GNRC_PROD_ID
              type: varchar
              width: 20
              map: "adj_drug_clm.GNRC_PROD_ID"
            - name: filter_adj_drug_clm_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_drug_clm_lkup.CLM_ID) = @FALSE) then 1 else 0"
        - name: drug_clm.filter
          type: filter
          source: drug_clm.lookup
          filter: "filter_adj_drug_clm_out = 1"
          schema: 
            - name: adj_drug_clm_out_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_drug_clm_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_NDC_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_drug_clm_out_FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_drug_clm_out_RECON_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_drug_clm_out_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: adj_drug_clm_out_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: adj_drug_clm_out_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_PDX_NTWK_ID
              type: varchar
              width: 20
            - name: adj_drug_clm_out_RX_NO
              type: varchar
              width: 20
            - name: adj_drug_clm_out_RFL_NO
              type: varchar
              width: 20
            - name: adj_drug_clm_out_VNDR_CLM_NO
              type: varchar
              width: 20
            - name: adj_drug_clm_out_VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: adj_drug_clm_out_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: adj_drug_clm_out_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_drug_clm_out_SPEC_DRUG_IN
              type: char
              width: 1
            - name: adj_drug_clm_out_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_drug_clm_out_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
            - name: adj_drug_clm_out_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_drug_clm_out_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_drug_clm_out_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_drug_clm_out_GNRC_PROD_ID
              type: varchar
              width: 20
        - name: drug_clm
          type: map
          source: drug_clm.filter
          alias: adj_drug_clm_out
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_FILL_DT_SK"
            - name: RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_RECON_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_AVG_WHLSL_PRICE_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_INCNTV_FEE_AMT"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: "adj_drug_clm_out.adj_drug_clm_out_SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_SUBMT_PROD_ID_QLFR_CD_SK"
            - name: CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_drug_clm_out.adj_drug_clm_out_CNTNGNT_THER_CD_SK"
            - name: CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_CNTNGNT_THER_SCHD_ID"
            - name: MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_NTWK_DIFF_PD_AMT"
            - name: MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_SLS_TAX_AMT"
            - name: MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_drug_clm_out.adj_drug_clm_out_MBR_PRCS_FEE_AMT"
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              map: "adj_drug_clm_out.adj_drug_clm_out_GNRC_PROD_ID"
        - name: clm.lookup
          type: lookup
          stageVars:
            - name: svStatusCd
              type: decimal
              width: 0
              map: "GetFkeyCodes(adj_clm.SRC_SYS_CD, orig_clm_lkup.ORIG_CLM_SK, \"CLAIM STATUS\", \"A09\", 'X')"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_clm
              joinFilter: ""
              joinExpression: []
            - name: clm_and_drug_clm_hash.orig_clm_lkup
              alias: orig_clm_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_clm_lkup.CLM_SK"
                toExpr: "adj_clm.CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_clm_lkup.CLM_SK = adj_clm.CLM_SK"
              caseInSensitive: true
          schema: 
            - name: adj_claims_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.CLM_SK"
            - name: adj_claims_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.SRC_SYS_CD_SK"
            - name: adj_claims_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm.CLM_ID"
            - name: adj_claims_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_claims_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_claims_out_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_SK) = @TRUE then 0 else orig_clm_lkup.ORIG_CLM_SK"
            - name: adj_claims_out_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.ADJ_TO_CLM_SK"
            - name: adj_claims_out_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.ALPHA_PFX_SK) = @TRUE then 0 else orig_clm_lkup.ALPHA_PFX_SK"
            - name: adj_claims_out_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_EOB_EXCD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_EOB_EXCD_SK"
            - name: adj_claims_out_CLS_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLS_SK) = @TRUE then 0 else orig_clm_lkup.CLS_SK"
            - name: adj_claims_out_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLS_PLN_SK) = @TRUE then 0 else orig_clm_lkup.CLS_PLN_SK"
            - name: adj_claims_out_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.EXPRNC_CAT_SK) = @TRUE then 0 else orig_clm_lkup.EXPRNC_CAT_SK"
            - name: adj_claims_out_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.FNCL_LOB_SK) = @TRUE then 0 else orig_clm_lkup.FNCL_LOB_SK"
            - name: adj_claims_out_GRP_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.GRP_SK) = @TRUE then 0 else orig_clm_lkup.GRP_SK"
            - name: adj_claims_out_MBR_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.MBR_SK) = @TRUE then 0 else orig_clm_lkup.MBR_SK"
            - name: adj_claims_out_NTWK_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.NTWK_SK) = @TRUE then 0 else orig_clm_lkup.NTWK_SK"
            - name: adj_claims_out_PROD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROD_SK) = @TRUE then 0 else orig_clm_lkup.PROD_SK"
            - name: adj_claims_out_SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUBGRP_SK) = @TRUE then 0 else orig_clm_lkup.SUBGRP_SK"
            - name: adj_claims_out_SUB_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_SK) = @TRUE then 0 else orig_clm_lkup.SUB_SK"
            - name: adj_claims_out_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACDNT_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACDNT_CD_SK"
            - name: adj_claims_out_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACDNT_ST_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACDNT_ST_CD_SK"
            - name: adj_claims_out_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: adj_claims_out_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_AGMNT_SRC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_AGMNT_SRC_CD_SK"
            - name: adj_claims_out_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_BTCH_ACTN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_BTCH_ACTN_CD_SK"
            - name: adj_claims_out_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CAP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CAP_CD_SK"
            - name: adj_claims_out_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CAT_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CAT_CD_SK"
            - name: adj_claims_out_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK"
            - name: adj_claims_out_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_COB_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_COB_CD_SK"
            - name: adj_claims_out_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_FINL_DISP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_FINL_DISP_CD_SK"
            - name: adj_claims_out_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INPT_METH_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INPT_METH_CD_SK"
            - name: adj_claims_out_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INPT_SRC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INPT_SRC_CD_SK"
            - name: adj_claims_out_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK"
            - name: adj_claims_out_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_NTWK_STTUS_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_NTWK_STTUS_CD_SK"
            - name: adj_claims_out_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: adj_claims_out_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_OTHR_BNF_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_OTHR_BNF_CD_SK"
            - name: adj_claims_out_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_PAYE_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_PAYE_CD_SK"
            - name: adj_claims_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: adj_claims_out_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK"
            - name: adj_claims_out_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK"
            - name: adj_claims_out_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK"
            - name: adj_claims_out_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(adj_clm.CLM_STTUS_CD_SK) = @TRUE then 0 else adj_clm.CLM_STTUS_CD_SK"
            - name: adj_claims_out_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: adj_claims_out_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK"
            - name: adj_claims_out_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUBTYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUBTYP_CD_SK"
            - name: adj_claims_out_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_TYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_TYP_CD_SK"
            - name: adj_claims_out_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.ATCHMT_IN) = @TRUE then ' ' else orig_clm_lkup.ATCHMT_IN"
            - name: adj_claims_out_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLNCL_EDIT_IN) = @TRUE then ' ' else orig_clm_lkup.CLNCL_EDIT_IN"
            - name: adj_claims_out_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.COBRA_CLM_IN) = @TRUE then ' ' else orig_clm_lkup.COBRA_CLM_IN"
            - name: adj_claims_out_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.FIRST_PASS_IN) = @TRUE then ' ' else orig_clm_lkup.FIRST_PASS_IN"
            - name: adj_claims_out_HOST_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.HOST_IN) = @TRUE then ' ' else orig_clm_lkup.HOST_IN"
            - name: adj_claims_out_LTR_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.LTR_IN) = @TRUE then ' ' else orig_clm_lkup.LTR_IN"
            - name: adj_claims_out_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.MCARE_ASG_IN) = @TRUE then ' ' else orig_clm_lkup.MCARE_ASG_IN"
            - name: adj_claims_out_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.NOTE_IN) = @TRUE then ' ' else orig_clm_lkup.NOTE_IN"
            - name: adj_claims_out_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PCA_AUDIT_IN) = @TRUE then ' ' else orig_clm_lkup.PCA_AUDIT_IN"
            - name: adj_claims_out_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PCP_SUBMT_IN) = @TRUE then ' ' else orig_clm_lkup.PCP_SUBMT_IN"
            - name: adj_claims_out_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROD_OOA_IN) = @TRUE then ' ' else orig_clm_lkup.PROD_OOA_IN"
            - name: adj_claims_out_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACDNT_DT_SK) = @TRUE then 0 else orig_clm_lkup.ACDNT_DT_SK"
            - name: adj_claims_out_INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If adj_clm.SRC_SYS_CD = 'BCBSSC' Then adj_clm.INPT_DT_SK Else  If IsNull(orig_clm_lkup.INPT_DT_SK) = @TRUE then 0 else orig_clm_lkup.INPT_DT_SK"
            - name: adj_claims_out_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.MBR_PLN_ELIG_DT_SK) = @TRUE then 0 else orig_clm_lkup.MBR_PLN_ELIG_DT_SK"
            - name: adj_claims_out_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.NEXT_RVW_DT_SK) = @TRUE then 0 else orig_clm_lkup.NEXT_RVW_DT_SK"
            - name: adj_claims_out_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(adj_clm.PD_DT_SK) = @TRUE then 0 else adj_clm.PD_DT_SK"
            - name: adj_claims_out_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK) = @TRUE then 0 else orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK"
            - name: adj_claims_out_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(adj_clm.PRCS_DT_SK) = @TRUE then 0 else adj_clm.PRCS_DT_SK"
            - name: adj_claims_out_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If adj_clm.SRC_SYS_CD = 'BCBSSC' Then adj_clm.RCVD_DT_SK Else  If IsNull(orig_clm_lkup.RCVD_DT_SK) = @TRUE then 0 else orig_clm_lkup.RCVD_DT_SK"
            - name: adj_claims_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SVC_STRT_DT_SK) = @TRUE then 0 else orig_clm_lkup.SVC_STRT_DT_SK"
            - name: adj_claims_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SVC_END_DT_SK) = @TRUE then 0 else orig_clm_lkup.SVC_END_DT_SK"
            - name: adj_claims_out_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SMLR_ILNS_DT_SK) = @TRUE then 0 else orig_clm_lkup.SMLR_ILNS_DT_SK"
            - name: adj_claims_out_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(adj_clm.STTUS_DT_SK) = @TRUE then 0 else adj_clm.STTUS_DT_SK"
            - name: adj_claims_out_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.WORK_UNABLE_BEG_DT_SK) = @TRUE then 0 else orig_clm_lkup.WORK_UNABLE_BEG_DT_SK"
            - name: adj_claims_out_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.WORK_UNABLE_END_DT_SK) = @TRUE then 0 else orig_clm_lkup.WORK_UNABLE_END_DT_SK"
            - name: adj_claims_out_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACDNT_AMT) = @TRUE then 0 else orig_clm_lkup.ACDNT_AMT * (-1)"
            - name: adj_claims_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACTL_PD_AMT) = @TRUE then 0 else orig_clm_lkup.ACTL_PD_AMT * (-1)"
            - name: adj_claims_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ALW_AMT) = @TRUE then 0 else orig_clm_lkup.ALW_AMT * (-1)"
            - name: adj_claims_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.DSALW_AMT) = @TRUE then 0 else orig_clm_lkup.DSALW_AMT * (-1)"
            - name: adj_claims_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.COINS_AMT) = @TRUE then 0 else orig_clm_lkup.COINS_AMT * (-1)"
            - name: adj_claims_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CNSD_CHRG_AMT) = @TRUE then 0 else orig_clm_lkup.CNSD_CHRG_AMT * (-1)"
            - name: adj_claims_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.COPAY_AMT) = @TRUE then 0 else orig_clm_lkup.COPAY_AMT * (-1)"
            - name: adj_claims_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CHRG_AMT) = @TRUE then 0 else orig_clm_lkup.CHRG_AMT * (-1)"
            - name: adj_claims_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.DEDCT_AMT) = @TRUE then 0 else orig_clm_lkup.DEDCT_AMT * (-1)"
            - name: adj_claims_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYBL_AMT) = @TRUE then 0 else orig_clm_lkup.PAYBL_AMT * (-1)"
            - name: adj_claims_out_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CT) = @TRUE then 0 else orig_clm_lkup.CLM_CT * (-1)"
            - name: adj_claims_out_MBR_AGE
              type: decimal
              width: 3
              map: " if IsNull(orig_clm_lkup.MBR_AGE) = @TRUE then 0 else orig_clm_lkup.MBR_AGE"
            - name: adj_claims_out_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_ID) = @TRUE then ' ' else orig_clm_lkup.ORIG_CLM_ID"
            - name: adj_claims_out_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(adj_clm.ADJ_TO_CLM_ID) = @TRUE then ' ' else adj_clm.ADJ_TO_CLM_ID"
            - name: adj_claims_out_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: " if IsNull(orig_clm_lkup.DOC_TX_ID) = @TRUE then ' ' else orig_clm_lkup.DOC_TX_ID"
            - name: adj_claims_out_MCAID_RESUB_NO
              type: char
              width: 15
              map: " if IsNull(orig_clm_lkup.MCAID_RESUB_NO) = @TRUE then @NULL else orig_clm_lkup.MCAID_RESUB_NO"
            - name: adj_claims_out_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: " if IsNull(orig_clm_lkup.MCARE_ID) = @TRUE then ' ' else orig_clm_lkup.MCARE_ID"
            - name: adj_claims_out_MBR_SFX_NO
              type: char
              width: 2
              map: " if IsNull(orig_clm_lkup.MBR_SFX_NO) = @TRUE then ' ' else orig_clm_lkup.MBR_SFX_NO"
            - name: adj_claims_out_PATN_ACCT_NO
              type: varchar
              width: 35
              map: " if IsNull(orig_clm_lkup.PATN_ACCT_NO) = @TRUE then @NULL else orig_clm_lkup.PATN_ACCT_NO"
            - name: adj_claims_out_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYMT_REF_ID) = @TRUE then ' ' else orig_clm_lkup.PAYMT_REF_ID"
            - name: adj_claims_out_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROV_AGMNT_ID) = @TRUE then ' ' else orig_clm_lkup.PROV_AGMNT_ID"
            - name: adj_claims_out_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: " if IsNull(orig_clm_lkup.RFRNG_PROV_TX) = @TRUE then @NULL else orig_clm_lkup.RFRNG_PROV_TX"
            - name: adj_claims_out_SUB_ID
              type: char
              width: 14
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_ID) = @TRUE then ' ' else orig_clm_lkup.SUB_ID"
            - name: adj_claims_out_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: adj_claims_out_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: adj_claims_out_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: adj_claims_out_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: adj_claims_out_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_ID) = @TRUE then ' ' else adj_clm.MCAID_STTUS_ID"
            - name: adj_claims_out_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.PATN_PD_AMT) = @TRUE then 0.00 else (-1 * (orig_clm_lkup.PATN_PD_AMT))"
            - name: adj_claims_out_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: adj_claims_out_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: " if IsNull(orig_clm_lkup.CLM_TXNMY_CD) = @TRUE then @NULL else orig_clm_lkup.CLM_TXNMY_CD"
            - name: adj_claims_out_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_lkup.BILL_PAYMT_EXCL_IN"
            - name: orig_clm_out_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_SK) = @TRUE then 0 else orig_clm_lkup.ORIG_CLM_SK"
            - name: orig_clm_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SRC_SYS_CD_SK) = @TRUE then 0 else orig_clm_lkup.SRC_SYS_CD_SK"
            - name: orig_clm_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_ID) = @TRUE then ' ' else orig_clm_lkup.ORIG_CLM_ID"
            - name: orig_clm_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK) = @TRUE then 0 else orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK"
            - name: orig_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: orig_clm_out_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.ADJ_FROM_CLM_SK) = @TRUE then 0 else orig_clm_lkup.ADJ_FROM_CLM_SK"
            - name: orig_clm_out_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.CLM_SK"
            - name: orig_clm_out_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.ALPHA_PFX_SK) = @TRUE then 0 else orig_clm_lkup.ALPHA_PFX_SK"
            - name: orig_clm_out_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_EOB_EXCD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_EOB_EXCD_SK"
            - name: orig_clm_out_CLS_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLS_SK) = @TRUE then 0 else orig_clm_lkup.CLS_SK"
            - name: orig_clm_out_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLS_PLN_SK) = @TRUE then 0 else orig_clm_lkup.CLS_PLN_SK"
            - name: orig_clm_out_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.EXPRNC_CAT_SK) = @TRUE then 0 else orig_clm_lkup.EXPRNC_CAT_SK"
            - name: orig_clm_out_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.FNCL_LOB_SK) = @TRUE then 0 else orig_clm_lkup.FNCL_LOB_SK"
            - name: orig_clm_out_GRP_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.GRP_SK) = @TRUE then 0 else orig_clm_lkup.GRP_SK"
            - name: orig_clm_out_MBR_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.MBR_SK) = @TRUE then 0 else orig_clm_lkup.MBR_SK"
            - name: orig_clm_out_NTWK_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.NTWK_SK) = @TRUE then 0 else orig_clm_lkup.NTWK_SK"
            - name: orig_clm_out_PROD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROD_SK) = @TRUE then 0 else orig_clm_lkup.PROD_SK"
            - name: orig_clm_out_SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUBGRP_SK) = @TRUE then 0 else orig_clm_lkup.SUBGRP_SK"
            - name: orig_clm_out_SUB_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_SK) = @TRUE then 0 else orig_clm_lkup.SUB_SK"
            - name: orig_clm_out_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACDNT_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACDNT_CD_SK"
            - name: orig_clm_out_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACDNT_ST_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACDNT_ST_CD_SK"
            - name: orig_clm_out_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: orig_clm_out_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_AGMNT_SRC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_AGMNT_SRC_CD_SK"
            - name: orig_clm_out_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_BTCH_ACTN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_BTCH_ACTN_CD_SK"
            - name: orig_clm_out_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CAP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CAP_CD_SK"
            - name: orig_clm_out_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CAT_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CAT_CD_SK"
            - name: orig_clm_out_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK"
            - name: orig_clm_out_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_COB_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_COB_CD_SK"
            - name: orig_clm_out_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_FINL_DISP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_FINL_DISP_CD_SK"
            - name: orig_clm_out_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INPT_METH_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INPT_METH_CD_SK"
            - name: orig_clm_out_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INPT_SRC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INPT_SRC_CD_SK"
            - name: orig_clm_out_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK"
            - name: orig_clm_out_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_NTWK_STTUS_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_NTWK_STTUS_CD_SK"
            - name: orig_clm_out_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: orig_clm_out_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_OTHR_BNF_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_OTHR_BNF_CD_SK"
            - name: orig_clm_out_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_PAYE_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_PAYE_CD_SK"
            - name: orig_clm_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: orig_clm_out_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK"
            - name: orig_clm_out_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK"
            - name: orig_clm_out_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK"
            - name: orig_clm_out_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: svStatusCd
            - name: orig_clm_out_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: orig_clm_out_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK"
            - name: orig_clm_out_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_SUBTYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_SUBTYP_CD_SK"
            - name: orig_clm_out_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_TYP_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_TYP_CD_SK"
            - name: orig_clm_out_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.ATCHMT_IN) = @TRUE then ' ' else orig_clm_lkup.ATCHMT_IN"
            - name: orig_clm_out_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLNCL_EDIT_IN) = @TRUE then ' ' else orig_clm_lkup.CLNCL_EDIT_IN"
            - name: orig_clm_out_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.COBRA_CLM_IN) = @TRUE then ' ' else orig_clm_lkup.COBRA_CLM_IN"
            - name: orig_clm_out_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.FIRST_PASS_IN) = @TRUE then ' ' else orig_clm_lkup.FIRST_PASS_IN"
            - name: orig_clm_out_HOST_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.HOST_IN) = @TRUE then ' ' else orig_clm_lkup.HOST_IN"
            - name: orig_clm_out_LTR_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.LTR_IN) = @TRUE then ' ' else orig_clm_lkup.LTR_IN"
            - name: orig_clm_out_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.MCARE_ASG_IN) = @TRUE then ' ' else orig_clm_lkup.MCARE_ASG_IN"
            - name: orig_clm_out_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.NOTE_IN) = @TRUE then ' ' else orig_clm_lkup.NOTE_IN"
            - name: orig_clm_out_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PCA_AUDIT_IN) = @TRUE then ' ' else orig_clm_lkup.PCA_AUDIT_IN"
            - name: orig_clm_out_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PCP_SUBMT_IN) = @TRUE then ' ' else orig_clm_lkup.PCP_SUBMT_IN"
            - name: orig_clm_out_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROD_OOA_IN) = @TRUE then ' ' else orig_clm_lkup.PROD_OOA_IN"
            - name: orig_clm_out_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACDNT_DT_SK) = @TRUE then 0 else orig_clm_lkup.ACDNT_DT_SK"
            - name: orig_clm_out_INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.INPT_DT_SK) = @TRUE then 0 else orig_clm_lkup.INPT_DT_SK"
            - name: orig_clm_out_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.MBR_PLN_ELIG_DT_SK) = @TRUE then 0 else orig_clm_lkup.MBR_PLN_ELIG_DT_SK"
            - name: orig_clm_out_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.NEXT_RVW_DT_SK) = @TRUE then 0 else orig_clm_lkup.NEXT_RVW_DT_SK"
            - name: orig_clm_out_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.PD_DT_SK) = @TRUE then 0 else orig_clm_lkup.PD_DT_SK"
            - name: orig_clm_out_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK) = @TRUE then 0 else orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK"
            - name: orig_clm_out_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.PRCS_DT_SK) = @TRUE then 0 else orig_clm_lkup.PRCS_DT_SK"
            - name: orig_clm_out_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.RCVD_DT_SK) = @TRUE then 0 else orig_clm_lkup.RCVD_DT_SK"
            - name: orig_clm_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SVC_STRT_DT_SK) = @TRUE then 0 else orig_clm_lkup.SVC_STRT_DT_SK"
            - name: orig_clm_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SVC_END_DT_SK) = @TRUE then 0 else orig_clm_lkup.SVC_END_DT_SK"
            - name: orig_clm_out_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SMLR_ILNS_DT_SK) = @TRUE then 0 else orig_clm_lkup.SMLR_ILNS_DT_SK"
            - name: orig_clm_out_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.STTUS_DT_SK) = @TRUE then 0 else orig_clm_lkup.STTUS_DT_SK"
            - name: orig_clm_out_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.WORK_UNABLE_BEG_DT_SK) = @TRUE then 0 else orig_clm_lkup.WORK_UNABLE_BEG_DT_SK"
            - name: orig_clm_out_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.WORK_UNABLE_END_DT_SK) = @TRUE then 0 else orig_clm_lkup.WORK_UNABLE_END_DT_SK"
            - name: orig_clm_out_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACDNT_AMT) = @TRUE then 0 else orig_clm_lkup.ACDNT_AMT"
            - name: orig_clm_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ACTL_PD_AMT) = @TRUE then 0 else orig_clm_lkup.ACTL_PD_AMT"
            - name: orig_clm_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.ALW_AMT) = @TRUE then 0 else orig_clm_lkup.ALW_AMT"
            - name: orig_clm_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.DSALW_AMT) = @TRUE then 0 else orig_clm_lkup.DSALW_AMT"
            - name: orig_clm_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.COINS_AMT) = @TRUE then 0 else orig_clm_lkup.COINS_AMT"
            - name: orig_clm_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CNSD_CHRG_AMT) = @TRUE then 0 else orig_clm_lkup.CNSD_CHRG_AMT"
            - name: orig_clm_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.COPAY_AMT) = @TRUE then 0 else orig_clm_lkup.COPAY_AMT"
            - name: orig_clm_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CHRG_AMT) = @TRUE then 0 else orig_clm_lkup.CHRG_AMT"
            - name: orig_clm_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.DEDCT_AMT) = @TRUE then 0 else orig_clm_lkup.DEDCT_AMT"
            - name: orig_clm_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYBL_AMT) = @TRUE then 0 else orig_clm_lkup.PAYBL_AMT"
            - name: orig_clm_out_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CT) = @TRUE then 0 else orig_clm_lkup.CLM_CT"
            - name: orig_clm_out_MBR_AGE
              type: decimal
              width: 3
              map: " if IsNull(orig_clm_lkup.MBR_AGE) = @TRUE then 0 else orig_clm_lkup.MBR_AGE"
            - name: orig_clm_out_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.ADJ_FROM_CLM_ID) = @TRUE then ' ' else orig_clm_lkup.ADJ_FROM_CLM_ID"
            - name: orig_clm_out_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(adj_clm.CLM_ID) = @TRUE then ' ' else adj_clm.CLM_ID"
            - name: orig_clm_out_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: " if IsNull(orig_clm_lkup.DOC_TX_ID) = @TRUE then ' ' else orig_clm_lkup.DOC_TX_ID"
            - name: orig_clm_out_MCAID_RESUB_NO
              type: char
              width: 15
              map: " if IsNull(orig_clm_lkup.MCAID_RESUB_NO) = @TRUE then @NULL else orig_clm_lkup.MCAID_RESUB_NO"
            - name: orig_clm_out_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: " if IsNull(orig_clm_lkup.MCARE_ID) = @TRUE then ' ' else orig_clm_lkup.MCARE_ID"
            - name: orig_clm_out_MBR_SFX_NO
              type: char
              width: 2
              map: " if IsNull(orig_clm_lkup.MBR_SFX_NO) = @TRUE then ' ' else orig_clm_lkup.MBR_SFX_NO"
            - name: orig_clm_out_PATN_ACCT_NO
              type: varchar
              width: 35
              map: " if IsNull(orig_clm_lkup.PATN_ACCT_NO) = @TRUE then @NULL else orig_clm_lkup.PATN_ACCT_NO"
            - name: orig_clm_out_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYMT_REF_ID) = @TRUE then ' ' else orig_clm_lkup.PAYMT_REF_ID"
            - name: orig_clm_out_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: " if IsNull(orig_clm_lkup.PROV_AGMNT_ID) = @TRUE then ' ' else orig_clm_lkup.PROV_AGMNT_ID"
            - name: orig_clm_out_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: " if IsNull(orig_clm_lkup.RFRNG_PROV_TX) = @TRUE then @NULL else orig_clm_lkup.RFRNG_PROV_TX"
            - name: orig_clm_out_SUB_ID
              type: char
              width: 14
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_ID) = @TRUE then ' ' else orig_clm_lkup.SUB_ID"
            - name: orig_clm_out_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: orig_clm_out_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: orig_clm_out_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: orig_clm_out_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: orig_clm_out_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.SUB_ID) = @TRUE then '  ' else  IF TRIM(LEN(orig_clm_lkup.MCAID_STTUS_ID)) = 0 THEN 'NA' ELSE orig_clm_lkup.MCAID_STTUS_ID"
            - name: orig_clm_out_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.PATN_PD_AMT) = @TRUE then 0.00 else orig_clm_lkup.PATN_PD_AMT"
            - name: orig_clm_out_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: orig_clm_out_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: " if IsNull(orig_clm_lkup.CLM_TXNMY_CD) = @TRUE then @NULL else orig_clm_lkup.CLM_TXNMY_CD"
            - name: orig_clm_out_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm.BILL_PAYMT_EXCL_IN"
            - name: ClmMartAdj_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.SRC_SYS_CD_SK"
            - name: ClmMartAdj_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm.CLM_ID"
            - name: ClmMartAdj_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm.SRC_SYS_CD"
            - name: ClmMartAdj_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ClmMartOrig_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SRC_SYS_CD_SK) = @TRUE then 0 else orig_clm_lkup.SRC_SYS_CD_SK"
            - name: ClmMartOrig_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_ID) = @TRUE then ' ' else orig_clm_lkup.ORIG_CLM_ID"
            - name: ClmMartOrig_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm.SRC_SYS_CD"
            - name: ClmMartOrig_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: RowCount_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.SRC_SYS_CD_SK) = @TRUE then 0 else orig_clm_lkup.SRC_SYS_CD_SK"
            - name: RowCount_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(orig_clm_lkup.ORIG_CLM_ID) = @TRUE then ' ' else orig_clm_lkup.ORIG_CLM_ID"
            - name: RowCount_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: svStatusCd
            - name: RowCount_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CAT_CD_SK) = @TRUE then 0 else orig_clm_lkup.CLM_CAT_CD_SK"
            - name: RowCount_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.EXPRNC_CAT_SK) = @TRUE then 0 else orig_clm_lkup.EXPRNC_CAT_SK"
            - name: RowCount_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.FNCL_LOB_SK) = @TRUE then 0 else orig_clm_lkup.FNCL_LOB_SK"
            - name: RowCount_GRP_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.GRP_SK) = @TRUE then 0 else orig_clm_lkup.GRP_SK"
            - name: RowCount_MBR_SK
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.MBR_SK) = @TRUE then 0 else orig_clm_lkup.MBR_SK"
            - name: RowCount_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " if IsNull(orig_clm_lkup.SVC_STRT_DT_SK) = @TRUE then 0 else orig_clm_lkup.SVC_STRT_DT_SK"
            - name: RowCount_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.CHRG_AMT) = @TRUE then 0 else orig_clm_lkup.CHRG_AMT"
            - name: RowCount_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(orig_clm_lkup.PAYBL_AMT) = @TRUE then 0 else orig_clm_lkup.PAYBL_AMT"
            - name: RowCount_CLM_CT
              type: int
              width: 4
              nullable: N
              map: " if IsNull(orig_clm_lkup.CLM_CT) = @TRUE then 0 else orig_clm_lkup.CLM_CT"
            - name: RowCount_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: filter_adj_claims_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_lkup.CLM_SK) = @FALSE) then 1 else 0"
            - name: filter_orig_clm_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_lkup.CLM_SK) = @FALSE) then 1 else 0"
            - name: filter_ClmMartAdj
              type: smallint
              width: 2
              nullable: N
              map: "if (adj_clm.SVC_STRT_DT_SK > '#FillDtMin#' and IsNull(orig_clm_lkup.CLM_SK) = @FALSE and (adj_clm.SRC_SYS_CD = 'ARGUS' OR adj_clm.SRC_SYS_CD = 'ESI' OR adj_clm.SRC_SYS_CD = 'OPTUMRX')) then 1 else 0"
            - name: filter_ClmMartOrig
              type: smallint
              width: 2
              nullable: N
              map: "if (orig_clm_lkup.SVC_STRT_DT_SK > '#FillDtMin#' and IsNull(orig_clm_lkup.CLM_SK) = @FALSE and (adj_clm.SRC_SYS_CD = 'ARGUS' or adj_clm.SRC_SYS_CD = 'ESI' OR adj_clm.SRC_SYS_CD = 'OPTUMRX')) then 1 else 0"
            - name: filter_RowCount
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_clm_lkup.CLM_SK) = @FALSE) then 1 else 0"
        - name: clm.V167S20P3.filter
          type: filter
          source: clm.lookup
          filter: "filter_adj_claims_out = 1"
          schema: 
            - name: adj_claims_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLS_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_PROD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_SUB_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: adj_claims_out_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims_out_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: adj_claims_out_MBR_AGE
              type: decimal
              width: 3
            - name: adj_claims_out_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims_out_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims_out_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: adj_claims_out_MCAID_RESUB_NO
              type: char
              width: 15
            - name: adj_claims_out_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: adj_claims_out_MBR_SFX_NO
              type: char
              width: 2
            - name: adj_claims_out_PATN_ACCT_NO
              type: varchar
              width: 35
            - name: adj_claims_out_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: adj_claims_out_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: adj_claims_out_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: adj_claims_out_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: adj_claims_out_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims_out_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_claims_out_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims_out_CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: adj_claims_out_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
        - name: clm.V167S20P4.filter
          type: filter
          source: clm.lookup
          filter: "filter_orig_clm_out = 1"
          schema: 
            - name: orig_clm_out_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: orig_clm_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLS_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_PROD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_SUB_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: orig_clm_out_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: orig_clm_out_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: orig_clm_out_MBR_AGE
              type: decimal
              width: 3
            - name: orig_clm_out_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: orig_clm_out_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: orig_clm_out_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: orig_clm_out_MCAID_RESUB_NO
              type: char
              width: 15
            - name: orig_clm_out_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: orig_clm_out_MBR_SFX_NO
              type: char
              width: 2
            - name: orig_clm_out_PATN_ACCT_NO
              type: varchar
              width: 35
            - name: orig_clm_out_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: orig_clm_out_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: orig_clm_out_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: orig_clm_out_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: orig_clm_out_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: orig_clm_out_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: orig_clm_out_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: orig_clm_out_CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: orig_clm_out_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
        - name: clm.V167S20P5.filter
          type: filter
          source: clm.lookup
          filter: "filter_ClmMartAdj = 1"
          schema: 
            - name: ClmMartAdj_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmMartAdj_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmMartAdj_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: ClmMartAdj_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: clm.V167S20P6.filter
          type: filter
          source: clm.lookup
          filter: "filter_ClmMartOrig = 1"
          schema: 
            - name: ClmMartOrig_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmMartOrig_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmMartOrig_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: ClmMartOrig_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: clm.V167S20P7.filter
          type: filter
          source: clm.lookup
          filter: "filter_RowCount = 1"
          schema: 
            - name: RowCount_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: RowCount_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: RowCount_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: RowCount_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RowCount_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RowCount_CLM_CT
              type: int
              width: 4
              nullable: N
            - name: RowCount_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
        - name: clm.V167S20P3
          type: map
          source: clm.V167S20P3.filter
          alias: adj_claims_out
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims_out.adj_claims_out_WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "adj_claims_out.adj_claims_out_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims_out.adj_claims_out_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims_out.adj_claims_out_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "adj_claims_out.adj_claims_out_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "adj_claims_out.adj_claims_out_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "adj_claims_out.adj_claims_out_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "adj_claims_out.adj_claims_out_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "adj_claims_out.adj_claims_out_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "adj_claims_out.adj_claims_out_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "adj_claims_out.adj_claims_out_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "adj_claims_out.adj_claims_out_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "adj_claims_out.adj_claims_out_SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_REL_BASE_CLM_SK"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims_out.adj_claims_out_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_claims_out.adj_claims_out_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims_out.adj_claims_out_CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "adj_claims_out.adj_claims_out_CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "adj_claims_out.adj_claims_out_BILL_PAYMT_EXCL_IN"
        - name: clm.V167S20P4
          type: map
          source: clm.V167S20P4.filter
          alias: orig_clm_out
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_clm_out.orig_clm_out_WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "orig_clm_out.orig_clm_out_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_out.orig_clm_out_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_out.orig_clm_out_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "orig_clm_out.orig_clm_out_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "orig_clm_out.orig_clm_out_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_out.orig_clm_out_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "orig_clm_out.orig_clm_out_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "orig_clm_out.orig_clm_out_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "orig_clm_out.orig_clm_out_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "orig_clm_out.orig_clm_out_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "orig_clm_out.orig_clm_out_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "orig_clm_out.orig_clm_out_SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_REL_BASE_CLM_SK"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_clm_out.orig_clm_out_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "orig_clm_out.orig_clm_out_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_clm_out.orig_clm_out_CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "orig_clm_out.orig_clm_out_CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_clm_out.orig_clm_out_BILL_PAYMT_EXCL_IN"
        - name: clm.V167S20P5
          type: map
          source: clm.V167S20P5.filter
          alias: ClmMartAdj
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmMartAdj.ClmMartAdj_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmMartAdj.ClmMartAdj_CLM_ID"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmMartAdj.ClmMartAdj_SRC_SYS_CD"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmMartAdj.ClmMartAdj_LAST_UPDT_RUN_CYC_EXCTN_SK"
        - name: clm.V167S20P6
          type: map
          source: clm.V167S20P6.filter
          alias: ClmMartOrig
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmMartOrig.ClmMartOrig_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmMartOrig.ClmMartOrig_CLM_ID"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmMartOrig.ClmMartOrig_SRC_SYS_CD"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmMartOrig.ClmMartOrig_LAST_UPDT_RUN_CYC_EXCTN_SK"
        - name: clm.V167S20P7
          type: map
          source: clm.V167S20P7.filter
          alias: RowCount
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "RowCount.RowCount_CLM_ID"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_CLM_STTUS_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_CLM_CAT_CD_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_MBR_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "RowCount.RowCount_SVC_STRT_DT_SK"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "RowCount.RowCount_CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "RowCount.RowCount_PAYBL_AMT"
            - name: CLM_CT
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_CLM_CT"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "RowCount.RowCount_PCA_TYP_CD_SK"
        - name: xfm_drug_clm_price.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: adj_DrugClmPrice
              joinFilter: ""
              joinExpression: []
            - name: drug_clm_price_hash
              alias: orig_drug_clm_price_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "orig_drug_clm_price_lkup.CLM_SK"
                toExpr: "adj_DrugClmPrice.DRUG_CLM_SK"
                caseInSensitive: true
              - fromExpr: "orig_drug_clm_price_lkup.DRUG_CLM_SK"
                toExpr: "adj_DrugClmPrice.DRUG_CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "orig_drug_clm_price_lkup.CLM_SK = adj_DrugClmPrice.DRUG_CLM_SK"
              caseInSensitive: true
            - expression: "orig_drug_clm_price_lkup.DRUG_CLM_SK = adj_DrugClmPrice.DRUG_CLM_SK"
              caseInSensitive: true
          schema: 
            - name: adj_clm_drug_clm_price_out_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_DrugClmPrice.DRUG_CLM_SK"
            - name: adj_clm_drug_clm_price_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_DrugClmPrice.SRC_SYS_CD_SK"
            - name: adj_clm_drug_clm_price_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_DrugClmPrice.CLM_ID"
            - name: adj_clm_drug_clm_price_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_DrugClmPrice.CRT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_drug_clm_price_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_DrugClmPrice.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: adj_clm_drug_clm_price_out_BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_price_lkup.BUY_CST_SRC_CD_SK"
            - name: adj_clm_drug_clm_price_out_GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_price_lkup.GNRC_OVRD_CD_SK"
            - name: adj_clm_drug_clm_price_out_PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_price_lkup.PDX_NTWK_QLFR_CD_SK"
            - name: adj_clm_drug_clm_price_out_FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_price_lkup.FRMLRY_PROTOCOL_IN"
            - name: adj_clm_drug_clm_price_out_RECON_IN
              type: char
              width: 1
              map: "@NULL"
            - name: adj_clm_drug_clm_price_out_SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
              map: "orig_drug_clm_price_lkup.SPEC_PGM_IN"
            - name: adj_clm_drug_clm_price_out_FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_drug_clm_price_lkup.FINL_PLN_EFF_DT_SK"
            - name: adj_clm_drug_clm_price_out_ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "orig_drug_clm_price_lkup.ORIG_PD_TRANS_SUBMT_DT_SK"
            - name: adj_clm_drug_clm_price_out_PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
              map: "orig_drug_clm_price_lkup.PRORTD_DISPNS_QTY"
            - name: adj_clm_drug_clm_price_out_SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_price_lkup.SUBMT_DISPNS_METRIC_QTY"
            - name: adj_clm_drug_clm_price_out_AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.AVG_WHLSL_PRICE_UNIT_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.AVG_WHLSL_PRICE_UNIT_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.WHLSL_ACQSTN_CST_UNIT_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.WHLSL_ACQSTN_CST_UNIT_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_DISPNS_FEE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_DISPNS_FEE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_INGR_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_INGR_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "orig_drug_clm_price_lkup.BUY_RATE_PCT"
            - name: adj_clm_drug_clm_price_out_BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_SLS_TAX_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_SLS_TAX_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_TOT_DUE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_TOT_DUE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_TOT_OTHR_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_TOT_OTHR_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.CST_TYP_UNIT_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.CST_TYP_UNIT_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: adj_clm_drug_clm_price_out_SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.SELL_CST_TYP_UNIT_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SELL_CST_TYP_UNIT_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "orig_drug_clm_price_lkup.SELL_RATE_PCT"
            - name: adj_clm_drug_clm_price_out_SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.SPREAD_DISPNS_FEE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SPREAD_DISPNS_FEE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.SPREAD_INGR_CST_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SPREAD_INGR_CST_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(orig_drug_clm_price_lkup.SPREAD_SLS_TAX_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SPREAD_SLS_TAX_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.BUY_CST_TYP_ID"
            - name: adj_clm_drug_clm_price_out_BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.BUY_PRICE_TYP_ID"
            - name: adj_clm_drug_clm_price_out_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.CST_TYP_ID"
            - name: adj_clm_drug_clm_price_out_DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.DRUG_TYP_ID"
            - name: adj_clm_drug_clm_price_out_FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.FINL_PLN_ID"
            - name: adj_clm_drug_clm_price_out_GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.GNRC_PROD_ID"
            - name: adj_clm_drug_clm_price_out_SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.SELL_PRICE_TYP_ID"
            - name: adj_clm_drug_clm_price_out_SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
              map: "orig_drug_clm_price_lkup.SPEC_PGM_ID"
            - name: adj_clm_drug_clm_price_out_BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(orig_drug_clm_price_lkup.BUY_INCNTV_FEE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.BUY_INCNTV_FEE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(orig_drug_clm_price_lkup.SELL_OTHR_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SELL_OTHR_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SELL_OTHR_PAYER_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(orig_drug_clm_price_lkup.SELL_OTHR_PAYER_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SELL_OTHR_PAYER_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(orig_drug_clm_price_lkup.SELL_INCNTV_FEE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SELL_INCNTV_FEE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(orig_drug_clm_price_lkup.SPREAD_INCNTV_FEE_AMT) Then 0.00 Else orig_drug_clm_price_lkup.SPREAD_INCNTV_FEE_AMT * (-1)"
            - name: adj_clm_drug_clm_price_out_FRMLRY_SK
              type: int
              width: 4
              map: "orig_drug_clm_price_lkup.FRMLRY_SK"
            - name: adj_clm_drug_clm_price_out_TIER_ID
              type: varchar
              width: 20
              map: "orig_drug_clm_price_lkup.TIER_ID"
            - name: adj_clm_drug_clm_price_out_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "orig_drug_clm_price_lkup.DRUG_TYP_CD_SK"
            - name: adj_clm_drug_clm_price_out_PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_DrugClmPrice.PLN_DRUG_STTUS_CD_SK"
            - name: adj_clm_drug_clm_price_out_DRUG_PLN_TYP_ID
              type: varchar
              width: 20
              map: "adj_DrugClmPrice.DRUG_PLN_TYP_ID"
            - name: filter_adj_clm_drug_clm_price_out
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(orig_drug_clm_price_lkup.CLM_ID) = @FALSE) then 1 else 0"
        - name: xfm_drug_clm_price.filter
          type: filter
          source: xfm_drug_clm_price.lookup
          filter: "filter_adj_clm_drug_clm_price_out = 1"
          schema: 
            - name: adj_clm_drug_clm_price_out_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_drug_clm_price_out_RECON_IN
              type: char
              width: 1
            - name: adj_clm_drug_clm_price_out_SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
            - name: adj_clm_drug_clm_price_out_FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_clm_drug_clm_price_out_ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_clm_drug_clm_price_out_PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
            - name: adj_clm_drug_clm_price_out_SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: adj_clm_drug_clm_price_out_WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: adj_clm_drug_clm_price_out_INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: adj_clm_drug_clm_price_out_SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_clm_drug_clm_price_out_BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_clm_drug_clm_price_out_SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_clm_drug_clm_price_out_SELL_OTHR_PAYER_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_clm_drug_clm_price_out_SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_clm_drug_clm_price_out_SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: adj_clm_drug_clm_price_out_FRMLRY_SK
              type: int
              width: 4
            - name: adj_clm_drug_clm_price_out_TIER_ID
              type: varchar
              width: 20
            - name: adj_clm_drug_clm_price_out_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_clm_drug_clm_price_out_DRUG_PLN_TYP_ID
              type: varchar
              width: 20
        - name: xfm_drug_clm_price
          type: map
          source: xfm_drug_clm_price.filter
          alias: adj_clm_drug_clm_price_out
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_CST_SRC_CD_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_GNRC_OVRD_CD_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_PDX_NTWK_QLFR_CD_SK"
            - name: FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_FRMLRY_PROTOCOL_IN"
            - name: RECON_IN
              type: char
              width: 1
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_RECON_IN"
            - name: SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPEC_PGM_IN"
            - name: FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_FINL_PLN_EFF_DT_SK"
            - name: ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_ORIG_PD_TRANS_SUBMT_DT_SK"
            - name: PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_PRORTD_DISPNS_QTY"
            - name: SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SUBMT_DISPNS_METRIC_QTY"
            - name: AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_AVG_WHLSL_PRICE_UNIT_CST_AMT"
            - name: WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_WHLSL_ACQSTN_CST_UNIT_CST_AMT"
            - name: BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_DISPNS_FEE_AMT"
            - name: BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_INGR_CST_AMT"
            - name: BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_RATE_PCT"
            - name: BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_SLS_TAX_AMT"
            - name: BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_TOT_DUE_AMT"
            - name: BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_TOT_OTHR_AMT"
            - name: CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_CST_TYP_UNIT_CST_AMT"
            - name: INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_INVC_TOT_DUE_AMT"
            - name: SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_CST_TYP_UNIT_CST_AMT"
            - name: SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_RATE_PCT"
            - name: SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPREAD_DISPNS_FEE_AMT"
            - name: SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPREAD_INGR_CST_AMT"
            - name: SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPREAD_SLS_TAX_AMT"
            - name: BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_CST_TYP_ID"
            - name: BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_PRICE_TYP_ID"
            - name: CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_CST_TYP_ID"
            - name: DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_DRUG_TYP_ID"
            - name: FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_FINL_PLN_ID"
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_GNRC_PROD_ID"
            - name: SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_PRICE_TYP_ID"
            - name: SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPEC_PGM_ID"
            - name: BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_BUY_INCNTV_FEE_AMT"
            - name: SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_OTHR_AMT"
            - name: SELL_OTHR_PAYER_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_OTHR_PAYER_AMT"
            - name: SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SELL_INCNTV_FEE_AMT"
            - name: SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_SPREAD_INCNTV_FEE_AMT"
            - name: FRMLRY_SK
              type: int
              width: 4
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_FRMLRY_SK"
            - name: TIER_ID
              type: varchar
              width: 20
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_DRUG_TYP_CD_SK"
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_PLN_DRUG_STTUS_CD_SK"
            - name: DRUG_PLN_TYP_ID
              type: varchar
              width: 20
              map: "adj_clm_drug_clm_price_out.adj_clm_drug_clm_price_out_DRUG_PLN_TYP_ID"


