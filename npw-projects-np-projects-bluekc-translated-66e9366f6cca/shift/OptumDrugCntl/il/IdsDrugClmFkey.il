model:
  config:
    origin: datastage
  jobs:
    - name: IdsDrugClmFkey
      origin: datastage
      type: dataflow
      folder: "claim/DrugClaim"
      runAfterSubrOnJobSuccess: enabled
      description: "IDS Drug Claim  foreign keying."
      fullDescription: "COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \nCOPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nDESCRIPTION:     Takes the sorted file and applies the foreign keys.    \n      \nPROCESSING: get foreign key as stated above and create file to load to DRUG_CLM, create a file for adjusted claims\n                         NDC sk is determined in IdsDrugClmPkey, and just passed to this job\n                         Pull Argus claims only with fill dt => FillDtMin for Claim Mart\n\nMODIFICATIONS:\n            BJ Luce        8/2004          -   Originally Programmed - phase 2.0\n            Brent Leland 09/10/2004  -  Added Default rows for UNK and NA\n                                                      -  Added link partitioner\n            BJ Luce      09/24/2004   -   turn off logging for DAW, Legal Status and 3 tier\n            BJ Luce      03/2005             change logging to X for DAW, legal status and 3 tier\n                                                          name adjustment file to IdsDrugClmAdj.dat\n            BJ Luce     03/29/2005        for reversals, change check for A08 for argus claims\n            BJ Luce     08/2005             RX deductibles file format\n             BJ Luce     08/01/2005        add recon date\n            BJ Luce      08/31/2005        add output file for claim mart P_CLM_DM_EXTR.datDrug#RunID#\n            BJ Luce      12//2005          add new input and output fields for Medicare Part D staff tracker 1736\n            Brent Leland  04/18/2006   Changed table name P_DRUG_CLM to W_DRUG_CLM and change date field to char.\n                                                         Hard coded file names\n                                                         Changed parameters to environment values.\n                                                         Removed Fill date parameter and logic.  Input no longer has old records that need to be excluded from the Claim Mart.\n            Brent Leland 05/08/2006     Changed claim mart output column format and file name.\n\n\nDeveloper                    Date                                         Change Description                                                                       Project #          Development Project          Code Reviewer               Date Reviewed  \n------------------           ----------------------------        ----------------------------------------------------------------------------                                                ----------------         ------------------------------------       ----------------------------               ----------------\nParik                    2008-09-12                Added two new fields to the job                                                                                    #3784(PBM)       devlIDSnew\nSAndrew \x09            2008-10-10               Created stage var SrcSysCdToUseForCodes                                                                 #3784(PBM)       devlIDSnew               Steph Goddard             10/27/2008         \n                                                             Changed rule for stage var LglSttusCd.  \n                                                             Change rule for stage var AgjustedClaim \n                                                             Changed stage var McpartdCovdrugCd and PrauthCd to use SrcSysCdToUseForCodes \n                                                             Reduced number of link Collector lines out of IdsDrugClmDrugExt from 3 to one .\n                                                             parik added ADM_FEE_AMT and DRUG_CLM_BILL_BSS_CD_SK\nSAndrew            2009-02-20                 Added new criteria for WellDyne to test if claim is an adjustment                            3648 Labor Accnt     devlIDS                      Steph Goddard                02/21/2009\nSAndrew            2009-02-20                 Added Welldyne in stage variable LglSttusCd to do a code lookup for domain        3648 Labor Accnt    devlIDS                      Steph Goddard                03/30/2009\n                                                            \"NDC DRUG ABUSE CONTROL\"                                                                                 \nTracy Davis       2009-04-14                New field - AVG_WHLSL_PRICE_AMT                                                                  #3784(PBM)               devlIDS                      Steph Goddard                04/14/2009\nKalyan Neelam  2009-10-07                Added a new option MCSOURCE for DRUG_CLM_LGL_STTUS_CD_SK and        4098                         devlIDS                      Steph Goddard                10/15/2009\n                                                            ADJ_DT_SK\nKalyan Neelam  2009-12-29                Added a new option MCAID for DRUG_CLM_LGL_STTUS_CD_SK and                         4110                        IntegrateCurDevl\n                                                            DRUG_CLM_PRAUTH_CD_SK\nKalyan Neelam  2010-12-22                Added a new option MEDTRAK for DRUG_CLM_LGL_STTUS_CD_SK and                   4616              IntegrateNewDevl         Steph Goddard                12/23/2010\n                                                            DRUG_CLM_PRAUTH_CD_SK\nKalyan Neelam  2011-01-17               Added hashed file lookup for Medtrak VndrClmNo - hf_medtrak_drgclm_rvrsl_hf                4616              IntegrateNewDevl         Steph Goddard                 01/18/2011\n\nKarthik Chintalapani  2013-01-25                  Added a new column UCR_AMT                                                                            4963               IntegrateNewDevl         SAndrew                         2013-02-26\n\nRaja Gummadi    2013-08-28              Added 3 new columns to the table.                                                                           5115- BHI                IntegrateNewDevl           SAndrew                        2013-09-04\n                                                             DRUG_CLM_PRTL_FILL_CD_SK\n                                                             DRUG_CLM_PDX_TYP_CD_SK\n                                                             INCNTV_FEE_AMT\nKalyan Neelam   2013-11-12               Added a new option BCA for DRUG_CLM_LGL_STTUS_CD_SK                      5056 FEP Claims      IntegrateNewDevl              Bhoomi Dasari                11/30/2013\nKaushik Kapoor  2017-02-01               Added the source system SAVRX for LegalStatusCd Stage Variable                   5828                        IntegrateDev2                   Kalyan Neelam                    2018-02-27\n\nKaushik Kapoor  2018-03-19               Added the source system LDI for LegalStatusCd and MCPARTD_COVDRUG_CD \n                                                            stage variable and SAVRX for MCPARTD_COVDRUG_CD only                               5828                  IntegrateDev2                   Jaideep Mankala           03/21/2018\n\nKaushik Kapoor  2018-09-20               Added the source system CVS for LegalStatusCd and MCPARTD_COVDRUG_CD \n                                                            stage variable                                                                                                            5828                  IntegrateDev2                   Kalyan Neelam                    2018-10-01 \nNarender Kamatam  2019-10-21          Added \"OPTUMRX\" Value to the below stage variablesin Transformer Derivation.\n                                                                         LglSttusCd\n                                                                        AdjustedClaim\n                                                                        McpartDCovdrugCd\n                                                                        PrauthCd\n                                                                        svDrugClmPrtlFillCdSk\n                                                                        svDrugClmPdxTypCdSk\n                                                                                                                                                                                                   6131- \n                                                                                                                                                                                              PBM Replacement      IntegrateDev1                    \n\n\nBhargava Rampilla   2019-10-24           Modified feilds as per the                                                                   6131-PBM REPLACEMENT          IntegrateDEV1          Kalyan Neelam                    2019-11-20                              \n                                                                          OptumRX transformation rules:   \n                                                                           DRUG_CLM_LGL_ STTUS_CD_SK\n                                                                          DRUG_CLM_PRTL _FILL_CD_SK\n                                                                          DRUG_CLM_PDX_ TYP_CD_SK\n                                                                          DRUG_CLM_MCPARTD_COVDRUG_CD_SK\n                                                                          DRUG_CLM_PRAUTH_CD_SK\n                                                                          DRUG_CLM_SK\nKalyan Neelam        2020-01-13            Removed parameter $IDSDB which was not needed in the job                                                             IntegrateDev2\n\nRekha                     2020-03-27            Changed the DRUG_CLM_PDX_NTWK_CD_SK tranformation      6131-PBM REPLACEMENT          IntegrateDev2 \nRadhakrishna                                        to perform lookup on Substring[4,4] of PDX_NTWK_ID  \n                                                                          \nSagar Sayam           2020-03-31           Added the Speciality indicator filed                                                    6131-PBM REPLACEMENT          IntegrateDev2 \n\nSri Nannapaneni           2020-07-15      6131 PBM replacement      Added 6 new fields to source seq file and drug_clm target file                      IntegrateDev2                 \n\nSri Nannapaneni           2020-08-17      6131 PBM replacement      Added GNRC_PROD_IN to end seq file for drug_clm                                  IntegrateDev2                 \n\n\nGoutham Kalidindi         2020-11-12       US-283560                      Added to the MEDIMPACT to StageVariable LglStsCdSk                              IntegrateDev2               Reddy Sanam 2020-11-13\n\nGoutham Kalidindi        2020-11-19        US-317229                     Added MEDIMPACT to stageVariable McPartdMedovcodeSk                     IntegrateDev2             Kalyan Neelam                    2020-11-19"
      parameters:
        - name: Source
          type: string
          description: "Source System"
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
        - name: RunCycle
          type: integer
          description: "Current cycle"
        - name: Logging
          type: string
          description: "Logging option"
        - name: RunID
          type: string
          description: RunID
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\1;hf_medtrak_drgclm_rvrsl_hf"
        subr: "1;hf_medtrak_drgclm_rvrsl_hf"
      steps:
        - name: IdsDrugClmDrugExtr
          type: fileReader
          urls: 
            - "#$FilePath#/key/IdsDrugClmDrugExtr.DrugClmDrug.uniq"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
              type: varchar
              width: 20
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: MAC_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: ADJ_DT
              type: char
              width: 10
              nullable: N
            - name: FILL_DT
              type: char
              width: 10
              nullable: N
            - name: RECON_DT
              type: char
              width: 10
              nullable: N
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
            - name: RX_NO
              type: varchar
              width: 20
            - name: RFL_NO
              type: varchar
              width: 20
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
            - name: DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
            - name: DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DRUG_CLM_BILL_BSS_CD
              type: char
              width: 2
              nullable: N
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
              nullable: N
            - name: PDX_TYP
              type: char
              width: 1
              nullable: N
            - name: INCNTV_FEE
              type: decimal
              width: 8
              precision: 2
              nullable: N
            - name: SPEC_DRUG_IN
              type: char
              width: 1
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
          keys:
            - DRUG_CLM_SK
        - name: W_DRUG_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_DRUG_CLM.dat"
          source: "Trns1.V0S20P16"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: DrugClmEdit
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM.#Source#.dat"
          source: Collector1
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: W_WEBDM_ETL_DRVR
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.Drug.#RunID#"
          source: "Trns1.V0S20P17"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_medtrak_drgclm_rvrsl_hf
          type: dbReader
          originalDb: hashedFile
          table: "/hf_medtrak_drgclm_rvrsl_hf"
          schema: 
            - name: CLAIM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ESI_CLNT_GNRL_PRPS_AREA
              type: char
              width: 40
          keys:
            - CLAIM_ID
            # Hashed File has no directory specified
        - name: hf_recycle
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "Trns1.V0S20P14"
            # Hashed File has no directory specified
        - name: Trns1.lookup
          type: lookup
          stageVars:
            - name: NDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(Key.DRUG_CLM_SK, Key.NDC, 'X')"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: DawCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"DRUG CLAIM DAW\", Key.DRUG_CLM_DAW_CD, Logging)"
              initValue: "0"
            - name: LglSttusCd
              type: decimal
              width: 18
              precision: 4
              map: " If (Trim(Key.SRC_SYS_CD) = \"ESI\" or Trim(Key.SRC_SYS_CD) = \"MEDIMPACT\" OR Trim(Key.SRC_SYS_CD) = \"WELLDYNERX\" Or Trim(Key.SRC_SYS_CD) = \"MCSOURCE\" Or Trim(Key.SRC_SYS_CD) = \"MCAID\" Or Trim(Key.SRC_SYS_CD) = \"MEDTRAK\" Or Trim(Key.SRC_SYS_CD) = \"BCBSSC\" Or Trim(Key.SRC_SYS_CD) = \"BCA\" Or Trim(Key.SRC_SYS_CD) = \"BCBSA\" Or Trim(Key.SRC_SYS_CD) = \"SAVRX\" Or Trim(Key.SRC_SYS_CD) = \"LDI\" Or Trim(Key.SRC_SYS_CD) = \"CVS\" Or Trim(Key.SRC_SYS_CD) = \"OPTUMRX\") then GetFkeyCodes(\"FDB\", Key.DRUG_CLM_SK, \"NDC DRUG ABUSE CONTROL\", TRIM(Key.DRUG_CLM_LGL_STTUS_CD), Logging) else GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"DRUG CLAIM LEGAL STATUS\", Key.DRUG_CLM_LGL_STTUS_CD, Logging)"
              initValue: "0"
            - name: TierCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"DRUG CLAIM TIER\", Key.DRUG_CLM_TIER_CD, Logging)"
              initValue: "0"
            - name: VndrSttusCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"DRUG CLAIM VENDOR STATUS\", Key.DRUG_CLM_VNDR_STTUS_CD, Logging)"
              initValue: "0"
            - name: AdjustedClaim
              type: decimal
              width: 0
              map: " iF ((Key.CLM_STTUS_CD = '11' and Key.SRC_SYS_CD = 'ARGUS') or (Key.CLM_STTUS_CD = '70' and Key.SRC_SYS_CD = 'ARGUS') or (Key.CLM_STTUS_CD = 'R' and Key.SRC_SYS_CD = 'ESI') or (Key.CLM_STTUS_CD = 'X' and Key.SRC_SYS_CD = 'OPTUMRX') or (Key.CLM_STTUS_CD = 'X' and Key.SRC_SYS_CD = 'WELLDYNERX') or (Key.CLM_STTUS_CD = 'R' and Key.SRC_SYS_CD = 'MEDTRAK') or ((Key.CLM_STTUS_CD = '3' or Key.CLM_STTUS_CD = '4') and Key.SRC_SYS_CD = 'BCBSSC')) THEN @TRUE else @FALSE"
              initValue: "0"
            - name: AdjDtSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.SRC_SYS_CD) = \"MCSOURCE\" Then 'NA' Else GetFkeyDate('IDS', Key.DRUG_CLM_SK, Key.ADJ_DT, Logging)"
              initValue: "0"
            - name: DEASk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvDEA(Key.DRUG_CLM_SK, Key.PRSCRB_PROV_DEA, 'X')"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(\"NABP\", Key.DRUG_CLM_SK, Key.PROV_ID, 'X')"
              initValue: "0"
            - name: FillDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', Key.DRUG_CLM_SK, Key.FILL_DT, Logging)"
              initValue: "0"
            - name: BnfFrmlryPolCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"BENEFIT FORMULARY POLICY\", Key.DRUG_CLM_BNF_FRMLRY_POL_CD, Logging)"
              initValue: "0"
            - name: BnfRstrctCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"BENEFIT RESTRICTION\", Key.DRUG_CLM_BNF_RSTRCT_CD, Logging)"
              initValue: "0"
            - name: McpartdCovdrugCd
              type: decimal
              width: 18
              precision: 4
              map: " If (Key.SRC_SYS_CD = 'ESI' Or Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' Or Key.SRC_SYS_CD = 'ARGUS' Or Key.SRC_SYS_CD = 'MEDTRAK' Or Key.SRC_SYS_CD = 'BCBSSC' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"MEDICARE PART D COVERED DRUG\", Key.DRUG_CLM_MCPARTD_COVDRUG_CD, Logging) Else 1"
              initValue: "0"
            - name: PdsNtwkCd
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'OPTUMRX' Then ( If (Key.PDX_NTWK_ID = 'NA' Or Key.PDX_NTWK_ID = 'UNK') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"PHARMACY NETWORK\", Key.PDX_NTWK_ID, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"PHARMACY NETWORK\", Key.PDX_NTWK_ID[4, 4], Logging)) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"PHARMACY NETWORK\", Key.PDX_NTWK_ID, Logging)"
              initValue: "0"
            - name: PrauthCd
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = \"ESI\" Or Key.SRC_SYS_CD = 'ARGUS' Or Key.SRC_SYS_CD = 'MCAID' Or Key.SRC_SYS_CD = 'BCBSSC' Then GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \"PHARMACY PRIOR AUTH\", Key.DRUG_CLM_PRAUTH_CD, Logging) Else 1"
              initValue: "0"
            - name: ReconDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', Key.DRUG_CLM_SK, Key.RECON_DT, Logging)"
              initValue: "0"
            - name: DrugClmBillBssCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, \" DRUG CLAIM BILLED BASIS\", Key.DRUG_CLM_BILL_BSS_CD, Logging)"
              initValue: "0"
            - name: svDrugClmPrtlFillCdSk
              type: decimal
              width: 0
              map: " If (Key.SRC_SYS_CD = \"OPTUMRX\") then GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'DRUG PARTIAL FILL', 'OPTUMRX', 'DRUG PARTIAL FILL', 'IDS', Key.PRTL_FILL_STTUS_CD, Logging) Else GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'DRUG PARTIAL FILL', 'ESI', 'DRUG PARTIAL FILL', 'IDS', Key.PRTL_FILL_STTUS_CD, Logging)"
              initValue: "0"
            - name: svDrugClmPdxTypCdSk
              type: decimal
              width: 0
              map: " If Key.SRC_SYS_CD = \"OPTUMRX\" then GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'PHARMACY TYPE', 'OPTUMRX', 'PHARMACY TYPE', 'IDS', Key.PDX_TYP, Logging) Else GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'PHARMACY TYPE', 'ESI', 'PHARMACY TYPE', 'IDS', Key.PDX_TYP, Logging)"
              initValue: "0"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.DRUG_CLM_SK)"
              initValue: "0"
            - name: svCntngntTherCdSk
              type: decimal
              width: 0
              map: " If (Key.SRC_SYS_CD = \"OPTUMRX\") then GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'CONTINGENT THERAPY', 'OPTUMRX', 'CONTINGENT THERAPY', 'IDS', Key.CNTNGNT_THER_FLAG, Logging) Else 0"
              initValue: "0"
            - name: svSubmtProdIdQlfrCdSk
              type: decimal
              width: 0
              map: " If (Key.SRC_SYS_CD = \"OPTUMRX\") then GetFkeyClctnDomainCodes(Key.SRC_SYS_CD, Key.DRUG_CLM_SK, 'SUBMITTED PRODUCT IDENTIFIER QUALIFIER', 'OPTUMRX', 'SUBMITTED PRODUCT IDENTIFIER QUALIFIER', 'IDS', Key.SUBMT_PROD_ID_QLFR, Logging) Else 0"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: IdsDrugClmDrugExtr
              alias: Key
              joinFilter: ""
              joinExpression: []
            - name: hf_medtrak_drgclm_rvrsl_hf
              alias: rvrsl_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "rvrsl_lkup.CLAIM_ID"
                toExpr: "Key.CLM_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "rvrsl_lkup.CLAIM_ID = Key.CLM_ID"
              caseInSensitive: true
          schema: 
            - name: Output1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " If Key.SRC_SYS_CD = 'OPTUMRX' then ClmSk else Key.DRUG_CLM_SK"
            - name: Output1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: Output1_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: Output1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: Output1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: Output1_CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmSk
            - name: Output1_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Key.NDC_SK"
            - name: Output1_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: DEASk
            - name: Output1_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: DawCd
            - name: Output1_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: LglSttusCd
            - name: Output1_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: TierCd
            - name: Output1_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: VndrSttusCd
            - name: Output1_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Key.CMPND_IN"
            - name: Output1_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Key.FRMLRY_IN"
            - name: Output1_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.GNRC_DRUG_IN"
            - name: Output1_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MAIL_ORDER_IN"
            - name: Output1_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MNTN_IN"
            - name: Output1_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MAC_REDC_IN"
            - name: Output1_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.NON_FRMLRY_DRUG_IN"
            - name: Output1_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SNGL_SRC_IN"
            - name: Output1_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: AdjDtSk
            - name: Output1_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: FillDtSk
            - name: Output1_RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: ReconDtSk
            - name: Output1_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DISPNS_FEE_AMT"
            - name: Output1_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.HLTH_PLN_EXCL_AMT"
            - name: Output1_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.HLTH_PLN_PD_AMT"
            - name: Output1_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_CST_ALW_AMT"
            - name: Output1_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_CST_CHRGD_AMT"
            - name: Output1_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_SAV_AMT"
            - name: Output1_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_DEDCT_EXCL_AMT"
            - name: Output1_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_DIFF_PD_AMT"
            - name: Output1_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_OOP_AMT"
            - name: Output1_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_OOP_EXCL_AMT"
            - name: Output1_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.OTHR_SAV_AMT"
            - name: Output1_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Key.RX_ALW_QTY"
            - name: Output1_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Key.RX_SUBMT_QTY"
            - name: Output1_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.SLS_TAX_AMT"
            - name: Output1_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Key.RX_ALW_DAYS_SUPL_QTY"
            - name: Output1_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Key.RX_ORIG_DAYS_SUPL_QTY"
            - name: Output1_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Key.PDX_NTWK_ID"
            - name: Output1_RX_NO
              type: varchar
              width: 20
              map: "Key.RX_NO"
            - name: Output1_RFL_NO
              type: varchar
              width: 20
              map: "Key.RFL_NO"
            - name: Output1_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Key.VNDR_CLM_NO"
            - name: Output1_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Key.VNDR_PREAUTH_ID"
            - name: Output1_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: BnfFrmlryPolCd
            - name: Output1_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: BnfRstrctCd
            - name: Output1_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: McpartdCovdrugCd
            - name: Output1_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: PdsNtwkCd
            - name: Output1_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: PrauthCd
            - name: Output1_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MNDTRY_MAIL_ORDER_IN"
            - name: Output1_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ADM_FEE_AMT"
            - name: Output1_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: DrugClmBillBssCdSk
            - name: Output1_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.AVG_WHLSL_PRICE_AMT"
            - name: Output1_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.UCR_AMT"
            - name: Output1_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: svDrugClmPrtlFillCdSk
            - name: Output1_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: svDrugClmPdxTypCdSk
            - name: Output1_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INCNTV_FEE"
            - name: Output1_SPEC_DRUG_IN
              type: char
              width: 1
              map: "Key.SPEC_DRUG_IN"
            - name: Output1_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: svSubmtProdIdQlfrCdSk
            - name: Output1_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: svCntngntTherCdSk
            - name: Output1_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "Key.CNTNGNT_THER_SCHD"
            - name: Output1_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Key.CLNT_PATN_PAY_ATRBD_NTWK_AMT"
            - name: Output1_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Key.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"
            - name: Output1_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Key.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"
            - name: Output1_GNRC_PROD_IN
              type: varchar
              width: 14
              map: "Key.GNRC_PROD_IN"
            - name: DefaultUNK_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "0"
            - name: DefaultUNK_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "0"
            - name: DefaultUNK_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_RX_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_RFL_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultUNK_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultUNK_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultUNK_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultUNK_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultUNK_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultUNK_GNRC_PROD_IN
              type: varchar
              width: 14
              map: "'UNK'"
            - name: DefaultNA_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "0"
            - name: DefaultNA_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "0"
            - name: DefaultNA_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultNA_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultNA_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_RX_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_RFL_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultNA_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultNA_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0"
            - name: DefaultNA_GNRC_PROD_IN
              type: varchar
              width: 14
              map: "'NA'"
            - name: Recycle1_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(Key.DRUG_CLM_SK)"
            - name: Recycle1_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: Recycle1_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: Recycle1_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: Recycle1_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: Recycle1_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: Recycle1_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT + 1"
            - name: Recycle1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: Recycle1_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: Recycle1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Key.DRUG_CLM_SK"
            - name: Recycle1_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: Recycle1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: Recycle1_NDC
              type: varchar
              width: 20
              nullable: N
              map: "Key.NDC"
            - name: Recycle1_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Key.NDC_SK"
            - name: Recycle1_PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
              map: "Key.PRSCRB_PROV_DEA"
            - name: Recycle1_DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.DRUG_CLM_DAW_CD"
            - name: Recycle1_DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.DRUG_CLM_LGL_STTUS_CD"
            - name: Recycle1_DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.DRUG_CLM_TIER_CD"
            - name: Recycle1_DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.DRUG_CLM_VNDR_STTUS_CD"
            - name: Recycle1_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Key.CMPND_IN"
            - name: Recycle1_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Key.FRMLRY_IN"
            - name: Recycle1_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.GNRC_DRUG_IN"
            - name: Recycle1_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MAIL_ORDER_IN"
            - name: Recycle1_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MNTN_IN"
            - name: Recycle1_MAC_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MAC_REDC_IN"
            - name: Recycle1_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.NON_FRMLRY_DRUG_IN"
            - name: Recycle1_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SNGL_SRC_IN"
            - name: Recycle1_ADJ_DT
              type: char
              width: 10
              nullable: N
              map: "Key.ADJ_DT"
            - name: Recycle1_FILL_DT
              type: char
              width: 10
              nullable: N
              map: "Key.FILL_DT"
            - name: Recycle1_RECON_DT
              type: char
              width: 10
              nullable: N
              map: "Key.RECON_DT"
            - name: Recycle1_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DISPNS_FEE_AMT"
            - name: Recycle1_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.HLTH_PLN_EXCL_AMT"
            - name: Recycle1_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.HLTH_PLN_PD_AMT"
            - name: Recycle1_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_CST_ALW_AMT"
            - name: Recycle1_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_CST_CHRGD_AMT"
            - name: Recycle1_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INGR_SAV_AMT"
            - name: Recycle1_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_DEDCT_EXCL_AMT"
            - name: Recycle1_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_DIFF_PD_AMT"
            - name: Recycle1_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_OOP_AMT"
            - name: Recycle1_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.MBR_OOP_EXCL_AMT"
            - name: Recycle1_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.OTHR_SAV_AMT"
            - name: Recycle1_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.SLS_TAX_AMT"
            - name: Recycle1_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Key.RX_ALW_QTY"
            - name: Recycle1_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Key.RX_SUBMT_QTY"
            - name: Recycle1_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Key.RX_ALW_DAYS_SUPL_QTY"
            - name: Recycle1_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Key.RX_ORIG_DAYS_SUPL_QTY"
            - name: Recycle1_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Key.PDX_NTWK_ID"
            - name: Recycle1_RX_NO
              type: varchar
              width: 20
              map: "Key.RX_NO"
            - name: Recycle1_RFL_NO
              type: varchar
              width: 20
              map: "Key.RFL_NO"
            - name: Recycle1_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Key.VNDR_CLM_NO"
            - name: Recycle1_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Key.VNDR_PREAUTH_ID"
            - name: Recycle1_CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_STTUS_CD"
            - name: Recycle1_PROV_ID
              type: char
              width: 12
              nullable: N
              map: "Key.PROV_ID"
            - name: Recycle1_NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "Key.NDC_LABEL_NM"
            - name: Recycle1_DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
              map: "Key.DRUG_CLM_BNF_FRMLRY_POL_CD"
            - name: Recycle1_DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
              map: "Key.DRUG_CLM_BNF_RSTRCT_CD"
            - name: Recycle1_DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
              map: "Key.DRUG_CLM_MCPARTD_COVDRUG_CD"
            - name: Recycle1_DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
              map: "Key.DRUG_CLM_PRAUTH_CD"
            - name: Recycle1_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Key.MNDTRY_MAIL_ORDER_IN"
            - name: Recycle1_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ADM_FEE_AMT"
            - name: Recycle1_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.DRUG_CLM_BILL_BSS_CD"
            - name: Recycle1_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.AVG_WHLSL_PRICE_AMT"
            - name: Recycle1_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.UCR_AMT"
            - name: adj_claims1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Key.DRUG_CLM_SK"
            - name: adj_claims1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: adj_claims1_CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmSk
            - name: adj_claims1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: adj_claims1_RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "Key.RX_NO"
            - name: adj_claims1_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Key.FILL_DT"
            - name: adj_claims1_PROV_SK
              type: int
              width: 4
              nullable: N
              map: ProvSk
            - name: adj_claims1_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: adj_claims1_VNDR_CLM_NO
              type: varchar
              width: 20
              nullable: N
              map: " If Key.SRC_SYS_CD = 'MEDTRAK' Then  If IsNull(rvrsl_lkup.ESI_CLNT_GNRL_PRPS_AREA) = @TRUE Then 'UNK' Else rvrsl_lkup.ESI_CLNT_GNRL_PRPS_AREA Else Key.VNDR_CLM_NO"
            - name: TempFile_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: TempFile_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: TempFile_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: TempFile_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: filter_Output1
              type: smallint
              width: 2
              nullable: N
              map: "if (Key.PASS_THRU_IN = \"Y\" or ErrCount = 0) then 1 else 0"
            - name: filter_DefaultUNK
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
            - name: filter_DefaultNA
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
            - name: filter_Recycle1
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount > 0) then 1 else 0"
            - name: filter_adj_claims1
              type: smallint
              width: 2
              nullable: N
              map: "if (AdjustedClaim) then 1 else 0"
            - name: filter_TempFile
              type: smallint
              width: 2
              nullable: N
              map: "if (Key.SRC_SYS_CD = 'ARGUS' or Key.SRC_SYS_CD = 'ESI' or Key.SRC_SYS_CD = 'OPTUMRX') then 1 else 0"
        - name: Trns1.V0S20P5.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_Output1 = 1"
          schema: 
            - name: Output1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: Output1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_NDC_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
            - name: Output1_FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: Output1_RECON_DT_SK
              type: char
              width: 10
              nullable: N
            - name: Output1_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: Output1_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: Output1_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: Output1_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: Output1_PDX_NTWK_ID
              type: varchar
              width: 20
            - name: Output1_RX_NO
              type: varchar
              width: 20
            - name: Output1_RFL_NO
              type: varchar
              width: 20
            - name: Output1_VNDR_CLM_NO
              type: varchar
              width: 20
            - name: Output1_VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: Output1_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: Output1_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Output1_SPEC_DRUG_IN
              type: char
              width: 1
            - name: Output1_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Output1_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
            - name: Output1_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: Output1_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: Output1_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: Output1_GNRC_PROD_IN
              type: varchar
              width: 14
        - name: Trns1.V0S20P10.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_DefaultUNK = 1"
          schema: 
            - name: DefaultUNK_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_NDC_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_RECON_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: DefaultUNK_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: DefaultUNK_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_RX_NO
              type: varchar
              width: 20
            - name: DefaultUNK_PDX_NTWK_ID
              type: varchar
              width: 20
            - name: DefaultUNK_RFL_NO
              type: varchar
              width: 20
            - name: DefaultUNK_VNDR_CLM_NO
              type: varchar
              width: 20
            - name: DefaultUNK_VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: DefaultUNK_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
            - name: DefaultUNK_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultUNK_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultUNK_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultUNK_GNRC_PROD_IN
              type: varchar
              width: 14
        - name: Trns1.V0S20P11.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_DefaultNA = 1"
          schema: 
            - name: DefaultNA_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_NDC_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_ADJ_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_RECON_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: DefaultNA_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: DefaultNA_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PDX_NTWK_ID
              type: varchar
              width: 20
            - name: DefaultNA_RX_NO
              type: varchar
              width: 20
            - name: DefaultNA_RFL_NO
              type: varchar
              width: 20
            - name: DefaultNA_VNDR_CLM_NO
              type: varchar
              width: 20
            - name: DefaultNA_VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: DefaultNA_DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
            - name: DefaultNA_MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultNA_MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultNA_MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DefaultNA_GNRC_PROD_IN
              type: varchar
              width: 14
        - name: Trns1.V0S20P14.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_Recycle1 = 1"
          schema: 
            - name: Recycle1_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: Recycle1_ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: Recycle1_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: Recycle1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: Recycle1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_NDC
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_NDC_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_CMPND_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_FRMLRY_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_MAC_REDC_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_ADJ_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_FILL_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_RECON_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: Recycle1_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
            - name: Recycle1_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: Recycle1_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
            - name: Recycle1_PDX_NTWK_ID
              type: varchar
              width: 20
            - name: Recycle1_RX_NO
              type: varchar
              width: 20
            - name: Recycle1_RFL_NO
              type: varchar
              width: 20
            - name: Recycle1_VNDR_CLM_NO
              type: varchar
              width: 20
            - name: Recycle1_VNDR_PREAUTH_ID
              type: varchar
              width: 20
            - name: Recycle1_CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_PROV_ID
              type: char
              width: 12
              nullable: N
            - name: Recycle1_NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
            - name: Recycle1_DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
        - name: Trns1.V0S20P16.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_adj_claims1 = 1"
          schema: 
            - name: adj_claims1_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims1_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims1_RX_NO
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims1_FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: adj_claims1_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: adj_claims1_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: adj_claims1_VNDR_CLM_NO
              type: varchar
              width: 20
              nullable: N
        - name: Trns1.V0S20P17.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_TempFile = 1"
          schema: 
            - name: TempFile_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TempFile_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: TempFile_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: TempFile_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: Trns1.V0S20P5
          type: map
          source: Trns1.V0S20P5.filter
          alias: Output1
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Output1.Output1_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Output1.Output1_ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Output1.Output1_FILL_DT_SK"
            - name: RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Output1.Output1_RECON_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Output1.Output1_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Output1.Output1_RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Output1.Output1_PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Output1.Output1_RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "Output1.Output1_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Output1.Output1_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Output1.Output1_VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Output1.Output1_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_AVG_WHLSL_PRICE_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Output1.Output1_INCNTV_FEE_AMT"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: "Output1.Output1_SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_SUBMT_PROD_ID_QLFR_CD_SK"
            - name: CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Output1.Output1_CNTNGNT_THER_CD_SK"
            - name: CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "Output1.Output1_CNTNGNT_THER_SCHD_ID"
            - name: MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Output1.Output1_MBR_NTWK_DIFF_PD_AMT"
            - name: MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Output1.Output1_MBR_SLS_TAX_AMT"
            - name: MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Output1.Output1_MBR_PRCS_FEE_AMT"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: "Output1.Output1_GNRC_PROD_IN"
        - name: Trns1.V0S20P10
          type: map
          source: Trns1.V0S20P10.filter
          alias: DefaultUNK
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_FILL_DT_SK"
            - name: RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_RECON_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "DefaultUNK.DefaultUNK_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "DefaultUNK.DefaultUNK_RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_RX_ORIG_DAYS_SUPL_QTY"
            - name: RX_NO
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_RX_NO"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_PDX_NTWK_ID"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_AVG_WHLSL_PRICE_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_INCNTV_FEE_AMT"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SUBMT_PROD_ID_QLFR_CD_SK"
            - name: CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CNTNGNT_THER_CD_SK"
            - name: CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_CNTNGNT_THER_SCHD_ID"
            - name: MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultUNK.DefaultUNK_MBR_NTWK_DIFF_PD_AMT"
            - name: MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultUNK.DefaultUNK_MBR_SLS_TAX_AMT"
            - name: MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultUNK.DefaultUNK_MBR_PRCS_FEE_AMT"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: "DefaultUNK.DefaultUNK_GNRC_PROD_IN"
        - name: Trns1.V0S20P11
          type: map
          source: Trns1.V0S20P11.filter
          alias: DefaultNA
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_NDC_SK"
            - name: PRSCRB_PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PRSCRB_PROV_DEA_SK"
            - name: DRUG_CLM_DISPNS_AS_WRTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_DISPNS_AS_WRTN_CD_SK"
            - name: DRUG_CLM_LGL_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_LGL_STTUS_CD_SK"
            - name: DRUG_CLM_TIER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_TIER_CD_SK"
            - name: DRUG_CLM_VNDR_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_VNDR_STTUS_CD_SK"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_MNTN_IN"
            - name: MAX_ALW_CST_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_MAX_ALW_CST_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_SNGL_SRC_IN"
            - name: ADJ_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_DT_SK"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_FILL_DT_SK"
            - name: RECON_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_RECON_DT_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "DefaultNA.DefaultNA_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "DefaultNA.DefaultNA_RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_VNDR_PREAUTH_ID"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_BNF_FRMLRY_POL_CD_SK"
            - name: DRUG_CLM_BNF_RSTRCT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_BNF_RSTRCT_CD_SK"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_MCPARTD_COVDRUG_CD_SK"
            - name: DRUG_CLM_PDX_NTWK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_PDX_NTWK_CD_SK"
            - name: DRUG_CLM_PRAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_PRAUTH_CD_SK"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_AVG_WHLSL_PRICE_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_UCR_AMT"
            - name: DRUG_CLM_PRTL_FILL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_PRTL_FILL_CD_SK"
            - name: DRUG_CLM_PDX_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_DRUG_CLM_PDX_TYP_CD_SK"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_INCNTV_FEE_AMT"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SUBMT_PROD_ID_QLFR_CD_SK"
            - name: CNTNGNT_THER_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CNTNGNT_THER_CD_SK"
            - name: CNTNGNT_THER_SCHD_ID
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_CNTNGNT_THER_SCHD_ID"
            - name: MBR_NTWK_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultNA.DefaultNA_MBR_NTWK_DIFF_PD_AMT"
            - name: MBR_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultNA.DefaultNA_MBR_SLS_TAX_AMT"
            - name: MBR_PRCS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "DefaultNA.DefaultNA_MBR_PRCS_FEE_AMT"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: "DefaultNA.DefaultNA_GNRC_PROD_IN"
        - name: Trns1.V0S20P14
          type: map
          source: Trns1.V0S20P14.filter
          alias: Recycle1
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Recycle1.Recycle1_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Recycle1.Recycle1_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Recycle1.Recycle1_PRI_KEY_STRING"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_NDC"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_NDC_SK"
            - name: PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_PRSCRB_PROV_DEA"
            - name: DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_DAW_CD"
            - name: DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_LGL_STTUS_CD"
            - name: DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_TIER_CD"
            - name: DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_VNDR_STTUS_CD"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_MNTN_IN"
            - name: MAC_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_MAC_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_SNGL_SRC_IN"
            - name: ADJ_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_ADJ_DT"
            - name: FILL_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_FILL_DT"
            - name: RECON_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_RECON_DT"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_OTHR_SAV_AMT"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_SLS_TAX_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Recycle1.Recycle1_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Recycle1.Recycle1_RX_SUBMT_QTY"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_VNDR_PREAUTH_ID"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_CLM_STTUS_CD"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_PROV_ID"
            - name: NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "Recycle1.Recycle1_NDC_LABEL_NM"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_BNF_FRMLRY_POL_CD"
            - name: DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_BNF_RSTRCT_CD"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_MCPARTD_COVDRUG_CD"
            - name: DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_PRAUTH_CD"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_DRUG_CLM_BILL_BSS_CD_SK"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_AVG_WHLSL_PRICE_AMT"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_UCR_AMT"
        - name: Trns1.V0S20P16
          type: map
          source: Trns1.V0S20P16.filter
          alias: adj_claims1
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims1.adj_claims1_DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims1.adj_claims1_SRC_SYS_CD_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims1.adj_claims1_CLM_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims1.adj_claims1_SRC_SYS_CD"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims1.adj_claims1_RX_NO"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "adj_claims1.adj_claims1_FILL_DT_SK"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "adj_claims1.adj_claims1_PROV_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims1.adj_claims1_CLM_ID"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              nullable: N
              map: "adj_claims1.adj_claims1_VNDR_CLM_NO"
        - name: Trns1.V0S20P17
          type: map
          source: Trns1.V0S20P17.filter
          alias: TempFile
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "TempFile.TempFile_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "TempFile.TempFile_CLM_ID"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "TempFile.TempFile_SRC_SYS_CD"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "TempFile.TempFile_LAST_UPDT_RUN_CYC_EXCTN_SK"
        - name: Collector1
          type: unknown
          source: W_DRUG_CLMTrns1.lookupDrugClmEdithf_recycleW_WEBDM_ETL_DRVR
          alias: 
          origType: CCollector


