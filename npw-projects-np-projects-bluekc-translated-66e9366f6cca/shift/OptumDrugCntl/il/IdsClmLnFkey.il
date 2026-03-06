model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmLnFkey
      origin: datastage
      type: dataflow
      folder: "claim/ClaimLine/ClmLn"
      runAfterSubrOnJobSuccess: enabled
      description: "IdsClmLnFkey - IDS Claim Line Foreign Keying"
      fullDescription: "********************************************************************************\r\n� Copyright 2005 Blue Cross/Blue Shield of Kansas City\r\n� Copyright 2009 Blue Cross/Blue Shield of Kansas City\r\n� Copyright 2021 Blue Cross/Blue Shield of Kansas City\r\n� Copyright 2023 Blue Cross/Blue Shield of Kansas City\r\n\r\nJOB NAME:  IdsClmLnFkey\r\nCALLED BY:  IdsEmrProcedureClmLnLoadSeq\r\nDESCRIPTION:\r\n\x09Foreign key and code lookup for Claim Line data in \r\n\x09preparation for load into IDS table CLM_LN\r\n\r\nPRIMARY SOURCE:   .../key/FctsClmLnExtr.FctsClmLn.uniq\r\n\r\nMODIFICATIONS:\r\nProgrammer  \x09Date\x09\x09Project\x09\x09Comment\x09\x09\x09\x09\x09\x09\x09\x09\x09Environment                Reviewer               Date\r\n=========================================================================================================================================================================\r\nKevin Soderlund\x0903/09/2004\x09\x09\x09Originally Programmed             \r\nKevin Soderlund\x0905/01/2004\x09\x09\x09Coding complete for release 1.0\r\nKevin Soderlund\x0906/28/2004\x09\x09\x09Updated with changes for release 1.1\r\nKevin Soderlund\x0909/01/2004\x09\x09\x09Added claim type to metadata; Updated call for procedure code\r\nBrent Leland            09/07/2004             \x09\x09Added default rows for UNK and NA\r\nKevin Soderlund\x0909/29/2004\x09\x09\x09Claim Line pricing source - changed \\\"FACETS\\\" source to check source code (verified by Mike)\r\nSHaron Andrew \x0902/01/2005\x09IDS 3.0\r\n BJ Luce                   1/23/2006               \x09\x09add columns CLM_LN_SVC_LOC_TYP_CD_SK, CLM_LN_SVC_PRICE_RULE_CD_SK \r\n\x09\x09\x09\x09\x09\x09and NON_PAR_SAV_AMT\r\n                                                                \x09\x09 set SVC_PRICE_RULE_ID to spaces\r\n                                                                 \x09\x09change format of CRF and the recycle records for SVC_LOC_TYP_CD and NON_PAR_SAV_AMT\r\nBrent Leland           03/28/2006              \x09\x09Removed unneed logic in Trans4 for GetRecycleKey() call\r\nBrent Leland           08/07/2006              \x09\x09Added current run cycle to output records.  Without the current run cycle, error recycle records \r\n                                                                 \x09\x09would have an old run cycle and would be removed in delete process.\r\n                                                                \x09\x09Changed file path parameter to evironment variable. \r\nBhoomi Dasari        03/12/2007              \x09\x09Using PCS_PHAR_NBR and SRC _SYS_CD - 'NABP' for looking up the provider sk on the \r\n\x09\x09\x09\x09\x09\x09PROV table for SVC_PROV_SK\r\n                                                                \x09\x09Retrieve sk for 'ACPTD' with target domain - IDS.Claim.finalize.diposition for \r\n\x09\x09\x09\x09\x09\x09CLM_LN_FINL_DISP_CD_SK\r\n                                                                \x09\x09 Code review by Steph Goddard 4/6/07\r\nSAndrew               2008-10-13              #3784 PBM Vndr       Added SrcSysCd =ESI conditions for SrvcProvSK\x09\x09\x09\x09\x09devlIDSnew               Steph Goddard  10/27/2008\r\nSAndrew               2009-02-24              #3494 Labor Accnt     Added SrcSysCd =WellDyneRX conditions for SrvcProvSK\x09\x09\x09\x09devlIDS                      Steph Goddard  03/30/2009\r\n                                                                 \x09\x09IF (\x09ClmLn1.SRC_SYS_CD = \\\"ESI\\\" \x09\x09or \r\n\x09                                                                 \x09\x09ClmLn1.SRC_SYS_CD = \\\"WELLDYNERX\\\"  \x09or \r\n\x09                                                                 \x09\x09ClmLn1.SRC_SYS_CD = \\\"PCS\\\" \x09\x09OR \r\n\x09                                                                 \x09\x09ClmLn1.SRC_SYS_CD = \\\"ARGUS\\\"\r\n\x09                                                                 \x09\x09)  then GetFkeyProv(\\\"NABP\\\", ClmLn1.CLM_LN_SK,  trim ( ClmLn1.SVC_PROV_ID) , Logging) \r\n                                                                 \x09\x09else  \r\n                                                                 \x09\x09    GetFkeyProv(\\\"FACETS\\\", ClmLn1.CLM_LN_SK,  trim ( ClmLn1.SVC_PROV_ID) , Logging)\r\nKalyan Neelam     2009-10-07             4098                             Added SrcSysCd =MCSOURCE conditions for SrvcProvSK\x09\x09\x09\x09devlIDS                    Steph Goddard      10/15/2009\r\nKalyan Neelam     2009-12-29             4110                             Added SrcSysCd =MCAID conditions for SrvcProvSK\x09\x09\x09\x09IntegrateCurDevl \x09  Steph Goddard       01/11/2010\x09\r\nBrent/Kalyan        2010-02-17            TTR-729                        Added Caremark to service provider logic\x09\x09\x09\x09\x09IntegrateCurDevl    Steph Goddard       02/24/2010\r\nKalyan Neelam     2010-12-22             4616                            Added SrcSysCd =MEDTRAK conditions for SrvcProvSK\x09\x09\x09\x09IntegrateNewDevl    Steph Goddard       12/23/2010\r\nRick Henry           2012-05-02              4896                            Proc_Cd_Typ_Cd and Proc_Cd_Cat_Cd  for Proc_Cd_Sk lookup\x09\x09\x09NewDevl                    SAndrew               2012-05-18\r\nKalyan Neelam     2013-01-28    4963 VBB Phase 3              Added 3 new columns on end - VBB_RULE_ID,\x09\x09\x09\x09\x09IntegrateNewDevl        Bhoomi Dasari       3/14/2013\r\n                                                                                                                      VBB_EXCD_ID, CLM_LN_VBB_IN\r\nKalyan Neelam     2013-11-12     5056 FEP Claims               Added SrcSysCd =BCA conditions for SrvcProvSK\x09\x09\x09\x09\x09IntegrateNewDevl            Bhoomi Dasari      11/30/2013\r\nManasa Andru      2014-10-17     TFS - 9580                        Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and\x09\x09\x09\x09IntegrateCurDevl              Kalyan Neelam        2014-10-22\r\n                                                                                                            ITS_SRCHRG_AMT) at the end.\r\nManasa Andru      2014-11-17   TFS - 9580(Post Prod Work)                 Added scale of 2 for the 2 new fields\x09\x09\x09\x09\x09IntegrateCurDevl              Kalyan Neelam        2014-11-19\r\n                                                                                               (ITS_SUPLMT_DSCNT_AMT and ITS_SRCHRG_AMT) \r\nKalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD\x09\x09\x09\x09IntegrateCurDevl              Bhoomi Dasari             02/04/2015                                                     \r\n                                                                                     in the stage variables and pass it to GetFkeyCodes because code sets are created under \r\n\x09\x09\x09\x09\x09\x09BCA for BCBSA\r\nHari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,\x09\x09\x09\x09IntegrateDev1               Kalyan Neelam        2017-08-29\r\n                                                                                    NDC_UNIT_CT) at the end\r\nSudhir Bomshetty    2017-10-20        5781                          Added SRC_SYS_CD = 'BCA' check for SvcProvSk stage variable\x09\x09\x09IntegrateDev2               Kalyan Neelam        2017-10-23  \r\nJaideep Mankala      2017-11-20      5828 \x09   Added new field to identify MED / PDX claim\x09\x09\x09\x09\x09\x09IntegrateDev2                 Kalyan Neelam        2017-11-20\r\n                                                      \x09\x09     when passing to Fkey job\r\nSudhir Bomshetty   2018-01-12        5781                          Added SRC_SYS_CD = 'BCA' check for SVC_END_DT_SK\x09\x09\x09\x09IntegrateDev2                          Kalyan Neelam        2018-01-16\r\n\r\nProgrammer  \x09\x09Date\x09\x09Project\x09\x09Comment\x09\x09\x09\x09\x09\x09\x09\x09\x09\x09Environment\x09Reviewer               \x09Date\r\n====================================================================================================================================================================================\r\nManasa Andru\x09\x092018-03-04\x09TFS21142\x09Modified SvcProvSk stage variable to not log errors when looking Fkey in If logic\x09\x09\x09IntegrateDev2\x09Kalyan Neelam\x092018-03-12\r\nMadhavan B\x09\x092018-02-06\x095792\x09\x09Changed the datatype of the column\x09\x09\x09\x09\x09\x09\x09IntegrateDev1\x09Kalyan Neelam\x092018-02-08\r\n                                                      \x09\x09     \x09\x09NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)\r\nKaushik Kapoor\x09\x092018-03-16\x095828\x09\x09Adding SrcSysCd = SAVRX and LDI to SvcProvSk stage variable\x09\x09\x09\x09IntegrateDev2\x09Jaideep Mankala\x0903/21/2018\r\nSudhir Bomshetty\x09\x092018-03-30\x095781\x09\x09Modified stage variable \\\"SvcProvSk\\\" for BCA to get values for BCA if NABP retruning 0 for Fkey\x09IntegrateDev2\x09Jaideep Mankala\x0904/02/2018      \r\nKaushik Kapoor\x09\x092018-09-20\x095828\x09\x09Adding SrcSysCd = CVS to SvcProvSk stage variable\x09\x09\x09\x09\x09IntegrateDev2\x09Kalyan Neelam\x092018-10-01   \r\nDeepa Bajaj\x09\x092019-10-11\x096131\x09\x09Added \\\"OPTUMRX\\\" Value to 'SvcProvsk' field under the stage variables in 'Trns1' \x09\x09IntegrateDev5\x09Kalyan Neelam\x092019-11-20\r\n\x09\x09\x09\x09\x09\x09\x09Transformer phase.  PBM Replacement \r\nGoutham Kalidindi\x09\x092020-11-12    \x09US-283560\x09Added MEDIMPACT to stageVariable SVC_PROV_SK\x09\x09\x09\x09\x09IntegrateDev2\x09Kalyan Neelam\x092021-02-04\r\nRekha Radhakrishna\x092020-08-17 \x096131\x09\x09Added new fields APC_ID and APC_STTUS_ID\x09\x09\x09\x09\x09\x09IntegrateDev2           \r\nGoutham Kalidindi\x09\x092021-01-14     \x09US-318408\x09LUMERIS (FACETS) Codes lookup\x09\x09\x09\x09\x09\x09\x09IntegrateDev2\x09Kalyan Neelam\x092021-01-14\r\nVikas Abbu\x09\x092021-02-04        \x09RA\x09\x09Added DentaQuest and EyeMed src cd in SVC_PRICE_RULE_ID logic\x09\x09\x09IntegrateDev2\x09Reddy Sanam\x092021-02-10\r\nGoutham Kalidindi\x09\x092021-02-16\x09US-352900\x09LHO Lumeris Codes lookup Conditions\x09\x09\x09\x09\x09\x09\x09IntegrateDev2\x09Kalyan Neelam\x092021-02-18\r\n                                                                                  \x09\x09Changes for \\\"CLAIM LINE ROOM TYPE PRICING METHOD\\\"\r\n\x09\x09\x09\x09\x09\x09\x09\\\"CLAIM LINE TYPE OF SERVICE\\\"\r\n\x09\x09\x09\x09\x09\x09\x09\\\"SERVICE PRICING RULE\\\"\r\nMrudula Kodali\x09\x092021-03-07   \x09US-356530\x09Added Source Sys cd values for EMR Procedures\x09\x09\x09\x09\x09\x09IntegrateDev2\x09Reddy Sanam\x092021-03-08\r\nMrudula Kodali\x09\x092021-03-11    \x09US-311337\x09Added Livongo Source system code in clmlneobexcdsk, clmlnposcdsk, clmlnunittypcdsk,\x09\x09IntegrateDev2\x09Kalyan Neelam\x09021-03-17\r\n\x09\x09\x09\x09\x09\x09\x09ClmLnLobCdSk               \r\nMrudula Kodali\x09\x092021-03-23   \x09US-311337\x09Updated svCdMpngSrcSysCd and removed LVNGHLTH in this variable not to use FACETS\x09\x09IntegrateDev2\x09Hugh Sisson\x092021-03-25\r\n\x09\x09\x09\x09\x09\x09\x09Updated ClmLnunitTypCdSk to use LVNGHLTH \r\nVikas Abbu\x09\x092021-03-29  \x09US-311872\x09Added SOLUTRAN Default value  'NA' in SVC_PRICE_RULE_ID Column\x09\x09\x09IntegrateDev2\x09Jaideep Mankala\x0903/29/2021\r\nMrudula Kodali\x09\x0905/03/2021  \x09US-373652\x09Updated the stage variables\x09\x09\x09\x09\x09\x09\x09\x09IntegrateDev2\x09Jaideep Mankala\x0905/10/2021\r\n\x09\x09\x09\x09\x09\x09\x09Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR\r\nGoutham K\x09\x092021-06-02\x09US-366403\x09Added 'EYEMED' for SVC_PROV_ID Fkey Stage Variable\x09\x09\x09\x09\x09IntegrateDev1\x09Jeyaprasanna\x092021-06-08\r\nVikas Abbu\x09\x092021-06-21\x09RA\x09\x09Updated CLM_LN_LOB_CD logic for EYEMED to use FACETS\x09\x09\x09\x09IntegrateDev1\x09Manasa Andru\x092021-06-30     \r\nVenkata Yama\x09\x092022-01-12\x09us480876\x09\x09Added src_sys_cd='HCA'\x09\x09\x09\x09\x09\x09\x09\x09IntegrateDev2\x09Harsha Ravuri\x092022-01-13 \r\nManisha Gandra\x09\x092022-01-15\x09US 459610\x09Added Nations Source System Code in \\\"Trns1\\\" stage ClmLnLobCdSK variable\x09\x09\x09IntegrateDev2\x09Jeyaprasanna\x092022-01-20\r\nKen Bradmon\x09\x092023-08-07\x09us559895\x09\x09Added 8 new Providers to the svEMRSrcSysCd variable of the Trns1 stage.\x09\x09\x09IntegrateDev2         Reddy Sanam          2023-10-27\x09\x09\r\n\x09\x09\x09\x09\x09\x09\x09CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, \r\n\x09\x09\x09\x09\x09\x09\x09WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.\r\n\x09\x09\x09\x09\x09\x09\x09This is the only change.\r\nDeepika C               \x09\x092023-08-01      \x09US 589700             Added two new fields SNOMED_CT_CD,CVX_VCCN_CD                                                       \x09IntegrateDevB\x09Harsha Ravuri\x092023-09-20\r\n\r\nSham Shankaranarayana       2023-12-21               US 599810             Added new Stage variable svSBVSrcSysCd and updated existing StageVar \r\n                                                                                                                ClmLnLobCdSk,SvcProvSk in Transformer Trns1                                                                                    IntegrateDev1         Jeyaprasanna         2023-12-21\r\nDeepika C                            2024-04-09               US 614484               Updated StageVar ClmLnLobCdSk,SvcProvSk in Transformer Trns1 - added NATIONBFTSFLEX         IntegrateDev2         Jeyaprasanna         2024-04-10\r\nHarsha Ravuri\x09\x092025-05-06\x09US#649651\x09Added new provider 'ASCENTIST' to the svEMRSrcSysCd variable of the Trns1 stage.\x09\x09IntegrateDev2         Jeyaprasanna         2025-05-20"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "Run Cycle"
        - name: Logging
          type: string
          description: Logging
        - name: Source
          type: string
          description: "Source System"
        - name: InFile
          type: string
          description: InFile
      steps:
        - name: ClmLnExtr
          type: fileReader
          urls: 
            - "#$FilePath#/key/#InFile#"
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
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT
              type: timestamp
              nullable: N
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              nullable: N
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              nullable: N
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              nullable: N
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 20
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 3
              nullable: N
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC
              type: varchar
              width: 20
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
            - name: MED_PDX_IND
              type: char
              width: 3
            - name: APC_ID
              type: varchar
              width: 20
            - name: APC_STTUS_ID
              type: varchar
              width: 20
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
        - name: CLM_LN
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_LN.#Source#.dat"
          source: Collector
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_recycle
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "Trns1.V54S1P3"
            # Hashed File has no directory specified
        - name: hf_claim_recycle_keys
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_claim_recycle_keys"
          tableAction: append
          source: "Trns1.V54S1P8"
            # Hashed File has no directory specified
        - name: Trns1.V54S1P2.map
          type: map
          source: ClmLnExtr
          alias: ClmLn1
          stageVars: 
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmLn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmLn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmLn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmLn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmLn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmLn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmLn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmLn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmLn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmLn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmLn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmLn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmLn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmLn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmLn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmLn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmLn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmLn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmLn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmLn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmLn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  If (ClmLn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: "Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", ClmLn1.CLM_LN_SK, \"SOURCE SYSTEM\", ClmLn1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(SrcSysCd, ClmLn1.CLM_LN_SK, trim(ClmLn1.CLM_ID), Logging)"
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PROC_CD_TYP_CD <> \"ICD10\" Then ClmLn1.PROC_CD[1, 5] else ClmLn1.PROC_CD"
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(sProcCd) = 'UNK' Then 0 Else  If Trim(sProcCd) = 'NA' Then 1 Else GetFkeyProcCd(\"FACETS\", ClmLn1.CLM_LN_SK, sProcCd, ClmLn1.PROC_CD_TYP_CD, ClmLn1.PROC_CD_CAT_CD, Logging)"
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmLn1.SVC_PROV_ID) = 'UNK' Then 0 Else  If Trim(ClmLn1.SVC_PROV_ID) = 'NA' Then 1 Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC') Then GetFkeyProv(\"NATIONBFTSOTC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB') Then GetFkeyProv(\"NATIONBFTSBBB\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD') Then GetFkeyProv(\"NATIONBFTSRWD\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG') Then GetFkeyProv(\"NATIONBFTSHRNG\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX') Then GetFkeyProv(\"NATIONBFTSFLEX\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'DOMINION') Then GetFkeyProv(\"DOMINION\", ClmLn1.CLM_LN_SK, trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ', '', ClmLn1.SVC_PROV_ID)), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'MARC') Then GetFkeyProv(\"MARC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'EYEMED') Then GetFkeyProv(\"EYEMED\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then GetFkeyProv(\"SOLUTRAN\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'ESI' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'MEDIMPACT' Or ClmLn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\" Or ClmLn1.SRC_SYS_CD = \"ARGUS\" Or ClmLn1.SRC_SYS_CD = \"MCSOURCE\" Or ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmLn1.SRC_SYS_CD = \"SAVRX\" Or ClmLn1.SRC_SYS_CD = \"LDI\" Or ClmLn1.SRC_SYS_CD = \"CVS\") then  If ((ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\") And GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), \"X\") = 0) Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging)"
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_DSALW_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM FINALIZE DISPOSITION\", ClmLn1.CLM_LN_FINL_DISP_CD, Logging)"
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyExcd(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging) Else GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'DENTAQUEST' or ClmLn1.SRC_SYS_CD = 'SOLUTRAN' or ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or ClmLn1.SRC_SYS_CD = 'EYEMED' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX' or svSBVSrcSysCd = 'SBV') Then GetFkeyCodes('FACETS', ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging)"
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PLACE OF SERVICE\", ClmLn1.CLM_LN_POS_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION\", ClmLn1.CLM_LN_PREAUTH_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION SOURCE\", ClmLn1.CLM_LN_PREAUTH_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PRICING SOURCE\", ClmLn1.CLM_LN_PRICE_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE REFERRAL\", ClmLn1.CLM_LN_RFRL_CD, Logging)"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyRvnu(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_RVNU_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE\", ClmLn1.CLM_LN_ROOM_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging)"
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes(ClmLn1.SRC_SYS_CD, ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging) Else GetFkeyCodes(\"FACETS\", ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE LOCATION TYPE\", ClmLn1.SVC_LOC_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging)"
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PRI_LOB_IN = 'NA' Then 'X' Else ClmLn1.PRI_LOB_IN"
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_STRT_DT, Logging)"
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyVbbRule(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_RULE_ID, Logging)"
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_EXCD_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmLn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmLn1.CLM_LN_SK)"
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(ClmLn1.CLM_LN_SK, ClmLn1.NDC, Logging)"
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: "GetFkeyClctnDomainCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, 'FACETS DBO', 'CLAIM SUBTYPE', 'IDS', 'CLAIM SUBTYPE', ClmLn1.NDC_DRUG_FORM_CD, 'Logging')"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmLn1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmLn1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmLn1.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CLM_LN_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_SEQ_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: PriLobIn
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "ClmLn1.PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "ClmLn1.RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.LMT_PFX_ID"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "ClmLn1.PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              map: "ClmLn1.RFRL_ID_TX"
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: " If (ClmLn1.SRC_SYS_CD = 'BCBSSC' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'BCBSSC' Or ClmLn1.SRC_SYS_CD = 'BCBSA' Or ClmLn1.SRC_SYS_CD = 'BCA' OR ClmLn1.SRC_SYS_CD = 'EYEMED' OR ClmLn1.SRC_SYS_CD = 'DENTAQUEST' OR ClmLn1.SRC_SYS_CD = 'LVNGHLTH' OR ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then 'NA' Else space(1)"
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_RULE_TYP_TX"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NON_PAR_SAV_AMT"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "ClmLn1.VBB_RULE_ID"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.VBB_EXCD_ID"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CLM_LN_VBB_IN"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SUPLMT_DSCNT_AMT"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SRCHRG_AMT"
            - name: NDC
              type: varchar
              width: 20
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "ClmLn1.NDC_UNIT_CT"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "ClmLn1.MED_PDX_IND"
            - name: APC_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_ID"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_STTUS_ID"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "ClmLn1.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "ClmLn1.CVX_VCCN_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmSk
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProcCdSk
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: SvcProvSk
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnDsalwExcdSk
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnEobExcdSk
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnFinlDispCdSk
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnLobCdSk
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnPosCdSk
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnPreauthCdSk
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnPreauthSrcCd
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnPriceSrcCdSk
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnRflCdSk
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnRvnuCdSk
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnRoomPriceMethSk
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnRoomTypCdSk
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnTosCdSk
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnunitTypCdSk
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If (ClmLn1.SRC_SYS_CD = 'BCA' OR ClmLn1.SRC_SYS_CD = 'EYEMED') Then  if SvcEndDt = 'UNK' Then '2199-12-31' Else SvcEndDt Else SvcEndDt"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: SvcStrtDt
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnSvcLocTypCdSk
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnSvcPriceRuleCdSk
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: svVbbRuleSk
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: svVbbExcdSk
            - name: NDC_SK
              type: int
              width: 4
              map: svNDCSk
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: svNDCDrugCDFormSk
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svEMRSrcSysCd
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svSBVSrcSysCd
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svCdMpngSrcSysCd
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCd
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: sProcCd
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProcCdSk
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: SvcProvSk
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnDsalwExcdSk
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnFinlDispCdSk
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnEobExcdSk
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnLobCdSk
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPosCdSk
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthCdSk
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthSrcCd
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPriceSrcCdSk
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRflCdSk
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRvnuCdSk
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomPriceMethSk
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomTypCdSk
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnTosCdSk
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnunitTypCdSk
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcLocTypCdSk
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcPriceRuleCdSk
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: PriLobIn
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: SvcEndDt
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: SvcStrtDt
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbRuleSk
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbExcdSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: svNDCSk
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: svNDCDrugCDFormSk
              initValue: "0"
        - name: Trns1.V54S1P2
          type: filter
          source: Trns1.V54S1P2.map
          filter: "ErrCount = 0 Or PassThru = 'Y'"
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
            - name: RFRL_ID_TX
              type: varchar
              width: 20
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
            - name: APC_ID
              type: varchar
              width: 20
            - name: APC_STTUS_ID
              type: varchar
              width: 20
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
          keys:
            - CLM_LN_SK
        - name: Trns1.V54S1P3.map
          type: map
          source: ClmLnExtr
          alias: ClmLn1
          stageVars: 
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmLn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmLn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmLn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmLn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmLn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmLn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmLn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmLn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmLn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmLn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmLn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmLn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmLn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmLn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmLn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmLn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmLn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmLn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmLn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmLn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmLn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  If (ClmLn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: "Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", ClmLn1.CLM_LN_SK, \"SOURCE SYSTEM\", ClmLn1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(SrcSysCd, ClmLn1.CLM_LN_SK, trim(ClmLn1.CLM_ID), Logging)"
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PROC_CD_TYP_CD <> \"ICD10\" Then ClmLn1.PROC_CD[1, 5] else ClmLn1.PROC_CD"
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(sProcCd) = 'UNK' Then 0 Else  If Trim(sProcCd) = 'NA' Then 1 Else GetFkeyProcCd(\"FACETS\", ClmLn1.CLM_LN_SK, sProcCd, ClmLn1.PROC_CD_TYP_CD, ClmLn1.PROC_CD_CAT_CD, Logging)"
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmLn1.SVC_PROV_ID) = 'UNK' Then 0 Else  If Trim(ClmLn1.SVC_PROV_ID) = 'NA' Then 1 Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC') Then GetFkeyProv(\"NATIONBFTSOTC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB') Then GetFkeyProv(\"NATIONBFTSBBB\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD') Then GetFkeyProv(\"NATIONBFTSRWD\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG') Then GetFkeyProv(\"NATIONBFTSHRNG\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX') Then GetFkeyProv(\"NATIONBFTSFLEX\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'DOMINION') Then GetFkeyProv(\"DOMINION\", ClmLn1.CLM_LN_SK, trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ', '', ClmLn1.SVC_PROV_ID)), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'MARC') Then GetFkeyProv(\"MARC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'EYEMED') Then GetFkeyProv(\"EYEMED\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then GetFkeyProv(\"SOLUTRAN\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'ESI' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'MEDIMPACT' Or ClmLn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\" Or ClmLn1.SRC_SYS_CD = \"ARGUS\" Or ClmLn1.SRC_SYS_CD = \"MCSOURCE\" Or ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmLn1.SRC_SYS_CD = \"SAVRX\" Or ClmLn1.SRC_SYS_CD = \"LDI\" Or ClmLn1.SRC_SYS_CD = \"CVS\") then  If ((ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\") And GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), \"X\") = 0) Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging)"
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_DSALW_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM FINALIZE DISPOSITION\", ClmLn1.CLM_LN_FINL_DISP_CD, Logging)"
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyExcd(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging) Else GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'DENTAQUEST' or ClmLn1.SRC_SYS_CD = 'SOLUTRAN' or ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or ClmLn1.SRC_SYS_CD = 'EYEMED' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX' or svSBVSrcSysCd = 'SBV') Then GetFkeyCodes('FACETS', ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging)"
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PLACE OF SERVICE\", ClmLn1.CLM_LN_POS_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION\", ClmLn1.CLM_LN_PREAUTH_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION SOURCE\", ClmLn1.CLM_LN_PREAUTH_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PRICING SOURCE\", ClmLn1.CLM_LN_PRICE_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE REFERRAL\", ClmLn1.CLM_LN_RFRL_CD, Logging)"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyRvnu(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_RVNU_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE\", ClmLn1.CLM_LN_ROOM_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging)"
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes(ClmLn1.SRC_SYS_CD, ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging) Else GetFkeyCodes(\"FACETS\", ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE LOCATION TYPE\", ClmLn1.SVC_LOC_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging)"
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PRI_LOB_IN = 'NA' Then 'X' Else ClmLn1.PRI_LOB_IN"
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_STRT_DT, Logging)"
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyVbbRule(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_RULE_ID, Logging)"
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_EXCD_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmLn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmLn1.CLM_LN_SK)"
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(ClmLn1.CLM_LN_SK, ClmLn1.NDC, Logging)"
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: "GetFkeyClctnDomainCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, 'FACETS DBO', 'CLAIM SUBTYPE', 'IDS', 'CLAIM SUBTYPE', ClmLn1.NDC_DRUG_FORM_CD, 'Logging')"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(ClmLn1.CLM_LN_SK)"
            - name: INSRT_UPDT_CD
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: date
              nullable: N
              map: "ClmLn1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmLn1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmLn1.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CLM_LN_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_SEQ_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PRI_LOB_IN"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.LMT_PFX_ID"
            - name: PREAUTH_ID
              type: char
              width: 9
              nullable: N
              map: "ClmLn1.PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: char
              width: 9
              nullable: N
              map: "ClmLn1.RFRL_ID_TX"
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLn1.SVC_PRICE_RULE_ID"
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              nullable: N
              map: "ClmLn1.SVC_RULE_TYP_TX"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NON_PAR_SAV_AMT"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "ClmLn1.VBB_RULE_ID"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.VBB_EXCD_ID"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CLM_LN_VBB_IN"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SUPLMT_DSCNT_AMT"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SRCHRG_AMT"
            - name: NDC
              type: varchar
              width: 20
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "ClmLn1.NDC_UNIT_CT"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "ClmLn1.MED_PDX_IND"
            - name: APC_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_ID"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_STTUS_ID"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "ClmLn1.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "ClmLn1.CVX_VCCN_CD"
            - name: RECYC_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.RECYCLE_CT + 1"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "ClmLn1.PRI_KEY_STRING"
            - name: NDC_SK
              type: int
              width: 4
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svEMRSrcSysCd
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svSBVSrcSysCd
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svCdMpngSrcSysCd
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCd
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: sProcCd
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProcCdSk
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: SvcProvSk
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnDsalwExcdSk
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnFinlDispCdSk
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnEobExcdSk
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnLobCdSk
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPosCdSk
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthCdSk
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthSrcCd
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPriceSrcCdSk
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRflCdSk
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRvnuCdSk
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomPriceMethSk
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomTypCdSk
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnTosCdSk
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnunitTypCdSk
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcLocTypCdSk
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcPriceRuleCdSk
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: PriLobIn
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: SvcEndDt
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: SvcStrtDt
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbRuleSk
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbExcdSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: svNDCSk
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: svNDCDrugCDFormSk
              initValue: "0"
        - name: Trns1.V54S1P3
          type: filter
          source: Trns1.V54S1P3.map
          filter: "ErrCount > 0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: int
              width: 4
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
              type: date
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYC_CT
              type: smallint
              width: 2
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT
              type: timestamp
              nullable: N
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
            - name: PREAUTH_ID
              type: char
              width: 9
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
            - name: RFRL_ID_TX
              type: char
              width: 9
              nullable: N
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              nullable: N
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
          keys:
            - JOB_EXCTN_RCRD_ERR_SK
        - name: Trns1.V54S1P4.map
          type: map
          source: ClmLnExtr
          alias: ClmLn1
          stageVars: 
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmLn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmLn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmLn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmLn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmLn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmLn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmLn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmLn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmLn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmLn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmLn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmLn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmLn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmLn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmLn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmLn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmLn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmLn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmLn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmLn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmLn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  If (ClmLn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: "Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", ClmLn1.CLM_LN_SK, \"SOURCE SYSTEM\", ClmLn1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(SrcSysCd, ClmLn1.CLM_LN_SK, trim(ClmLn1.CLM_ID), Logging)"
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PROC_CD_TYP_CD <> \"ICD10\" Then ClmLn1.PROC_CD[1, 5] else ClmLn1.PROC_CD"
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(sProcCd) = 'UNK' Then 0 Else  If Trim(sProcCd) = 'NA' Then 1 Else GetFkeyProcCd(\"FACETS\", ClmLn1.CLM_LN_SK, sProcCd, ClmLn1.PROC_CD_TYP_CD, ClmLn1.PROC_CD_CAT_CD, Logging)"
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmLn1.SVC_PROV_ID) = 'UNK' Then 0 Else  If Trim(ClmLn1.SVC_PROV_ID) = 'NA' Then 1 Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC') Then GetFkeyProv(\"NATIONBFTSOTC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB') Then GetFkeyProv(\"NATIONBFTSBBB\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD') Then GetFkeyProv(\"NATIONBFTSRWD\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG') Then GetFkeyProv(\"NATIONBFTSHRNG\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX') Then GetFkeyProv(\"NATIONBFTSFLEX\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'DOMINION') Then GetFkeyProv(\"DOMINION\", ClmLn1.CLM_LN_SK, trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ', '', ClmLn1.SVC_PROV_ID)), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'MARC') Then GetFkeyProv(\"MARC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'EYEMED') Then GetFkeyProv(\"EYEMED\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then GetFkeyProv(\"SOLUTRAN\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'ESI' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'MEDIMPACT' Or ClmLn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\" Or ClmLn1.SRC_SYS_CD = \"ARGUS\" Or ClmLn1.SRC_SYS_CD = \"MCSOURCE\" Or ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmLn1.SRC_SYS_CD = \"SAVRX\" Or ClmLn1.SRC_SYS_CD = \"LDI\" Or ClmLn1.SRC_SYS_CD = \"CVS\") then  If ((ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\") And GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), \"X\") = 0) Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging)"
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_DSALW_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM FINALIZE DISPOSITION\", ClmLn1.CLM_LN_FINL_DISP_CD, Logging)"
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyExcd(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging) Else GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'DENTAQUEST' or ClmLn1.SRC_SYS_CD = 'SOLUTRAN' or ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or ClmLn1.SRC_SYS_CD = 'EYEMED' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX' or svSBVSrcSysCd = 'SBV') Then GetFkeyCodes('FACETS', ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging)"
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PLACE OF SERVICE\", ClmLn1.CLM_LN_POS_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION\", ClmLn1.CLM_LN_PREAUTH_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION SOURCE\", ClmLn1.CLM_LN_PREAUTH_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PRICING SOURCE\", ClmLn1.CLM_LN_PRICE_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE REFERRAL\", ClmLn1.CLM_LN_RFRL_CD, Logging)"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyRvnu(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_RVNU_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE\", ClmLn1.CLM_LN_ROOM_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging)"
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes(ClmLn1.SRC_SYS_CD, ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging) Else GetFkeyCodes(\"FACETS\", ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE LOCATION TYPE\", ClmLn1.SVC_LOC_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging)"
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PRI_LOB_IN = 'NA' Then 'X' Else ClmLn1.PRI_LOB_IN"
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_STRT_DT, Logging)"
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyVbbRule(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_RULE_ID, Logging)"
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_EXCD_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmLn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmLn1.CLM_LN_SK)"
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(ClmLn1.CLM_LN_SK, ClmLn1.NDC, Logging)"
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: "GetFkeyClctnDomainCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, 'FACETS DBO', 'CLAIM SUBTYPE', 'IDS', 'CLAIM SUBTYPE', ClmLn1.NDC_DRUG_FORM_CD, 'Logging')"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmLn1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmLn1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmLn1.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: PREAUTH_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 40
              nullable: N
              map: "'UNK'"
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              map: "'UNK'"
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "space(1)"
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.0"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "ClmLn1.VBB_RULE_ID"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.VBB_EXCD_ID"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NDC
              type: varchar
              width: 20
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "@Null"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "ClmLn1.MED_PDX_IND"
            - name: APC_ID
              type: varchar
              width: 20
              map: "'UNK'"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "'UNK'"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "ClmLn1.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "ClmLn1.CVX_VCCN_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_SK
              type: int
              width: 4
              map: "0"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "0"
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svEMRSrcSysCd
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svSBVSrcSysCd
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svCdMpngSrcSysCd
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCd
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: sProcCd
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProcCdSk
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: SvcProvSk
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnDsalwExcdSk
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnFinlDispCdSk
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnEobExcdSk
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnLobCdSk
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPosCdSk
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthCdSk
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthSrcCd
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPriceSrcCdSk
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRflCdSk
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRvnuCdSk
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomPriceMethSk
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomTypCdSk
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnTosCdSk
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnunitTypCdSk
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcLocTypCdSk
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcPriceRuleCdSk
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: PriLobIn
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: SvcEndDt
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: SvcStrtDt
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbRuleSk
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbExcdSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: svNDCSk
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: svNDCDrugCDFormSk
              initValue: "0"
        - name: Trns1.V54S1P4
          type: filter
          source: Trns1.V54S1P4.map
          filter: "@OUTROWNUM = 1"
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 40
              nullable: N
            - name: RFRL_ID_TX
              type: varchar
              width: 20
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
            - name: APC_ID
              type: varchar
              width: 20
            - name: APC_STTUS_ID
              type: varchar
              width: 20
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
          keys:
            - CLM_LN_SK
        - name: Trns1.V54S1P6.map
          type: map
          source: ClmLnExtr
          alias: ClmLn1
          stageVars: 
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmLn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmLn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmLn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmLn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmLn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmLn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmLn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmLn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmLn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmLn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmLn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmLn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmLn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmLn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmLn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmLn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmLn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmLn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmLn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmLn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmLn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  If (ClmLn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: "Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", ClmLn1.CLM_LN_SK, \"SOURCE SYSTEM\", ClmLn1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(SrcSysCd, ClmLn1.CLM_LN_SK, trim(ClmLn1.CLM_ID), Logging)"
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PROC_CD_TYP_CD <> \"ICD10\" Then ClmLn1.PROC_CD[1, 5] else ClmLn1.PROC_CD"
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(sProcCd) = 'UNK' Then 0 Else  If Trim(sProcCd) = 'NA' Then 1 Else GetFkeyProcCd(\"FACETS\", ClmLn1.CLM_LN_SK, sProcCd, ClmLn1.PROC_CD_TYP_CD, ClmLn1.PROC_CD_CAT_CD, Logging)"
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmLn1.SVC_PROV_ID) = 'UNK' Then 0 Else  If Trim(ClmLn1.SVC_PROV_ID) = 'NA' Then 1 Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC') Then GetFkeyProv(\"NATIONBFTSOTC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB') Then GetFkeyProv(\"NATIONBFTSBBB\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD') Then GetFkeyProv(\"NATIONBFTSRWD\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG') Then GetFkeyProv(\"NATIONBFTSHRNG\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX') Then GetFkeyProv(\"NATIONBFTSFLEX\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'DOMINION') Then GetFkeyProv(\"DOMINION\", ClmLn1.CLM_LN_SK, trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ', '', ClmLn1.SVC_PROV_ID)), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'MARC') Then GetFkeyProv(\"MARC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'EYEMED') Then GetFkeyProv(\"EYEMED\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then GetFkeyProv(\"SOLUTRAN\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'ESI' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'MEDIMPACT' Or ClmLn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\" Or ClmLn1.SRC_SYS_CD = \"ARGUS\" Or ClmLn1.SRC_SYS_CD = \"MCSOURCE\" Or ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmLn1.SRC_SYS_CD = \"SAVRX\" Or ClmLn1.SRC_SYS_CD = \"LDI\" Or ClmLn1.SRC_SYS_CD = \"CVS\") then  If ((ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\") And GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), \"X\") = 0) Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging)"
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_DSALW_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM FINALIZE DISPOSITION\", ClmLn1.CLM_LN_FINL_DISP_CD, Logging)"
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyExcd(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging) Else GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'DENTAQUEST' or ClmLn1.SRC_SYS_CD = 'SOLUTRAN' or ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or ClmLn1.SRC_SYS_CD = 'EYEMED' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX' or svSBVSrcSysCd = 'SBV') Then GetFkeyCodes('FACETS', ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging)"
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PLACE OF SERVICE\", ClmLn1.CLM_LN_POS_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION\", ClmLn1.CLM_LN_PREAUTH_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION SOURCE\", ClmLn1.CLM_LN_PREAUTH_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PRICING SOURCE\", ClmLn1.CLM_LN_PRICE_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE REFERRAL\", ClmLn1.CLM_LN_RFRL_CD, Logging)"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyRvnu(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_RVNU_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE\", ClmLn1.CLM_LN_ROOM_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging)"
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes(ClmLn1.SRC_SYS_CD, ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging) Else GetFkeyCodes(\"FACETS\", ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE LOCATION TYPE\", ClmLn1.SVC_LOC_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging)"
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PRI_LOB_IN = 'NA' Then 'X' Else ClmLn1.PRI_LOB_IN"
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_STRT_DT, Logging)"
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyVbbRule(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_RULE_ID, Logging)"
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_EXCD_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmLn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmLn1.CLM_LN_SK)"
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(ClmLn1.CLM_LN_SK, ClmLn1.NDC, Logging)"
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: "GetFkeyClctnDomainCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, 'FACETS DBO', 'CLAIM SUBTYPE', 'IDS', 'CLAIM SUBTYPE', ClmLn1.NDC_DRUG_FORM_CD, 'Logging')"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmLn1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmLn1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmLn1.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "1"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "'NA'"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              map: "'NA'"
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "space(1)"
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.0"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "ClmLn1.VBB_RULE_ID"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.VBB_EXCD_ID"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NDC
              type: varchar
              width: 20
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "@Null"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "ClmLn1.MED_PDX_IND"
            - name: APC_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "ClmLn1.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "ClmLn1.CVX_VCCN_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_SK
              type: int
              width: 4
              map: "1"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              map: "1"
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svEMRSrcSysCd
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svSBVSrcSysCd
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svCdMpngSrcSysCd
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCd
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: sProcCd
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProcCdSk
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: SvcProvSk
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnDsalwExcdSk
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnFinlDispCdSk
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnEobExcdSk
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnLobCdSk
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPosCdSk
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthCdSk
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthSrcCd
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPriceSrcCdSk
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRflCdSk
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRvnuCdSk
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomPriceMethSk
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomTypCdSk
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnTosCdSk
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnunitTypCdSk
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcLocTypCdSk
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcPriceRuleCdSk
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: PriLobIn
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: SvcEndDt
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: SvcStrtDt
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbRuleSk
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbExcdSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: svNDCSk
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: svNDCDrugCDFormSk
              initValue: "0"
        - name: Trns1.V54S1P6
          type: filter
          source: Trns1.V54S1P6.map
          filter: "@OUTROWNUM = 1"
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: SVC_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_DSALW_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_LOB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_POS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RFRL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_TOS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
            - name: RFRL_ID_TX
              type: varchar
              width: 20
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SVC_LOC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_SVC_PRICE_RULE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: VBB_RULE_SK
              type: int
              width: 4
              nullable: N
            - name: VBB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
            - name: APC_ID
              type: varchar
              width: 20
            - name: APC_STTUS_ID
              type: varchar
              width: 20
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
          keys:
            - CLM_LN_SK
        - name: Trns1.V54S1P8.map
          type: map
          source: ClmLnExtr
          alias: ClmLn1
          stageVars: 
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmLn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmLn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmLn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmLn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmLn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmLn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmLn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmLn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmLn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmLn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmLn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmLn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmLn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmLn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmLn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmLn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmLn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmLn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmLn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmLn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmLn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmLn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmLn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmLn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  If (ClmLn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: "Trim(ClmLn1.SRC_SYS_CD)"
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", ClmLn1.CLM_LN_SK, \"SOURCE SYSTEM\", ClmLn1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(SrcSysCd, ClmLn1.CLM_LN_SK, trim(ClmLn1.CLM_ID), Logging)"
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PROC_CD_TYP_CD <> \"ICD10\" Then ClmLn1.PROC_CD[1, 5] else ClmLn1.PROC_CD"
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(sProcCd) = 'UNK' Then 0 Else  If Trim(sProcCd) = 'NA' Then 1 Else GetFkeyProcCd(\"FACETS\", ClmLn1.CLM_LN_SK, sProcCd, ClmLn1.PROC_CD_TYP_CD, ClmLn1.PROC_CD_CAT_CD, Logging)"
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmLn1.SVC_PROV_ID) = 'UNK' Then 0 Else  If Trim(ClmLn1.SVC_PROV_ID) = 'NA' Then 1 Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC') Then GetFkeyProv(\"NATIONBFTSOTC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB') Then GetFkeyProv(\"NATIONBFTSBBB\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD') Then GetFkeyProv(\"NATIONBFTSRWD\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG') Then GetFkeyProv(\"NATIONBFTSHRNG\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX') Then GetFkeyProv(\"NATIONBFTSFLEX\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'DOMINION') Then GetFkeyProv(\"DOMINION\", ClmLn1.CLM_LN_SK, trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ', '', ClmLn1.SVC_PROV_ID)), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'MARC') Then GetFkeyProv(\"MARC\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'EYEMED') Then GetFkeyProv(\"EYEMED\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  if (ClmLn1.SRC_SYS_CD = 'SOLUTRAN') Then GetFkeyProv(\"SOLUTRAN\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"BCA\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCA\" and GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), 'X') <> 0 Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" and ClmLn1.MED_PDX_IND = 'MED' Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If ClmLn1.SRC_SYS_CD = \"BCBSA\" Then GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else  If (ClmLn1.SRC_SYS_CD = 'ESI' Or ClmLn1.SRC_SYS_CD = 'OPTUMRX' Or ClmLn1.SRC_SYS_CD = 'MEDIMPACT' Or ClmLn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\" Or ClmLn1.SRC_SYS_CD = \"ARGUS\" Or ClmLn1.SRC_SYS_CD = \"MCSOURCE\" Or ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmLn1.SRC_SYS_CD = \"SAVRX\" Or ClmLn1.SRC_SYS_CD = \"LDI\" Or ClmLn1.SRC_SYS_CD = \"CVS\") then  If ((ClmLn1.SRC_SYS_CD = \"MCAID\" Or ClmLn1.SRC_SYS_CD = \"PCS\" Or ClmLn1.SRC_SYS_CD = \"CAREMARK\") And GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), \"X\") = 0) Then GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"NABP\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging) Else GetFkeyProv(\"FACETS\", ClmLn1.CLM_LN_SK, trim(ClmLn1.SVC_PROV_ID), Logging)"
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_DSALW_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM FINALIZE DISPOSITION\", ClmLn1.CLM_LN_FINL_DISP_CD, Logging)"
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyExcd(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging) Else GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_EOB_EXCD, Logging)"
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'DENTAQUEST' or ClmLn1.SRC_SYS_CD = 'SOLUTRAN' or ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or ClmLn1.SRC_SYS_CD = 'EYEMED' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSBBB' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSOTC' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSRWD' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSHRNG' or ClmLn1.SRC_SYS_CD = 'NATIONBFTSFLEX' or svSBVSrcSysCd = 'SBV') Then GetFkeyCodes('FACETS', ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE LOB\", ClmLn1.CLM_LN_LOB_CD, Logging)"
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PLACE OF SERVICE\", ClmLn1.CLM_LN_POS_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION\", ClmLn1.CLM_LN_PREAUTH_CD, Logging)"
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PREAUTHORIZATION SOURCE\", ClmLn1.CLM_LN_PREAUTH_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE PRICING SOURCE\", ClmLn1.CLM_LN_PRICE_SRC_CD, Logging)"
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE REFERRAL\", ClmLn1.CLM_LN_RFRL_CD, Logging)"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyRvnu(\"FACETS\", ClmLn1.CLM_LN_SK, ClmLn1.CLM_LN_RVNU_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE PRICING METHOD\", ClmLn1.CLM_LN_ROOM_PRICE_METH_CD, Logging)"
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE ROOM TYPE\", ClmLn1.CLM_LN_ROOM_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"CLAIM LINE TYPE OF SERVICE\", ClmLn1.CLM_LN_TOS_CD, Logging)"
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmLn1.SRC_SYS_CD = 'LVNGHLTH' or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes(ClmLn1.SRC_SYS_CD, ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging) Else GetFkeyCodes(\"FACETS\", ClmLn1.CLM_LN_SK, \"CLAIM LINE UNIT TYPE\", ClmLn1.CLM_LN_UNIT_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE LOCATION TYPE\", ClmLn1.SVC_LOC_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.SRC_SYS_CD = 'LUMERIS' and GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) = 0 Then GetFkeyCodes('LUMERIS', ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, \"SERVICE PRICING RULE\", ClmLn1.SVC_PRICE_RULE_ID, Logging)"
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: " If ClmLn1.PRI_LOB_IN = 'NA' Then 'X' Else ClmLn1.PRI_LOB_IN"
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmLn1.CLM_LN_SK, ClmLn1.SVC_STRT_DT, Logging)"
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyVbbRule(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_RULE_ID, Logging)"
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyExcd(SrcSysCd, ClmLn1.CLM_LN_SK, ClmLn1.VBB_EXCD_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmLn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmLn1.CLM_LN_SK)"
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNDC(ClmLn1.CLM_LN_SK, ClmLn1.NDC, Logging)"
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: "GetFkeyClctnDomainCodes(svCdMpngSrcSysCd, ClmLn1.CLM_LN_SK, 'FACETS DBO', 'CLAIM SUBTYPE', 'IDS', 'CLAIM SUBTYPE', ClmLn1.NDC_DRUG_FORM_CD, 'Logging')"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmLn1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmLn1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmLn1.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CLM_LN_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_SEQ_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLn1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.PRI_LOB_IN"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "ClmLn1.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmLn1.UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.LMT_PFX_ID"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: "ClmLn1.PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.RFRL_ID_TX"
            - name: SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_PRICE_RULE_ID"
            - name: SVC_RULE_TYP_TX
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.SVC_RULE_TYP_TX"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "ClmLn1.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.NON_PAR_SAV_AMT"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn1.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "ClmLn1.VBB_RULE_ID"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "ClmLn1.VBB_EXCD_ID"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "ClmLn1.CLM_LN_VBB_IN"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SUPLMT_DSCNT_AMT"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLn1.ITS_SRCHRG_AMT"
            - name: NDC
              type: varchar
              width: 20
              map: "ClmLn1.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "ClmLn1.NDC_DRUG_FORM_CD"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "ClmLn1.NDC_UNIT_CT"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "ClmLn1.MED_PDX_IND"
            - name: APC_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_ID"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "ClmLn1.APC_STTUS_ID"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "ClmLn1.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "ClmLn1.CVX_VCCN_CD"
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svEMRSrcSysCd
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svSBVSrcSysCd
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: svCdMpngSrcSysCd
              initValue: "0"
            - name: SrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCd
              initValue: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: sProcCd
              type: decimal
              width: 18
              precision: 4
              map: sProcCd
              initValue: "0"
            - name: ProcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProcCdSk
              initValue: "0"
            - name: SvcProvSk
              type: decimal
              width: 18
              precision: 4
              map: SvcProvSk
              initValue: "0"
            - name: ClmLnDsalwExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnDsalwExcdSk
              initValue: "0"
            - name: ClmLnFinlDispCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnFinlDispCdSk
              initValue: "0"
            - name: ClmLnEobExcdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnEobExcdSk
              initValue: "0"
            - name: ClmLnLobCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnLobCdSk
              initValue: "0"
            - name: ClmLnPosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPosCdSk
              initValue: "0"
            - name: ClmLnPreauthCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthCdSk
              initValue: "0"
            - name: ClmLnPreauthSrcCd
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPreauthSrcCd
              initValue: "0"
            - name: ClmLnPriceSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnPriceSrcCdSk
              initValue: "0"
            - name: ClmLnRflCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRflCdSk
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRvnuCdSk
              initValue: "0"
            - name: ClmLnRoomPriceMethSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomPriceMethSk
              initValue: "0"
            - name: ClmLnRoomTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnRoomTypCdSk
              initValue: "0"
            - name: ClmLnTosCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnTosCdSk
              initValue: "0"
            - name: ClmLnunitTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnunitTypCdSk
              initValue: "0"
            - name: ClmLnSvcLocTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcLocTypCdSk
              initValue: "0"
            - name: ClmLnSvcPriceRuleCdSk
              type: decimal
              width: 18
              precision: 4
              map: ClmLnSvcPriceRuleCdSk
              initValue: "0"
            - name: PriLobIn
              type: decimal
              width: 18
              precision: 4
              map: PriLobIn
              initValue: "0"
            - name: SvcEndDt
              type: decimal
              width: 18
              precision: 4
              map: SvcEndDt
              initValue: "0"
            - name: SvcStrtDt
              type: decimal
              width: 18
              precision: 4
              map: SvcStrtDt
              initValue: "0"
            - name: svVbbRuleSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbRuleSk
              initValue: "0"
            - name: svVbbExcdSk
              type: decimal
              width: 18
              precision: 4
              map: svVbbExcdSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNDCSk
              type: decimal
              width: 18
              precision: 4
              map: svNDCSk
              initValue: "0"
            - name: svNDCDrugCDFormSk
              type: decimal
              width: 0
              map: svNDCDrugCDFormSk
              initValue: "0"
        - name: Trns1.V54S1P8
          type: filter
          source: Trns1.V54S1P8.map
          filter: "ErrCount > 0"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Collector
          type: unknown
          source: CLM_LNhf_recyclehf_claim_recycle_keys
          alias: 
          origType: CCollector


