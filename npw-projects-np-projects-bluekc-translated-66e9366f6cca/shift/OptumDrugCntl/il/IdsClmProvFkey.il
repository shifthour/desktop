model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmProvFkey
      origin: datastage
      type: dataflow
      folder: "claim/provider/claim"
      runAfterSubrOnJobSuccess: enabled
      description: "IDSClaim Provider foreign keying.  "
      fullDescription: "*****************************************************************************************************************************************************************************\nCOPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \nCOPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \nCOPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nDESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.\n\n\nMODIFICATIONS:\n\nMODIFICATIONS:\nDeveloper              Date                 Project/Altiris #      Change Description                                                                                                     Development Project      Code Reviewer          Date Reviewed      \n------------------            --------------------     ------------------------      -----------------------------------------------------------------------                                                           --------------------------------       -------------------------------   ----------------------------       \nTom Harrocks        04/12/2004                                   Originally Programmed\n BJ Luce                08/2004                                        Phase 2 Added stage variable to transform SRC_SYS_CD to SrcSysCdProv \n                                                                                    using TRANSFORM SrcSysCdProvFkey. \n                                                                                    This SrcSysCdProv will be used as the source system in the provider foreign keying. \n                                                                                    Change input format to match pkey format\n Brent Leland         08/26/2004                                   Added default rows.\nSteph Goddard      09/02/2004                                   Changed SK on default rows from 'NA' and 'UNK' to 1 and 0\nBrent Leland          11/01/2004                                   Corrected the error recycle common record format.\nSteph Goddard      02/14/2006                                   Changed for sequencer\nSAndrew               12/05/2007      IAD Qrt Release    TTR204 - Changed stage variable ProvSK rule if provider role                                         devlIDS                          Steph Goddard          01/10/2008\n                                                                                     type is for PCP, set the source system code value used in GetFkeyProv to be FACETS.                                                                                                                                         \n                                                                                     All PCP Providers are FACETS providers.\x09\nSAndrew                  2008-10-10  3784 PBM Vnedr    Changed rule behind called routine SrcSysCdProvFkey .                                                  devlIDSnew                    Steph Goddard           10/27/2008\n                                                                                    Changed rule for stage variable ProvSk\nSAndrew               2009-02-24      494 Labor Accnt      Changed stage variable ProvSK rule from                                                                       devlIDS                           Steph Goddard           03/30/2009\n                                                                             If (Key.CLM_PROV_ROLE_TYP_CD=\"PCP\") or (Key.CLM_PROV_ROLE_TYP_CD=\"PRSCRB\" AND Key.SRC_SYS_CD='ESI') then GetFkeyProv(\"FACETS\",Key.CLM_PROV_SK,Key.PROV_ID, Logging) \n                                                                               else  GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)\n                                                                            TO If ( Key.CLM_PROV_ROLE_TYP_CD = \"PCP\" ) or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'ESI')  OR\n                                                                            ( Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX' )) then  GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging)  \n                                                                            else  GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)\nKalyan Neelam     2009-10-07       4098                    Changed stage variable ClmProvRoleSk rule                                                                                                devlIDS Steph Goddard          10/15/2009\n                                                                                  If Key.SRC_SYS_CD = 'MCSOURCE' Then \n                                                                                  GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) \n                                                                                  Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)\nKalyan Neelam     2010-01-04       4110                    Added new option 'MCAID' in stage variable provsk also added new logic for MCAID svc_prov_sk  IntegrateCurDevl  Steph Goddard      01/11/2010\n\nKalyan Neelam     2010-05-24       TTR729              Added new logic for PCS and CAREMARK Prov Sk                                                        IntegrateCurDevl                  Steph Goddard           05/28/2010\nKalyan Neelam     2010-12-22       4616                   Added new option 'MEDTRAK' in stage variable provsk                                                  IntegrateNewDevl                Steph Goddard           12/23/2010\n\nKarthik Chintalapani 2012-01-22  4784              Added SRC_SYS_CD = 'BCBSSC' for stage variable ProvSk                                                IntegrateCurDevl                   Brent Leland               02-08-2012\nKalyan Neelam       2013-11-12    5056 FEP Claims      Added new option 'BCA' in stage variable provsk                                                        IntegrateNewDevl                Bhoomi Dasari             11/30/2013\n                                                                                 \nKarthik Chintalapani 2016-05-20  5212             Added  'BCA'  for SRC_SYS_CD = 'BCBSA' for stage variable ClmProvRoleSk                      IntegrateDev2                        Kalyan Neelam            2016-05-23\n                                                                         to retrieve the correct SK's\n\nMadhavan B\x092017-06-20   5788 - BHI Updates    Added new column                                                                                                  IntegrateDev2                        Kalyan Neelam            2017-07-12\n                                                                                        SVC_FCLTY_LOC_NTNL_PROV_ID\n\nSudhir Bomshetty     2017-10-19   5781          In Stage Variable ProvSk, added If  Key.SRC_SYS_CD = 'BCA' Then If                                     IntegrateDev2                    Kalyan Neelam            2017-10-23\n                                                                        (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) = 0) \n\x09     \x09\x09\x09        Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)\n\x09     \x09                                         Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging)\nManasa Andru    2018-03-04   TFS21154     Modified ProvSk stage variable to not log errors when looking Fkey in If logic                        IntegrateDev2                          Kalyan Neelam            2018-03-12\n\nKaushik Kapoor  2018-03-19    5828             Adding SAVRX and LDI source system code in PROV_SK                                                     IntegrateDev2                         Jaideep Mankala        03/21/2018\n\nRavi Abburi        2018-01-29    5781 - HEDIS            Added the NTNL_PROV_ID to lookup for the NTNL_PROV_SK                               IntegrateDev1                          Kalyan Neelam            2018-04-25\n                                                                                        by using the GetFkeyNtnlProvId\n\nKaushik Kapoor     2018-09-20    5828                                   Adding CVS source system code in PROV_SK                                           IntegrateDev2                             Kalyan Neelam            2018-10-01\n\nSrinidhi Kambham  2019-10-11   6131- PBM Replacement    Added \"OPTUMRX\" Value to 'ProvSk' stage variable  field  in Transformer Derivation.        IntegrateDev1  Kalyan Neelam            2019-11-20\n\nReddy Sanam       2020-10-09                                             ClmProvRoleSk stage variable derivation is mapped to \n                                                                                             FACETS when src sys cd is 'LUMERIS'\n\nGoutham K           2020-11-12      US-283560                    Added MEDIMPACT to StageVar ProvSK and ClmProvRoleSk                        IntegrateDev2                           Kalyan Neelam           2020-11-12\n\nSravya Sree Y     2021-02-03      RA                         Updated Stage Variable \"ClmProvRoleSk\" and \"PROV_SK\" to Include \"DentaQuest\"        IntegrateDev2                Kalyan Neelam          2021-02-03\nMrudula Kodali    2021-15-03      311337                         Updated Stage Variable svNtnlProvSk for Livongo                                                               IntegrateDev2                Jaideep Mankala      03/18/2021\n                                                                                   and Stage Variable \"ProvSk\" to Include \"SOLUTRAN\"\nMrudula Kodali                05/03/2021  US-373652        Updated the stage variables ClmProvRoleSk\x09\x09\x09\x09\x09IntegrateDev2     Jaideep Mankala     05/10/2021\n                                                                                       Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR\nAbhishek Pulluri     2021-06 -03     RA                    Removed LIVONGO If condition in Stage Varibale- svNtnlProvSk                 IntegrateDev2                                          Kalyan Nelam            20201-06-03                    \n\nManisha Gandra    2021-11-29               US 459610        Added NATIONS to StageVar ProvSK                                                  IntegrateDev2                    Jeyaprasanna                          2022-01-05     \n \nSham Shankaranarayana 2023-12-21    US 599810       Updated existing StageVar ProvSk in Transformer ForeignKey                 IntegrateDev1                 Jeyaprasanna                          2023-12-21"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: Logging
          type: string
          description: Logging
          defaultType: expr
          defaultValue: X
        - name: Source
          type: string
          description: "Source system"
        - name: InFile
          type: string
          description: InFile
          defaultType: expr
          defaultValue: "ClmProv.dat"
      steps:
        - name: ClmProvCrf
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
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
        - name: CLM_PROV
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_PROV.#Source#.dat"
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
          source: "ForeignKey.V0S20P4"
            # Hashed File has no directory specified
        - name: hf_claim_recycle_keys
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_claim_recycle_keys"
          tableAction: append
          source: "ForeignKey.V0S20P7"
            # Hashed File has no directory specified
        - name: ForeignKey.V0S20P3.map
          type: map
          source: ClmProvCrf
          alias: Key
          stageVars: 
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull((Key.SRC_SYS_CD)) or Len(Trim((Key.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((Key.SRC_SYS_CD)) = 'PCS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'CAREMARK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ARGUS' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ESI' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'OPTUMRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'WELLDYNERX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCSOURCE' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCAID' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MEDTRAK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCBSSC' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCA' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((Key.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' Else  If Trim((Key.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  if Trim((Key.SRC_SYS_CD)) = 'SAVRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'LDI' then 'NABP' Else  If Trim((Key.SRC_SYS_CD)) = 'EYEMED' Then 'EYEMED' else  if Trim((Key.SRC_SYS_CD)) = 'CVS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'MEDIMPACT' then 'NABP' else 'FACETS'"
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'BCBSA' Then GetFkeyCodes('BCA', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) else  If Key.SRC_SYS_CD = 'LUMERIS' Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If (Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' And (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' Or Key.CLM_PROV_ROLE_TYP_CD = 'PRSCRB')) Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' AND (Key.CLM_PROV_ROLE_TYP_CD = 'BILL' OR Key.CLM_PROV_ROLE_TYP_CD = 'SVC') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.PROV_ID) = 'UNK' Then 0 Else  If Trim(Key.PROV_ID) = 'NA' Then 1 Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PCP\") Or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"OPTUMRX\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"MEDIMPACT\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"ESI\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCSOURCE') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'CAREMARK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MEDTRAK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCBSSC') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCA')) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (((Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'CAREMARK')) And GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then  If (GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCA' Then  If (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'SOLUTRAN' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD[1, 6] = 'NATION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DOMINION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'MARC' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.CLM_PROV_SK)"
              initValue: "0"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_PROV_SK) Else 0"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNtnlProvId('CMS', Key.CLM_PROV_SK, Key.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_PROV_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "Key.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Key.CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              map: "Key.PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
              map: "Key.TAX_ID"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.SVC_FCLTY_LOC_NTNL_PROV_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.NTNL_PROV_ID"
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmProvRoleSk
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmSk
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: ProvSk
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
              map: " If (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' or Key.SRC_SYS_CD = 'FACETS' or Key.SRC_SYS_CD = 'LVNGHLTH') Then  If svNtnlProvSK = 0 Then 1 Else svNtnlProvSK Else  If Key.NTNL_PROV_ID = 'NA' Then 1 Else  If IsNull(Key.NTNL_PROV_ID) = @TRUE then 0 Else svNtnlProvSK"
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdProv
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: ClmProvRoleSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSk
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
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey.V0S20P3
          type: filter
          source: ForeignKey.V0S20P3.map
          filter: "ErrCount = 0 Or PassThru = 'Y'"
          schema: 
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_PROV_SK
        - name: ForeignKey.V0S20P4.map
          type: map
          source: ClmProvCrf
          alias: Key
          stageVars: 
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull((Key.SRC_SYS_CD)) or Len(Trim((Key.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((Key.SRC_SYS_CD)) = 'PCS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'CAREMARK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ARGUS' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ESI' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'OPTUMRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'WELLDYNERX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCSOURCE' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCAID' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MEDTRAK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCBSSC' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCA' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((Key.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' Else  If Trim((Key.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  if Trim((Key.SRC_SYS_CD)) = 'SAVRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'LDI' then 'NABP' Else  If Trim((Key.SRC_SYS_CD)) = 'EYEMED' Then 'EYEMED' else  if Trim((Key.SRC_SYS_CD)) = 'CVS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'MEDIMPACT' then 'NABP' else 'FACETS'"
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'BCBSA' Then GetFkeyCodes('BCA', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) else  If Key.SRC_SYS_CD = 'LUMERIS' Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If (Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' And (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' Or Key.CLM_PROV_ROLE_TYP_CD = 'PRSCRB')) Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' AND (Key.CLM_PROV_ROLE_TYP_CD = 'BILL' OR Key.CLM_PROV_ROLE_TYP_CD = 'SVC') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.PROV_ID) = 'UNK' Then 0 Else  If Trim(Key.PROV_ID) = 'NA' Then 1 Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PCP\") Or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"OPTUMRX\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"MEDIMPACT\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"ESI\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCSOURCE') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'CAREMARK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MEDTRAK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCBSSC') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCA')) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (((Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'CAREMARK')) And GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then  If (GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCA' Then  If (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'SOLUTRAN' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD[1, 6] = 'NATION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DOMINION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'MARC' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.CLM_PROV_SK)"
              initValue: "0"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_PROV_SK) Else 0"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNtnlProvId('CMS', Key.CLM_PROV_SK, Key.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: JobExecRecSK
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT + 1"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_PROV_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "Key.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Key.CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
              map: "Key.TAX_ID"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.SVC_FCLTY_LOC_NTNL_PROV_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.NTNL_PROV_ID"
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdProv
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: ClmProvRoleSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSk
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
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey.V0S20P4
          type: filter
          source: ForeignKey.V0S20P4.map
          filter: "ErrCount > 0"
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
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - JOB_EXCTN_RCRD_ERR_SK
        - name: ForeignKey.V0S20P5.map
          type: map
          source: ClmProvCrf
          alias: Key
          stageVars: 
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull((Key.SRC_SYS_CD)) or Len(Trim((Key.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((Key.SRC_SYS_CD)) = 'PCS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'CAREMARK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ARGUS' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ESI' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'OPTUMRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'WELLDYNERX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCSOURCE' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCAID' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MEDTRAK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCBSSC' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCA' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((Key.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' Else  If Trim((Key.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  if Trim((Key.SRC_SYS_CD)) = 'SAVRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'LDI' then 'NABP' Else  If Trim((Key.SRC_SYS_CD)) = 'EYEMED' Then 'EYEMED' else  if Trim((Key.SRC_SYS_CD)) = 'CVS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'MEDIMPACT' then 'NABP' else 'FACETS'"
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'BCBSA' Then GetFkeyCodes('BCA', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) else  If Key.SRC_SYS_CD = 'LUMERIS' Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If (Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' And (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' Or Key.CLM_PROV_ROLE_TYP_CD = 'PRSCRB')) Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' AND (Key.CLM_PROV_ROLE_TYP_CD = 'BILL' OR Key.CLM_PROV_ROLE_TYP_CD = 'SVC') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.PROV_ID) = 'UNK' Then 0 Else  If Trim(Key.PROV_ID) = 'NA' Then 1 Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PCP\") Or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"OPTUMRX\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"MEDIMPACT\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"ESI\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCSOURCE') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'CAREMARK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MEDTRAK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCBSSC') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCA')) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (((Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'CAREMARK')) And GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then  If (GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCA' Then  If (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'SOLUTRAN' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD[1, 6] = 'NATION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DOMINION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'MARC' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.CLM_PROV_SK)"
              initValue: "0"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_PROV_SK) Else 0"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNtnlProvId('CMS', Key.CLM_PROV_SK, Key.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "'UNK'"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Key.CLM_PROV_ROLE_TYP_CD"
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
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: TAX_ID
              type: char
              width: 9
              map: "'UNK'"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.NTNL_PROV_ID"
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdProv
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: ClmProvRoleSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSk
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
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey.V0S20P5
          type: filter
          source: ForeignKey.V0S20P5.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
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
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TAX_ID
              type: char
              width: 9
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_PROV_SK
        - name: ForeignKey.V0S20P6.map
          type: map
          source: ClmProvCrf
          alias: Key
          stageVars: 
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull((Key.SRC_SYS_CD)) or Len(Trim((Key.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((Key.SRC_SYS_CD)) = 'PCS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'CAREMARK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ARGUS' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ESI' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'OPTUMRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'WELLDYNERX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCSOURCE' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCAID' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MEDTRAK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCBSSC' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCA' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((Key.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' Else  If Trim((Key.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  if Trim((Key.SRC_SYS_CD)) = 'SAVRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'LDI' then 'NABP' Else  If Trim((Key.SRC_SYS_CD)) = 'EYEMED' Then 'EYEMED' else  if Trim((Key.SRC_SYS_CD)) = 'CVS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'MEDIMPACT' then 'NABP' else 'FACETS'"
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'BCBSA' Then GetFkeyCodes('BCA', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) else  If Key.SRC_SYS_CD = 'LUMERIS' Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If (Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' And (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' Or Key.CLM_PROV_ROLE_TYP_CD = 'PRSCRB')) Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' AND (Key.CLM_PROV_ROLE_TYP_CD = 'BILL' OR Key.CLM_PROV_ROLE_TYP_CD = 'SVC') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.PROV_ID) = 'UNK' Then 0 Else  If Trim(Key.PROV_ID) = 'NA' Then 1 Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PCP\") Or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"OPTUMRX\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"MEDIMPACT\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"ESI\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCSOURCE') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'CAREMARK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MEDTRAK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCBSSC') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCA')) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (((Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'CAREMARK')) And GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then  If (GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCA' Then  If (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'SOLUTRAN' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD[1, 6] = 'NATION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DOMINION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'MARC' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.CLM_PROV_SK)"
              initValue: "0"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_PROV_SK) Else 0"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNtnlProvId('CMS', Key.CLM_PROV_SK, Key.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "'NA'"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Key.CLM_PROV_ROLE_TYP_CD"
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
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: TAX_ID
              type: char
              width: 9
              map: "'NA'"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.NTNL_PROV_ID"
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdProv
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: ClmProvRoleSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSk
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
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey.V0S20P6
          type: filter
          source: ForeignKey.V0S20P6.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
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
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TAX_ID
              type: char
              width: 9
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_PROV_SK
        - name: ForeignKey.V0S20P7.map
          type: map
          source: ClmProvCrf
          alias: Key
          stageVars: 
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull((Key.SRC_SYS_CD)) or Len(Trim((Key.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((Key.SRC_SYS_CD)) = 'PCS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'CAREMARK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ARGUS' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'ESI' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'OPTUMRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'WELLDYNERX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCSOURCE' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MCAID' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'MEDTRAK' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCBSSC' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'BCA' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((Key.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' Else  If Trim((Key.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  if Trim((Key.SRC_SYS_CD)) = 'SAVRX' then 'NABP' else  if Trim((Key.SRC_SYS_CD)) = 'LDI' then 'NABP' Else  If Trim((Key.SRC_SYS_CD)) = 'EYEMED' Then 'EYEMED' else  if Trim((Key.SRC_SYS_CD)) = 'CVS' then 'NABP' else  If Trim((Key.SRC_SYS_CD)) = 'MEDIMPACT' then 'NABP' else 'FACETS'"
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: " If Key.SRC_SYS_CD = 'BCBSA' Then GetFkeyCodes('BCA', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) else  If Key.SRC_SYS_CD = 'LUMERIS' Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If (Key.SRC_SYS_CD = 'OPTUMRX' Or Key.SRC_SYS_CD = 'MEDIMPACT' And (Key.CLM_PROV_ROLE_TYP_CD = 'PCP' Or Key.CLM_PROV_ROLE_TYP_CD = 'PRSCRB')) Then GetFkeyCodes('FACETS', Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' AND (Key.CLM_PROV_ROLE_TYP_CD = 'BILL' OR Key.CLM_PROV_ROLE_TYP_CD = 'SVC') Then GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging) Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, \"CLAIM PROVIDER ROLE TYPE\", Key.CLM_PROV_ROLE_TYP_CD, Logging)"
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.CLM_ID, Logging)"
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(Key.PROV_ID) = 'UNK' Then 0 Else  If Trim(Key.PROV_ID) = 'NA' Then 1 Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PCP\") Or ((Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"OPTUMRX\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"MEDIMPACT\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = \"ESI\") Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'WELLDYNERX') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCSOURCE') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'CAREMARK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'MEDTRAK') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCBSSC') Or (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND Key.SRC_SYS_CD = 'BCA')) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.CLM_PROV_ROLE_TYP_CD = \"PRSCRB\" AND (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') and GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv(\"FACETS\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (((Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'MCAID') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'PCS') Or (Key.CLM_PROV_ROLE_TYP_CD = \"SVC\" AND Key.SRC_SYS_CD = 'CAREMARK')) And GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") <> 0) Then GetFkeyProv('FACETS', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If (Key.SRC_SYS_CD = 'BCBSA' Or Key.SRC_SYS_CD = 'SAVRX' Or Key.SRC_SYS_CD = 'LDI' Or Key.SRC_SYS_CD = 'CVS') Then  If (GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(\"NABP\", Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'BCA' Then  If (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, \"X\") = 0) Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DENTAQUEST' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'SOLUTRAN' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD[1, 6] = 'NATION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'DOMINION' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else  If Key.SRC_SYS_CD = 'MARC' Then GetFkeyProv(Key.SRC_SYS_CD, Key.CLM_PROV_SK, Key.PROV_ID, Logging) Else GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.CLM_PROV_SK)"
              initValue: "0"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_PROV_SK) Else 0"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyNtnlProvId('CMS', Key.CLM_PROV_SK, Key.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_PROV_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Key.CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
              map: "Key.TAX_ID"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.SVC_FCLTY_LOC_NTNL_PROV_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.NTNL_PROV_ID"
            - name: SrcSysCdProv
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdProv
              initValue: "0"
            - name: ClmProvRoleSk
              type: decimal
              width: 18
              precision: 4
              map: ClmProvRoleSk
              initValue: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "0"
            - name: ProvSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSk
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
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 18
              precision: 4
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey.V0S20P7
          type: filter
          source: ForeignKey.V0S20P7.map
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
          source: CLM_PROVhf_recyclehf_claim_recycle_keys
          alias: 
          origType: CCollector


