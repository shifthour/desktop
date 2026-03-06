model:
  config:
    origin: datastage
  jobs:
    - name: IdsProvDEAFkey
      origin: datastage
      type: dataflow
      folder: "provider/DEA"
      runAfterSubrOnJobSuccess: enabled
      description: "IDS Provider DEA  Data Foreign Keying.  "
      fullDescription: "COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:     IdsProvDEAFkey\n\nDESCRIPTION:     Takes the sorted file and applies the Foreign key.  \n      \n\nINPUTS:  File from IdsProvDEAPkey\n\x09\n  \nHASH FILES:     hf_recycle - contains records that had errors on them for recycle.  Recycle is not used in this program - file is completely updated from Facets each time.\n\n\nTRANSFORMS:  GetFkeyCodes \n                                 \n                           Unknown and Not Applicable rows are created - all rows are sent through collector to combine into output file\n                           \n\nPROCESSING:  This job would be run whenever there is an update to the NDC table/file.  Until now, we got the NDC (National Drug Code) file from CRMS, but now are looking for a \n  different source.  When this source is located and we get new files, these jobs will probably need to be re-written.\n\nOUTPUTS:  File for PROV_DEA table\n\nMODIFICATIONS:\n            BJ Luce        08/2004         -   Originally Programmed\n            Brent Leland 09/20/2004    -  Added link partioner.\n            Steph Goddard  06/01/2005  changed for sequencer\n\nDeveloper                  Date                 Project/Altiris #      Change Description                                                            Development Project      Code Reviewer          Date Reviewed       \n------------------              --------------------     ------------------------      -----------------------------------------------------------------------                       --------------------------------       -------------------------------   ----------------------------       \nKalyan Neelam         2009-10-28            TTR-525         Added new fields CLM_TRANS_ADD_IN, ADDR_LN2,             devlIDS                      Steph Goddard        10/29/2009\n                                                                                      NTNL_PROV_ID, NTNL_PROV_ID_SPEC_DESC                                                           \n\nManasa Andru         2014-03-14           TFS - 4011      Updated the fields EFF_DT_SK and TERM_DT_SK               IntegrateCurDevl              Kalyan Neelam            2014-03-20\n                                                                                   as per the mapping rules in the ForeignKey1 transformer.\nKalyan Neelam        2015-10-30      5403/TFS1048     Added 2 new columns on end -                                                 IntegrateDev1              Bhoomi Dasari           11/9/2015\n                                                                                 NTNL_PROV_ID_PROV_TYP_DESC and NTNL_PROV_ID_PROV_TYP_DESC\n\nRavi Abburi             2018-01-29    5781 - HEDIS            Added the NTNL_PROV_SK                                               IntegrateDev2                   Kalyan Neelam            2018-04-19"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: Logging
          type: string
          description: Logging
          defaultType: expr
          defaultValue: N
      steps:
        - name: DEA_PKey
          type: fileReader
          urls: 
            - "#$FilePath#/key/IdsProvDEAExtr.ProvDEA.uniq"
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
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
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
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 35
            - name: ADDR_LN_1
              type: varchar
              width: 40
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
            - name: POSTAL_CD
              type: char
              width: 11
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
          keys:
            - PROV_DEA_SK
        - name: ProvDea
          type: fileWriter
          urls: 
            - "#$FilePath#/load/PROV_DEA.dat"
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
          source: "ForeignKey1.V0S20P4"
            # Hashed File has no directory specified
        - name: ForeignKey1.V0S20P3.map
          type: map
          source: DEA_PKey
          alias: CRF1
          stageVars: 
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct(\"VCAC\", CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If svCmnPrctFacetsSK = 0 then svCmnPrctVcacSK else svCmnPrctFacetsSK"
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes('IDS', CRF1.PROV_DEA_SK, 'STATE', CRF1.PROV_DEA_ST_CD, Logging)"
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.EFF_DT_SK, Logging)"
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.TERM_DT_SK, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "CRF1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(CRF1.PROV_DEA_SK)"
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', CRF1.PROV_DEA_SK, CRF1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "CRF1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "CRF1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "CRF1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "CRF1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "CRF1.PRI_KEY_STRING"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.PROV_DEA_SK"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.DEA_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
              map: "CRF1.CMN_PRCT_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.CLM_TRANS_ADD_IN"
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If (svEffDtSK = 'UNK' or svEffDtSK = 'NA') then '1753-01-01' else svEffDtSK"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If (svTermDtSK = 'UNK' or svTermDtSK = 'NA') then '2199-12-31' else svTermDtSK"
            - name: PROV_NM
              type: varchar
              width: 35
              map: "CRF1.PROV_NM"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
              map: "CRF1.ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "CRF1.ADDR_LN_2"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "CRF1.CITY_NM"
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
              map: "CRF1.PROV_DEA_ST_CD"
            - name: POSTAL_CD
              type: char
              width: 11
              map: "CRF1.POSTAL_CD"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.NTNL_PROV_ID"
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_SPEC_DESC"
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_PROV_TYP_DESC"
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_PROV_CLS_DESC"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: CmnPrctSk
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: svStCdSK
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: " If CRF1.NTNL_PROV_ID = 'NA' Then 1 Else  If IsNull(CRF1.NTNL_PROV_ID) then 0 Else svNtnlProvSK"
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: svCmnPrctFacetsSK
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: svCmnPrctVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: svStCdSK
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: svEffDtSK
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: svTermDtSK
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P3
          type: filter
          source: ForeignKey1.V0S20P3.map
          filter: "ErrCount = 0 Or PassThru = 'Y'"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 35
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: POSTAL_CD
              type: char
              width: 11
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_SK
              type: int
              width: 4
          keys:
            - PROV_DEA_SK
        - name: ForeignKey1.V0S20P4.map
          type: map
          source: DEA_PKey
          alias: CRF1
          stageVars: 
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct(\"VCAC\", CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If svCmnPrctFacetsSK = 0 then svCmnPrctVcacSK else svCmnPrctFacetsSK"
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes('IDS', CRF1.PROV_DEA_SK, 'STATE', CRF1.PROV_DEA_ST_CD, Logging)"
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.EFF_DT_SK, Logging)"
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.TERM_DT_SK, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "CRF1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(CRF1.PROV_DEA_SK)"
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', CRF1.PROV_DEA_SK, CRF1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(CRF1.PROV_DEA_SK)"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "CRF1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "CRF1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "CRF1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "CRF1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "CRF1.PRI_KEY_STRING"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.PROV_DEA_SK"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.DEA_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
              map: "CRF1.CMN_PRCT_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.CLM_TRANS_ADD_IN"
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "CRF1.EFF_DT_SK"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "CRF1.TERM_DT_SK"
            - name: PROV_NM
              type: varchar
              width: 35
              map: "CRF1.PROV_NM"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
              map: "CRF1.ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "CRF1.ADDR_LN_2"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "CRF1.CITY_NM"
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
              map: "CRF1.PROV_DEA_ST_CD"
            - name: POSTAL_CD
              type: char
              width: 11
              map: "CRF1.POSTAL_CD"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.NTNL_PROV_ID"
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_SPEC_DESC"
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_PROV_TYP_DESC"
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
              map: "CRF1.NTNL_PROV_ID_PROV_CLS_DESC"
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: svCmnPrctFacetsSK
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: svCmnPrctVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: svStCdSK
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: svEffDtSK
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: svTermDtSK
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P4
          type: filter
          source: ForeignKey1.V0S20P4.map
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
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
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
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 35
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
            - name: POSTAL_CD
              type: char
              width: 11
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
          keys:
            - JOB_EXCTN_RCRD_ERR_SK
        - name: ForeignKey1.V0S20P5.map
          type: map
          source: DEA_PKey
          alias: CRF1
          stageVars: 
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct(\"VCAC\", CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If svCmnPrctFacetsSK = 0 then svCmnPrctVcacSK else svCmnPrctFacetsSK"
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes('IDS', CRF1.PROV_DEA_SK, 'STATE', CRF1.PROV_DEA_ST_CD, Logging)"
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.EFF_DT_SK, Logging)"
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.TERM_DT_SK, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "CRF1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(CRF1.PROV_DEA_SK)"
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', CRF1.PROV_DEA_SK, CRF1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "CRF1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "CRF1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "CRF1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "CRF1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "CRF1.PRI_KEY_STRING"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
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
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
              map: "CRF1.CMN_PRCT_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'2199-12-31'"
            - name: PROV_NM
              type: varchar
              width: 35
              map: "'UNK'"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
              map: "'UNK'"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "'UNK'"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "'UNK'"
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
              map: "CRF1.PROV_DEA_ST_CD"
            - name: POSTAL_CD
              type: char
              width: 11
              map: "'UNK'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: "0"
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: svCmnPrctFacetsSK
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: svCmnPrctVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: svStCdSK
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: svEffDtSK
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: svTermDtSK
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P5
          type: filter
          source: ForeignKey1.V0S20P5.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 35
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: POSTAL_CD
              type: char
              width: 11
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_SK
              type: int
              width: 4
          keys:
            - PROV_DEA_SK
        - name: ForeignKey1.V0S20P6.map
          type: map
          source: DEA_PKey
          alias: CRF1
          stageVars: 
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct(\"VCAC\", CRF1.PROV_DEA_SK, CRF1.CMN_PRCT_CD, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If svCmnPrctFacetsSK = 0 then svCmnPrctVcacSK else svCmnPrctFacetsSK"
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes('IDS', CRF1.PROV_DEA_SK, 'STATE', CRF1.PROV_DEA_ST_CD, Logging)"
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.EFF_DT_SK, Logging)"
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", CRF1.PROV_DEA_SK, CRF1.TERM_DT_SK, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "CRF1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(CRF1.PROV_DEA_SK)"
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', CRF1.PROV_DEA_SK, CRF1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "CRF1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "CRF1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "CRF1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "CRF1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "CRF1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "CRF1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "CRF1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "CRF1.PRI_KEY_STRING"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
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
            - name: CMN_PRCT_CD
              type: char
              width: 5
              nullable: N
              map: "CRF1.CMN_PRCT_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'2199-12-31'"
            - name: PROV_NM
              type: varchar
              width: 35
              map: "'NA'"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
              map: "'NA'"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "'NA'"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "'NA'"
            - name: PROV_DEA_ST_CD
              type: char
              width: 2
              nullable: N
              map: "CRF1.PROV_DEA_ST_CD"
            - name: POSTAL_CD
              type: char
              width: 11
              map: "'NA'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: "1"
            - name: svCmnPrctFacetsSK
              type: decimal
              width: 0
              map: svCmnPrctFacetsSK
              initValue: "-1"
            - name: svCmnPrctVcacSK
              type: decimal
              width: 18
              precision: 4
              map: svCmnPrctVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: svStCdSK
              type: decimal
              width: 18
              precision: 4
              map: svStCdSK
              initValue: "-1"
            - name: svEffDtSK
              type: decimal
              width: 18
              precision: 4
              map: svEffDtSK
              initValue: "-1"
            - name: svTermDtSK
              type: decimal
              width: 18
              precision: 4
              map: svTermDtSK
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P6
          type: filter
          source: ForeignKey1.V0S20P6.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 35
            - name: ADDR_LN_1
              type: varchar
              width: 40
              nullable: N
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: PROV_DEA_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: POSTAL_CD
              type: char
              width: 11
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID_SPEC_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_TYP_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_ID_PROV_CLS_DESC
              type: varchar
              width: 255
            - name: NTNL_PROV_SK
              type: int
              width: 4
          keys:
            - PROV_DEA_SK
        - name: Collector
          type: unknown
          source: ProvDeahf_recycle
          alias: 
          origType: CCollector


