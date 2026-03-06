model:
  config:
    origin: datastage
  jobs:
    - name: IdsProvLocFkey
      origin: datastage
      type: dataflow
      folder: "provider/Location"
      runAfterSubrOnJobSuccess: enabled
      description: "Foreign key translation for provider Location"
      fullDescription: "COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY\n\nDESCRIPTION:     IDS ProvLocation foreign key job.\n\n\nMODIFICATIONS:\nDeveloper                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       \n--------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       \nGiri Mallavaram        10/16/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                 Kalyan Neelam          2019-11-20"
      parameters:
        - name: InFile
          type: string
          description: InFile
          defaultType: expr
          defaultValue: "IdsProvLocExtr.ProvLoc.uniq\n\n"
        - name: Logging
          type: string
          description: "Log Error"
          defaultType: expr
          defaultValue: Y
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
      steps:
        - name: IdsProvLocExtr
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
            - name: PROV_LOC_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_TYP_CD
              type: char
              width: 12
              nullable: N
            - name: PROV_ADDR_EFF_DT
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
              nullable: N
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
            - name: REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
          keys:
            - PROV_LOC_SK
        - name: ProvLoc
          type: fileWriter
          urls: 
            - "#$FilePath#/load/PROV_LOC.dat"
          source: Collector
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_prov_addr_lkup
          type: dbReader
          originalDb: hashedFile
          table: "/hf_prov_addr"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_EFF_DT
              type: char
              width: 10
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - PROV_ADDR_ID
            - PROV_ADDR_TYP_CD
            - PROV_ADDR_EFF_DT
            # Hashed File has no directory specified
        - name: hf_recycle
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "ForeignKey.V0S41P4"
            # Hashed File has no directory specified
        - name: ForeignKey.lookup
          type: lookup
          stageVars:
            - name: svEffDt
              type: decimal
              width: 0
              map: "GetFkeyDate(\"IDS\", Key.PROV_LOC_SK, Key.PROV_ADDR_EFF_DT, Logging)"
              initValue: "0"
            - name: svSrcSysCd
              type: decimal
              width: 0
              map: "GetFkeyCodes(\"IDS\", Key.PROV_LOC_SK, 'SOURCE SYSTEM', Key.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: svProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(Key.SRC_SYS_CD, Key.PROV_LOC_SK, Key.PROV_ID, Logging)"
              initValue: "-1"
            - name: svAddrTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(Key.SRC_SYS_CD, Key.PROV_LOC_SK, 'PROVIDER ADDRESS TYPE', Key.PROV_ADDR_TYP_CD, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "\"Y\""
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(Key.PROV_LOC_SK)"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: IdsProvLocExtr
              alias: Key
              joinFilter: ""
              joinExpression: []
            - name: hf_prov_addr_lkup
              alias: lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "lkup.SRC_SYS_CD"
                toExpr: "Key.SRC_SYS_CD"
                caseInSensitive: true
              - fromExpr: "lkup.PROV_ADDR_ID"
                toExpr: "Key.PROV_ADDR_ID"
                caseInSensitive: true
              - fromExpr: "lkup.PROV_ADDR_TYP_CD"
                toExpr: "Key.PROV_ADDR_TYP_CD"
                caseInSensitive: true
              - fromExpr: "lkup.PROV_ADDR_EFF_DT"
                toExpr: "Key.PROV_ADDR_EFF_DT"
                caseInSensitive: true
          joinCondition: 
            - expression: "lkup.SRC_SYS_CD = Key.SRC_SYS_CD"
              caseInSensitive: true
            - expression: "lkup.PROV_ADDR_ID = Key.PROV_ADDR_ID"
              caseInSensitive: true
            - expression: "lkup.PROV_ADDR_TYP_CD = Key.PROV_ADDR_TYP_CD"
              caseInSensitive: true
            - expression: "lkup.PROV_ADDR_EFF_DT = Key.PROV_ADDR_EFF_DT"
              caseInSensitive: true
          schema: 
            - name: Fkey_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "Key.PROV_LOC_SK"
            - name: Fkey_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: svSrcSysCd
            - name: Fkey_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ID"
            - name: Fkey_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ADDR_ID"
            - name: Fkey_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: svAddrTypCdSk
            - name: Fkey_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Key.PROV_ADDR_EFF_DT"
            - name: Fkey_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: Fkey_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: Fkey_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: " If IsNull(lkup.PROV_ADDR_SK) = @TRUE then 0 else lkup.PROV_ADDR_SK"
            - name: Fkey_PROV_SK
              type: int
              width: 4
              nullable: N
              map: svProvSk
            - name: Fkey_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PRI_ADDR_IN"
            - name: Fkey_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Key.REMIT_ADDR_IN"
            - name: Recycle_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(Key.PROV_LOC_SK)"
            - name: Recycle_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: Recycle_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: Recycle_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: Recycle_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: Recycle_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: Recycle_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT + 1"
            - name: Recycle_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: Recycle_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: Recycle_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "Key.PROV_LOC_SK"
            - name: Recycle_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: Recycle_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ID"
            - name: Recycle_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.PROV_ADDR_ID"
            - name: Recycle_PROV_ADDR_TYP_CD
              type: char
              width: 12
              nullable: N
              map: "Key.PROV_ADDR_TYP_CD"
            - name: Recycle_PROV_ADDR_EFF_DT
              type: char
              width: 10
              nullable: N
              map: "Key.PROV_ADDR_EFF_DT"
            - name: Recycle_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: Recycle_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: Recycle_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.PROV_ADDR_SK"
            - name: Recycle_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Key.PROV_SK"
            - name: Recycle_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PRI_ADDR_IN"
            - name: Recycle_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Key.REMIT_ADDR_IN"
            - name: DefaultNA_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
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
            - name: DefaultNA_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultUNK_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
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
            - name: DefaultUNK_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultUNK_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: filter_Fkey
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount = 0 Or PassThru = 'Y') then 1 else 0"
            - name: filter_Recycle
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount > 0) then 1 else 0"
            - name: filter_DefaultNA
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
            - name: filter_DefaultUNK
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
        - name: ForeignKey.V0S41P1.filter
          type: filter
          source: ForeignKey.lookup
          filter: "filter_Fkey = 1"
          schema: 
            - name: Fkey_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: Fkey_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: Fkey_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: Fkey_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: Fkey_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
            - name: Fkey_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
        - name: ForeignKey.V0S41P4.filter
          type: filter
          source: ForeignKey.lookup
          filter: "filter_Recycle = 1"
          schema: 
            - name: Recycle_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle_DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle_FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: Recycle_ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: Recycle_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: Recycle_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: Recycle_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle_PROV_ADDR_TYP_CD
              type: char
              width: 12
              nullable: N
            - name: Recycle_PROV_ADDR_EFF_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
        - name: ForeignKey.V0S41P12.filter
          type: filter
          source: ForeignKey.lookup
          filter: "filter_DefaultNA = 1"
          schema: 
            - name: DefaultNA_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
        - name: ForeignKey.V0S41P13.filter
          type: filter
          source: ForeignKey.lookup
          filter: "filter_DefaultUNK = 1"
          schema: 
            - name: DefaultUNK_PROV_LOC_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
        - name: ForeignKey.V0S41P1
          type: map
          source: ForeignKey.V0S41P1.filter
          alias: Fkey
          schema: 
            - name: PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_PROV_LOC_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_SRC_SYS_CD_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Fkey.Fkey_PROV_ID"
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Fkey.Fkey_PROV_ADDR_ID"
            - name: PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_PROV_ADDR_TYP_CD_SK"
            - name: PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Fkey.Fkey_PROV_ADDR_EFF_DT_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_PROV_ADDR_SK"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Fkey.Fkey_PROV_SK"
            - name: PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Fkey.Fkey_PRI_ADDR_IN"
            - name: REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Fkey.Fkey_REMIT_ADDR_IN"
        - name: ForeignKey.V0S41P4
          type: map
          source: ForeignKey.V0S41P4.filter
          alias: Recycle
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle.Recycle_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle.Recycle_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle.Recycle_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Recycle.Recycle_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Recycle.Recycle_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Recycle.Recycle_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle.Recycle_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Recycle.Recycle_PRI_KEY_STRING"
            - name: PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_PROV_LOC_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_SRC_SYS_CD_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle.Recycle_PROV_ID"
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle.Recycle_PROV_ADDR_ID"
            - name: PROV_ADDR_TYP_CD
              type: char
              width: 12
              nullable: N
              map: "Recycle.Recycle_PROV_ADDR_TYP_CD"
            - name: PROV_ADDR_EFF_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle.Recycle_PROV_ADDR_EFF_DT"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_PROV_ADDR_SK"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle.Recycle_PROV_SK"
            - name: PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle.Recycle_PRI_ADDR_IN"
            - name: REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle.Recycle_REMIT_ADDR_IN"
        - name: ForeignKey.V0S41P12
          type: map
          source: ForeignKey.V0S41P12.filter
          alias: DefaultNA
          schema: 
            - name: PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_LOC_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SRC_SYS_CD_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_ID"
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_ADDR_ID"
            - name: PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_ADDR_TYP_CD_SK"
            - name: PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_ADDR_EFF_DT_SK"
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
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_ADDR_SK"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_SK"
            - name: PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_PRI_ADDR_IN"
            - name: REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_REMIT_ADDR_IN"
        - name: ForeignKey.V0S41P13
          type: map
          source: ForeignKey.V0S41P13.filter
          alias: DefaultUNK
          schema: 
            - name: PROV_LOC_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_LOC_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SRC_SYS_CD_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_ID"
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_ADDR_ID"
            - name: PROV_ADDR_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_ADDR_TYP_CD_SK"
            - name: PROV_ADDR_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_ADDR_EFF_DT_SK"
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
            - name: PROV_ADDR_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_ADDR_SK"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_SK"
            - name: PRI_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_PRI_ADDR_IN"
            - name: REMIT_ADDR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_REMIT_ADDR_IN"
        - name: Collector
          type: unknown
          source: hf_recycleProvLocForeignKey.lookup
          alias: 
          origType: CCollector


