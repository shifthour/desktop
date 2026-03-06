model:
  config:
    origin: datastage
  jobs:
    - name: ProcessRptExtr
      origin: datastage
      type: dataflow
      folder: "util/ProcessRpt"
      runAfterSubrOnJobSuccess: enabled
      description: "Extract <Subject> Information"
      fullDescription: "© Copyright 2008 Blue Cross/Blue Shield of Kansas City\n\n\nCALLED BY:  Various job controls\n               \n\nPROCESSING:   Select row from P_RUN_CYC based on input source, target, subject, and run cycle.  Caculate run time durartion and append information to file /ids/prod/landing/ids_processing.dat.\n\n\nMODIFICATIONS:\nDeveloper                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       \n------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       \nBrent Leland            2008-02-14       IAD Prod. Supp     Original Programming.                                                                   devlIDScur                     Steph Goddard          02/22/2008\n\nJaideep Mankala     2017-03-13           5321                 Updated Aliase for output column 'DURATION' in                        IntegrateDev2                Jag Yelavarthi             2017-03-13         \n                                                                                        IDS stage to match with stage meta data.\n                                                                                      Changed Timestamp precision from 3 to 6"
      parameters:
        - name: IDSFilePath
          type: string
          description: "IDS File Path"
        - name: RunCycle
          type: integer
          description: "Run Cycle"
          defaultType: expr
          defaultValue: "470"
        - name: Subject
          type: string
          description: Subject
          defaultType: expr
          defaultValue: CLAIM
        - name: SourceSys
          type: string
          description: "Source System"
          defaultType: expr
          defaultValue: FACETS
        - name: TargetSys
          type: string
          description: "Target System"
          defaultType: expr
          defaultValue: IDS
        - name: RowCount
          type: integer
          description: "Row Count"
          defaultType: expr
          defaultValue: "10000"
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
          defaultType: expr
          defaultValue: u08717
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
          defaultType: expr
          defaultValue: "L4;@0KVAN9;L4OFI4HJ41KH5B<N"
      steps:
        - name: processing_rpt
          type: fileWriter
          urls: 
            - "#IDSFilePath#/scripts/log/processing_rpt.dat"
          source: Trans1
          option: 
            updateMode: append
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: IDS
          type: dbReader
          originalDb: db2
          query: "SELECT \nr.SUBJ_CD,\nr.SRC_SYS_CD,\nr.TRGT_SYS_CD,\nr.STRT_DTM,  \nr.END_DTM,\nc.DAY_OF_WK_FULL_NM, \n(r.END_DTM - r.STRT_DTM) AS DURATION\n\nFROM #$IDSOwner#.P_RUN_CYC r,\n           #$IDSOwner#.P_BTCH_CYC b,\n           #$IDSOwner#.CLNDR_DT c\nWHERE r.SUBJ_CD = '#Subject#'\n      AND r.TRGT_SYS_CD = '#TargetSys#'\n      AND r.SRC_SYS_CD = '#SourceSys#'\n      AND r.BTCH_CYC_SK = b.BTCH_CYC_SK\n      AND b.BTCH_CYC_DESC = c.CLNDR_DT_SK\n      AND r.RUN_CYC_NO = #RunCycle#\n order by r.STRT_DTM\n"
          table: "#$IDSOwner#"
          tableAction: append
          schema: 
            - name: SUBJ_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRGT_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: STRT_DTM
              type: timestamp
              nullable: N
            - name: END_DTM
              type: timestamp
              nullable: N
            - name: DAY_OF_WK_FULL_NM
              type: char
              width: 10
            - name: DURATION
              type: decimal
              width: 20
              precision: 6
              nullable: N
        - name: Trans1
          type: map
          source: IDS
          alias: IDS_Extract
          stageVars: 
            - name: Value
              type: decimal
              width: 0
              map: "'000000' : trim(IDS_Extract.DURATION)"
              initValue: "0"
            - name: PerPos
              type: decimal
              width: 0
              map: "Index(Value, \".\", 1)"
              initValue: "0"
            - name: Sec
              type: decimal
              width: 0
              map: "Value[PerPos - 2, 2]"
              initValue: "0"
            - name: Min
              type: decimal
              width: 0
              map: "Value[PerPos - 4, 2]"
              initValue: "0"
            - name: Hrs
              type: decimal
              width: 0
              map: "Value[PerPos - 6, 2]"
              initValue: "0"
          schema: 
            - name: SUBJ_CD
              type: varchar
              width: 20
              nullable: N
              map: "IDS_Extract.SUBJ_CD"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "IDS_Extract.SRC_SYS_CD"
            - name: TRGT_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "IDS_Extract.TRGT_SYS_CD"
            - name: STRT_DTM
              type: timestamp
              nullable: N
              map: "IDS_Extract.STRT_DTM"
            - name: END_DTM
              type: timestamp
              nullable: N
              map: "IDS_Extract.END_DTM"
            - name: DAY_OF_WK_FULL_NM
              type: char
              width: 10
              map: "IDS_Extract.DAY_OF_WK_FULL_NM"
            - name: TIME_DURATION
              type: varchar
              width: 20
              nullable: N
              map: "Hrs : ':' : Min"
            - name: ROW_COUNT
              type: int
              width: 4
              nullable: N
              map: RowCount
            - name: Value
              type: varchar
              width: 0
              map: Value
              dropStageVar: true
              initValue: "0"
            - name: PerPos
              type: varchar
              width: 0
              map: PerPos
              dropStageVar: true
              initValue: "0"
            - name: Sec
              type: varchar
              width: 0
              map: Sec
              dropStageVar: true
              initValue: "0"
            - name: Min
              type: varchar
              width: 0
              map: Min
              dropStageVar: true
              initValue: "0"
            - name: Hrs
              type: varchar
              width: 0
              map: Hrs
              dropStageVar: true
              initValue: "0"


