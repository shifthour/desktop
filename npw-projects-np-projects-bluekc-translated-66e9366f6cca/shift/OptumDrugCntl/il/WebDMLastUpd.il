model:
  config:
    origin: datastage
  jobs:
    - name: WebDMLastUpd
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/CycUpdt"
      description: "Update last update date time for Web DM subject"
      fullDescription: "* VC LOGS *\n^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm\n^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm\n^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_1 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test\n\nCOPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:     WebDMLastUpd\n\nDESCRIPTION:     Update reference table with subject area last update date and time\n      \n\nINPUTS:          Subject name\n                        Date time\n\x09\n  \nHASH FILES: None\n\n\nTRANSFORMS:  None\n                           \n\nPROCESSING:  The sysdummy table is used to initiate the SQL server update.\n\n\nOUTPUTS:     direct update of REF_DM_LAST_UPDT\n\n\nMODIFICATIONS:\n           Brent Leland  05/17/2006  -   Originally Programmed"
      parameters:
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: ClmMartServer
          type: string
          description: "Claim Mart Server"
          defaultType: project
        - name: ClmMartDB
          type: string
          description: "Claim Mart Database"
          defaultType: project
        - name: ClmMartOwner
          type: string
          description: "Claim Mart Table Owner"
          defaultType: project
        - name: ClmMartAcct
          type: string
          description: "Claim Mart Account"
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
        - name: SubjectName
          type: string
          description: "Subject Name"
        - name: UpdateDate
          type: string
          description: "Update Date"
      steps:
        - name: SYSDUMMY1
          type: dbReader
          originalDb: db2
          query: "SELECT IBMREQD  FROM sysibm.sysdummy1;"
          tableAction: append
          schema: 
            - name: IBMREQD
              type: char
              width: 1
              nullable: N
        - name: CreateRow
          type: map
          source: SYSDUMMY1
          alias: Trigger
          schema: 
            - name: DM_SUBJ_AREA_NM
              type: varchar
              width: 35
              nullable: N
              map: SubjectName
            - name: DM_LAST_UPDT_DTM
              type: timestamp
              nullable: N
              map: UpdateDate
          keys:
            - DM_SUBJ_AREA_NM
        - name: REF_DM_LAST_UPDT
          type: unknown
          source: REF_DM_LAST_UPDT
          origType: CODBCStage


