model:
  config:
    origin: datastage
  jobs:
    - name: ProcessCheckExtr
      origin: datastage
      type: dataflow
      folder: "JobSequencer/CommonComponents"
      description: "Check for running processes"
      fullDescription: "* VC LOGS *\n^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm\n^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/21/07 15:09:17 Batch  14570_54560 INIT bckcetl ids20 dsadm dsadm\n^1_1 08/23/07 08:22:09 Batch  14480_30134 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/07/06 10:31:19 Batch  14191_37885 PROMOTE bckcetl ids20 dsadm Keith for Brent\n^1_1 11/07/06 10:29:22 Batch  14191_37768 INIT bckcett testIDS30 dsadm Keith for Brent\n\n\nCOPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:    ProcessCheckExtr\n\n\nDESCRIPTION:   Using the P_RUN_CYC table check for other processes running that impact the callers ability to extract and update data.\n      \n\nINPUTS:\x09 Inputs are values from the callers perspective.\n\n                 Subject name    (CLAIM, MEMBER, etc.)\n                 Source System  (IDS, EDW, CDM)\n                 Target System   (IDS, EDW, CDM)\n\n  \nHASH FILES:  none\n\n\nTRANSFORMS:  None\n                        \n   \nPROCESSING:   Search P_RUN_CYC table for any rows with a status of \"RUNNING\" for a subject where the callers source system is being update or the callers target system is being extracted from.  Calling Sequencer will check row count exiting transform to determine whether to continue.\n \n\nOUTPUTS:  Output is not written anywhere.  Calling sequencer only cares about row count coming out of the transform stage.\n                    \nTEST CASES:  \n                         Test                                                                                                          Results\n                         1. Source being updated by another process                                           Returns one row\n                         2. Source being extracted from by another source                                   Returns zero rows\n                         3. Target being updated by another process for same subject                 Returns one row\n                         4. Target being extracted from for same subject                                       Returns one row\n\nMODIFICATIONS:\n              B Leland           03/22/2006  -  Initial programming\n              B Leland           08/14/2006  -  Changed SQL logic to correctly see jobs running.\n              B Leland           10/30/2006  -  Added additional SQL to union statement to look for jobs extracting from the system being updated."
      parameters:
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
          defaultType: expr
          defaultValue: u08717
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
          defaultType: expr
          defaultValue: "HL;@IJV@L93>0O5I<IJ<BKIFB<>"
        - name: Subject
          type: string
          description: "Subject Name"
          longDescription: "Passed in by the calling job."
          defaultType: expr
          defaultValue: CLAIM
        - name: TargetSys
          type: string
          description: "Target System"
          defaultType: expr
          defaultValue: EDW
        - name: SourceSys
          type: string
          description: "Source System"
          defaultType: expr
          defaultValue: IDS
      steps:
        - name: Null
          type: fileWriter
          urls: 
            - "/dev/null"
          source: Trans1
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: P_RUN_CYC
          type: dbReader
          originalDb: db2
          query: "select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD\n  from #$IDSOwner#.P_RUN_CYC\nwhere CYC_STTUS_CD = 'RUNNING'\n   and SUBJ_CD = '#Subject#'\n   and TRGT_SYS_CD = '#SourceSys#'\n\nUNION\n\nselect SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD\n  from #$IDSOwner#.P_RUN_CYC\nwhere CYC_STTUS_CD = 'RUNNING'\n   and SUBJ_CD = '#Subject#'\n   and SRC_SYS_CD <> '#SourceSys#'\n   and TRGT_SYS_CD = '#TargetSys#'\n\nUNION\n\nselect SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD\n  from #$IDSOwner#.P_RUN_CYC\nwhere CYC_STTUS_CD = 'RUNNING'\n  and SUBJ_CD = '#Subject#'\n   and SRC_SYS_CD = '#TargetSys#'\n"
          table: "#$IDSOwner#.W_TASK_EXCL"
          tableAction: append
          schema: 
            - name: SUBJ_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRGT_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
        - name: Trans1
          type: map
          source: P_RUN_CYC
          alias: Extract
          schema: 
            - name: SUBJ_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.SUBJ_CD"
            - name: TRGT_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.TRGT_SYS_CD"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.SRC_SYS_CD"


