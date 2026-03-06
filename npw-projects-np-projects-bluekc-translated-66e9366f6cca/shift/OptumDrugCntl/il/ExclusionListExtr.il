model:
  config:
    origin: datastage
  jobs:
    - name: ExclusionListExtr
      origin: datastage
      type: dataflow
      folder: "JobSequencer/CommonComponents"
      description: "Extract subject records from W_TASK_EXCL"
      fullDescription: "* VC LOGS *\n^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm\n^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/21/07 15:09:17 Batch  14570_54560 INIT bckcetl ids20 dsadm dsadm\n^1_1 08/23/07 08:22:09 Batch  14480_30134 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina\n^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent\n^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent\n^1_1 07/06/05 08:44:13 Batch  13702_31459 PROMOTE bckcetl ids20 dsadm Gina Parr\n^1_1 07/06/05 08:41:57 Batch  13702_31322 INIT bckcett testIDS30 dsadm Gina Parr\n^1_4 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent\n^1_3 06/15/05 11:06:14 Batch  13681_39980 PROMOTE bckcett VERSION u08717 Brent\n^1_3 06/15/05 11:05:50 Batch  13681_39953 INIT bckcett devlIDS30 u08717 Brent\n^1_2 05/25/05 14:24:33 Batch  13660_51876 INIT bckcett devlIDS30 u08717 Brent\n^1_1 05/25/05 14:13:44 Batch  13660_51232 PROMOTE bckcett VERSION u08717 Brent\n^1_1 05/25/05 14:02:38 Batch  13660_50563 INIT bckccdt ids20 dsadm Brent\n\nCOPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:    ExclusionListExtr\n\n\nDESCRIPTION:   Extract the records for a subject from the DB2 - W_TASK_EXCL table.\n      \n\nINPUTS:\x09 Subject name\n\n  \nHASH FILES:  TASK_EXCL\n\n\nTRANSFORMS:  None\n                        \n   \nPROCESSING:   Task rows from DB2 table are concatenated together with a semi-colon delimiter.  Each row overwrites the previous row in the TASK_EXCL UV table.  The last entry is the combination of all DB2 entries.\n \n\nOUTPUTS:  A semi-colon delimited string of tasks to exclude for a subject.\n                    \n\n\nMODIFICATIONS:\n              B Leland           05/25/2005  -  Initial programming"
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
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
          defaultType: expr
          defaultValue: "HDI@IJV8O9JN064IL:JD1K95"
        - name: Subject
          type: string
          description: "Subject Name"
          longDescription: "Passed in by the calling job."
        - name: DSE_UVNLSOFF_SKIP_WARNINGS
          type: string
          description: Warning
          defaultType: expr
          defaultValue: "1"
        - name: UVNLSOFF
          type: string
          description: Warn
          defaultType: expr
          defaultValue: "1"
      steps:
        - name: W_TASK_EXCL
          type: dbReader
          originalDb: db2
          query: "SELECT TASK_SUBJ_NM as TASK_SUBJ_NM,TASK_NM as TASK_NM FROM #$IDSOwner#.W_TASK_EXCL WHERE TASK_SUBJ_NM = '#Subject#'"
          table: "#$IDSOwner#.W_TASK_EXCL"
          tableAction: append
          schema: 
            - name: TASK_SUBJ_NM
              type: char
              width: 50
              nullable: N
            - name: TASK_NM
              type: char
              width: 50
              nullable: N
        - name: Concat
          type: map
          source: W_TASK_EXCL
          alias: extract
          stageVars: 
            - name: svTask
              type: decimal
              width: 18
              precision: 4
              map: "Trim(extract.TASK_NM)"
              initValue: "\";\""
            - name: svTaskList
              type: decimal
              width: 18
              precision: 4
              map: "svTaskList : svTask : \";\""
              initValue: "\";\""
          schema: 
            - name: SUBJECT
              type: varchar
              width: 50
              nullable: N
              map: "Trim(extract.TASK_SUBJ_NM)"
            - name: TASKLIST
              type: varchar
              width: 1000
              map: svTaskList
            - name: svTask
              type: decimal
              width: 18
              precision: 4
              map: svTask
              dropStageVar: true
              initValue: "\";\""
            - name: svTaskList
              type: decimal
              width: 18
              precision: 4
              map: svTaskList
              dropStageVar: true
              initValue: "\";\""
        - name: TASK_EXCL
          type: unknown
          source: TASK_EXCL
          origType: CUvStage


