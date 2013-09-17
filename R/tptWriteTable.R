tptWriteTable = function(server, databasename, tablename, username, password, df, primaryIndex = NULL, partitionDate = NULL) {
  if(Sys.getenv("COPERR") == "") Sys.setenv(COPERR="/Library/Application Support/teradata/client/14.10/lib")
  tmp = tempfile()
  write.table(df, tmp, sep=",", col.names = F, row.names=F)
  if(!tdExistsTable(databasename, tablename)) {
    tdQueryUpdate(dbBuildTableDefinition(databasename, tablename, df, 
                                         primaryIndex, partitionDate))
    tpt = .tptEmptyLoad(server, databasename, tablename, username, password, colnames(df), tmp)
  }
  else {
    tdQueryUpdate(dbBuildTableDefinition(databasename, sprintf("%s_stg", tablename),
                                         df, primaryIndex, partitionDate))
    tpt = .tptMiniBatch(server, databasename, tablename, username, password, colnames(df), tmp)
  } 
  tmp2 = tempfile()
  writeLines(tpt, tmp2)
  system(sprintf("'/Library/Application Support/teradata/client/14.10/tbuild/bin/tbuild' -f %s", tmp2))
  unlink(tmp); unlink(tmp2)
}

.tptSchema = function(colnames) {
  sprintf("%s", paste(paste(colnames, collapse = " varchar(1024),\n"), " varchar(1024)"))
}

.tptSchemaColon = function(colnames) {
  paste(sprintf(":%s", colnames), collapse=',')
}

.tptEmptyLoad = function(server, databasename, tablename, username, password, colnames, filepath) {
  tpt = sprintf("
                DEFINE JOB LOAD_TABLE_FROM_FILE
                DESCRIPTION 'LOAD EMPTY TABLE FROM A FILE'
                (
                DEFINE SCHEMA MY_SCHEMA
                DESCRIPTION 'MY SCHEMA'
                (
                %s
                );
                
                DEFINE OPERATOR LOAD_OPERATOR()
                DESCRIPTION 'TERADATA PARALLEL TRANSPORTER LOAD OPERATOR'
                TYPE LOAD
                SCHEMA MY_SCHEMA
                ATTRIBUTES
                (
                VARCHAR PrivateLogName    = 'GT62_loadoper_privatelog',
                INTEGER MaxSessions       =  32,
                INTEGER MinSessions       =  1,
                VARCHAR TargetTable       = '%s_stg',
                VARCHAR TdpId             = '%s',
                VARCHAR UserName          = '%s',
                VARCHAR UserPassword      = '%s',
                VARCHAR AccountId,
                VARCHAR ErrorTable1       = 'GT62_LOADOPER_ERRTABLE1',
                VARCHAR ErrorTable2       = 'GT62_LOADOPER_ERRTABLE2',
                VARCHAR LogTable          = 'GT62_LOADOPER_LOGTABLE'
                );
                
                DEFINE OPERATOR FILE_READER()
                DESCRIPTION 'TERADATA PARALLEL TRANSPORTER DATA CONNECTOR OPERATOR'
                TYPE DATACONNECTOR PRODUCER
                SCHEMA MY_SCHEMA
                ATTRIBUTES
                (
                VARCHAR PrivateLogName    = 'GT62_dataconnoper_reader_privatelog',
                VARCHAR FileName          = '%s',
                VARCHAR Format            = 'Delimited',
                VARCHAR OpenMode          = 'Read',
                VARCHAR TextDelimiter     = ','
                );
                
                STEP load_data_from_file
                (
                APPLY
                ('INSERT INTO %s_stg (%s);')
                TO OPERATOR (LOAD_OPERATOR[ 5] )
                
                SELECT * FROM OPERATOR (FILE_READER[ 5] );
                );
                );
                ", .tptSchema(colnames), server, username, password,
                tdPath(databasename, tablename), filepath, 
                tdPath(databasename, tablename), .tptSchemaColon(colnames))
  return (tpt)
}

.tptMiniBatch = function(server, databasename, tablename, username, password, colnames, filepath) {
  tpt = sprintf("
                DEFINE JOB LOAD_TABLE_FROM_FILE
                DESCRIPTION 'LOAD TABLE FROM A FILE USING MINI-BATCH'
                (
                DEFINE SCHEMA MY_SCHEMA
                DESCRIPTION 'MY SCHEMA'
                (
                %s
                );
                
                DEFINE OPERATOR DDL_OPERATOR
                TYPE DDL
                ATTRIBUTES
                (
                VARCHAR PrivateLogName = 'ddl_log',
                VARCHAR TdpId = '%s',
                VARCHAR UserName = '%s',
                VARCHAR UserPassword = '%s',
                VARCHAR ARRAY ErrorList = ['3807','3803','5980']
                );
                
                DEFINE OPERATOR LOAD_OPERATOR()
                DESCRIPTION 'TERADATA PARALLEL TRANSPORTER LOAD OPERATOR'
                TYPE LOAD
                SCHEMA MY_SCHEMA
                ATTRIBUTES
                (
                VARCHAR PrivateLogName    = 'GT62_loadoper_privatelog',
                INTEGER MaxSessions       =  32,
                INTEGER MinSessions       =  1,
                VARCHAR TargetTable       = '%s_stg',
                VARCHAR TdpId             = '%s',
                VARCHAR UserName          = '%s',
                VARCHAR UserPassword      = '%s',
                VARCHAR AccountId,
                VARCHAR ErrorTable1       = 'GT62_LOADOPER_ERRTABLE1',
                VARCHAR ErrorTable2       = 'GT62_LOADOPER_ERRTABLE2',
                VARCHAR LogTable          = 'GT62_LOADOPER_LOGTABLE'
                );
                
                DEFINE OPERATOR FILE_READER()
                DESCRIPTION 'TERADATA PARALLEL TRANSPORTER DATA CONNECTOR OPERATOR'
                TYPE DATACONNECTOR PRODUCER
                SCHEMA MY_SCHEMA
                ATTRIBUTES
                (
                VARCHAR PrivateLogName    = 'GT62_dataconnoper_reader_privatelog',
                VARCHAR FileName          = '%s',
                VARCHAR Format            = 'Delimited',
                VARCHAR OpenMode          = 'Read',
                VARCHAR TextDelimiter     = ','
                );
                
                STEP load_data_from_file
                (
                APPLY
                ('INSERT INTO %s_stg (%s);')
                TO OPERATOR (LOAD_OPERATOR[ 5] )
                
                SELECT * FROM OPERATOR (FILE_READER[ 5] );
                );
                
                STEP mini_batch
                (
                APPLY
                ('insert into %s select * from %s_stg;'),
                ('drop table %s_stg;')
                TO OPERATOR (DDL_OPERATOR);
                );
                );
                ", .tptSchema(colnames), server, username, password,
     tdPath(databasename, tablename), server, username, password,
     filepath, tdPath(databasename, tablename),
     .tptSchemaColon(colnames),
     tdPath(databasename, tablename), tdPath(databasename, tablename), tdPath(databasename, tablename))
  return (tpt)
}

