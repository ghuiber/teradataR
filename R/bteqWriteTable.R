bteqWriteTable = function(server, databasename, tablename, numSessions=10, username, password, df, primaryIndex = NULL, partitionDate = NULL) {
  if(Sys.getenv("COPERR") == "") Sys.setenv(COPERR="/Library/Application Support/teradata/client/14.10/lib")
  if(!tdExistsTable(databasename, tablename)) tdQueryUpdate(dbBuildTableDefinition(databasename, tablename, df, primaryIndex, partitionDate))
  tmp = tempfile()
  write.table(df, tmp, row.names=F, col.names=T, sep=",")
  bteq.using = .bteqUsing(colnames(df))
  bteq.insert = .bteqInsert(databasename, tablename, colnames(df))
  bteq.values = .bteqValues(colnames(df), lapply(df, tdDataType))

  sql = sprintf("%s\n\n%s\n\n%s;", bteq.using, bteq.insert, bteq.values)
  bteq = sprintf(".SESSIONS %s
                  .logmech ldap
                  LOGON %s/%s,%s;
                  .import vartext ',' file = '%s' skip = 1
                 .QUIET ON
                 .REPEAT *
                 %s
                 .QUIT", numSessions, server,username,password,tmp,sql)
  tmp2 = tempfile()
  writeLines(bteq, tmp2)
  system(sprintf("'/Library/Application Support/teradata/client/14.10/bin/bteq' < %s", tmp2))
  unlink(tmp); unlink(tmp2)
}

.bteqUsing = function(colnames) {
  sprintf("USING %s", paste(paste(colnames, collapse = " (varchar(1024)),\n"), " (varchar(1024))"))
}

.bteqInsert = function(databasename, tablename, colnames) {
  sprintf("INSERT INTO %s (%s)", tdPath(databasename, tablename), paste(colnames, collapse=',')) 
}

.bteqValues = function(colnames, coltypes) {
  values = paste(sprintf("CAST(:%s)", paste(colnames, coltypes, sep = " as ")), collapse=",\n")
  paste("VALUES (", values, ")")
}
