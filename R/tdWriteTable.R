timeToCharacter = function(df) {
  timeCols = unlist(lapply(df, function(i) return (data.class(i) == "POSIXct" | data.class(i) == "Date")))
  df[,timeCols] = lapply(df[,timeCols,export=F], as.character)
  return (df)
}

R_to_TD = function(databasename, tablename, df, primaryIndex=NULL, partitionDate=NULL) {
  if(!tdExistsTable(databasename, tablename)) tdQueryUpdate(dbBuildTableDefinition(databasename, tablename, df, primaryIndex, partitionDate))
  tmp = tempfile()
  write.table(timeToCharacter(df), tmp, row.names=F, col.names=F, sep=",")
  tmp2 = readLines(tmp); tmp2 = gsub("\"", "'", tmp2); tmp2 = strsplit(tmp2, "\n")
  unlink(tmp)
  sql = lapply(tmp2, function(d) {
    sprintf("insert into %s (%s) values (%s)", tdPath(databasename, tablename), paste(colnames(df), collapse=","), d)   
  })
  sql = paste(sql, collapse = ";")
  return(sql)
}

tdWriteTable = function(databasename, tablename, df, primaryIndex = NULL, partitionDate = NULL) {
  tdQueryUpdate(R_to_TD(databasename, tablename, df, primaryIndex, partitionDate))
}
