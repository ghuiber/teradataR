timeToCharacter = function(df) {
  timeCols = unlist(lapply(df, function(i) return (data.class(i) == "POSIXct" | data.class(i) == "Date")))
  df[,timeCols] = lapply(df[,timeCols,drop=F], as.character)
  return (df)
}

R_to_TD = function(databasename, tablename, df, primaryIndex=NULL, partitionDate=NULL) {
  if(!tdExistsTable(databasename, tablename)) tdQueryUpdate(dbBuildTableDefinition(databasename, tablename, df, primaryIndex, partitionDate))
  tmp = tempfile()
  write.table(timeToCharacter(df), tmp, row.names=F, col.names=F, sep=",")
  tmp2 = readLines(tmp); tmp2 = gsub("\"", "'", tmp2); tmp2 = strsplit(tmp2, "\n")
  unlink(tmp)
  sql = lapply(tmp2, function(d) {
    values = unlist(strsplit(d, ','))
    values[values == 'NA'] = "NULL"
    sprintf("insert into %s (%s) values (%s)", tdPath(databasename, tablename), paste(colnames(df), collapse=","), paste(values, collapse = ","))
  })
  sql = paste(sql, collapse = ";")
  return(sql)
}

tdWriteTable = function(databasename, tablename, df, primaryIndex = NULL, partitionDate = NULL, verbose = T) {
  if (nrow(df) > 2000) warning("Attempting to upload a large data frame.  You may want to use tdWriteTable_ps or bteqWriteTable instead")
  numElements = length(df)
  numBatches = ceiling(numElements / 20000)
  rowsPerBatch = ceiling(20000 / ncol(df))
  for (batch in numBatches) {
    if (verbose) print(sprintf("Begin batch %s", batch))
    row.start = (batch-1) * rowsPerBatch + 1
    row.end = min(nrow(df), batch*rowsPerBatch)
    tdQueryUpdate(R_to_TD(databasename, tablename, df[row.start : row.end, ], primaryIndex, partitionDate))
  }
}
