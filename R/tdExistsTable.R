tdExistsTable = function(dbname, tablename) {
  sql = sprintf("select tablename, databasename from dbc.tables where databasename = '%s' and tablename = '%s'", dbname, tablename)
  result = tdQuery(sql)
  return (nrow(result) > 0)
}
