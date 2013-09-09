tdQueryUpdate <-
function(q, verbose=F, ...)
{
  if(class(tdConnection) == "RODBC")
    return(sqlQuery(tdConnection, q, ...))
  if(class(tdConnection) == "JDBCConnection")
  {
    dbSendUpdate(tdConnection, q, ...)
    if (verbose) cat("tdQueryUpdate does not return data.  If this was a mistake use tdUpdate instead")
    return (NULL)
  }
}
