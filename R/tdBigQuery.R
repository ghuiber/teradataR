tdBigQuery = function(q, fetchSize = 100000, verbose = T,...) {
  if(class(tdConnection) == "JDBCConnection") {
    send = dbSendQuery(tdConnection, q, ...)
    data = list()
    try({while (1) {
      moredata = fetch(send, fetchSize)
      data.n = nrow(moredata)
      data = rbind(data, moredata)
      if (verbose) print(sprintf("%s rows fetched", data.n))
      if (data.n < fetchSize) break
    }}, error = function(e) { })
    dbClearResult(send)
    return (data)
  }
}
