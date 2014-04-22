td_fun_export = function(q, fetchSize = 100000, verbose = T, file = "export.txt", FUN = NULL, ...) {
  options(scipen=666)
  require("data.table")
  if(class(tdConnection) == "JDBCConnection") {
    send = dbSendQuery(tdConnection, q, ...)
    data = list()
    top = fetch(send, 1)
    top = FUN(top)
    write.table(top, file = file, quote = FALSE, append = FALSE, sep = ";",
                dec = ",", row.names = FALSE, col.names = FALSE)
    tryCatch({while (1) {
      moredata = fetch(send, fetchSize)
      data.n = nrow(moredata)
      data = FUN(moredata)
      write.table(data, file = file, quote = FALSE, append = TRUE, sep = ";",
                  dec = ",", row.names = FALSE, col.names = FALSE)      
      if (verbose) print(sprintf("%s rows fetched", data.n))
      if (data.n < fetchSize) break
    }}, error = function(e) { e })
    dbClearResult(send)
    return (data)
    }
}
