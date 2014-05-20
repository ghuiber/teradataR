td_export = function(q, fetchSize = 100000, verbose = T, file = "export.txt", ...) {
  if(class(tdConnection) == "JDBCConnection") {
    send = dbSendQuery(tdConnection, q, ...)
    top = fetch(send, 1)
    write.table(top, file = file, quote = FALSE, append = FALSE, sep = ";",
                dec = ",", row.names = FALSE, col.names = FALSE)
    tryCatch({while (1) {
      moredata = fetch(send, fetchSize)
      data.n = nrow(moredata)
      write.table(moredata, file = file, quote = FALSE, append = TRUE, sep = ";",
                  dec = ",", row.names = FALSE, col.names = FALSE)      
      if (verbose) print(sprintf("%s rows fetched", data.n))
      if (data.n < fetchSize) break
    }}, error = function(e) { e })
    dbClearResult(send)
  }
}
