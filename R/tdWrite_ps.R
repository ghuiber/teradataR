jni.transform = function(setter, x) {
  switch(setter,
         'setInt' = as.integer(x),
         'setFloat' = .jfloat(x),
         'setDate' = .jnew("java/sql/Date",.jlong(as.numeric(as.POSIXct(x)))*1000),
         'setString' = as.character(x)
  )
}

tdInsertPS = function(ps, df) {
  classes = unlist(lapply(df,data.class))
  numeric.indices = which(classes == 'numeric')
  if(length(numeric.indices) > 0) {
    int.indices = which(unlist(lapply(df[,numeric.indices,drop=F],is.integer)))
    classes[numeric.indices][int.indices] = 'integer'
  }
  java.setters = sapply(classes, function(i) {
    switch(i,
           'integer' = 'setInt',
           'numeric' = 'setFloat',
           'factor' = 'setString',
           'character' = 'setString',
           'Date' = 'setDate')
  })
  javatype = .jnew("java/sql/Types")
  java.typeint = sapply(classes, function(i) {
    switch(i,
    'integer' = .jfield(javatype, "I", "INTEGER"),
    'numeric' = .jfield(javatype, "I", "FLOAT"),
    'factor' = .jfield(javatype, "I", "VARCHAR"),
    'character' = .jfield(javatype, "I", "VARCHAR"),
    'Date' = .jfield(javatype, "I", "DATE"))
  })
  
  for (i in 1:nrow(df)) {
    print (i)
    r = df[i,]
    for (j in 1:ncol(df)) {
      value = unlist(r[,j])
      if (is.null(value) | is.na(value) | is.nan(value)) {
        .jcall(ps,"V","setNull",as.integer(j), java.typeint[j])
      }
      else {
        .jcall(ps,"V",java.setters[j],as.integer(j), jni.transform(java.setters[j], value))
      }
    }
    .jcall(ps,"V","addBatch")
    if (i %% 5000 == 0) .jcall(ps,"[I","executeBatch")
  }
  .jcall(ps,"[I","executeBatch")
  return (ps)
}

tdWriteTable_ps = function(databasename, tablename, df, primaryIndex = NULL, partitionDate = NULL) {
  if(!tdExistsTable(databasename, tablename)) tdQueryUpdate(dbBuildTableDefinition(databasename, tablename, df, primaryIndex, partitionDate))
  .jcall(tdConnection@jc,"V","setAutoCommit",FALSE)
  ps = .jcall(tdConnection@jc,
              "Ljava/sql/PreparedStatement;",
              "prepareStatement",
              sprintf("insert into %s values(%s)", tdPath(databasename, tablename), paste(rep("?", ncol(df)), collapse=",") ))
  ps = tdInsertPS(ps, df)
  dbCommit(tdConnection)
  .jcall(ps,"V","close")
  .jcall(tdConnection@jc,"V","setAutoCommit",TRUE)
}
