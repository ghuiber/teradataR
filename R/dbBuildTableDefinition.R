dbBuildTableDefinition =
function (databasename, tablename, df, field.types = NULL, primaryIndex = NULL, partitionDate = NULL, ...)
{
    if (!is.data.frame(df))
        df <- as.data.frame(df)
    if (is.null(field.types)) {
        field.types <- lapply(df, tdDataType)
    }
    flds <- paste(names(field.types), field.types)
    base = sprintf("CREATE MULTISET TABLE %s, no fallback, no before journal, no after journal
                   (%s)", tdPath(databasename, tablename),
                   paste(flds, collapse = ",\n"))
    if (!is.null(primaryIndex))
      base = sprintf("%s primary index(%s)", base, primaryIndex)
    if (!is.null(partitionDate))
      base = sprintf("%s PARTITION BY RANGE_N(%s BETWEEN DATE '2005-01-01' AND DATE '2015-12-31'
                     EACH INTERVAL '7' DAY, NO RANGE, UNKNOWN)", base, partitionDate)
    return (base)
}

tdDataType = function (obj)
{
    rs.class <- data.class(obj)
    rs.mode <- storage.mode(obj)
    if (rs.class %in% c("integer", "int")) "bigint"
    else switch(rs.class,
                character = "varchar(1024)",
                logical = "byteint",
                Factor = "varchar(1024)",
                Date = "DATE format 'YYYY-MM-DD'",
                POSIXct = "timestamp(0)",
                numeric = "float",
                "varchar(1024)")
}
