dbBuildTableDefinition =
function (databasename, tablename, obj, field.types = NULL, ...)
{
    if (!is.data.frame(obj))
        obj <- as.data.frame(obj)
    if (is.null(field.types)) {
        field.types <- lapply(obj, tdDataType)
    }
    flds <- paste(names(field.types), field.types)
    paste("CREATE TABLE", tdPath(databasename, tablename), "\n(", paste(flds, collapse = ",\n\t"),
        "\n)")
}

tdDataType = 
function (obj)
{
    rs.class <- data.class(obj)
    rs.mode <- storage.mode(obj)
    if (rs.class == "numeric" || rs.class == "integer") sql.type <- ifelse(rs.mode == "integer", "bigint", "float")
    else {
        sql.type <- switch(rs.class,
                           character = "varchar(1024)",
                           logical = "byteint",
                           Factor = "varchar(1024)", 
                           Date = "DATE format 'YYYY-MM-DD'",
                           POSIXct = "timestamp(0)",
                           "varchar(1024)")
    }
    sql.type
}
