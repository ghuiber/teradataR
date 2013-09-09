\name{tdWriteTable}
\alias{tdWriteTable}
\title{
Write R data frame to a Teradata Table
}
\description{
Write R data frame to a Teradata Table.  Automatically tries to detect the table definition
and constructs the table if it does not exists.  Uses SQL INSERT INTO syntax to populate 
the table.
}
\usage{
tdWriteTable(databasename, tablename, df)
}
\arguments{
  \item{databasename}{
    Database name such as "gdw_prod", or "sbox_play".
}
  \item{tablename}{
    Table name such as "jeffreyw_teradatar_test"
}
  \item{df}{
    The data frame to write
}
}
\details{
}
\value{
Returns NULL
}
\examples{

\dontrun{
df = data.frame(account_id = 1:3,
                feature1 = as.character(c("a","b","c")),
                feature2 = factor(c("a", "b", "c")),
                feature3 = rnorm(3),
                event_date = c(Sys.Date(), Sys.Date() + 1, Sys.Date() - 1),
                event_ts = rep(Sys.time(), 3))
df$feature1 = as.character(df$feature1)
tdWriteTable("sbox_play", "jeffreyw_teradatar_test", df)
}
}
