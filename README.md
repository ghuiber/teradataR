teradataR
=========

mirror for R package to communicate with Teradata

This package contains add ons in addition to Teradata's release of TeradataR.  The version here allows the user
to write an R data frame to Teradata, as one would see in RSQLite and RMySQL.

Teradata JARs
---------
There are two JARs that you will need to reference whenever you initialize teradataR

```
terajdbc4.jar
tdgssconfig.jar
```

Example
---------
```
require(teradataR)
require(RJDBC)

.jinit()
.jaddClassPath(<path to terajdbc4.jar>)
.jaddClassPath(<path to tdgssconfig.jar>)
tdConnect(<server address>,<username>,<password>, dType='jdbc')

df = data.frame(account_id = 1:3,
                feature1 = as.character(c("a","b","c")),
                feature2 = factor(c("a", "b", "c")),
                feature3 = rnorm(3),
                event_date = c(Sys.Date(), Sys.Date() + 1, Sys.Date() - 1),
                event_ts = rep(Sys.time(), 3))
df$feature1 = as.character(df$feature1)
tdWriteTable(<database>, <table>, df)
```
