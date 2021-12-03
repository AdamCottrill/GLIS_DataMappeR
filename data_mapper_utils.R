# function to get queries from Access mapper database

get_trg_table_names <- function(trg_db, table){
  DBConnection <- odbcConnectAccess2007(trg_db, uid = "", pwd = "")
  stmt <- sprintf("select * from [%s] where FALSE;", table)
  dat <- sqlQuery(DBConnection, stmt, as.is=TRUE, stringsAsFactors=FALSE, na.strings = "")
  odbcClose(DBConnection)
  return(toupper(names(dat)))
}

check_table_names <- function(trg_db, table, src_data){
  trg_names <- get_trg_table_names(trg_db, table)
  missing <- setdiff(trg_names, names(src_data))
  extra <- setdiff(names(src_data), trg_names)
  if(length(extra)) {
    msg <- sprintf("The source data frame has extra fields: %s",
      paste(extra,collapse = ', '))
      warning(msg)
    }
  if(length(missing)) {
    msg <- sprintf("The source data frame is missing fields: %s",
      paste(missing, collapse = ', '))
      warning(msg)
  }
  return(c(extra, missing))
}




fetch_data <- function(table, prj_cd, src_db, toupper=T){
  DBConnection <- odbcConnectAccess2007(src_db,uid = "", pwd = "")
  stmt <- sprintf("EXEC get_%s @prj_cd='%s'", table, prj_cd)
  dat <- sqlQuery(DBConnection, stmt, as.is=TRUE, stringsAsFactors=FALSE, na.strings = "")
  odbcClose(DBConnection)

  if (toupper)names(dat) <- toupper(names(dat))
  return(dat)
}

# function to append data to the template
append_data <- function(dbase, trg_table, data, append=T, safer=T,
                        toupper=T){
  if (toupper)names(data) <- toupper(names(data))

  field_check <- check_table_names(dbase, trg_table, data)
  if(length(field_check)) stop("Please fix field differences before proceeding.")

  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlSave(conn, data, tablename = trg_table, rownames = F,
          safer = safer, append=append, nastring = NULL)
  return(odbcClose(conn))

}


# Expected pattern of a valid lamprey wound
#wnd_regex <- "^0$|^([A|B][1-4])+$|^([A|B][1-4][1-5][0-9])+$"

## update_lamijc <- function(df){
##   # Helper function to add additional lamprey wounds recorded in comments to the LAMIJC column and
##   # remove the wound code from the comment column
##   df %>%
##     mutate(LAMIJC = ifelse(grepl(wnd_regex, COMMENT_LAM), paste0(LAMIJC, COMMENT_LAM), LAMIJC),
##            COMMENT_LAM = ifelse(grepl(wnd_regex, COMMENT_LAM), NA, COMMENT_LAM))
## }

## list_lamijc <- function(wound){
##   # Helper function to check if lamprey wounds have a valid code and then create a list of
##   # individual wounds for fish with multiple lamprey wounds

##   if (!grepl(wnd_regex, wound)){
##     msg <- sprintf("'%s' is not a valid lamijc.", wound)
##     stop(msg)
##   }

##   a123_regex <- "^([A|B][1-4][1-5][0-9])+$"
##   if (grepl(a123_regex, wound)){
##     #wound with diameter
##     wounds <- strsplit(wound, "(?<=.{4})", perl = TRUE)[[1]]
##   } else {
##     wounds <- strsplit(wound, "(?<=.{2})", perl = TRUE)[[1]]
##   }

##   #create a list of individual wounds:
##   wounds <- t(sapply(wounds, function(x) c(substr(x,1,4))))
##   return(wounds)
## }

## split_lamijc <- function(df){
##   # Function to split lamprey wound codes into two columns (LAMIJC_TYPE and LAMIJC_SIZE),
##   # add rows for each lamprey wound, and remove unnecessary columns
##   df %>%

##     update_lamijc() %>%

##     # create tibble from wound list-column:
##     tibble(wound = sapply(LAMIJC, FUN = list_lamijc, simplify = FALSE)) %>%

##     # add a column for each wound:
##     unnest_wider(wound, "") %>%

##     # pivot wide dataframe to long so each wound has its own row:
##     pivot_longer(cols = starts_with("wound"), values_to = "wound", values_drop_na = TRUE) %>%

##     # break wound code into type and size:
##     mutate(LAMIJC_TYPE = ifelse(wound == 0, 0, substr(wound, 1, 2)),
##            LAMIJC_SIZE = as.numeric(ifelse(nchar(wound) == 4, substr(wound, 3, 4), NA))) %>%

##     # drop unnecessary columns:
##     select(-name, -wound, -LAMIJC) %>%

##     as.data.frame()
## }



split_lamijc <- function(wound){
  wnd_regex <- "^0$|^([A|B][1-4])+$|^([A|B][1-4][1-5][0-9])+$"
  if (!grepl(wnd_regex, wound)){
    msg <- sprintf("'%s' is not a valid lamijc.", wound)
    stop(msg)
  }

  if (wound=="0"){
    #retrun a matrix to match other output types:
    return(matrix(c("0", "")))
  }

  a123_regex <- "^([A|B][1-4][1-5][0-9])+$"
  if (grepl(a123_regex,wound)){
    #wound with diameter
    wounds <- strsplit(wound, "(?<=.{4})", perl = TRUE)[[1]]
  } else {
    wounds <- strsplit(wound, "(?<=.{2})", perl = TRUE)[[1]]
  }

  #split the wounds up in to wound and diameter components:
  wounds <- t(sapply(wounds, function(x) c(substr(x,1,2), substr(x,3,4))))
  return(wounds)
}




process_fn125_lamprey <- function(df) {
  tmp <- df[FALSE, ]
  tmp$LAMICJ_TYPE <- character()
  tmp$LAMICJ_SIZE <- character()

  for (i in 1:nrow(df)) {
    fish <- df[i, ]
    wounds <- split_lamijc(df$LAMIJC[i])
    if(wounds[1,1]=='0'){
      fish$LAMIJC_TYPE <- "0"
      fish$LAMIJC_SIZE <- NA
      tmp <- rbind(tmp, fish)
    } else {
    for (w in 1:nrow(wounds)) {
      fish$LAMID <- w
      fish$LAMIJC_TYPE <- wounds[w, 1]
      fish$LAMIJC_SIZE <- wounds[w, 2]
      tmp <- rbind(tmp, fish)
    }
    }



  }

  tmp$LAMIJC <- NULL
  return(tmp)
}


update_FN122_waterhaul <- function(dbase){
  sql <- "UPDATE FN122 LEFT JOIN FN123
ON (FN122.EFF = FN123.EFF)
AND(FN122.SAM = FN123.SAM)
AND(FN122.PRJ_CD = FN123.PRJ_CD)
SET FN122.WATERHAUL = 'True'
WHERE (((FN123.PRJ_CD) Is Null));"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)
  return(odbcClose(conn))

}


update_FN125_stom_flag <- function(dbase){

  sql <- "UPDATE FN125 LEFT JOIN FN126
ON
FN126.FISH = FN125.FISH
AND FN126.SPC = FN125.SPC
AND FN126.GRP = FN125.GRP
AND FN126.EFF = FN125.EFF
AND FN126.SAM = FN125.SAM
AND FN126.PRJ_CD = FN125.PRJ_CD
SET FN125.STOM_FLAG = 'True'
WHERE FN126.PRJ_CD Is NOT Null;"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)

  return(odbcClose(conn))

}


update_FN125_age_flag <- function(dbase){

  sql <- "UPDATE FN125 LEFT JOIN FN127
ON
FN127.FISH = FN125.FISH
AND FN127.SPC = FN125.SPC
AND FN127.GRP = FN125.GRP
AND FN127.EFF = FN125.EFF
AND FN127.SAM = FN125.SAM
AND FN127.PRJ_CD = FN125.PRJ_CD
SET FN125.AGE_FLAG = 'True'
WHERE FN127.PRJ_CD Is NOT Null;"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)

  return(odbcClose(conn))

}


update_FN125_lam_flag <- function(dbase){

  sql <- "UPDATE FN125 LEFT JOIN FN125_Lamprey
ON
FN125_Lamprey.FISH = FN125.FISH
AND FN125_Lamprey.SPC = FN125.SPC
AND FN125_Lamprey.GRP = FN125.GRP
AND FN125_Lamprey.EFF = FN125.EFF
AND FN125_Lamprey.SAM = FN125.SAM
AND FN125_Lamprey.PRJ_CD = FN125.PRJ_CD
SET FN125.LAM_FLAG = 'True'
WHERE FN125_Lamprey.PRJ_CD Is NOT Null;"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)

  return(odbcClose(conn))

}

update_FN125_tag_flag <- function(dbase){
  # stom_flag
  sql <- "UPDATE FN125 LEFT JOIN FN125_tags
ON
FN125_tags.FISH = FN125.FISH
AND FN125_tags.SPC = FN125.SPC
AND FN125_tags.GRP = FN125.GRP
AND FN125_tags.EFF = FN125.EFF
AND FN125_tags.SAM = FN125.SAM
AND FN125_tags.PRJ_CD = FN125.PRJ_CD
SET FN125.TAG_FLAG = 'True'
WHERE FN125_tags.PRJ_CD Is NOT Null;"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)
  return(odbcClose(conn))

}

add_mode <- function(fn121, fn028){
  # populate the correct mode for each sam:
  x121 <- subset(fn121, select=c("PRJ_CD", "SAM", "GR", "GRUSE", "ORIENT"))
  x028 <- subset(fn028, select=c("PRJ_CD",  "GR", "GRUSE", "ORIENT", "MODE"))
  tmp <- merge(x121, x028, by=c("PRJ_CD","GR", "GRUSE", "ORIENT"))
  foo <- merge(fn121, tmp, by=c("PRJ_CD", "SAM", "GR", "GRUSE",
    "ORIENT"))

}
