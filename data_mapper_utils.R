# function to get queries from Access mapper database

get_trg_table_names <- function(trg_db, table){
  DBConnection <- odbcConnectAccess2007(trg_db, uid = "", pwd = "")
  stmt <- sprintf("select * from [%s] where FALSE;", table)
  dat <- sqlQuery(DBConnection, stmt, as.is=TRUE, stringsAsFactors=FALSE, na.strings = "")
  odbcClose(DBConnection)
  return(toupper(names(dat)))
}

# checks column names for differences between the source and target and lists the missing/extra column(s) in 
# the console
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


# fetches data from Access based on the query name
fetch_data <- function(table, prj_cd, src_db, toupper=T){
  DBConnection <- odbcConnectAccess2007(src_db,uid = "", pwd = "")
  stmt <- sprintf("EXEC get_%s @prj_cd='%s'", table, prj_cd)
  dat <- sqlQuery(DBConnection, stmt, as.is=TRUE, stringsAsFactors=FALSE, na.strings = "")
  odbcClose(DBConnection)

  if (toupper)names(dat) <- toupper(names(dat))
  return(dat)
}


get_time <- function(datetime){
  # extract time from date/time string
  time_regex <- ".*([0-2][0-9]:[0-5][0-9]:[0-5][0-9])$"
  datetime <- gsub(time_regex, "\\1", datetime, perl = TRUE)
  msg <- "One or more values is not a valid time or datetime. The time component should be in the format HH:MM:SS."
  
  ifelse(!is.na(datetime) & !grepl(time_regex, datetime),
         stop(msg),
         datetime)
  
}


# function to append data to the template
append_data <- function(dbase, trg_table, data, append=T, safer=T,
                        toupper=T, verbose=F){
  if (toupper)names(data) <- toupper(names(data))

  field_check <- check_table_names(dbase, trg_table, data)
  if(length(field_check)) stop("Please fix field differences before proceeding.")

  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlSave(conn, data, tablename = trg_table, rownames = F,
          safer = safer, append = append, nastring = NULL, verbose = verbose)
  return(odbcClose(conn))

}



# helper function to split lamprey wound strings into individual wounds and the wound and diameter components
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



# adds one row per wound to the lamprey wound table
process_fn125_lamprey_lamijc <- function(df) {
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

# Checks for null values in LAMIJC and if all values are null (i.e. XLAM was used), returns empty columns for
# LAMIJC_TYPE and LAMIJC_SIZE
process_fn125_lamprey <- function(df){
  
  sub_df <- subset(df, select = "LAMIJC")
  
  w <- sapply(sub_df, function(sub_df)all(is.na(sub_df)))
  if (any(w)) {
    df$LAMIJC_TYPE <- NA
    df$LAMIJC_SIZE <- NA
    df <- subset(df, select = -c(LAMIJC))
    
    return(df)
  }
  
  else {
    process_fn125_lamprey_lamijc(df)
  }
  
  
}

# sets FN122.WATERHAUL values to TRUE or 0 based on values in the FN123 table
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

# series of functions to update stomach, age, lamprey, and tag flags in FN125 based on presence/absence of records in
# subsequent tables
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
