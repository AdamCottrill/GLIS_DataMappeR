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


fetch_data <- function(table, prj_cd, src_db){
  DBConnection <- odbcConnectAccess2007(src_db,uid = "", pwd = "")
  stmt <- sprintf("EXEC get_%s @prj_cd='%s'", table, prj_cd)
  dat <- sqlQuery(DBConnection, stmt, as.is=TRUE, stringsAsFactors=FALSE, na.strings = "")
  odbcClose(DBConnection)
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
          safer = safer, append=append, nastring = NULL, verbose = verbose)
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

update_FN012_sizsam <- function(dbase){
  sql <- "UPDATE FN012 LEFT JOIN FN123
ON (FN012.SPC = FN123.SPC)
AND(FN012.GRP = FN123.GRP)
AND(FN012.PRJ_CD = FN123.PRJ_CD)
SET FN012.SIZSAM = 1
WHERE (((FN123.PRJ_CD) Is NOT Null));"
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
SET FN125.STOM_FLAG = '2'
WHERE FN126.PRJ_CD Is NOT Null;"
  conn <- odbcConnectAccess2007(dbase, uid = "", pwd = "")
  sqlQuery(conn, sql)

  return(odbcClose(conn))

}

