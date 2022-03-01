library(RODBC)
# install.packages("devtools")
# library(devtools)
# install_github("AdamCottrill/glfishr")
library(glfishr)

source("./data_mapper_utils.R")

# Select the project code and process type (enter both as strings)
# Refer to the data dictionary for a list of process types and their definitions
prj_cd <- 'LHA_IA20_813'
process_type <- "2"
overwrite <- TRUE

# source database
src_dbase <- "../../data_modernization/fn_portal_data_in/NearshoreMapper_template3.accdb"

#src_dbase <-"C:/Users/COTTRILLAD/1work/Python/djcode/apps/fn_portal/utils/data_upload_src/OffshoreMapper.accdb"

# template database:
template_db <- "../../data_modernization/fn_portal_data_in/templates/Great_Lakes_Template3_template.accdb"

trg_db <- file.path(dirname(src_dbase),'build', paste0(prj_cd, '.accdb'))

file.copy(template_db, trg_db, overwrite=overwrite)


fn011 <- fetch_data('FN011', prj_cd, src_dbase)
head(fn011)
fn011$PRJ_DATE0 <- as.Date(fn011$PRJ_DATE0)
fn011$PRJ_DATE1 <- as.Date(fn011$PRJ_DATE1)
append_data(trg_db, 'FN011', fn011)


fn012 <- fetch_data('FN012', prj_cd, src_dbase)
head(fn012)
append_data(trg_db, 'FN012', fn012)


fn022 <- fetch_data('FN022', prj_cd, src_dbase)
head(fn022)
fn022$SSN_DATE0 <- as.Date(fn022$SSN_DATE0)
fn022$SSN_DATE1 <- as.Date(fn022$SSN_DATE1)
append_data(trg_db, 'FN022', fn022)


fn026 <- fetch_data('FN026', prj_cd, src_dbase)
head(fn026)
append_data(trg_db, 'FN026', fn026)


fn028 <- fetch_data('FN028', prj_cd, src_dbase)
head(fn028)
fn028$EFFTM0_GE <- get_time(fn028$EFFTM0_GE)
fn028$EFFTM0_LT <- get_time(fn028$EFFTM0_LT)
#increment mode here:
fn028$MODE <- paste0(seq(1, nrow(fn028)), 0)
append_data(trg_db, 'FN028', fn028)


# Get list of gear/effort/process types from the glfishr package (requires VPN connection)
gear_effort_process_types <- get_gear_process_types()
gear_effort_process_types <- subset(gear_effort_process_types, GR %in% fn028$GR & PROCESS_TYPE==process_type)

append_data(trg_db, 'Gear_Effort_Process_Types', gear_effort_process_types)


fn121 <- fetch_data("FN121", prj_cd, src_dbase)
fn121 <- add_mode(fn121, fn028)
fn121$SSN <- "00"
fn121$SPACE <- "00"
fn121$PROCESS_TYPE <- process_type
fn121$EFFDT0 <- as.Date(fn121$EFFDT0)
fn121$EFFDT1 <- as.Date(fn121$EFFDT1)
fn121$EFFTM0 <- get_time(fn121$EFFTM0)
fn121$EFFTM1 <- get_time(fn121$EFFTM1)

fn121 <- subset(fn121, select=-c(GR,GRUSE, ORIENT))
head(fn121)
append_data(trg_db, 'FN121', fn121)


fn122 <- fetch_data('FN122', prj_cd, src_dbase)
head(fn122)
append_data(trg_db, 'FN122', fn122)


fn123 <- fetch_data('FN123', prj_cd, src_dbase)
head(fn123)
append_data(trg_db, 'FN123', fn123)
update_FN122_waterhaul(trg_db)


fn125 <- fetch_data('FN125', prj_cd, src_dbase)
head(fn125)
append_data(trg_db, 'FN125', fn125)


fn125_tags <- fetch_data("FN125_tags", prj_cd, src_dbase)
#we might need to increment FISH_TAGID here
head(fn125_tags)
append_data(trg_db, 'FN125_tags', fn125_tags)
update_FN125_tag_flag(trg_db)


fn125_lamprey <- fetch_data('FN125_lamprey', prj_cd, src_dbase)
fn125_lamprey <- process_fn125_lamprey(fn125_lamprey)
head(fn125_lamprey)
append_data(trg_db, 'FN125_lamprey', fn125_lamprey)
update_FN125_lam_flag(trg_db)


fn126 <- fetch_data('FN126', prj_cd, src_dbase)
head(fn126)
append_data(trg_db, 'FN126', fn126)
update_FN125_stom_flag(trg_db)


fn127 <- fetch_data('FN127', prj_cd, src_dbase)
head(fn127)
append_data(trg_db, 'FN127', fn127)
update_FN125_age_flag(trg_db)