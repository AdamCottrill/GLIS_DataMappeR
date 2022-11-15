library(RODBC)
# install.packages("devtools")
# library(devtools)
# install_github("AdamCottrill/glfishr")
library(glfishr)

source("./data_mapper_utils.R")

# Select the project code, process type, protocol, and lake (enter all as strings)
# Refer to the data dictionary for a list of process types and their definitions
prj_cd <- 'LHA_IA15_808'
process_type <- "2"
protocol <- "BSM"
lake <- "HU"
lamsam <- "2"
overwrite <- TRUE

# source database
#src_dbase <- "./HuronOffshoreMapper.accdb"
src_dbase <- "./NearshoreMapper_template4.accdb"
#src_dbase <- "./SmallfishMapper.accdb"

# template database:
template_db <- "./templates/Great_Lakes_Template_4.accdb"

trg_db <- file.path(dirname(src_dbase),'build', paste0(prj_cd, '.accdb'))

file.copy(template_db, trg_db, overwrite=overwrite)


fn011 <- fetch_data('FN011', prj_cd, src_dbase)
head(fn011)
fn011$PRJ_DATE0 <- as.Date(fn011$PRJ_DATE0)
fn011$PRJ_DATE1 <- as.Date(fn011$PRJ_DATE1)
append_data(trg_db, 'FN011', fn011)


fn012 <- fetch_data('FN012', prj_cd, src_dbase)
head(fn012)
names(fn012) <- toupper(names(fn012))
fn012_default <- get_FN012_protocol(list(lake = lake, protocol = protocol))
fn012_default$FLEN_MIN[is.na(fn012_default$FLEN_MIN)] <- 1
fn012_default$FLEN_MAX[is.na(fn012_default$FLEN_MAX)] <- 1999
fn012_default$TLEN_MIN[is.na(fn012_default$TLEN_MIN)] <- 1
fn012_default$TLEN_MAX[is.na(fn012_default$TLEN_MAX)] <- 1999
fn012_default$RWT_MIN[is.na(fn012_default$RWT_MIN)] <- 0.1
fn012_default$RWT_MAX[is.na(fn012_default$RWT_MAX)] <- 10000
fn012_default$K_MIN_ERROR[is.na(fn012_default$K_MIN_ERROR)] <- 0.05
fn012_default$K_MAX_ERROR[is.na(fn012_default$K_MAX_ERROR)] <- 0.05
fn012_default$K_MIN_WARN[is.na(fn012_default$K_MIN_WARN)] <- 3.9
fn012_default$K_MAX_WARN[is.na(fn012_default$K_MAX_WARN)] <- 3.9
           
#fn012_default <- unique(subset(fn012_default, select = c(-BIOSAM, -PROTOCOL)))
fn012_default <- subset(fn012_default, select = c(-BIOSAM))
fn012 <- merge(fn012, fn012_default, by = c("SPC", "GRP"), all.x = TRUE)
fn012 <- subset(fn012, select= c(-LAKE, -PROTOCOL, -AGEDEC))
fn012$GRP_DES[is.na(fn012$GRP_DES)] <- "default group"
fn012$SIZSAM[is.na(fn012$SIZSAM)] <- "0"
fn012$SIZATT[is.na(fn012$SIZATT)] <- "FLEN"
fn012$SIZINT[is.na(fn012$SIZINT)] <- 1
fn012$FDSAM[is.na(fn012$FDSAM)] <- 0
fn012$SPCMRK[is.na(fn012$SPCMRK)] <- "0"
fn012$AGEST[is.na(fn012$AGEST)] <- "0"
fn012$FLEN_MIN[is.na(fn012$FLEN_MIN)] <- 1
fn012$FLEN_MAX[is.na(fn012$FLEN_MAX)] <- 1999
fn012$TLEN_MIN[is.na(fn012$TLEN_MIN)] <- 1
fn012$TLEN_MAX[is.na(fn012$TLEN_MAX)] <- 1999
fn012$RWT_MIN[is.na(fn012$RWT_MIN)] <- 1
fn012$RWT_MAX[is.na(fn012$RWT_MAX)] <- 10000
fn012$K_MIN_ERROR[is.na(fn012$K_MIN_ERROR)] <- 0.05
fn012$K_MIN_WARN[is.na(fn012$K_MIN_WARN)] <- 3.9
fn012$K_MAX_ERROR[is.na(fn012$K_MAX_ERROR)] <- 0.05
fn012$K_MAX_WARN[is.na(fn012$K_MAX_WARN)] <- 3.9
fn012$LAMSAM <- lamsam
append_data(trg_db, 'FN012', fn012)


fn022 <- fetch_data('FN022', prj_cd, src_dbase)
head(fn022)
fn022$SSN_DATE0 <- as.Date(fn022$SSN_DATE0)
fn022$SSN_DATE1 <- as.Date(fn022$SSN_DATE1)
append_data(trg_db, 'FN022', fn022)


fn026 <- fetch_data('FN026', prj_cd, src_dbase)
head(fn026)
append_data(trg_db, 'FN026', fn026)

fn026_subspace <- fetch_data('FN026_Subspace', prj_cd, src_dbase)
fn026_subspace$SPACE <- "00"
fn026_subspace$SUBSPACE <- "00"
fn026_subspace$SUBSPACE_DES <- "Whole sampling area"
fn026_subspace <- merge(fn026_subspace, fn026)
fn026_subspace <- subset(fn026_subspace, select = -c(SPACE_DES, SPACE_WT))
head(fn026_subspace)
append_data(trg_db, "FN026_Subspace", fn026_subspace)


fn028 <- fetch_data('FN028', prj_cd, src_dbase)
head(fn028)
fn028$EFFTM0_GE <- get_time(fn028$EFFTM0_GE)
fn028$EFFTM0_LT <- get_time(fn028$EFFTM0_LT)
#increment mode here:
fn028$MODE <- paste0(seq(1, nrow(fn028)), 0)
append_data(trg_db, 'FN028', fn028)


# Get list of gear/effort/process types from the glfishr package (requires VPN connection)
gear_effort_process_types <- get_gear_process_types()
gear_effort_process_types <- subset(gear_effort_process_types, GR %in% fn028$GR & PROCESS_TYPE %in% process_type)

# Filter out unnecessary combos
#gear_effort_process_types <- subset(gear_effort_process_types, GR != "SIN1" | PROCESS_TYPE != "1")

append_data(trg_db, 'Gear_Effort_Process_Types', gear_effort_process_types)


fn121 <- fetch_data("FN121", prj_cd, src_dbase)
fn121 <- add_mode(fn121, fn028)
fn121$SSN <- "00"
fn121 <- merge(fn121, unique(subset(gear_effort_process_types, select = -c(EFFDST, EFF))))
fn121$EFFDT0 <- as.Date(fn121$EFFDT0)
fn121$EFFDT1 <- as.Date(fn121$EFFDT1)
fn121$EFFTM0 <- get_time(fn121$EFFTM0)
fn121$EFFTM1 <- get_time(fn121$EFFTM1)
fn121$SUBSPACE <- "00"
fn121 <- subset(fn121, select=-c(GR, GRUSE, ORIENT))
head(fn121)
append_data(trg_db, 'FN121', fn121)



fn122 <- fetch_data('FN122', prj_cd, src_dbase)
head(fn122)
append_data(trg_db, 'FN122', fn122)


fn123 <- fetch_data('FN123', prj_cd, src_dbase)
head(fn123)
append_data(trg_db, 'FN123', fn123)
update_FN122_waterhaul(trg_db)
update_FN012_sizsam(trg_db)


fn125 <- fetch_data('FN125', prj_cd, src_dbase)
head(fn125)
append_data(trg_db, 'FN125', fn125)


fn125_tags <- fetch_data("FN125_tags", prj_cd, src_dbase)
#we might need to increment FISH_TAGID here
head(fn125_tags)
append_data(trg_db, 'FN125_tags', fn125_tags)


fn125_lamprey <- fetch_data('FN125_lamprey', prj_cd, src_dbase)
fn125_lamprey <- process_fn125_lamprey(fn125_lamprey)
head(fn125_lamprey)
append_data(trg_db, 'FN125_lamprey', fn125_lamprey)


fn126 <- fetch_data('FN126', prj_cd, src_dbase)
head(fn126)
append_data(trg_db, 'FN126', fn126)
update_FN125_stom_flag(trg_db)


fn127 <- fetch_data('FN127', prj_cd, src_dbase)
head(fn127)
append_data(trg_db, 'FN127', fn127)

