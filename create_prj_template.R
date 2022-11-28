# Use this script after creating a project in FN Portal with the wizard to
# populate a template database.
#
# Use one folder to store the R scripts. In that same folder, create the folders
# 'templates' and 'new_projects'. Put an empty copy of template 5 in the 'templates' folder. 
# The file paths should look like this:
#
# ./create_prj_template.R
# ./data_mapper_utils.R
# ./fn_portal_data_in.RPROJ (optional)
# ./templates/Great_Lakes_Assessment_Template_5.accdb
# ./new_projects/
#
# R. Henderson, November 2022

# Load packages
library(RODBC)
library(glfishr) # Use devtools::install_github("AdamCottrill/glfishr") to get the newest version.
                 # Remember that you can't be connected to VPN to download glfishr, but you must
                 # be connected to VPN to use the package!

# Load helper functions
source("./data_mapper_utils.R")


# Project set-up: Edit these details for your project
prj_cd <- 'LHA_IA22_000'
process_type <- "2"
protocol <- "BSM"
lake <- "HU"
lamsam <- "2"
overwrite <- TRUE


# File set-up: Only change if your file path/name differs - see lines 9-13
template_db <- "./templates/Great_Lakes_Assessment_Template_5.accdb"

trg_db <- file.path(dirname("./create_prj_template.R"), 'new_projects', paste0(prj_cd, "_template", '.accdb'))

file.copy(template_db, trg_db, overwrite=overwrite)


# After editing lines 27-32, run the rest of the code:

# Get design tables
all_spc <- get_species()
FN011 <- get_FN011(list(prj_cd = prj_cd))
#FN012 <- get_FN012(list(prj_cd = prj_cd))
FN012 <- get_FN012_protocol(list(protocol = protocol, lake = lake))
FN022 <- get_FN022(list(prj_cd = prj_cd))
FN026 <- get_FN026(list(prj_cd = prj_cd))
FN026_Subspace <- get_FN026_Subspace(list(prj_cd = prj_cd))
FN028 <- get_FN028(list(prj_cd = prj_cd))
gear_effort_process_types <- get_gear_process_types()
gear_effort_process_types <- subset(gear_effort_process_types, GR %in% FN028$GR & PROCESS_TYPE %in% process_type)

# Adjust some aspects of the design tables to fit the template
FN011$LAKE = FN011$LAKE.ABBREV
FN011$PRJ_LDR = paste(FN011$PRJ_LDR.FIRST_NAME, FN011$PRJ_LDR.LAST_NAME)
FN011 <- subset(FN011, select = -c(SOURCE, PRJ_LDR.USERNAME, PRJ_LDR.FIRST_NAME, PRJ_LDR.LAST_NAME, LAKE.LAKE_NAME,
                          LAKE.ABBREV))
FN012$PRJ_CD <- prj_cd
FN012 <- merge(FN012, all_spc)
FN012 <- subset(FN012, select = -c(PROTOCOL, LAKE, AGEDEC, SPC_NMSC))
FN026_Subspace <- merge(FN026_Subspace, FN026)
FN026_Subspace <- subset(FN026_Subspace, select = -c(SPACE_WT, SPACE_DES))

# Append data to template database
append_data(trg_db, 'FN011', FN011)
append_data(trg_db, 'FN012', FN012)
append_data(trg_db, 'FN022', FN022)
append_data(trg_db, 'FN026', FN026)
append_data(trg_db, 'FN026_Subspace', FN026_Subspace)
append_data(trg_db, 'FN028', FN028)
append_data(trg_db, 'Gear_Effort_Process_Types', gear_effort_process_types)
