# Author: Mark Barrett - mbarrett@rand.org
## Using the IRS 990 Master Concordance file to produce a list of all the possible xpaths for the variables we are interested in.
## This is an improved approach because it reduces the chance that I misinterpreted a variable name based on its xpath and mis-labeled it as a result.

## The previous approach was based on my own exploration of the 990 file, but recently I found that this has actually been done by many others, 
## given the scope of the problem.  In fact data extraction from the IRS 990 has become an industry.

# This script does the following:
# 1. reads in the master concordance file (see here: https://nonprofit-open-data-collective.github.io/irs-efile-master-concordance-file/),
# 2. Identifies the versions we have in our sample, and selects only variables from those versions.
# 3. Selects the variables we want to pull for each part of the form
# 4. Outputs a list structure where each element is a variable e.g. Filer EIN, 
#    which contains a list of xpaths that are associated with that variable across all the different versions


library(tidyverse)
library(openxlsx)

convert_to_paths_list = function(df){
  df %>% 
    # Remove columns with no extra information
    select(-form, -part, -location_code) %>% 
    # Remove duplicate rows (i.e. rows where the variable name and xpath are the same, but may be specifying different file versions)
    distinct(variable_name, xpath, .keep_all = T) %>% 
    mutate(xpath = paste0("/", xpath)) %>% 
    group_by(variable_name) %>% 
    nest(xpath) %>% 
    map(function(x) pluck(x))
}


# Clean concordance file --------------------------------------------------



cc = read_csv("efiler_master_concordance.csv")

# These are the versions in our subset of the 990 data
all_versions = future_map_chr(files_total, function(file) read_xml(file) %>% xml_attr("returnVersion"), .progress=T) %>% 
  tibble(version = .) %>% 
  count(version)

versions_pulled = all_versions %>% 
  mutate(version = str_replace_all(version, "\\.", "\\\\\\.")) %>% 
  pull(version)
versions = c("2011v1\\.2","2011v1\\.5","2012v2\\.0",
             "2012v2\\.1","2012v2\\.2","2012v2\\.3",
             "2013v3\\.0","2013v3\\.1","2012v3\\.0",
             "2013v4\\.0","2014v5\\.0")
cc_filtered = cc %>% 
  filter(form %in% c("F990", "SCHED-A", "SCHED-H", "SCHED-R"), str_detect(version, str_flatten(versions_pulled, collapse = "|"))|is.na(version)) %>% 
  # Remove variables which are completely empty (i.e. all NA, or all the same)
  select(-production_rule, -rdb_table, -cardinality, -last_version_modified)


# Get list of various xpaths for each variable in:
#     - Header
#     - Main form
#     - Schedule A
#     - Schedule H
#     - Schedule R


# Identify variables of interest ------------------------------------------
# This is done by going through PDFs of each schedule part, comparing to the concordance file,
# and selecting those which match the variables Jose specified in an email
head_other = c("F9_00_PZ_FORMORGASSN", "F9_00_PZ_FORMORGCORP", "F9_00_PZ_FORMORGOTHER", "F9_00_PZ_FORMORGOTHERDESC", "F9_00_PZ_FORMORGTRUST", 
               "F9_00_PZ_TYPEORGACORP","F9_00_PZ_EXEMPT501C3", "F9_00_PZ_EXEMPT501C", "F9_00_PZ_EXEMPT4947A1")
head_selected = c("F9_00_HD_FILEREIN", "F9_00_HD_FILERNAME1", "F9_00_HD_FILERPHONE", "F9_00_HD_FILERUS1", "F9_00_HD_FILERFOR1", "F9_00_HD_FILERUSCITY", "F9_00_HD_FILERFORCITY", 
                  "F9_00_HD_FILERUSSTATE", "F9_00_HD_FILERFORSTATE", "F9_00_HD_FILERUSZIP", "F9_00_HD_FILERFORPOST", "F9_00_HD_FILERFORCTRY", "F9_00_HD_TAXYEAR", "F9_00_PZ_FORMORGCORP", 
                  "F9_00_PZ_TYPEORGACORP", "F9_00_PZ_FORMORGASSN", "F9_00_PZ_FORMORGTRUST", "F9_00_PZ_FORMORGOTHER", "F9_00_PZ_FORMORGOTHERDESC", "F9_00_PC_YEARFORM", 
                  "F9_00_PC_DOMICILESTATE", "F9_00_PC_DOMICILECTRY")

## Main form variables
main_vars = c("F9_02_PZ_DISCONDISPO", 
              "F9_01_PZ_ORGMISS", "F9_03_PZ_MISSIONDESC", "F9_03_PZ_ORGMISS",
              "F9_01_EZ_CONGIFGRAETC", "F9_01_PC_CONTRGNTCY", "F9_01_PC_CONTRGNTPY", 
              "F9_01_PC_PROGSERREVPY", "F9_01_PZ_PROGSERREVCY",
              "F9_01_PC_INVESTMTINCPY", "F9_01_PZ_INVESTMTINCCY", "F9_01_PZ_INVINCCURYEA",
              "F9_01_PC_OTHREVPY", "F9_01_PZ_OTHREV", "F9_01_PZ_OTHREVCURYEA", "F9_01_PZ_OTHREVCY", "F9_03_PZ_OTHREVTOT",
              "F9_01_PC_TOTREVPY", "F9_01_PZ_TOTREVCURYEA", "F9_01_PZ_TOTREVCY", "F9_03_EZ_TOTREV",
              "F9_01_PC_GRNTSPAIDPY", "F9_01_PC_PYYGGRSIPAAI","F9_01_PZ_GRNTSPAIDCY", "F9_01_PZ_GRSIAMCYY",
              "F9_01_PC_BENSPDTOMEMSPY", "F9_01_PZ_BENSPDTOMEMSCY", "F9_01_PZ_BENSPDTOMEMSCY",
              "F9_01_PZ_PYSCEBPAID", "F9_01_PZ_SALCOMPBENS", "F9_01_PZ_SALCOMPBENSCY", "F9_01_PZ_SALETCCURYEA", "F9_01_PZ_SALETCPRIYEA",
              "F9_01_PC_PROFNDRSGEXPSCY", "F9_01_PC_PROFNDRSGEXPSPY",
              "F9_01_PC_TOTFNDRSGEXPSCY", "F9_01_PC_FUNDRSGEXPS",
              "F9_01_PC_OTHEXPSPY", "F9_01_PZ_OTHEXPCURYEA", "F9_01_PZ_OTHEXPSCY",
              "F9_01_PC_TOTEXPSCY", "F9_01_PC_TOTEXPSPY", "F9_01_PZ_TOTEXPCURYEA", "F9_01_PZ_TOTEXPSCY",
              "F9_01_PC_PYYRRELEEXXP", "F9_01_PC_RELEEXPRYEEA", "F9_01_PC_RELEEXCYY",
              "F9_01_PC_TOTASSETSEOY", "F9_01_PZ_TOASBOOYY", "F9_01_PZ_TOTASSETSBOY",
              "F9_01_PC_TOTLIABS", "F9_01_PZ_TOLIBOOYY", "F9_01_PZ_TOTLIABS",
              "F9_01_PC_NETASSFNDBALBOY", "F9_01_PZ_NAFBBOY", "F9_01_PZ_NETASSFNDBALEOY",
              # Part IV
              "F9_04_PC_DESCIN50C3", "F9_04_PC_DESINSSECIND",
              "F9_04_PC_HOSPITALOSPI", "F9_04_PZ_OPERATHOSPIT", "F9_05_EZ_DOEORGHAVHOS",
              "F9_04_PC_AUDFINSTMATT", "F9_04_PC_FINASTMTATTA",
              "F9_04_PC_TERMINATEOPS", 
              "F9_04_PC_PARTIALLIQ",
              "F9_04_PC_OWNDISRGDENT",
              "F9_04_PC_RELATEDENT",
              "F9_04_PC_CONTROLLEDENT", "F9_04_PZ_CONTROLLEDENT", "F9_04_PZ_RELORGCTRENT", "F9_04_PZ_TRANRELAENTI",
              "F9_04_PZ_TRANCONTRDENT", "F9_04_PC_TRANSFEXEMPTORG", "F9_06_EZ_TREXNOCHREOR",
              "F9_04_PC_ACTPARTNER")

main_selected = c("F9_01_PC_PYYRRELEEXXP", "F9_01_PC_RELEEXPRYEEA", "F9_01_PC_RELEEXCYY", "F9_03_PF_CINAFBTNAEOY", "F9_03_PF_CINAFBTNAFBE", "F9_02_PF_BSTNAEOY", 
                  "F9_02_PF_BSTNAOFBEOY", "F9_02_PF_BSTNAOFBEOY", "F9_03_PF_CINAFBTNABOY", "F9_03_PF_CINAFBTNAFBB", "F9_02_PF_BSTNABOY", "F9_02_PF_BSTNAOFBBOY", 
                  "F9_03_PZ_MISSIODESCES", "F9_03_PZ_MISSIODESCRI", "F9_03_PZ_MISSIONDESC", "F9_01_PZ_ORGMISS", "F9_05_EZ_DOEORGHAVHOS", "F9_04_PC_HOSPITALOSPI", 
                  "F9_04_PZ_OPERATHOSPIT", "F9_04_PC_OWNDISRGDENT", "F9_04_PC_RELATEDENT", "F9_04_PC_CONTROLLEDENT", "F9_04_PZ_CONTROLLEDENT")

## Sched A variables
a_vars = c("SA_01_PZ_HOSPITAIIIII", "SA_01_PZ_HOSPITALOSPI",
           "SA_01_PZ_HNASONBNLINE1", "SA_01_PZ_HNANBNLINE11",
           "SA_01_PZ_HNASONBNLINE2", "SA_01_PZ_HNANBNLINE22",
           "SA_01_PZ_MEDIRESEORGA", "SA_01_PZ_MEDRESORGAII",
           "SA_01_PZ_COLLEGORGANI", "SA_01_PZ_COLSUPORGAIV",
           "SA_01_PZ_SUPPORGAINDN", "SA_01_PZ_SUPPORORGANI3",
           "SA_01_PZ_SUPPORG5TYPE1", "SA_01_PZ_SUPORGTYPIND1",
           "SA_01_PZ_SUPPORG5TYPE2", "SA_01_PZ_SUPORGTYPIND2",
           "SA_01_PZ_SUORTYFUINNT", "SA_01_PZ_SUORTYFUININ",
           "SA_01_PZ_SUORTYNOFUUN", "SA_01_PZ_SUORTYNOFUIN",
           "SA_01_PZ_IRRSSWWRDEET", "SA_01_PZ_CERTIFICATIO", "SA_01_PZ_CERTIFCHECKB",
           "SA_01_PZ_TOTNUMSUPORG", 
           # Table
           "SA_01_PZ_SOINBNLINE11", "SA_01_PZ_SOISONBNLINE1",
           "SA_01_PZ_SOINBNLINE22", "SA_01_PZ_SOISONBNLINE2",
           "SA_01_PZ_SUORINEIINN", "SA_01_PZ_SUPPORGACNTN",
           "SA_01_PZ_SUORINORTYYP", "SA_01_PZ_SUORINTYOFOR",
           "SA_01_PZ_SOIGDLIND", "SA_01_PZ_SOILIGDOC",
           "SA_01_PZ_SUPORGINFAMO", "SA_01_PZ_SUPORGINFSUP",
           "SA_01_PZ_SUMAMOAMOUNT", "SA_01_PZ_SUPPORSUMUM")

a_selected = c("SA_01_PZ_HOSPITAIIIII", "SA_01_PZ_HOSPITALOSPI", "SA_01_PZ_MEDIRESEORGA", "SA_01_PZ_MEDRESORGAII")

## Sched R variables
r_vars = c("SH_01_PC_HOSPFACICNTN",
           "SH_05_PC_BHFNBNLINE11", "SH_05_PC_HOFAADADLIIN1", "SH_05_PC_BHFNBNLINE22", "SH_05_PC_HOFAADADLIIN2",
           "SH_05_PC_AGGEMEANSUUR", 
           "SH_05_PC_HFGMASIND", "SH_05_PC_ALICLICEHOSP", "SH_05_PC_HOFALIHOINND", 
           "SH_05_PC_ARESRESEFACI", "SH_05_PC_OHCFNHOHCFAS",
           "SH_05_PC_HFSHEIN", "SH_05_PC_HFSHNBNLINE11", "SH_05_PC_HOFATEHOINND", "SH_05_PC_HOSFACADDCIT", "SH_05_PC_HOSFACOTHDES",
           "SH_05_PC_NUMBHOSPFACI", "SH_05_PC_HOFAEMROHRIN", "SH_05_PC_HOFAEMROOTIN", 
           "SH_05_PC_HOFAADSTABBB", "SH_05_PC_HOFAADZIIPP","SH_05_PC_HOFACHHOINND",
           "SH_05_PC_HOFACRACHOIN")


# Pull out xpaths for relevant variables, by form part --------------------


## Header variables
head_paths = cc_filtered %>% 
  filter(scope == "HD"|variable_name %in% head_other) %>% 
  convert_to_paths_list()

head_xpath_list = set_names(head_paths$data, head_paths$variable_name) %>% 
  map(pull)


## Main form variables
main_paths = cc_filtered %>% 
  filter(scope != "HD", form == "F990", variable_name %in% main_vars) %>% 
  convert_to_paths_list()

main_xpath_list = set_names(main_paths$data, main_paths$variable_name) %>% 
  map(pull)


## Schedule A
a_paths = cc_filtered %>% 
  filter(scope != "HD", form == "SCHED-A", variable_name %in% a_vars) %>% 
  convert_to_paths_list()

a_xpath_list = set_names(a_paths$data, a_paths$variable_name) %>% 
  map(pull)


## Schedule H
h_paths = cc_filtered %>% 
  filter(scope != "HD", str_detect(xpath, "HospitalFacilities|ScheduleHPartVSectionA|ManagementCoAndJntVenturesGrp|ScheduleHPartIV")) %>% 
  convert_to_paths_list()

h_xpath_list = set_names(h_paths$data, h_paths$variable_name) %>% 
  map(pull)

## Schedule R
r_paths = cc_filtered %>% 
  filter(form == "SCHED-R", str_detect(xpath, "Form990ScheduleRPart(I{1,3}V?|VII)|Id(Disregarded|Related)|UnrelatedOrgTxblPartnership|SupplementalInformation")) %>% 
  convert_to_paths_list()

r_xpath_list = set_names(r_paths$data, r_paths$variable_name) %>% 
  map(pull)

## EIN Paths

ein_paths = cc_filtered %>% 
  filter(str_detect(description, "EIN")) %>% 
  convert_to_paths_list()

ein_xpath_list = set_names(ein_paths$data, ein_paths$variable_name) %>% 
  map(pull)

var_desc = bind_rows(head_paths,
                     main_paths,
                     a_paths,
                     h_paths,
                     r_paths,
                     ein_paths)

# Use concordance file to create data dictionary --------------------------

## Merge concordance file on  systems_990data.xlsx colnames for filtered data dictionary
# systems990 = readxl::read_excel("systems_990data.xlsx")

# head(sites_df2) %>%
#   # pull out column of unique variable names and join to concordance file on these names
#   gather(variable, value) %>%
#   mutate(variable = str_remove_all(variable, "_[0-9]{1,3}$")) %>%
#   distinct(variable) %>%
#   inner_join(cc, by = c("variable" = "variable_name")) %>% 
#   distinct(variable, location_code, .keep_all = T) %>% 
#   arrange(form, part, description)-> data_dict

# write.xlsx(data_dict, "systems990_dictionary.xlsx")
