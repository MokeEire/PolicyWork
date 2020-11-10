## Script to read IRS990 forms into dataframe using furrr
## Author: Mark Barrett
## Start date: Feb 27 2019
## Last edit date: Sep 04 2019

## Libraries
options(repos=structure(c(CRAN="http://cran.rand.org")))
library(tidyverse)
library(here)
library(xml2)
library(furrr)
library(magrittr)
library(tictoc)
library(openxlsx)
plan(multiprocess)
# library(igraph)
# library(ggraph)


# Functions ---------------------------------------------------------------

extract_from_varlist = function(files, varlist){
  ## This function takes two arguments:
  #   files => a list of file paths for IRS990 XML files
  #   varlist => a list of variables of the structure
  #         $ VARNAME   : chr xpath 1, xpath 2, ..., xpath N
  #
  #   This function maps over each file and does the following in each case:
  #   1. Read in the XML file
  #   2. Strip the XML namespace
  #   For each variable in varlist, the following happens in each case:
  #       a. Search for each xpath associated with the variable
  #       b. Convert to text IF the result is a single element (leaving multiple elements as XML nodes)
  #   3. Remove empty elements at the second level of the list (i.e. the xpath level) and flatten this list
  #   4. Remove empty elements at the first level of the list (i.e. the variable level)
  #   5. Convert remaining XML nodes to text
  #   6. Convert to two column dataframe (variable, value)
  #   7. Unnest each list element in the value column (make each element its own row)
  #   8. Count the appearance of each variable in the return and append a count in cases 
  #      when a variable occurs more than once
  #   9. Create a filename variable that indicates the return's filepath
  
  returns_df = future_map_dfr(files, 
                              function(file) {
                                # 1. Read in the XML file
                                file_xml = read_xml(file) %>% 
                                  # 2. Strip the XML namespace
                                  xml_ns_strip()
                                
                                # For each variable in varlist:
                                map(varlist, 
                                    function(var) {
                                      map(var, 
                                          function(path) file_xml %>% 
                                            # a. Search for each xpath associated with the variable
                                            xml_find_all(path) %>% 
                                            # b. Convert to text IF the result is a single element (leaving multiple elements as XML nodes)
                                            when(xml_length(.) < 1 ~ xml_text(.),
                                                 T ~ (.)))
                                    }) %>% 
                                  # 3. Remove empty elements at the second level of the list (i.e. the xpath level) and flatten this list
                                  modify_depth(1, function(x) compact(x) %>% flatten()) %>% 
                                  # 4. Remove empty elements at the first level of the list (i.e. the variable level)
                                  compact() %>% 
                                  # 5. Convert remaining XML nodes to text
                                  map(., function(list) map_if(list, ~class(.) == "xml_node", function(element) xml_text(element))) %>% 
                                  # 6. Convert to two column dataframe (variable, value)
                                  enframe() %>% 
                                  # 7. Unnest each list element in the value column (make each element its own row)
                                  unnest() %>% 
                                  unnest() %>% 
                                  # 8. Count the appearance of each variable in the return and append a count in cases 
                                  #    when a variable occurs more than once
                                  when(nrow(.) > 0 ~ (.) %>% 
                                         group_by(name) %>% 
                                         mutate(n = row_number(),
                                                n_max = max(n),
                                                num = case_when(n_max > 99 ~ paste0("_", str_pad(n, 3, pad = "0")),
                                                                n_max > 1 & n > 1 ~ paste0("_", str_pad(n, 2, pad = "0")),
                                                                T ~ "")) %>% 
                                         select(-n:-n_max) %>% 
                                         unite(name, name, num, sep = "") %>% 
                                         spread(name, value),
                                       T ~ tibble()) %>% 
                                  # 9. Create a filename variable that indicates the return's filepath
                                  mutate(filename = str_remove_all(file, "/home/mbarrett/u19-irs-data/"))
                              }, .progress = T
  )
  
  # Arrange columns by filer, and tax year
  returns_df = returns_df %>% 
    select(filename, everything()) %>% 
    select(F9_00_HD_FILERNAME1, F9_00_HD_FILEREIN, sort(everything())) %>% 
    arrange(F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR)
  
  return(returns_df)
}

# Duplicate above function, but for one file
extract_xml = function(filepath, varlist){
  file_xml = read_xml(filepath) %>% 
    # 2. Strip the XML namespace
    xml_ns_strip()
  
  # For each variable in varlist:
  map(varlist, 
      function(var) {
        map(var, 
            function(path) file_xml %>% 
              # a. Search for each xpath associated with the variable
              xml_find_all(path) %>% 
              # b. Convert to text IF the result is a single element (leaving multiple elements as XML nodes)
              when(xml_length(.) < 1 ~ xml_text(.),
                   T ~ (.)))
      }) %>% 
    # 3. Remove empty elements at the second level of the list (i.e. the xpath level) and flatten this list
    modify_depth(1, function(x) compact(x) %>% flatten()) %>% 
    # 4. Remove empty elements at the first level of the list (i.e. the variable level)
    compact() %>% 
    # 5. Convert remaining XML nodes to text
    map(., function(list) map_if(list, ~class(.) == "xml_node", function(element) xml_text(element))) %>% 
    # 6. Convert to two column dataframe (variable, value)
    enframe() %>% 
    # 7. Unnest each list element in the value column (make each element its own row)
    unnest() %>% 
    unnest() %>% 
    # 8. Count the appearance of each variable in the return and append a count in cases 
    #    when a variable occurs more than once
    when(nrow(.) > 0 ~ (.) %>% 
           group_by(name) %>% 
           mutate(n = row_number(),
                  n_max = max(n),
                  num = case_when(n_max > 99 ~ paste0("_", str_pad(n, 3, pad = "0")),
                                  n_max > 1 & n > 1 ~ paste0("_", str_pad(n, 2, pad = "0")),
                                  T ~ "")) %>% 
           select(-n:-n_max) %>% 
           unite(name, name, num, sep = "") %>% 
           spread(name, value),
         T ~ tibble()) %>% 
    # 9. Create a filename variable that indicates the return's filepath
    mutate(filename = str_remove_all(filepath, "/home/mbarrett/u19-irs-data/"))
  
}

# Function for collecting EINs and searching those too
collect_related_eins = function(dataframe){
  # Search all EIN variables and collect unique EINs
  related_eins = dataframe %>% 
    select(matches(paste(names(ein_xpath_list), collapse = "|"))) %>% 
    gather(variable, ein, -F9_00_HD_FILEREIN) %>% 
    distinct(ein) %>% 
    filter(!is.na(ein)) %>% 
    pull()
  
  # Search the Return headers for the related EINs and pull out their filenames
  related_files = all_headers %>% 
    filter(F9_00_HD_FILEREIN %in% related_eins) %>% 
    pull(filename)
  
  # pull the related filenames
  related_df = extract_from_varlist(related_files, all_xpaths)
  
  return(related_df)
}

# Create row of variable descriptions for the excel file
get_var_descriptions = function(data){
  # Requires:
  #     - All xpath lists to be loaded
  #     - Master concordance file to be loaded
  #     - A dataframe created by extract_from_varlist() function
  
  # Get names from data
  df_names = data %>% 
    slice(1) %>% 
    gather(df_varname, value) %>% 
    # Extract the base variable name to join to the concordance file
    mutate(base_variable_name = str_remove_all(df_varname, "_[0-9]{2}$")) %>% 
    distinct(df_varname, base_variable_name)
  
  # Get descriptions from concordance file
  var_desc = bind_rows(header_paths,
                       main_paths,
                       a_paths,
                       h_paths,
                       r_paths,
                       ein_paths) %>%
    distinct(variable_name, .keep_all = T) %>% # This is letting dplyr arbitrarily choose one description where there are multiple.  
    filter(!is.na(description))                # Seems unimportant now, but in case descriptions are inconsistent later, this is a likely source
  
  # Join the colnames to their respective descriptions
  joined_descriptions = df_names %>% 
    left_join(var_desc, 
              by = c("base_variable_name" = "variable_name")) %>% 
    distinct(df_varname, description) %>% 
    replace_na(list(description = NA_character_))
  
  # Output row of labels
  df_labels = joined_descriptions %>% 
    spread(df_varname, description) %>% 
    # Order labels by original df
    select(colnames(data))
  
  return(df_labels)
}


# Function for outputting 990 returns dataframe to excel
write_990excel = function(dataframe, filename, wb_name = "Workbook", ...){
  if(missing(filename)){
    stop("You need to provide a filename")
  }
  wb990 = createWorkbook(wb_name)
  addWorksheet(wb990, "990 Returns")
  ##TODO: Add in style
  headStyle = createStyle(fontSize = 12, textDecoration = "bold", border = "bottom", borderStyle = "thick", borderColour = "mediumseagreen")
  labStyle = createStyle(fontSize = 12, wrapText = T)
  
  # 1. Write column names & labels
  df_labels = get_var_descriptions(dataframe)
  writeData(wb990, df_labels, sheet = "990 Returns", headerStyle = headStyle)
  addStyle(wb990, sheet = 1, style = labStyle, rows = 2, cols = 1:ncol(dataframe))
  
  
  # 2. Write data
  writeData(wb990, dataframe, sheet = "990 Returns",
            startRow = 3, colNames = F)
  
  setColWidths(wb990, sheet = 1, cols = 1:ncol(dataframe), widths = "auto")
  freezePane(wb990, sheet = 1, firstActiveRow = 3, firstActiveCol = 3)
  
  # 3. Output file
  saveWorkbook(wb990, file = filename, ...)
}


get_xpaths = function(files){
  ## This function produces a list of xpaths for variables of interest containing all of their associated xpaths.
  ##  The master concordance data file (the source for producing these xpaths) can be found here: https://nonprofit-open-data-collective.github.io/irs-efile-master-concordance-file/data_dictionary.html
  
  ##  Arguments:
  ##  files       a list of files to be extracted
  
  
  ## 1. Find the Return versions we are using
  versions = future_map_chr(files, 
                            function(file) read_xml(file) %>% xml_attr("returnVersion"), .progress=T) %>% 
    tibble(version = .) %>% 
    count(version) %>% 
    # Add in backslashes so the versions can be used in regex
    mutate(version = str_replace_all(version, "\\.", "\\\\\\.")) %>% 
    pull(version)
  
  ## 2. Filter the concordance file to only variables which appear in one of the forms of interest and match a version we are using
  cc_filtered = cc %>% 
    filter(form %in% c("F990", "SCHED-A", "SCHED-H", "SCHED-R"), 
           str_detect(version, str_flatten(versions, collapse = "|"))|is.na(version)) %>% 
    # remove unnecessary variables
    select(-production_rule, -rdb_table, -cardinality, -last_version_modified)
  
  ## 3. Define variables of interest
  
  ## Header variables
  # We actually pull in all variables whose scope in the concordance file is "HD", 
  #   but there are some header variables not in scope hd
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
  
  ## 4. Pull the xpaths of the specified variables above
  
  head_paths = cc_filtered %>% 
    # Select rows with scope HD or where the name is in the other header variables
    filter(scope == "HD"|variable_name %in% head_other) %>% 
    convert_to_paths_list()
  
  head_xpath_list = set_names(head_paths$data, head_paths$variable_name) %>% 
    map(pull)
  
  ## Main Form
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
  
  return(splice(head_xpath_list, main_xpath_list, a_xpath_list, h_xpath_list, r_xpath_list))
  
  
}


# Function used inside get_xpaths to convert the filtered concordance dataframe to a list of xpaths
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



# Data --------------------------------------------------------------------


## Files list for each year
files_2013 = here("2013", list.files("2013"))
files_2014 = here("2014", list.files("2014"))
files_2015 = here("2015v2", list.files("2015v2"))
files_2016 = here("2016", list.files("2016", pattern = "\\.xml$")) 
#1798 has a missing bracket somewhere it seems
files_2016 = files_2016[-1798]
files_2017 = here("2017", list.files("2017", pattern = "\\.xml$"))
# A number of problems in 2017: 1607, 1778, 1822, 1840, 1845, 2500
files_2017 = files_2017[-c(1607, 1778,1822, 1840, 1845, 2500)]
files_total = c(files_2013, files_2014, files_2015, files_2016, files_2017)



# Load xpaths -------------------------------------------------------------

source('master_concordance_xpaths.R')

# Extract Return Headers --------------------------------------------------

# Test out the function on a small sample
test_sample = sample(files_total, 100)

# Get a dataframe of all the return headers: 15-20 mins
tic()
test_headers = future_map_dfr(test_sample, extract_xml, varlist = head_xpath_list, .progress = T) 
toc()

if("all_headers.RData" %in% list.files()){
  load("all_headers.RData")
} else {
  tic()
  all_headers = future_map_dfr(files_total, extract_xml, varlist = head_xpath_list, .progress = T)
  toc()
}
tic()
all_headers = future_map_dfr(files_total, extract_xml, varlist = head_xpath_list, .progress = T)
toc()
# save(all_headers, file = "all_headers.RData")


# Select sites ------------------------------------------------------------


# List of site names to look for
site_names = c(
  # California
  "Cedars-?\\s?Sinai",  "Citrus Valley",  "Dignity",  "John Muir",  "Loma Linda",  
  "Memorial\\s?Care|Hospital of Long Beach", "Greater Newport Physicians",
  "PIH|InterHealth|presbyterian inter",  "Sharp",  "Stanford",  "Sutter",  "UC Davis",  "UC San Diego",
  # Minnesota
  "Allina",  "Health\\s?Partners",  "Hennepin",  
  "MHealth", "University of Minnesota", "Fairview Health Services",
  "St Luke's Hospital of Duluth", 
  # Washington
  "EVERGREENHEALTH",  "MultiCare",  "Virginia Mason",
  # Wisconsin
  "Aspirus",  "Bellin\\b",  "Froedtert",  "Marshfield",  "Mayo Clinic",  "ThedaCare"
)

# Filter on the site names above
# Keep filers from the states we are interested in
# Remove obviously irrelevant cases
site_headers = all_headers %>% 
  filter(str_detect(tolower(F9_00_HD_FILERNAME1), str_flatten(tolower(site_names), collapse = "|")),
         F9_00_HD_FILERUSSTATE %in% c("CA", "MN", "WA", "WI"),
         # Remove clearly unassociated returns
         !str_detect(tolower(F9_00_HD_FILERNAME1), tolower("S(ain)?T JOSEPH|San Jose|museum|alumni association|SEVENTH-DAY ADVENTISTS LOMA LINDA|AMERICAN LEGION EVERGREEN|FRIENDS OF THE STANFORD DAILY|ARCH HEALTH PARTNERS INC|Pioneer Memorial Care Center Foundation"))) %>% 
  arrange(F9_00_HD_FILERUSSTATE, F9_00_HD_FILERNAME1)

# save(site_headers, file = "site_headers.RData")

# Pull the filenames of these returns
site_files = site_headers %>% 
  mutate(filename = paste0("/home/mbarrett/u19-irs-data/", filename)) %>% 
  pull(filename)


# write_990excel(site_headers, "site_headers.xlsx", overwrite = T)

#cedars = "/home/mbarrett/u19-irs-data/2013/201321589349300532_public.xml"

# Create dataframe of the 990 returns for selected systems: 1 min
tic()
sites_full_returns = future_map_dfr(site_files, extract_xml, get_xpaths(site_files), .progress = T)
toc()
# save(sites_full_returns, file = "sites_returns.RData")
# write_990excel(sites_df2, "Returns990_AllSites.xlsx", overwrite = T)



removed_headers = all_headers %>% 
  filter(str_detect(tolower(F9_00_HD_FILERNAME1), str_flatten(tolower(site_names), collapse = "|")),
         F9_00_HD_FILERUSSTATE %in% c("CA", "MN", "WA", "WI"),
         # Remove clearly unassociated returns
         str_detect(tolower(F9_00_HD_FILERNAME1), tolower("S(ain)?T JOSEPH|San Jose|museum|alumni association"))) %>% 
  arrange(F9_00_HD_FILERUSSTATE, F9_00_HD_FILERNAME1)

#write.xlsx(removed_headers, "irrelevant_cases.xlsx")

# Create dataframe of all the 990 returns we have: 12.5 hours
tic()
all_irs990_df = future_map_dfr(files_total, extract_xml, all_xpaths)
toc()








# Dignity only ------------------------------------------------------------


annotated_vars = c("F9_00_HD_FILEREIN","F9_00_HD_FILERNAME1","F9_00_HD_FILERUSCITY","F9_00_HD_FILERUSSTATE","F9_00_HD_TAXYEAR",
                   "F9_00_PZ_EXEMPT501C3","F9_01_PZ_ORGMISS","F9_03_PZ_MISSIONDESC","F9_04_PC_ACTPARTNER","F9_04_PC_CONTROLLEDENT", "F9_04_PZ_CONTROLLEDENT",
                   "F9_04_PC_HOSPITALOSPI","F9_04_PC_OWNDISRGDENT","F9_04_PC_RELATEDENT","SA_01_PZ_HOSPITAIIIII","F9_04_PZ_OPERATHOSPIT","F9_05_EZ_DOEORGHAVHOS",
                   "SA_01_PZ_HOSPITALOSPI","F9_04_PZ_CONTROLLEDENT","SA_01_PZ_SOINBNLINE11","SA_01_PZ_SUORINEIINN","SA_01_PZ_SUORINTYOFOR",
                   "SA_01_PZ_SUPPORORGANI3","SA_01_PZ_TOTNUMSUPORG","SA_01_PZ_SOISONBNLINE1","SA_01_PZ_SUORINORTYYP","SA_01_PZ_SUPPORGAINDN",
                   "SA_01_PZ_HNASONBNLINE1","SA_01_PZ_MEDIRESEORGA",
                   "SH_05_PC_NUMBHOSPFACI","SH_05_PC_HOSFACOTHDES","SH_05_PC_HOFACRACHOIN","SH_05_PC_HOFAEMROOTIN","SH_05_PC_HOFACHHOINND",
                   "SH_01_PC_HOSPFACICNTN","SH_05_PC_HOFAEMROHRIN","SH_05_PC_HOFALIHOINND","SH_05_PC_HOFAREFAINND","SH_05_PC_HOFASTLINUUM",
                   "SH_05_PC_HOFATEHOINND","SH_05_PC_HOSFACFACNUM","SH_05_PC_HOSFACWEBADD",
                   "SR_01_PC_IDEDCENBNLIN1","SR_01_PC_IDEDENBNLINE1","SR_01_PC_DICOENNAA", "SR_01_PC_IDEDCNA","SR_01_PC_DCENBNLINE11","SR_01_PC_NODEBNLINE11",
                   "SR_02_PC_DICOENNAA","SR_02_PC_IRTEODCNA","SR_02_PC_IRTEODCENBNL1","SR_02_PC_IRTEODENBNLI1","SR_02_PC_DCENBNLINE11","SR_02_PC_NODEBNLINE11",
                   "SR_03_PC_IROPDCENBNLI1","SR_03_PC_IROPRONBNLIN1","SR_03_PC_DICOENNAA", "SR_03_PC_IROPDCNA","SR_03_PC_NOROBNLINE11","SR_03_PC_DCENBNLINE11",
                   "SR_04_PC_DICOENNAA", "SR_04_PC_IROCTDCNA", "SR_04_PC_IROCTDCENBNL1","SR_04_PC_DCENBNLINE11", "SR_04_PC_NOROBNLINE11", "SR_04_PC_IROCTRONBNLI1")


## 1. Get the return headers of dignity only
dignity_headers = all_headers %>% 
  filter(str_detect(tolower(F9_00_HD_FILERNAME1), "dignity"),
         F9_00_HD_FILERUSSTATE %in% c("CA", "MN", "WA", "WI")) %>% 
  arrange(F9_00_HD_FILERUSSTATE, F9_00_HD_FILERNAME1)

## 2. Pull the filenames of these returns
dignity_files = dignity_headers %>% 
  mutate(filename = paste0("/home/mbarrett/u19-irs-data/", filename)) %>% 
  pull(filename)


tic()
dignity_df = extract_from_varlist(dignity_files, all_xpaths)
toc()


dignity_related_eins = collect_related_eins(dignity_df)


# save(dignity_related_eins, file = "dignity_df.RData")
# write.xlsx(dignity_related_eins, "Returns990_DignityAndRelated.xlsx")

# Pull only the EIN numbers for Mark
dignity_eins = dignity_related_eins %>% 
  select(matches(paste(names(ein_xpath_list), collapse = "|"))) %>% 
  gather(variable, ein, -F9_00_HD_FILEREIN) %>% 
  distinct(ein) %>% 
  filter(!is.na(ein)) %>% 
  pull()
# write_lines(dignity_eins, "EINs_DignityAndRelated.txt")


# Output to Excel

dignity_related_eins_annotated = dignity_related_eins %>% 
  select(matches(paste(annotated_vars, collapse = "|")))


# write_990excel(dignity_related_eins, "Returns990_DignityAndRelated.xlsx", overwrite = T)


write_990excel(dignity_related_eins_annotated, "Returns990_DignityAndRelated_Annotated.xlsx", overwrite = T)



# Beth Israel only --------------------------------------------------------

# 1) find the returns associated with each EIN
# 2) find returns which contain any of the EINs in the myriad of EIN variables
# 3) find returns related to all these

beth_eins = c("042103881", "043228556", "222768204", "320058309", "362177139", "522228444") 

# Find the returns which match one of these eins
beth_headers = all_headers %>% 
  filter(F9_00_HD_FILEREIN %in% beth_eins)

beth_headers %>% distinct(F9_00_HD_FILEREIN) %>% pull() # We only find two organizations
beth_headers %>% distinct(F9_00_HD_FILERNAME1)
beth_files = beth_headers %>% distinct(filename) %>% pull()

beth_df = extract_from_varlist(beth_files, all_xpaths)

beth_related_df = collect_related_eins(beth_df)
beth_related_df %>% 
  select(matches(paste(ein_paths %>% distinct(variable_name) %>% pull(), collapse = "|"))) %>% 
  gather(variable, ein) %>% 
  distinct(ein) %>% 
  filter(ein %in% beth_eins) %>% 
  pull() # We find four organizations

beth_related_df %>% 
  # Select filer and all ein variables
  select(F9_00_HD_FILERNAME1, matches(paste(ein_paths %>% distinct(variable_name) %>% pull(), collapse = "|"))) %>% 
  # pivot to key:ein pairs
  gather(variable, ein, -F9_00_HD_FILERNAME1) %>% 
  # Keep only EINs we are interested in
  filter(ein %in% beth_eins) %>% 
  distinct(F9_00_HD_FILERNAME1, variable, ein) %>% 
  spread(variable, ein) %>% 
  bind_rows(get_var_descriptions(.), .) %>% 
  View()

beth_related_df %>% 
  filter(F9_00_HD_FILEREIN %in% beth_eins) %>% 
  distinct(F9_00_HD_FILERNAME1)

arizona_hosp = "860800150"
arizona_headers = all_headers %>% 
  filter(F9_00_HD_FILEREIN == arizona_hosp)
arizona_headers %>% distinct(F9_00_HD_FILEREIN) %>% pull() # We only find two organizations
arizona_headers %>% distinct(F9_00_HD_FILERNAME1)
arizona_files = beth_headers %>% distinct(filename) %>% pull()

arizona_df = extract_from_varlist(beth_files, all_xpaths)

## Potential network data

jointventure_vars = str_c(c(
  # Part IV Section / Management Co & Joint Ventures
  "SH_04_PC_MCJVENBNLINE1", "SH_04_PC_NAOFENBUNALI1",
  "SH_04_PC_DESENTPRIACT", "SH_04_PC_MACOJNVEPRAC"
  # "SH_04_PC_MCJVPPOOPCT", "SH_04_PC_PHYPROOROOWN",
  # "SH_04_PC_MCJVOEPOOPCT", "SH_04_PC_OFETPROROWWN",
  # "SH_04_PC_MCJVOPOOPCT", "SH_04_PC_ORGPROOROOWN"
), collapse = "|")

hospfacility_vars = str_c(c(
  # Part V Section A / Hospital Facilities
  # "SH_05_PC_HOFASTLINUUM",
  "SH_05_PC_HOSFACFACNUM", "SH_05_PC_AFACFACINUMB",
  "SH_05_PC_HOFAFAREGRRO", "SH_05_PC_AFAFACREPGRO",
  "SH_05_PC_HFBNBNLINE11", "SH_05_PC_HFSHNBNLINE11", "SH_05_PC_ANNABUNALIIN1", "SH_05_PC_HFPPHFNBNLIN1",
  # subordinate hospital ein
  "SH_05_PC_HFSHEIN",
  # Hospital city/state
  "SH_05_PC_ADDRADDRCITY","SH_05_PC_AADDADDRCITY","SH_05_PC_HOSFACADDCIT",
  "SH_05_PC_ADDRADDRSTAT",  "SH_05_PC_AADDADDRSTAT",
  "SH_05_PC_HOFAADSTABBB",
  # Hospital Zip
  "SH_05_PC_ADADZIIPPCCO",  "SH_05_PC_AAADZIIPPCCO", "SH_05_PC_HOFAADZIIPP", "SH_05_PC_HOFAADZIIPP"
), collapse = "|")

disregardedentities_vars = str_c(c(
  # Direct controlling entity?
  # "SR_01_PC_DICOENNAA", "SR_01_PC_IDEDCNA",
  # Direct controlling entity name
  "SR_01_PC_DCENBNLINE11", "SR_01_PC_IDEDCENBNLIN1",
  # Disregarded Entity name
  "SR_01_PC_IDEDENBNLINE1", "SR_01_PC_NODEBNLINE11",
  # Primary activities
  "SR_01_PC_IDDIENPRACCT", "SR_01_PC_PRIMARACTIVI",
  "SR_01_PC_IDDIENEIINN",
  # City, State, Zip
  "SR_01_PC_IDDIENADCIIT", "SR_01_PC_IDDIENADSTAB","SR_01_PC_IDEAZIP",
  "SR_01_PC_ADDRESCITYIT",
  "SR_01_PC_ADDRESSTATET",
  "SR_01_PC_ADZIIPPCCOOD"
),
collapse = "|")

relatextaxexempt_vars = str_c(c(
  # 512b13 controlled org
  # "SR_02_PC_CONTROORGRG", "SR_02_PC_IRTEOCOIND",
  # Direct controlling entity?
  # "SR_02_PC_DICOENNAA", "SR_02_PC_IRTEODCNA",
  # Direct Controlling Entity Name
  "SR_02_PC_IRTEODCENBNL1", "SR_02_PC_DCENBNLINE11",
  # Primary activities
  "SR_02_PC_IRTEOPACTIVI", "SR_02_PC_PRIMARACTIVI",
  # Disregarded entity name
  "SR_02_PC_IRTEODENBNLI1", "SR_02_PC_NODEBNLINE11",
  "SR_02_PC_IRTEOEIN",
  # City, State, Zip
  "SR_02_PC_IRTEOACITY","SR_02_PC_IRTEOASABBRE", "SR_02_PC_IRTEOAZIP",
  "SR_02_PC_ADDRESCITYIT",
  "SR_02_PC_ADDRESSTATET",
  "SR_02_PC_ADZIIPPCCOOD"), 
  collapse = "|")

relatedpartnership_vars = str_c(c(
  # "SR_03_PC_DICOENNAA", "SR_03_PC_IROPDCNA",
  "SR_03_PC_DCENBNLINE11", "SR_03_PC_IROPDCENBNLI1",
  # "SR_03_PC_GENORMMANPAR", "SR_03_PC_IROPGOMPIND",
  "SR_03_PC_NOROBNLINE11", "SR_03_PC_IROPRONBNLIN1",
  "SR_03_PC_IROPEIN",
  # City, State, Zip
  "SR_03_PC_IDREORPAADCI",
  "SR_03_PC_IROPASABBREV",
  "SR_03_PC_IROPAZIP",
  "SR_03_PC_ADDRESCITYIT",
  "SR_03_PC_ADDRESSTATET",
  "SR_03_PC_ADZIIPPCCOOD"), 
             collapse = "|")

relatedcorptrust_vars = str_c(c(
  "SR_04_PC_NOROBNLINE11", "SR_04_PC_IROCTRONBNLI1", 
  "SR_04_PC_DCENBNLINE11", "SR_04_PC_IROCTDCENBNL1",
  # "SR_04_PC_DICOENNAA", "SR_04_PC_IROCTDCNA",
  "SR_04_PC_PRIMARACTIVI","SR_04_PC_IROCTPACTIVI",
  "SR_04_PC_TYPEOFENENTI", "SR_04_PC_IROCTETYPE",
  # "SR_00_PC_CONTROORGRG", "SR_04_PC_IROCTCOIND",
  "SR_04_PC_IROCTEIN",
  # City, State, Zip
  "SR_04_PC_IROCTACITY",
  "SR_04_PC_IROCTASABBRE",
  "SR_04_PC_IROCTAZIP",
  "SR_04_PC_ADDRESCITYIT",
  "SR_04_PC_ADDRESSTATET",
  "SR_04_PC_ADZIIPPCCOOD"
  ), collapse = "|")

renaming = c("SH_04_PC_MCJVENBNLINE1|SH_04_PC_NAOFENBUNALI1|SH_05_PC_HFBNBNLINE11|SH_05_PC_HFSHNBNLINE11|SH_05_PC_ANNABUNALIIN1|SH_05_PC_HFPPHFNBNLIN11|SR_01_PC_IDEDENBNLINE1|SR_01_PC_NODEBNLINE11|SR_02_PC_IRTEODENBNLI1|SR_02_PC_NODEBNLINE11|SR_03_PC_NOROBNLINE11|SR_03_PC_IROPRONBNLIN1|SR_04_PC_NOROBNLINE11|SR_04_PC_IROCTRONBNLI1" = "RelatedOrgName",
             "SH_05_PC_HFSHEIN|SR_01_PC_IDDIENEIINN|SR_02_PC_IRTEOEIN|SR_03_PC_IROPEIN|SR_04_PC_IROCTEIN" = "RelatedOrgEIN",
             "SH_04_PC_DESENTPRIACT|SH_04_PC_MACOJNVEPRAC|SR_01_PC_IDDIENPRACCT|SR_01_PC_PRIMARACTIVI|SR_02_PC_PRIMARACTIVI|SR_02_PC_IRTEOPACTIVI|SR_04_PC_PRIMARACTIVI|SR_04_PC_IROCTPACTIVI" = "PrimaryActivity", 
             "SR_01_PC_DCENBNLINE11|SR_01_PC_IDEDCENBNLIN1|SR_02_PC_IRTEODCENBNL1|SR_02_PC_DCENBNLINE11|SR_03_PC_DCENBNLINE11|SR_03_PC_IROPDCENBNLI1|SR_04_PC_DCENBNLINE11|SR_04_PC_IROCTDCENBNL1" = "ControllingOrgName",
             "SR_04_PC_TYPEOFENENTI|SR_04_PC_IROCTETYPE" = "EntityType",
             "SH_05_PC_HOSFACFACNUM|SH_05_PC_AFACFACINUMB" = "HospitalFacilityNumber",
             "SH_05_PC_HOFAFAREGRRO|SH_05_PC_AFAFACREPGRO" = "HospitalFacilityReportingGroup",
             "SH_05_PC_ADDRADDRCITY|SH_05_PC_AADDADDRCITY|SH_05_PC_HOSFACADDCIT|SR_01_PC_IDDIENADCIIT|SR_01_PC_ADDRESCITYIT|SR_02_PC_IRTEOACITY|SR_02_PC_ADDRESCITYIT|SR_03_PC_IDREORPAADCI|SR_03_PC_ADDRESCITYIT|SR_04_PC_IROCTACITY|SR_04_PC_ADDRESCITYIT" = "City",
             "SH_05_PC_ADDRADDRSTAT|SH_05_PC_AADDADDRSTAT|SH_05_PC_HOFAADSTABBB|SR_01_PC_IDDIENADSTAB|SR_01_PC_ADDRESSTATET|SR_02_PC_IRTEOASABBRE|SR_02_PC_ADDRESSTATET|SR_03_PC_IROPASABBREV|SR_03_PC_ADDRESSTATET|SR_04_PC_IROCTASABBRE|SR_04_PC_ADDRESSTATET" = "State",
             "SH_05_PC_ADADZIIPPCCO|SH_05_PC_AAADZIIPPCCO|SH_05_PC_HOFAADZIIPP|SH_05_PC_HOFAADZIIPP|SR_01_PC_IDEAZIP|SR_01_PC_ADZIIPPCCOOD|SR_02_PC_IRTEOAZIP|SR_02_PC_ADZIIPPCCOOD|SR_03_PC_IROPAZIP|SR_03_PC_ADZIIPPCCOOD|SR_04_PC_IROCTAZIP|SR_04_PC_ADZIIPPCCOOD" = "ZipCode")

beth_nested = beth_related_df %>% 
  distinct(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, .keep_all = T) %>% 
  group_by(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR) %>% 
  nest() %>% 
  ungroup() %>% 
  mutate(JointVentures = map(data, function(df) select(df, matches(jointventure_vars))), 
         HospFacilities = map(data, function(df) select(df, matches(hospfacility_vars))),
         DisregardedEntities = map(data, function(df) select(df, matches(disregardedentities_vars))),
         RelatedTaxExemptOrgs = map(data, function(df) select(df, matches(relatextaxexempt_vars))),
         RelatedPartnershipOrgs = map(data, function(df) select(df, matches(relatedpartnership_vars))),
         RelatedCorpTrustOrgs = map(data, function(df) select(df, matches(relatedcorptrust_vars)))) %>% 
  select(-data) %>% 
  mutate_if(is_list, 
            list(function(var) 
              map(
                var, function(df) df %>% 
                  filter_all(any_vars(!is.na(.))) %>% 
                  select_if(~sum(!is.na(.)) > 0)  %>% 
                  when(ncol(.) > 0 & nrow(.) > 0 ~ (.) %>%                 # This part of code SHOULD allow to arrange each group of organizations in a table format
                         gather(variable, value, na.rm=T) %>%              #  Currently the spread is causing problems due to duplicate row identifiers
                         separate(variable, into = c("variable", "number"), sep = "_(?=[0-9]{2}$)", fill = "right") %>% #    This is happening at indices 58:59
                         replace_na(., list(number = "01")) %>%            #  For some reason EIN, Filer name, and Tax year do not uniquely identify the returns
                         arrange(number, variable) %>%                     #  For now, we'll use distinct() but unclear how it chooses
                         spread(variable, value) %>%
                         rename_all(list(~str_replace_all(., renaming))),
                       ~ (.))
                )
              )
            )

# Pull EINs
beth_nested_eins = beth_nested %>% distinct(F9_00_HD_FILEREIN) %>% pull()
# Pull names of list columns
beth_list_names = beth_nested %>% select_if(is_list) %>% names()

# Unnest each list column and rowbind them together
# Loop through each list column, and then through each EIN
beth_org_relationships_df = future_map(beth_list_names, 
           function(list_name) map(beth_nested_eins, 
                                   function(ein) beth_nested %>% 
                                     filter(F9_00_HD_FILEREIN == ein) %>% 
                                     select(1:3, list_name) %>% 
                                     unnest() %>% 
                                     mutate(relationship_type = list_name) %>% 
                                     select(-matches("^number$")))) %>% 
  flatten_dfr() %>% 
  filter(!str_detect(RelatedOrgName, "1 NA$|1 NONE")) %>% 
  mutate(ControllingOrgName = na_if(ControllingOrgName, "NONE")) %>% 
  select(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, RelatedOrgName, ControllingOrgName, relationship_type, PrimaryActivity, HospitalFacilityNumber, EntityType) %>% 
  arrange(F9_00_HD_FILEREIN, F9_00_HD_TAXYEAR, relationship_type)
  
beth_org_relationships_df %>% 
  distinct(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, RelatedOrgName, relationship_type)



## Doing the same for 24 sites
sites_nested = sites_full_returns %>% 
  distinct(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, .keep_all = T) %>% 
  group_by(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR) %>% 
  nest() %>% 
  ungroup() %>% 
  mutate(JointVentures = map(data, function(df) select(df, matches(jointventure_vars))), 
         HospFacilities = map(data, function(df) select(df, matches(hospfacility_vars))),
         DisregardedEntities = map(data, function(df) select(df, matches(disregardedentities_vars))),
         RelatedTaxExemptOrgs = map(data, function(df) select(df, matches(relatextaxexempt_vars))),
         RelatedPartnershipOrgs = map(data, function(df) select(df, matches(relatedpartnership_vars))),
         RelatedCorpTrustOrgs = map(data, function(df) select(df, matches(relatedcorptrust_vars)))) %>% 
  select(-data) %>% 
  # Mutate each list column
  mutate_if(is_list, 
            list(function(var) 
              map(
                var, function(df) df %>% 
                  # Keep rows with any data
                  filter_all(any_vars(!is.na(.))) %>% 
                  # Keep columns with any data
                  select_if(~sum(!is.na(.)) > 0)  %>% 
                  # Use when to avoid applying this code to empty sets of data
                  when(ncol(.) > 0 & nrow(.) > 0 ~ (.) %>%                 # This part of code allows to arrange each group of organizations in a table format
                         gather(variable, value, na.rm=T) %>%              
                         separate(variable, into = c("variable", "number"), sep = "_(?=[0-9]{2}$)", fill = "right") %>% #   
                         replace_na(., list(number = "01")) %>%            
                         arrange(number, variable) %>%                     
                         spread(variable, value) %>%
                         rename_all(list(~str_replace_all(., renaming))),
                       ~ (.))
              )
            )
  )

# Pull EINs
sites_nested_eins = sites_nested %>% distinct(F9_00_HD_FILEREIN) %>% pull()
# Pull names of list columns
sites_list_names = sites_nested %>% select_if(is_list) %>% names()

# Unnest each list column and rowbind them together
# Loop through each list column, and then through each EIN
sites_org_relationships_df = future_map(sites_list_names, 
                                       function(list_name) map(sites_nested_eins, 
                                                               function(ein) sites_nested %>% 
                                                                 filter(F9_00_HD_FILEREIN == ein) %>% 
                                                                 select(1:3, list_name) %>% 
                                                                 unnest() %>% 
                                                                 mutate(relationship_type = list_name) %>% 
                                                                 select(-matches("^number$")))) %>% 
  flatten_dfr() %>% 
  filter(!str_detect(RelatedOrgName, "1 NA$|1 NONE")) %>% 
  mutate(ControllingOrgName = na_if(ControllingOrgName, "NONE")) %>% 
  # select(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, RelatedOrgName, ControllingOrgName, relationship_type, PrimaryActivity, EntityType, HospitalFacilityNumber, ZipCode, City, State) %>% 
  select_if(names(.) %in% c("F9_00_HD_FILEREIN", "F9_00_HD_FILERNAME1", "F9_00_HD_TAXYEAR", "RelatedOrgName", "ControllingOrgName", "relationship_type", "PrimaryActivity", "EntityType", "HospitalFacilityNumber", "ZipCode", "City", "State")) %>% 
  arrange(F9_00_HD_FILEREIN, F9_00_HD_TAXYEAR, relationship_type)

sites_org_relationships_df %>% 
  filter(is.na(State)) %>% 
  count(relationship_type)


write.table(sites_org_relationships_df, file = "SitesOrganizations", row.names = F)
write_csv(sites_org_relationships_df, "SitesOrganizations.csv")



# create a data frame giving the hierarchical structure of your individuals
d1=tibble(from="origin", to=distinct(beth_org_relationships_df, F9_00_HD_FILERNAME1) %>% pull())
d2=beth_org_relationships_df %>% 
  distinct(F9_00_HD_FILERNAME1, RelatedOrgName, relationship_type) %>% 
  set_names(c("from", "to", "relationship_type"))
hierarchy=bind_rows(d2)

# create a vertices data.frame. One line per object of our hierarchy
vertices = tibble(name = unique(c(as.character(hierarchy$from), as.character(hierarchy$to))) )

# Create a graph object with the igraph library
mygraph <- graph_from_data_frame( hierarchy, vertices=vertices )
# This is a network object, you visualize it as a network like shown in the network section!

# With igraph: 
plot(mygraph, vertex.label="", edge.arrow.size=0, vertex.size=2)

# With ggraph:
ggraph(mygraph, layout = 'dendrogram', circular = FALSE) + 
  geom_edge_elbow() +
  theme_void()

ggraph(mygraph, layout = 'dendrogram', circular = TRUE) + 
  geom_edge_diagonal() +
  theme_void()

ggraph(mygraph, layout = 'dendrogram', circular = TRUE) + 
  geom_edge_diagonal(alpha=0.1) +
  geom_conn_bundle(data = get_con(from = c(18,20,30), to = c(19, 50, 70)), alpha=1, width=1, colour="skyblue", tension = 0) +
  theme_void()

ggraph(mygraph, layout = 'dendrogram', circular = TRUE) + 
  geom_edge_diagonal(alpha=0.1) +
  geom_conn_bundle(data = hierarchy, alpha=1, width=1, colour="skyblue", tension = 1) +
  theme_void()

all_leaves = beth_org_relationships_df %>% 
  distinct(RelatedOrgName)

# The connection object must refer to the ids of the leaves:
from = match( hierarchy$to, vertices$name)
to = match( connect$to, vertices$name)





return_related_orgs = function(returns_df){
  # This function takes one argument:
  #
  #     ::returns_df::  a dataframe of IRS returns produced by extract_xml() function
  # 
  # 
  #   It returns a dataframe in the format:
  #
  #   | FilerEIN  | FilerName | TaxYear | basevar_1 | basevar_2 | basevar_3 | ... | basevar_n | RelatedOrgName  | ControllingOrgName  | relationship_type | [variables associated with specific org types]
  #   |   ...     |     ...   |   ...   |     ...   |     ...   |     ...   | ... |     ...   |       ...       |         ...         |         ...       |                 ...
  #
  tic()
  nested_df = returns_df %>% 
    # Select distinct tax returns
    distinct(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR, .keep_all = T) %>% 
    group_by(F9_00_HD_FILEREIN, F9_00_HD_FILERNAME1, F9_00_HD_TAXYEAR) %>% 
    nest() %>% 
    ungroup() %>% 
    mutate(JointVentures = map(data, function(df) select(df, matches(jointventure_vars))), 
           HospFacilities = map(data, function(df) select(df, matches(hospfacility_vars))),
           DisregardedEntities = map(data, function(df) select(df, matches(disregardedentities_vars))),
           RelatedTaxExemptOrgs = map(data, function(df) select(df, matches(relatextaxexempt_vars))),
           RelatedPartnershipOrgs = map(data, function(df) select(df, matches(relatedpartnership_vars))),
           RelatedCorpTrustOrgs = map(data, function(df) select(df, matches(relatedcorptrust_vars)))) %>% 
    select(-data) %>% 
    # Mutate each list column
    mutate_if(is_list, 
              list(function(var) 
                map(
                  var, function(df) df %>% 
                    # Keep rows with any data
                    filter_all(any_vars(!is.na(.))) %>% 
                    # Keep columns with any data
                    select_if(~sum(!is.na(.)) > 0)  %>% 
                    # Use when to avoid applying this code to empty sets of data
                    when(ncol(.) > 0 & nrow(.) > 0 ~ (.) %>%                 # This part of code allows to arrange each group of organizations in a table format
                           gather(variable, value, na.rm=T) %>%              
                           separate(variable, into = c("variable", "number"), sep = "_(?=[0-9]{2}$)", fill = "right") %>% #   
                           replace_na(., list(number = "01")) %>%            
                           arrange(number, variable) %>%                     
                           spread(variable, value) %>%
                           rename_all(list(~str_replace_all(., renaming))),
                         ~ (.))
                )
              )
    )
  
  # Pull EINs
  nested_eins = nested_df %>% 
    distinct(F9_00_HD_FILEREIN) %>% 
    pull()
  
  # Pull names of list columns
  list_names = nested_df %>% 
    select_if(is_list) %>% 
    names()
  
  # Unnest each list column and rowbind them together
  # Loop through each list column, and then through each EIN
  relationships_df = future_map(list_names, 
                                          function(list_name) map(nested_eins, 
                                                                  function(ein) nested_df %>% 
                                                                    filter(F9_00_HD_FILEREIN == ein) %>% 
                                                                    select(1:3, list_name) %>% 
                                                                    unnest() %>% 
                                                                    mutate(relationship_type = list_name) %>% 
                                                                    select(-matches("^number$")))) %>% 
    flatten_dfr() %>% 
    filter(!str_detect(RelatedOrgName, "1 NA$|1 NONE")) %>% 
    mutate(ControllingOrgName = na_if(ControllingOrgName, "NONE")) %>% 
    select_if(names(.) %in% c("F9_00_HD_FILEREIN", "F9_00_HD_FILERNAME1", "F9_00_HD_TAXYEAR", "RelatedOrgName", "ControllingOrgName", "relationship_type", "PrimaryActivity", "EntityType", "HospitalFacilityNumber", "ZipCode", "City", "State")) %>% 
    arrange(F9_00_HD_FILEREIN, F9_00_HD_TAXYEAR, relationship_type)
  toc()
  return(relationships_df)
}

