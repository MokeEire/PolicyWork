## Read xml data
# Sample file for now
library(tidyverse)
library(here)
library(xml2)
library(XML)
library(lubridate)

#setwd("~/Documents/Projects/U19/IRS/673objects")
# setwd("/Volumes/veloce2_f/U19/DATA/IRS990/673objects")
# irs_files = list.files(file.path(here::here(), "IRS990/673objects"))
# Get list of xml filenames
irs_files = list.files("IRS/673objects")
# Choose a sample to examine
sample_file = irs_files[67] # 8, 13, 14
# Read in xml
sample_xml = read_xml(file.path("IRS/673objects", test_filenames[1]))
xml_ns_strip(sample_xml)

## Noticed that duplicate elements occur (in at least schedule R)
# This means that only the first element is read when being converted from a list
#  Potential remedies:
#  - Rename elements in XML
#  - Rename elements in list <=== This one is easier

# Function to check contents of specific nodes or nodesets
getNodeContents = function(xml_file, main_node, child_node = "", multiples = F){
  xml_ns_strip(xml_file)

  contents = xml_name(
    xml_contents(
      xml_find_all(
        xml_file, 
        xpath = paste0(paste0("//", main_node), if(child_node != ""){ paste0("/",child_node, "[1]")})
        )
      )
    )
  if(multiples){
    contents = unique(contents[duplicated(contents)])
  }
  return(contents)
}

## Schedule A and Schedule R
getNodeContents(sample_xml, "IRS990ScheduleR", "IdRelatedTaxExemptOrgGrp")

test_list = as_list(xml_find_all(sample_xml, "//IRS990ScheduleR"))
names(test_list[[1]])[names(test_list[[1]]) == "IdRelatedTaxExemptOrgGrp"]
names(test_list[[1]][["Form990ScheduleRPartII"]])
## Repeated variables need to be renamed:
# Form990ScheduleRPartI, Form990ScheduleRPartII, Form990ScheduleRPartIII, Form990ScheduleRPartIV, Form990ScheduleRPartVI


orgs = names(test_list[[1]])[str_detect(names(test_list[[1]]), "Form990ScheduleRPartII")]
test_list[[1]][names(test_list[[1]]) == "Form990ScheduleRPartII"]
names(test_list[[1]])[str_detect(names(test_list[[1]]), "ScheduleRPartII")] = paste(orgs, 1:length(orgs), sep = "_")

# Schedule R, Part II, Organizations
xml_name(xml_contents(xml_find_all(sample_xml, xpath = xml_path(nodes)[1])))






rename_list_elements = function(list_to_use, element_names){
  if(all(length(list_to_use) > 0, length(element_names) > 0)){
    # 1. Check whether an element is in the list
    # 2. If it is, identify the position of the element
    # 3. Paste names with an increasing integer as a suffix
    # 4. Set these new names and then copy the suffix to the child elements
    for(i in element_names){
      if(any(names(list_to_use[[1]]) == i)){
        matches = (names(list_to_use[[1]]) == i)
        new_names = paste(i, 1:sum(matches), sep = "_")
        names(list_to_use[[1]])[matches] = new_names
        
      }
    }
  }
  return(list_to_use)
}


read_irs = function(filenames){
  # Set up empty dataframe and counters
  return_df = data_frame()
  # Counters
  head_count = 0
  main_count = 0
  a_count = 0
  h_count = 0
  n_count = 0
  r_count = 0
  

  ## Progress bar for sanity!
  prog_bar <- txtProgressBar(min = 0, max = length(filenames), style = 3)
  
  ## Read and convert each file, append it to the dataframe if it has Schedule A or H
  for(file_path in filenames){
    #message(paste("Converting",file_path))
    # Read in file
    file = read_xml(file.path("IRS/673objects", file_path))
    # Remove XML namespaces for simplicity
    xml_ns_strip(file)
    
    #################################################################################
    ##           Create temp dataframes for each portion of the return             ##
    #################################################################################
    # 1. Find form part via XPATH
    # 2. Convert to list
    # 3. If list has elements with identical names, rename them
    # 4. Convert these lists to Dataframes
    # 5. Bind all these dataframes together
    
    ## Walking through the extraction process:
    bind_rows(
      unlist(
        as_list(
          xml_find_all(
            file, 
            xpath = "//ReturnHeader")
          )
        )
      )
    
    ## Return Header
    head_temp = bind_rows(unlist(as_list(xml_find_all(file, xpath = "//ReturnHeader")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("RH_",.))), ~ (.))
    
    ## IRS 990 Main form
    main_temp_list = as_list(xml_find_all(file, "//IRS990"))
    main_temp_list = rename_list_elements(main_temp_list, getNodeContents(file, "IRS990", multiples=T))
    main_temp = bind_rows(unlist(main_temp_list)) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("MAIN_",.))), ~ (.))
    
    ## Schedule A
    a_temp_list = as_list(xml_find_all(file, "//IRS990ScheduleA"))
    a_temp_list = rename_list_elements(a_temp_list, getNodeContents(file, "IRS990ScheduleA", multiples = T))
    a_temp = bind_rows(unlist(a_temp_list)) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("A_",.))), ~ (.))
    
    ## Schedule H
    h_temp_list = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleH"))))
    h_temp_list = rename_list_elements(h_temp_list, getNodeContents(file, "IRS990ScheduleH", multiples = T))
    h_temp = bind_rows(unlist(h_temp_list)) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("H_",.))), ~ (.))
    
    ## Schedule N
    n_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleN")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("N_",.))), ~ (.))
    
    ## Schedule R
    r_temp_list = as_list(xml_find_all(file, "//IRS990ScheduleR"))
    r_temp_list = rename_list_elements(r_temp_list, getNodeContents(file, "IRS990ScheduleR", multiples = T))
    r_temp = bind_rows(unlist(r_temp_list)) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("R_",.))), ~ (.))

    # Counters
    head_count = head_count+(nrow(head_temp) >0)
    main_count = main_count+(nrow(main_temp) >0)
    a_count = a_count+(nrow(a_temp) >0)
    h_count = h_count+(nrow(h_temp) >0)
    n_count = n_count+(nrow(n_temp) >0)
    r_count = r_count+(nrow(r_temp) >0)
    
    setTxtProgressBar(prog_bar, head_count)
    # Bind these temp dataframes together
    filename = data_frame(filename = file_path)
    file_df = bind_cols(filename, head_temp, main_temp, a_temp, h_temp, n_temp, r_temp)
    
    return_df = bind_rows(return_df, file_df)
  }

  ########################
  ##    Return Value    ##
  ########################
  message(
    paste("Schedule Frequencies:\n", 
          "Return Header -", head_count, "\n",
          "Main Form -", main_count, "\n",
          "Schedule A -", a_count, "\n",
          "Schedule H -", h_count, "\n",
          "Schedule N -", n_count, "\n",
          "Schedule R -", r_count, "\n")
  )
  return(return_df)
  close(prog_bar)
}




# Pass all the IRS files through this function and append them into a list
# Then convert this list into a dataframe

results_df = read_irs(irs_files) %>%
  # Convert # of hospitals (Sched. H, Part V, Section A) to numeric
  mutate(H_NumberOfHospitalFacilities = as.numeric(H_NumberOfHospitalFacilities)) %>% 
  # Convert date variables from character to date
  mutate_at(vars(contains("Date", ignore.case = F)), funs(as_date(.)))


save(results_df, file = "IRS/irs990compiled.RData")
write_csv(results_df, "IRS/irs990compiled.csv")
load("IRS/irs990compiled.RData")

## How many variables from each portion of the return:
# Return Header
results_df %>% select(starts_with("RH")) %>% colnames(.) %>% length(.)
# Schedule A
results_df %>% select(starts_with("A_")) %>% colnames(.) %>% length(.)
# Schedule H
results_df %>% select(starts_with("H_")) %>% colnames(.) %>% length(.)
# Schedule N
results_df %>% select(starts_with("N_")) %>% colnames(.) %>% length(.)
# Schedule R
results_df %>% select(starts_with("R_")) %>% colnames(.) %>% length(.)
# Main IRS form
results_df %>% select(-matches("^[A-Z]{1,2}_")) %>% colnames(.) %>% length(.)

## What time period are the tax returns from?
results_df %>% 
  summarise(BeginDate_min = min(RH_TaxPeriodBeginDate, na.rm=T),
            BeginDate_max = max(RH_TaxPeriodBeginDate, na.rm=T),
            EndDate_min = min(RH_TaxPeriodEndDate, na.rm=T),
            EndDate_max = max(RH_TaxPeriodEndDate, na.rm=T),
            PrepareDate_min = min(RH_Preparer.DatePrepared, na.rm=T),
            PrepareDate_max = max(RH_Preparer.DatePrepared, na.rm=T)
            )

results_df %>% group_by(Hospital) %>% 
  summarise(NumHospitals_Low = range(H_NumberOfHospitalFacilities)[1],
            NumHospitals_High = range(H_NumberOfHospitalFacilities)[2],
            NumHospitals_Mean = mean(H_NumberOfHospitalFacilities, na.rm=T),
            NumHospitals_Sum = sum(H_NumberOfHospitalFacilities, na.rm=T))
results_df %>% count(H_NumberOfHospitalFacilities) %>% summarise(N_Hospitals_Total = sum(H_NumberOfHospitalFacilities*n, na.rm=T))

# Easy way to look at variable names
# Using this to see which variable names correspond to form items
results_df %>% select(-filename) %>% gather(var, val, na.rm=T) %>% filter(str_detect(var, "^R_")) %>% separate(var, into = c("source", "var"), sep = "(?<=^R)_")
# How many organizations in each portion of Schedule H
results_df %>% select(-filename) %>% 
  # transpose to list of variable names and values
  gather(var, val, na.rm=T) %>% filter(str_detect(var, "^H_")) %>% 
  # Separate variable name from source prefix for simplicity
  separate(var, into = c("source", "var"), sep = "(?<=^H)_") %>% 
  # Find variables that relate to Org-identifying parts of Schedule R
  filter(str_detect(var, paste("Form990ScheduleRPartI", "Form990ScheduleRPartII", "Form990ScheduleRPartIII", "Form990ScheduleRPartIV", "Form990ScheduleRPartVI",
                               "IdDisregardedEntitiesGrp", "IdRelatedTaxExemptOrgGrp", "IdRelatedOrgTxblPartnershipGrp", "IdRelatedOrgTxblCorpTrGrp", sep = "|"))) %>% 
  # Separate the part from the attribute and select only the EIN to accurately count how many time each Number of Orgs occurs
  separate(var, into = c("var", "attribute"), sep = "(?<=[0-9])\\.") %>% filter(attribute == "EIN") %>% count(var) %>% 
  # Separate the org count to see the max in each case
  separate(var, into = c("Org", "number"), sep = "_", convert=T) %>% group_by(Org) %>% filter(number == max(number))


# How many organizations in each portion of Schedule R
results_df %>% select(-filename) %>% 
  # transpose to list of variable names and values
  gather(var, val, na.rm=T) %>% filter(str_detect(var, "^R_")) %>% 
  # Separate variable name from source prefix for simplicity
  separate(var, into = c("source", "var"), sep = "(?<=^R)_") %>% 
  # Find variables that relate to Org-identifying parts of Schedule R
  filter(str_detect(var, paste("Form990ScheduleRPartI", "Form990ScheduleRPartII", "Form990ScheduleRPartIII", "Form990ScheduleRPartIV", "Form990ScheduleRPartVI",
                               "IdDisregardedEntitiesGrp", "IdRelatedTaxExemptOrgGrp", "IdRelatedOrgTxblPartnershipGrp", "IdRelatedOrgTxblCorpTrGrp", sep = "|"))) %>% 
  # Separate the part from the attribute and select only the EIN to accurately count how many time each Number of Orgs occurs
  separate(var, into = c("var", "attribute"), sep = "(?<=[0-9])\\.") %>% filter(attribute == "EIN") %>% count(var) %>% 
  # Separate the org count to see the max in each case
  separate(var, into = c("Org", "number"), sep = "_", convert=T) %>% group_by(Org) %>% filter(number == max(number))


## Which files have 'HospitalInd'

get_filenames = function(var){
  var = enquo(var)
  
  results_df %>% 
    filter(!is.na(!!var)) %>% 
    pull(filename)
}
test_filenames = results_df %>% filter(!is.na(R_IdRelatedTaxExemptOrgGrp_105.EIN)) %>% pull(filename)

bind_rows(lapply(test_filenames, read_irs)) %>% gather(var, val) %>% separate(var, into = c("source", "var"), sep = "(?<=^[A-Z]{1,4})_") %>% filter(source == "A") %>% print(n=Inf)









## Reading in df as list



# count_colnames = function(file_path){
#   # Read in file
#   file = read_xml(file.path("IRS/673objects", file_path))
#   
#   # Create temp dataframes for each portion of the return
#   head_temp = bind_rows(unlist(as_list(xml_find_all(file, xpath = "//ReturnHeader")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("RH_",.))), ~ (.))
#   irs_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990"))))
#   a_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleA")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("A_",.))), ~ (.))
#   h_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleH")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("H_",.))), ~ (.))
#   n_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleN")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("N_",.))), ~ (.))
#   r_temp = bind_rows(unlist(as_list(xml_find_all(file, "//IRS990ScheduleR")))) %>% when(nrow(.) > 0 ~ (.) %>% rename_all(funs(paste0("R_",.))), ~ (.))
#   
#   # Bind these temp dataframes together
#   numcols_df = data_frame(Header = ncol(head_temp),
#                           IRS990 = ncol(irs_temp),
#                           ScheduleA = ncol(a_temp),
#                           ScheduleH = ncol(h_temp),
#                           ScheduleN = ncol(n_temp),
#                           ScheduleR = ncol(r_temp))
#   
#   # Return df if it has a schedule H OR if it has a schedule A w/Hospital checkbox
#   if(nrow(h_temp) > 0){
#     return(numcols_df)
#   } else {
#     return(NULL)
#   }
# }
# 
# sapply(irs_files, count_colnames)
