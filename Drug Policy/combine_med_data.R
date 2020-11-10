## Import MED excel doc reports & combine them into one dataset
# Author: Mark Barrett - mbarrett@rand.org

## This is a somewhat complicated process because we have 2011-2019 data which have been downloaded as PDFs from the
## Colorado Gov't website - https://www.colorado.gov/pacific/cdphe/medical-marijuana-statistics-and-data
## These PDFs were then exported to Excel spreadsheets using Adobe Acrobat

## Given the rapid changes in Marijuana legality and the subsequent creation or allocation of government entities
## to watch over the substance, the data changes quite often.
## Data changes (new information) occur at:
#   - July 2014
#   - April 2015
#   - April 2016
#   - January 2017
#   - May 2018

## However, the structure of the file changes at different intervals:
#   - April 2015
#   - January 2017

# This creates 3 distinct versions that need to be pulled in.  
# This file creates a set of functions that pull in information from different tables for each version (as necessary)
# and then uses these functions to create a dataframe

# Libraries
library(tidyverse)
library(readxl)
library(furrr)
library(haven)
library(pipeR)
library(lubridate)

plan(multiprocess)

folders = str_subset(list.dirs(getwd()), "20[0-9]{2}$")
files = map(folders, list.files, pattern = "\\.xlsx$", full.names=T)
test = files[[1]]


# Functions ---------------------------------------------------------------

pull_county_info_v1 = function(list){
  map_dfr(list,
          function(y) 
            read_excel(y, skip = if_else(str_detect(y, "0[4-6]_2013|12_2014|02_2015"), 20,19), 
                       col_names = c("county", "num_patients", "percent_patients"),
                       col_types = c("text", "numeric", "skip", "skip", "text"),
                       n_max = 64) %>% 
            mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                   month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                   percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                           T ~ str_remove_all(percent_patients, "%")))/100)
  ) %>>%
    (~ cat("Each file contains Adams?",(count(., county) %>% filter(county == "Adams") %>% pull(n) == sum(map_int(list, length))),"\n",
           "Each file contains Yuma?",(count(., county) %>% filter(county == "Yuma") %>% pull(n) == sum(map_int(list, length))),"\n"))
}

pull_county_info_v2 = function(list){
  map_dfr(list,
          function(y) {
            excel_df = tryCatch(
              error = function(cnd) {
                read_excel(y) %>% print(n=Inf)
                stop(paste("Problem reading in file for county_info:", y), call. = F)
                },
              read_excel(y, skip = case_when(str_detect(y, "0[4-8]_2015") ~ 33,
                                                        str_detect(y, "12_201[56]") ~ 36,
                                                        T ~ 35), 
                                    n_max = if_else(str_detect(y, "(04|1[01])_2016"), 66, 65))
            ) %>% 
              select_if(negate(is_logical)) %>% 
              mutate_if(is_numeric, as.character) %>% 
              set_names("county", "num_patients1", "num_patients2", "percent_patients1", "percent_patients2")

            tryCatch(
              error = function(cnd) stop(paste("Problem cleaning file:", y), call. = F),
              excel_df %>% 
                filter(!str_detect(county, "County")) %>% 
                mutate(num_patients = as.numeric(if_else(is.na(num_patients1), num_patients2, num_patients1)),
                       percent_patients = if_else(is.na(percent_patients1), percent_patients2, percent_patients1)) %>% 
                select(county, num_patients, percent_patients) %>% 
                mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                       month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                       percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                               T ~ str_remove_all(percent_patients, "%")))/100)
            )
          }
  )
}

pull_county_info_v3 = function(list){
  map_dfr(list,
          function(y) {
            excel_df = tryCatch(
              error = function(cnd) {
                read_excel(y) %>% print(n=Inf)
                stop(paste("Problem reading in file for county_info:", y), call. = F)
              },
              read_excel(y, skip = case_when(str_detect(y, "0[1-3]_2018|(0[7-9]|1[0-2])_2017") ~ 40,
                                             str_detect(y, "04_2018") ~ 39,
                                             str_detect(y, "0[1-6]_2017") ~ 38,
                                             T ~ 41), 
                         n_max = 65)
            ) %>% 
              select_if(negate(is_logical)) %>% 
              mutate_if(is_numeric, as.character)
            
            named_excel_df = tryCatch(
              error = function(cnd) {
                excel_df %>% print(n=Inf)
                stop(paste("Problem naming the file:", y), call.=F)
              },
              excel_df %>% 
                # Percent has two columns
                when(str_detect(y, "0[1-6]_2017") ~ (.) %>% 
                     set_names("county", "num_patients", "percent_patients1", "percent_patients2") %>% 
                     mutate(percent_patients = if_else(is.na(percent_patients1), percent_patients2, percent_patients1)) %>% 
                     select(county, num_patients, percent_patients),
                     # Both have two columns
                   str_detect(y, "(1[0-2]|07)_2017|201[89]") ~ (.) %>% 
                     set_names("county", "num_patients1", "num_patients2", "percent_patients1", "percent_patients2") %>%
                     mutate(num_patients = as.numeric(if_else(is.na(num_patients1), num_patients2, num_patients1)),
                            percent_patients = if_else(is.na(percent_patients1), percent_patients2, percent_patients1)) %>% 
                     select(county, num_patients, percent_patients),
                   T ~ (.) %>% 
                     set_names("county", "num_patients", "percent_patients")) %>% 
                filter(!str_detect(county, "County")) %>% 
                mutate(num_patients = as.numeric(num_patients),
                       county = Hmisc::capitalize(tolower(county)),
                       county = str_replace_all(county, "\\b.{1}", Hmisc::capitalize))
            )
            
            tryCatch(
              error = function(cnd) stop(paste("Problem cleaning file:", y), call. = F),
              named_excel_df %>% 
                mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                       month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                       percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                               T ~ str_remove_all(percent_patients, "%")))/100)
            )
          }
  )
}

pull_condition_info = function(list){
  
  cond_df = map_dfr(list,
          function(y){
            excel_df = tryCatch(
              error = function(cnd) {
                read_excel(y) %>% print(n=Inf)
                stop(paste("Problem reading in file for condition_info:", y), call. = F)
              },
              read_excel(y, 
                       # Lines change in April 2013
                       skip = case_when(str_detect(y, "0[4-6]_2013|12_2014|02_2015") ~ 87,
                                        str_detect(y, "0[4-8]_2015") ~ 22,
                                        str_detect(y, "(09|1[01])_2015") ~ 24,
                                        str_detect(y, "12_2015") ~ 25,
                                        T ~ 86), 
                       col_names = c("condition", "num_patients", "percent_patients"),
                       col_types = c("text", "skip", "numeric", "skip", "skip", "text"),
                       n_max = 8) 
            )
          tryCatch(
            error = function(cnd) { excel_df %>% print(n = Inf) 
              stop(paste("Problem cleaning year, month, and patients:", y), call. = F)
              },
            excel_df %>% 
              mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                     month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                     percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                             T ~ str_remove_all(percent_patients, "%")))/100)
          )}
  )
  
  tryCatch(
    error = function(cnd) {cond_df %>% print(n = Inf)
      stop(paste("Problem in user info gather-unite-spread:", y), call. = F)
    },
    cond_df %>% 
      gather(measure, value, num_patients:percent_patients) %>% 
      unite(col_name, condition, measure) %>% 
      mutate(col_name = str_remove_all(col_name, "_patients$")) %>% 
      spread(col_name, value)
  )
}

pull_condition_info_v2 = function(list){
  
  cond_df = map_dfr(list,
                    function(y){
                      excel_df = tryCatch(
                        error = function(cnd) {
                          read_excel(y) %>% print(n=Inf)
                          stop(paste("Problem reading in file for condition_info:", y), call. = F)
                        },
                        read_excel(y, 
                                   # Lines change in April 2013
                                   skip = case_when(str_detect(y, "0[4-8]_2015") ~ 21,
                                                    str_detect(y, "12_2015|(04|1[0-2])_2016") ~ 24,
                                                    str_detect(y, "(0[5-9]|1[0-2])_2018|0[1-3]_2019") ~ 29,
                                                    str_detect(y, "(0[1-4])_2018|2017") ~ 27,
                                                    T ~ 23),
                                   n_max = 8) %>% 
                          select(-contains("..")) %>% 
                          set_names("condition", "num_patients", "percent_patients")
                      )
                      tryCatch(
                        error = function(cnd) { excel_df %>% print(n = Inf) 
                          stop(paste("Problem cleaning year, month, and patients:", y), call. = F)
                        },
                        excel_df %>% 
                          mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                                 month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                                 percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                                         T ~ str_remove_all(percent_patients, "%")))/100)
                      )}
  )
  
  tryCatch(
    error = function(cnd) {cond_df %>% print(n = Inf)
      stop(paste("Problem in user info gather-unite-spread:", y), call. = F)
    },
    cond_df %>% 
      gather(measure, value, num_patients:percent_patients) %>% 
      unite(col_name, condition, measure) %>% 
      mutate(col_name = str_remove_all(col_name, "_patients$")) %>% 
      spread(col_name, value)
  )
}



pull_user_info = function(list){
  cond_df = map_dfr(list,
          function(y){
            excel_df = tryCatch(
              error = function(cnd) {
                read_excel(y) %>% print(n=Inf)
                stop(paste("Problem reading in file for user info:", y), call. = F) },
              read_excel(y, skip = if_else(str_detect(y, "0[4-6]_2013|12_2014|02_2015"), 99, 98), 
                       col_names = c("sex", "registry_percent", "avg_age"),
                       col_types = c("text", "skip", "skip", "text", "skip", "skip", "skip", "numeric"),
                       n_max = 3)
            )
          
          tryCatch(
            error = function(cnd) { excel_df %>% print(n = Inf) 
              stop(paste("Problem cleaning user info:", y), call. = F) },
            excel_df %>% 
              mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                   month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                   avg_age = if_else(is.na(avg_age), as.numeric(str_extract(sex, "[0-9]{2}")), avg_age),
                   sex = if_else(str_detect(sex, "overall"), "Overall", sex),
                   registry_percent = as.numeric(case_when(registry_percent == "<1%" ~ "1",
                                                           T ~ str_remove_all(registry_percent, "%")))/100)
          )}
  )
  
  tryCatch(
    error = function(cnd) {cond_df %>% print(n = Inf)
      stop(paste("Problem in user info gather-unite-spread:", y), call. = F)
    },
    cond_df %>% 
      gather(measure, value, registry_percent:avg_age) %>% 
      unite(col_name, sex, measure) %>% 
      spread(col_name, value) %>% 
      select(-Overall_registry_percent)
  )
}

pull_user_info_v2 = function(list){
  cond_df = map_dfr(list,
                    function(y){
                      excel_df = tryCatch(
                        error = function(cnd) {
                          read_excel(y) %>% print(n=Inf)
                          stop(paste("Problem reading in file for user info:", y), call. = F) },
                        read_excel(y, skip = case_when(str_detect(y, "0[4-8]_2015") ~ 7,
                                                       str_detect(y, "0[1-3|5-9]_2016|(09|1[01])_2015") ~ 9,
                                                       str_detect(y, "(0[5-9]|1[0-2])_2018|0[1-3]_2019") ~ 13,
                                                       str_detect(y, "(0[1-4])_2018|2017") ~ 11,
                                                       T ~ 10), 
                                   col_types = "text",
                                   n_max = 3) %>% 
                          select(-contains("..")) %>% 
                          set_names("sex", "avg_age", "registry_percent")
                      )
                      
                      tryCatch(
                        error = function(cnd) { excel_df %>% print(n = Inf) 
                          stop(paste("Problem cleaning user info:", y), call. = F) },
                        excel_df %>% 
                          mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                                 month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                                 avg_age = as.numeric(if_else(is.na(avg_age), str_extract(sex, "[0-9]{2}"), avg_age)),
                                 sex = if_else(str_detect(sex, "all patients|average"), "Overall", sex),
                                 registry_percent = as.numeric(case_when(registry_percent == "<1%" ~ "1",
                                                                         T ~ str_remove_all(registry_percent, "%")))/100)
                      )}
  )
  
  tryCatch(
    error = function(cnd) {cond_df %>% print(n = Inf)
      stop(paste("Problem in user info gather-unite-spread"), call. = F)
    },
    cond_df %>% 
      select(sex, registry_percent, avg_age, year, month) %>% 
      gather(measure, value, registry_percent:avg_age) %>% 
      unite(col_name, sex, measure) %>% 
      spread(col_name, value) %>% 
      select(-Overall_registry_percent)
  )
}





pull_agegroups = function(list){
  map_dfr(list,
          function(y){
            if(str_detect(y, "(0[7-9]|1[0-2])_2014|_2015_")){
              excel_df = tryCatch(
                error = function(cnd) {
                  read_excel(y) %>% print(n=Inf)
                  stop(paste("Problem reading in file for age groups:", y), call. = F)
                },
                read_excel(y, skip = if_else(str_detect(y, "12_2014|02_2015"), 103, 102), 
                         n_max = 8) 
                )
              tryCatch(
                error = function(cnd) { excel_df %>% print(n = Inf) 
                  stop(paste("Problem cleaning age groups info:", y), call. = F)
                },
                excel_df %>% 
                  select(-contains("...")) %>% 
                  set_names(c("age_group", "num", "percent")) %>% 
                  mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                         month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                         age_group = str_replace_all(age_group, c("\\-" = "_", "\\+" = "plus", "71 and Older" = "71plus")),
                         percent = as.numeric(case_when(percent == "<1%" ~ "1",
                                                        T ~ str_remove_all(percent, "%")))/100) %>% 
                gather(measure, value, num:percent) %>% 
                unite(col_name, measure, age_group) %>% 
                spread(col_name, value)
              )
            } else {
              tibble()
            }
            }
  )
}






pull_agegroups_v2 = function(list){
  map_dfr(list,
          function(y){
            excel_df = tryCatch(
                error = function(cnd) {
                  read_excel(y) %>% print(n=Inf)
                  stop(paste("Problem reading in file for age groups:", y), call. = F)
                },
                read_excel(y, skip = case_when(str_detect(y, "0[4-8]_2015") ~ 11,
                                               str_detect(y, "12_2015|(1[0-2]|04)_2016") ~ 14,
                                               str_detect(y, "(0[5-9]|1[0-2])_2018|0[1-3]_2019") ~ 18,
                                               str_detect(y, "(0[1-4])_2018|2017") ~ 16,
                                               T ~ 13), 
                           # case_when(str_detect(y, "0[4-8]_2015") ~ 8,
                           #           str_detect(y, "0[1-3|5-9]_2016") ~ 9,
                           #           str_detect(y, "12_2015") ~ 11,
                           #           T ~ 10)
                           n_max = 9)
              )
            
            tryCatch(
                error = function(cnd) { excel_df %>% print(n = Inf) 
                  stop(paste("Problem cleaning age groups info:", y), call. = F)
                },
                excel_df %>% 
                  select(-contains("...")) %>% 
                  set_names(c("age_group", "num", "percent", "top_condition")) %>% 
                  mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                         month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                         age_group = str_replace_all(age_group, c("\\-" = "_", "\\+" = "plus", "71 and Older" = "71plus")),
                         percent = as.numeric(case_when(percent == "<1%" ~ "1",
                                                        T ~ str_remove_all(percent, "%")))/100) %>% 
                  gather(measure, value, num:top_condition) %>% 
                  unite(col_name, measure, age_group) %>% 
                  spread(col_name, value) %>% 
                  mutate_at(vars(matches("num|percent")), as.numeric)
            )
            }
          )
}

pull_caregiver_distribution_v2 = function(list){
  cond_df = map_dfr(list,
                    function(y){
                      excel_df = tryCatch(
                        error = function(cnd) {
                          read_excel(y) %>% print(n=Inf)
                          stop(paste("Problem reading in file for condition_info:", y), call. = F)
                        },
                        read_excel(y, 
                                   # Lines change in April 2013
                                   skip = case_when(str_detect(y, "0[4-8]_2015") ~ 101,
                                                    str_detect(y, "12_2015") ~ 25,
                                                    T ~ 24), 
                                   col_names = c("patients_served", "num_caregivers", "percent_patients"),
                                   col_types = c("text", "skip", "skip", "skip", "numeric", "skip", "skip", "skip", "skip", "skip", "skip", "skip", "skip", "text"),
                                   n_max = 8) 
                      )
                      tryCatch(
                        error = function(cnd) { excel_df %>% print(n = Inf) 
                          stop(paste("Problem cleaning year, month, and patients:", y), call. = F)
                        },
                        excel_df %>% 
                          mutate(year = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/)201[0-9]"),
                                 month = str_extract(y, "(?<=/Users/mbarrett/Documents/Projects/Drug Policy/MED Data/201[0-9]/CHED_MMR_)[0-9]{2}"),
                                 percent_patients = as.numeric(case_when(percent_patients == "<1%" ~ "1",
                                                                         T ~ str_remove_all(percent_patients, "%")))/100)
                      )}
  )
  
  tryCatch(
    error = function(cnd) {cond_df %>% print(n = Inf)
      stop(paste("Problem in user info gather-unite-spread:", y), call. = F)
    },
    cond_df %>% 
      gather(measure, value, num_patients:percent_patients) %>% 
      unite(col_name, condition, measure) %>% 
      mutate(col_name = str_remove_all(col_name, "_patients$")) %>% 
      spread(col_name, value)
  )
}







# V1 data extraction ------------------------------------------------------


# January 2011 - March 2015
# Table I:  County Information
folders_v1 = folders[1:5]
files_v1 = future_map(folders_v1, list.files, pattern = "^[A-Z].*xlsx$", full.names=T)
# files_v1[[4]] = files_v1[[4]][-7:-12]
files_v1[[5]] = files_v1[[5]][-4:-12]

future_map_dfr(files_v1, function(x) pull_county_info_v1(x))# %>% View()


# County Long
county_long_v1 = future_map_dfr(files_v1, 
               function(x) pull_county_info_v1(x)) %>% 
  mutate(agency = "county")

# State-level data
state_v1 = future_map_dfr(files_v1, 
               function(x) {
                 condition_info = pull_condition_info(x)
                 
                 user_info = pull_user_info(x)
                 
                 age_groups = pull_agegroups(x)
                 
                 total = left_join(condition_info, user_info, by = c("year", "month")) %>% 
                   when(nrow(age_groups) > 0 ~ (.) %>% 
                          left_join(age_groups, by = c("year","month")),
                        T ~ (.))

                 return(total)
               }
)



# V2 data extraction ------------------------------------------------------


# April 2015 - December 2016
# Lines to skip for county
#   - 04_2015 - 08_2015: 34
#   - 09_2015 - 11_2015: 36
#   - 12_2015: 37

folders_v2 = folders[5:6]
files_v2 = future_map(folders_v2, list.files, pattern = "^[A-Z].*xlsx$", full.names=T)
files_v2[[1]] = files_v2[[1]][4:12] # 2015: April - December

county_long_v2 = future_map_dfr(files_v2, 
                                function(x) pull_county_info_v2(x)) %>% 
  mutate(agency = "county")

# State-level data
state_v2 = future_map_dfr(files_v2, 
               function(x) {
                 condition_info = pull_condition_info_v2(x)
                 
                 user_info = pull_user_info_v2(x)
                 
                 age_groups = pull_agegroups_v2(x)
                 
                 total = left_join(condition_info, user_info, by = c("year", "month")) %>% 
                   when(nrow(age_groups) > 0 ~ (.) %>% 
                          left_join(age_groups, by = c("year","month")),
                        T ~ (.))
                 
                 return(total)
               }
)


# Lines to skip for gender
#   - 04_2015 - 08_2015: 8
#   - 09_2015 - 11_2015: 10
#   - 12_2015: 11

# V3 data extraction ------------------------------------------------------


# January 2017 - March 2019
# Lines to skip for county
#   - 04_2015 - 08_2015: 34
#   - 09_2015 - 11_2015: 36
#   - 12_2015: 37

folders_v3 = folders[7:9]
files_v3 = future_map(folders_v3, list.files, pattern = "^[A-Z].*xlsx$", full.names=T)

county_long_v3 = future_map_dfr(files_v3, 
               function(x) pull_county_info_v3(x)) %>% 
  mutate(agency = "county")


# State-level data
state_v3 = future_map_dfr(files_v3, 
                          function(x) {
                            condition_info = pull_condition_info_v2(x)
                            
                            user_info = pull_user_info_v2(x)
                            
                            age_groups = pull_agegroups_v2(x)
                            
                            total = left_join(condition_info, user_info, by = c("year", "month")) %>% 
                              when(nrow(age_groups) > 0 ~ (.) %>% 
                                     left_join(age_groups, by = c("year","month")),
                                   T ~ (.))
                            
                            return(total)
                          }
)








# Create sample -----------------------------------------------------------


# med2011_14 = future_map_dfr(files_v1, 
#                             function(x) {
#                               condition_info = pull_condition_info(x)
#                               
#                               user_info = pull_user_info(x)
#                               
#                               total = left_join(condition_info, user_info, by = c("year", "month"))
#                               
#                               return(total)
#                             }
# ) %>% 
#   mutate(agency = "state") %>% 
#   full_join(county_long, by = c("agency", "year", "month")) %>% 
#   arrange(county, year, month) %>% 
#   select(year, month, agency, county, num_patients, percent_patients, everything())
# colnames(med2011_14) = tolower(str_replace_all(colnames(med2011_14), c("\\s" = "","\\/" = "_")))
# 
# write_dta(med2011_14, "med2011_14.dta")


# Join county-level variables ---------------------------------------------


med_full_county = bind_rows(county_long_v1, county_long_v2, county_long_v3)

# write_dta(med_full_county, "med2011_19_county.dta")

wide_vars = do.call(paste0, expand.grid(c("num_", "percent_"), do.call(paste0, expand.grid(c(paste0("0",1:9), 10:12), paste0("_", 2011:2019)))))
wide_vars = wide_vars[-199:-length(wide_vars)]

med_full_county_wide = med_full_county %>% 
  arrange(year, month) %>% 
  gather(measure, value, num_patients:percent_patients) %>% 
  mutate(measure = str_remove_all(measure, "_patients$")) %>% 
  unite(mmYYYY, month, year) %>% 
  unite(varname, measure, mmYYYY) %>% 
  spread(varname, value) %>% 
  select(agency, county, wide_vars)

# write_dta(med_full_county_wide, "med2011_19_county_wide.dta")



# Join state-level variables ----------------------------------------------


med_full_state = bind_rows(state_v1, state_v2, state_v3)
colnames(med_full_state) = tolower(str_replace_all(colnames(med_full_state), c("\\s" = "","\\/" = "_")))

# write_dta(med_full_state, "med2011_19_state.dta")

med_full_state %>% 
  mutate(num_11_20 = if_else(is.na(num_11_20) & !is.na(num_11_17), num_11_17+num_18_20, num_11_20),
         percent_11_20 = if_else(is.na(percent_11_20) & !is.na(percent_11_17), percent_11_17+percent_18_20, percent_11_20)) %>% 
  select(year, month, num_11_17, num_18_20, num_11_20, percent_11_17, percent_18_20, percent_11_20) %>% 
  View()


#med_full_state = read_dta("med2011_19_state.dta")

# Make a datetime var for graphing
med_full_state = med_full_state %>% 
  mutate(datetime = str_c(year, month, "01", sep = "-"),
         datetime = lubridate::ymd(datetime),
         num_11_20 = if_else(is.na(num_11_20) & !is.na(num_11_17), num_11_17+num_18_20, num_11_20),
         percent_11_20 = if_else(is.na(percent_11_20) & !is.na(percent_11_17), percent_11_17+percent_18_20, percent_11_20))


## Make codebook

med_full_state %>% 
  filter_all(all_vars(!is.na(.))) %>% 
  slice(1) %>% 
  gather(variable, example_value) %>% 
  select(variable) %>% 
  View()


# Visualization -----------------------------------------------------------


# Conditions reported by patients
med_full_state %>% 
  gather(condition, num, cachexia_num:severepain_num, -contains("percent")) %>% 
  mutate(condition = Hmisc::capitalize(str_remove_all(condition, "_num")),
         condition = str_replace_all(condition, c("Hiv_aids" = "HIV/AIDS", "spasms" = " Spasms", "nausea" = " Nausea", "pain" = " Pain"))) %>% 
  ggplot(., aes(x = datetime, y = num, colour = condition))+
  geom_line()+
  scale_colour_brewer(type = "qual", palette = 6, guide = guide_legend(title = NULL))+
  scale_y_continuous(labels = scales::comma)+
  scale_x_date(limits = c(ymd("2011-01-01"), ymd("2019-04-01")))+
  labs(x = "", y = "Number of Patients Reporting Condition", caption = "Source: Colorado Department of Public Health & Environment")+
  ggtitle("Conditions reported by medical marijuana patients in Colorado (2011-2019)")+
  theme_minimal()+
  theme(plot.caption = element_text(colour = "grey47"),
        plot.margin = margin(l = 5, t = 15, b = 15, r = 5),
        aspect.ratio = .65)

ggsave("conditionsReported2011-19.png", dpi = 320, height = 6, width = 10, units = "in")

# Sex of patients
med_full_state %>% 
  select(matches("(fe)*male_reg"), datetime) %>% 
  gather(sex, percent, -datetime) %>% 
  mutate(sex = Hmisc::capitalize(str_remove_all(sex, "_registry_percent"))) %>% 
  ggplot(., aes(x = datetime, y = percent, colour = sex))+
  geom_line()+
  scale_colour_brewer(type = "qual", palette = 6, guide = guide_legend(title = NULL))+
  scale_y_continuous(labels = scales::percent_format(accuracy = 1))+
  scale_x_date()+
  labs(x = "", y = "% of Patients", caption = "Source: Colorado Department of Public Health & Environment")+
  ggtitle("Gender distribution of medical marijuana patients in Colorado (2011-2019)")+
  theme_minimal()+
  theme(plot.caption = element_text(colour = "grey47"),
        plot.margin = margin(l = 5, t = 15, b = 15, r = 5),
        aspect.ratio = .6)

ggsave("gender2011-19.png", dpi = 320, height = 6, width = 10, units = "in")

# Age groups of patients
med_full_state %>% 
  select(matches("num_[0-9]"), datetime) %>% 
  filter_at(vars(-datetime), any_vars(!is.na(.))) %>% 
  mutate(num_11_20 = if_else(is.na(num_11_20) & !is.na(num_11_17), num_11_17+num_18_20, num_11_20)) %>% 
  select(-num_11_17, -num_18_20) %>% 
  gather(age_group, num, num_0_10:num_71plus) %>% 
  mutate(age_group = str_remove_all(age_group, "num_"),
         age_group = str_replace_all(age_group, c("_" = "-", "plus" = "+")),
         age_group = fct_rev(factor(age_group, levels = c("0-10", "11-20", "21-30", "31-40", "41-50", "51-60", "61-70", "71+"), ordered=T))) %>% 
  ggplot(., aes(x = datetime, y = num, fill = age_group))+
  geom_area()+
  scale_fill_brewer(type = "seq", palette = 1, direction = -1, guide = guide_legend(title = NULL, reverse = F))+
  scale_y_continuous(labels = scales::comma)+
  scale_x_date(limits = c(ymd("2011-01-01"), ymd("2019-04-01")))+
  labs(x = "", y = "% of Patients", caption = "Source: Colorado Department of Public Health & Environment")+
  ggtitle("Number of medical marijuana patients in Colorado, by age group (2014-2019)")+
  theme_minimal()+
  annotate("label", x = ymd("2011-01-01"), y = 97500, label = "Note:\nData on age groups was not\nreported prior to July 2014", hjust = 0, size = 5)+
  theme(plot.caption = element_text(colour = "grey47"),
        plot.margin = margin(l = 5, t = 15, b = 15, r = 5),
        aspect.ratio = .6)



ggsave("ageGroupsLabel2011-19.png", dpi = 320, height = 6, width = 10, units = "in")


