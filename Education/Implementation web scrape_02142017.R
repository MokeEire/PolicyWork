

# Create ALEKS dataset first

aleks <- data.frame(school = character(0), grades = character(0), scenario = character(0), purpose = character(0), portion.curriculum = character(0), timespent = character(0), course = character(0), description = character(0), sc.challenges = character(0), sc.daysperweek = character(0), sc.avguselength = character(0), imp.how = character(0), imp.order = character(0), imp.structure = character(0), imp.modify = character(0), imp.homework = character(0), imp.parents = character(0), gr.homework = character(0), gr.grading = character(0), gr.progress = character(0), learning.outcomes = character(0), best.practices = character(0))
aleks <- aleks[1:120,]
rownames(aleks) <- 1:120
aleks[,23] <- data.frame(location = character(0))
#Switching around columns to get location in the second position
aleks <- aleks[, c(colnames(aleks)[1], colnames(aleks)[23], colnames(aleks)[2:22])]





# Web-scraping

library(rvest)


# Supplement for Algebra 1

html <- read_html(curl("https://www.aleks.com/k12/implementations/popup?_form_=true&parse_list=m*160,e*172,h*47,m*138,m*330,h*116,e*178,h*257,h*356,m*64,h*243,m*213,e*92,m*294,m*93,m*146,h*184,e*28,m*98,m*240,h*36,m*81,e*201,e*186,m*336,h*79,m*302,h*206,h*245,h*321,h*35,e*26,h*34,m*179,m*188,m*248,h*155,h*64,h*112,m*229,m*155,e*78,m*301,m*317,h*227,m*253,h*265,h*216,e*174,m*165,h*195,h*149,h*362,h*274,m*6,h*249,h*31,h*165,h*156,e*245,m*207,h*255,h*365,h*167,h*377,m*83,h*367,h*285,m*68,m*238,h*326,h*283,h*113,m*237,e*47,m*309,m*126,h*71,m*17,h*144,e*7,m*86,m*296,h*53,m*134,h*100,h*101,h*296,m*80,h*233,h*272,h*351,h*12,h*308,h*95,e*222,h*93,m*242,h*133,m*292,m*268,h*218,h*332,m*203,e*149,m*221,m*153,h*228,m*102,m*82,m*312,h*171,m*116,h*18,h*186,h*63,h*98,e*30,m*225,h*138&parse_request=true&snum=5&sc1=All%20grades&sc2=All%20scenarios&sc3=Supplement&sc4=Algebra%201&sc5=All%20states&cmscache=parse_list:parse_request", handle = curl::new_handle("useragent" = "Mozilla/5.0")))

#Basic information being pulled

school <- as.vector(html_text(html_nodes(html, "#general_facts span:nth-child(1)")))
aleks[,1] <- school

location <- as.vector(html_text(html_nodes(html, ".black:nth-child(3) , .black:nth-child(4)")))
aleks[,2] <- location

grades <- as.vector(html_text(html_nodes(html, "#grades:nth-child(7) , #grades:nth-child(8)")))
aleks[,3] <- grades

scenario <- as.vector(html_text(html_nodes(html, "#grades:nth-child(10) , :nth-child(11) #grades:nth-child(11), #grades:nth-child(11)")))
aleks[,4] <- scenario

purpose <- as.vector(html_text(html_nodes(html, "#grades:nth-child(13) , #grades:nth-child(14)")))
aleks[, 5] <- purpose

portion.curriculum <- as.vector(html_text(html_nodes(html, "#grades:nth-child(17) , #grades:nth-child(17), #grades:nth-child(16)")))
aleks[,6]<-portion.curriculum

#Remove results when they are pulled from wrong heading (e.g. "X hours per day" from time spent heading)
aleks[c(3,23,24,38,43,49,58,66,73,94,96,98),6]<-NA


#Time spent was weird, couldn't reliably web scrape it by itself, so I had to extract it from basics (which collected all of the headings' information)

basics <- as.vector(html_text(html_nodes(html, "#grades")))
basicv <- as.vector(basics)
times <- basicv[grep("hour", basics)]
aleks[1:111,7] <- times

#Need to look through url manually to see where time spent is missing (luckily only for 9 teachers)

aleks[20:112, 7] <- times[19:111]
aleks[19,7] <- "Varies"

aleks[24:113, 7] <- aleks[23:112, 7]
aleks[23,7] <- NA

aleks[73:114, 7] <- aleks[72:113, 7]
aleks[72,7] <- NA

aleks[74:115, 7] <- aleks[73:114, 7]
aleks[73,7] <- "Varies"

aleks[82:116, 7] <- aleks[81:115, 7]
aleks[81,7] <- "Varies"

aleks[85:117, 7] <- aleks[84:116, 7]
aleks[84,7] <- "Varies"

aleks[90:118, 7] <- aleks[89:117, 7]
aleks[89,7] <- NA

aleks[95:119, 7] <- aleks[94:118, 7]
aleks[94,7] <- NA

aleks[97:120, 7] <- aleks[96:119, 7]
aleks[96,7] <- NA

# Course
course <- basicv[grep("Algebra", basics)]
aleks[,8] <- course


# The paragraph responses

description <- as.vector(html_text(html_nodes(html, "#general_facts .imp_paragraph")))
aleks[, 9] <- description


sc <- as.vector(html_text(html_nodes(html, "#scenario .imp_paragraph")))
aleks[,10]<-as.character(aleks[,10])
#Check for where teachers did not respond to this part of the questions
aleks[unique(c(1:22, 24:73, 75:120)),10] <- sc


implement <- as.vector(html_text(html_nodes(html, "#implementation .imp_paragraph")))
aleks[, 13] <- implement

grading <- as.vector(html_text(html_nodes(html, "#grading .imp_paragraph")))
aleks[,19]<-as.character(aleks[,19])
#Check for where teachers did not respond to this part of the questions
aleks[unique(c(1:22, 24:64, 66:120)),19] <- grading

outcomes <- as.vector(html_text(html_nodes(html, "#learning_outcomes .imp_paragraph")))
aleks[,22] <- as.character(aleks[,22])
aleks[unique(c(1:93, 95:112, 114:120)),22] <- outcomes
#Take the question out of the data
aleks[,22] <- gsub("\r\n\t\tSince using ALEKS, please describe the learning outcomes or progress you have seen.  ", "", aleks[,22])

aleks<- aleks[,-c(11, 12, 14,15,16,17,18,20,21)]


bp <- as.vector(html_text(html_nodes(html, "#best_practices .imp_paragraph")))
aleks[,14] <- as.character(aleks[,14])
aleks[unique(c(1:2, 4:5, 7:9, 11:14, 16:17, 21:27, 29:30, 32:33, 35, 37, 41:46, 49, 51:55, 59:61, 63:64, 66:68, 70, 72:73, 76:80, 82:85, 87:90, 92:96, 98:101, 105:111, 113, 115:118, 120)),14] <- bp
#Take the question out of the data
aleks[,14] <- gsub("\r\n\t\tAre there any best practices you would like to share with other teachers implementing ALEKS[?]  ", " ",  aleks[,14])











#                                                      #
#Supplement for "High School Preparation for Algebra 1"#
#                                                      #

html2 <- read_html(curl("https://www.aleks.com/k12/implementations/popup?_form_=true&parse_list=h*138,h*95,h*295,h*167,h*285,m*350,h*155,h*245,h*255,m*151,h*257,h*336,m*309&parse_request=true&snum=5&sc1=All%20grades&sc2=All%20scenarios&sc3=Supplement&sc4=High%20School%20Preparation%20for%20Algebra%201&sc5=All%20states&cmscache=parse_list:parse_request", handle = curl::new_handle("useragent" = "Mozilla/5.0")))

school2 <- as.vector(html_text(html_nodes(html2, "#general_facts span:nth-child(1)")))
aleks[121:133,1] <- school2

location2 <- as.vector(html_text(html_nodes(html2, ".black:nth-child(3) , .black:nth-child(4)")))
aleks[121:133,2] <- location2

grades2 <- as.vector(html_text(html_nodes(html2, "#grades:nth-child(7) , #grades:nth-child(8)")))
aleks[121:133,3] <- grades2

scenario2 <- as.vector(html_text(html_nodes(html2, "#grades:nth-child(10) , :nth-child(11) #grades:nth-child(11), #grades:nth-child(11)")))
aleks[121:133,4] <- scenario2

purpose2 <- as.vector(html_text(html_nodes(html2, "#grades:nth-child(13) , #grades:nth-child(14)")))
aleks[121:133, 5] <- purpose2

portion.curriculum2 <- as.vector(html_text(html_nodes(html2, "#grades:nth-child(17) , #grades:nth-child(17), #grades:nth-child(16)")))
aleks[121:133,6]<-portion.curriculum2


#Time spent 

basics2 <- as.vector(html_text(html_nodes(html2, "#grades")))
timespent <- as.vector(html_text(html_nodes(html2, "#grades:nth-child(19) , :nth-child(20)")))
timespent <- timespent[grep("hour", timespent)]
aleks[121:133,7] <- timespent

# Course
course2 <- basics2[grep("Algebra", basics2)]
aleks[121:133,8] <- course2




description2 <- as.vector(html_text(html_nodes(html2, "#general_facts .imp_paragraph")))
aleks[121:133, 9] <- description2


sc2 <- as.vector(html_text(html_nodes(html2, "#scenario .imp_paragraph")))
aleks[121:133,10] <- sc2


implement2 <- as.vector(html_text(html_nodes(html2, "#implementation .imp_paragraph")))
aleks[121:133, 11] <- implement2

grading2 <- as.vector(html_text(html_nodes(html2, "#grading .imp_paragraph")))
aleks[121:133,12] <- grading2

outcomes2 <- as.vector(html_text(html_nodes(html2, "#learning_outcomes .imp_paragraph")))
aleks[121:133,13] <- outcomes2
aleks[,13] <- gsub("\r\n\t\tSince using ALEKS, please describe the learning outcomes or progress you have seen.  ", "", aleks[,13])

bp2 <- as.vector(html_text(html_nodes(html2, "#best_practices .imp_paragraph")))
aleks[unique(c(121:128, 131:133)),14] <- bp2
aleks[,14] <- gsub("\r\n\t\tAre there any best practices you would like to share with other teachers implementing ALEKS[?]  ", " ",  aleks[,14])










#                                        #
# Supplement for "Traditional Algebra 1" #
#                                        #



html3 <- read_html(curl("https://www.aleks.com/k12/implementations/popup?_form_=true&parse_list=h*220,e*178,h*241,h*280,m*309&parse_request=true&snum=5&sc1=All%20grades&sc2=All%20scenarios&sc3=Supplement&sc4=Traditional%20Algebra%201&sc5=All%20states&cmscache=parse_list:parse_request", handle = curl::new_handle("useragent" = "Mozilla/5.0")))

school3 <- as.vector(html_text(html_nodes(html3, "#general_facts span:nth-child(1)")))
aleks[134:138,1] <- school3

location3 <- as.vector(html_text(html_nodes(html3, ".black:nth-child(3) , .black:nth-child(4)")))
aleks[134:138,2] <- location3

grades3 <- as.vector(html_text(html_nodes(html3, "#grades:nth-child(7) , #grades:nth-child(8)")))
aleks[134:138,3] <- grades3

scenario3 <- as.vector(html_text(html_nodes(html3, "#grades:nth-child(10) , :nth-child(11) #grades:nth-child(11), #grades:nth-child(11)")))
aleks[134:138,4] <- scenario3

purpose3 <- as.vector(html_text(html_nodes(html3, "#grades:nth-child(13) , #grades:nth-child(14)")))
aleks[134:138, 5] <- purpose3

portion.curriculum3 <- as.vector(html_text(html_nodes(html3, "#grades:nth-child(17) , #grades:nth-child(17), #grades:nth-child(16)")))
aleks[134:138,6]<-portion.curriculum3


#Timespent

basics3 <- as.vector(html_text(html_nodes(html3, "#grades")))

timespent2 <- as.vector(html_text(html_nodes(html3, "#grades:nth-child(19) , :nth-child(20)")))
timespent2 <- timespent2[grep("hour", timespent2)]
aleks[134:138,7] <- timespent2

# Course
course3 <- basics3[grep("Algebra", basics3)]
aleks[134:138,8] <- course3




description3 <- as.vector(html_text(html_nodes(html3, "#general_facts .imp_paragraph")))
aleks[134:138, 9] <- description3


sc3 <- as.vector(html_text(html_nodes(html3, "#scenario .imp_paragraph")))
aleks[134:138,10] <- sc3


implement3 <- as.vector(html_text(html_nodes(html3, "#implementation .imp_paragraph")))
aleks[134:138, 11] <- implement3

grading3 <- as.vector(html_text(html_nodes(html3, "#grading .imp_paragraph")))
aleks[134:138,12] <- grading3

outcomes3 <- as.vector(html_text(html_nodes(html3, "#learning_outcomes .imp_paragraph")))
aleks[134:138,13] <- outcomes3
aleks[,13] <- gsub("\r\n\t\tSince using ALEKS, please describe the learning outcomes or progress you have seen.  ", "", aleks[,13])

bp3 <- as.vector(html_text(html_nodes(html3, "#best_practices .imp_paragraph")))
aleks[c(134,135,138),14] <- bp3
aleks[,14] <- gsub("\r\n\t\tAre there any best practices you would like to share with other teachers implementing ALEKS[?]  ", " ",  aleks[,14])


write.csv(aleks, file = "/Users/mbarrett/Documents/ALEKS/TeacherImplementation_090217.csv", row.names = FALSE)

aleks[,3] <- gsub(" - ", "-", aleks[,3])
aleks[,3] <- gsub("12", "12th", aleks[,3])
aleks[,3] <- gsub("8", "8th", aleks[,3])














# Cleaning the file

#Mac
file <- read.xlsx("/Users/mbarrett/Documents/ALEKS/TeacherImplementation_090217.xlsx", sheetIndex = 1)
#Windows
file <- read_excel("C:/Users/Mark/Documents/RAND/ALEKS_Tasks/Implementation/TeacherImplementation_140217.xlsx", sheet = 1)

# Separate the Scenarios into subquestions
file <- file %>% separate(Scenarios, into = c("challenges", "Days.per.week"), sep = "How many days per week is class time dedicated to ALEKS\\?", fill = "warn")
file <- file %>% separate(Days.per.week, into = c("Days.per.week", "class.length"), sep = "What is the average length of a class period when ALEKS is used\\?", fill = "right")

# Separate imp.how into subquestions
file <- file %>% separate(imp.how, into = c("imp.how", "imp.order"), sep = "Do you cover ALEKS concepts in a particular order\\?", fill = "warn", extra = "merge")
file <- file %>% separate(imp.order, into = c("imp.order", "imp.structure"), sep = "How do you structure your class period with ALEKS\\?", fill = "warn", extra = "merge")
file <- file %>% separate(imp.structure, into = c("imp.structure", "imp.modify"), sep = "How did you modify your regular teaching approach as a result of ALEKS\\?", fill = "warn", extra = "merge")
file <- file %>% separate(imp.modify, into = c("imp.modify", "imp.homework"), sep = "How often are students required or encouraged to work on ALEKS at home\\?", fill = "warn", extra = "merge")
file <- file %>% separate(imp.homework, into = c("imp.homework", "imp.parents"), sep = "How do you cultivate parental involvement and support for ALEKS\\?", fill = "right", extra = "merge")

# Find the places where questions were skipped (causing separate to mess up)

grep("How did you modify your regular teaching approach as a result of ALEKS", file$imp.how)
grep("How often are students required or encouraged to work on ALEKS at home", file$imp.how)
grep("How often are students required or encouraged to work on ALEKS at home", file$imp.order)
grep("How do you cultivate parental involvement and support for ALEKS\\?", file$imp.how)
grep("How do you cultivate parental involvement and support for ALEKS\\?", file$imp.order)
grep("How do you cultivate parental involvement and support for ALEKS\\?", file$imp.structure)

# Separate Location into City and State

file <- file %>% separate(Location, into = c("city", "state"), sep = ",", fill = "right", extra = "merge", remove = TRUE)

# Separate gr.homework into subquestions

file <- file %>% separate(gr.homework, into = c("gr.homework", "gr.grading"), sep = "How do you incorporate ALEKS into your grading system\\?", fill = "right")
file <- file %>% separate(gr.grading, into = c("gr.grading", "gr.progress"), sep = "Do you require students to make regular amounts of progress in ALEKS\\?", fill = "right")

grep("Do you require students to make regular amounts of progress in ALEKS\\?", file$gr.homework)

# Remove questions from initial variables of each sequence


file$imp.how <- gsub("How do you implement ALEKS\\?", "", file$imp.how)
file$challenges <- gsub("What challenges did the class or school face in math prior to using ALEKS\\?", "", file$challenges)
file$gr.homework<- gsub("Is ALEKS assigned to your students as all or part of their homework responsibilities\\? If so, what part of the total homework load is it\\?", "", file$gr.homework)

file[,25] <- factor(c(0,1))
colnames(file)[25] <- c("9th Grade")
file[c(2,3,6,8,9,11,17,18,19,21,25,26,28,29,30,31,33,35,37,38,39,45,47,48,51,52,53,54,56,57,58,59,62,63,64,65,67,68,69,71,72,73,76,77,78,80,81,83,84,86,87,88,90,91,92,93,94,95,97,99,100,102,103,105,107,108,109,111,112,114,116,117,120,121,122,123,124,125,126,127,128,129,131,132,133,134,136,137,138), 25] <- 1

write.csv(file, file = "C:/Users/Mark/Documents/RAND/ALEKS_Tasks/Implementation/TeacherImplementation_02140217.csv", row.names = FALSE)

