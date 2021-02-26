#Hopper to mozart
setwd('C:/Tasks/Demo_AU/auto')
#install.packages('RJDBC')
#install.packages('plyr')
#install.packages('lubridate')
#install.packages('readr')
#install.packages('stringr')
#install.packages('sendmailR')
#install.packages('dplyr')
suppressMessages(library(RJDBC))
suppressMessages(library(plyr))
suppressMessages(library(lubridate))
suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(sendmailR))
suppressMessages(library(dplyr))
options(warn=-1)

#change system language and location
Sys.setenv("LANGUAGE"="En")
Sys.setlocale("LC_ALL", "English")

data_transfer = function(df){
  #to get the name of anargument in the function
  d = substitute(c(df))
  var_name = sapply(d[-1],deparse)
  ###############################################
  df[is.na(df)]<-0
  file= file(paste0("C:/Tasks/Demo_AU/auto/",var_name,".txt"), open="w")
  write.table(df,file,sep = ",",row.name = FALSE, col.name = FALSE, append = FALSE)
  close(file)
  temp_list = read_lines(paste0("C:/Tasks/Demo_AU/auto/",var_name,".txt"))
  temp_list = gsub("\"","",temp_list)
  file = file(paste0("C:/Tasks/Demo_AU/auto/",var_name,".txt"), open="w")
  for (each in temp_list){
    write_lines(each,file,sep = "\n")
  }
  close(file)
}

##DB Account&password
fileCon = file("C:/Tasks/JDBC_config/account_yfw.txt", open="r")                #---------------- change the user id and password
user = readLines(fileCon, n=1)
passwd = readLines(fileCon, n=1)
close(fileCon)
drv = JDBC("com.teradata.jdbc.TeraDriver", c("C:/Tasks/JDBC_config/terajdbc4.jar", "C:/Tasks/JDBC_config/tdgssconfig.jar")) #-------------upload the drv
dbCon_hop = dbConnect(drv,"jdbc:teradata://hopper/TMODE=ANSI",user,passwd)

#generating table on hopper
query = read_file("C:/Tasks/Demo_AU/auto/PL_VD_HOP.sql")
sql_list = strsplit(query, ";")[[1]]
for(i in 1:length(sql_list)){
  sql_list[i] = paste0(sql_list[i],';')
  dbSendUpdate(dbCon_hop,sql_list[i])
  print(i)
  print(sql_list[i])
}

print('generating done, start moving')

#table moving
Query_vd = "sel * from P_AUB2C_T.VOLUME_DISCOUNT;"
Query_pl = "sel * from P_AUB2C_T.PL_CLICK;"
volume_discount = dbGetQuery(dbCon_hop, Query_vd)
pl_click = dbGetQuery(dbCon_hop, Query_pl)
dbDisconnect(dbCon_hop)

data_transfer(volume_discount)
data_transfer(pl_click)

system("cmd.exe", input = paste("bteq<","C:/Tasks/Demo_AU/auto/bteq_volume_discount.txt"))
system("cmd.exe", input = paste("bteq<","C:/Tasks/Demo_AU/auto/bteq_pl_click.txt"))

#main R parallel
rm(list=ls())
write("START",file = "C:/Tasks/Demo_AU/log.txt", append = FALSE,sep='\n')
dir.create("C:/Tasks/Demo_AU/html_output/")
dir.create("C:/Tasks/Demo_AU/pdf_output/")

library(RODBC)
library(sqldf)
library(DBI)
library(rJava)
library(RJDBC)
library(rmarkdown)

library(ggplot2)
library(gridExtra)
library(knitr)
library(grid)
library(reshape2)
library(kableExtra)
library(htmltools)
library(parallel)
library(foreach)
library(doParallel)


#monthly settings
public_folder = "Z:/Marketing/Relationship_Marketing/eDM/CRM/c_C2C and B2C/SMB_seller_clinics_report/2019-05-08/pdf_new"

#check current path to see if wkhtmltopdf is included
Sys.getenv("PATH")

#path to convert html to pdf,if the path of wkhtmltopdf is not included
#Sys.setenv(PATH=paste(Sys.getenv("PATH"),"C:/Program Files/wkhtmltopdf/bin",sep=";"))

#change system language and location
Sys.setenv("LANGUAGE"="En")
Sys.setlocale("LC_ALL", "English")

#  BUILD JDBC CONNECTION
jdbc.drv<-JDBC("com.teradata.jdbc.TeraDriver", c("C:/Tasks/JDBC_config/terajdbc4.jar", "C:/Tasks/JDBC_config/tdgssconfig.jar"))

fileCon = file("C:/Tasks/JDBC_config/account_yfw.txt", open="r")                #--------- change the user id and password
user = readLines(fileCon, n=1)
passwd = readLines(fileCon, n=1)
close(fileCon)
jdbc.conn = dbConnect(jdbc.drv,"jdbc:teradata://mozart.vip.ebay.com/TMODE=ANSI",user,passwd)
#user="jumiao"
#passwd="EWQdsacxz$123456"
#jdbc.conn = dbConnect(jdbc.drv,"jdbc:teradata://hopper/TMODE=ANSI",user,passwd)


#set path to save user files
setwd("C:/Tasks/Demo_AU/")

#read table from csv file
mustShowSum <- read.csv(file="./input/mustShowSummary.csv", header=TRUE, sep=",")
actionContent <- read.csv(file="./input/actionContent_Remove_3.1.csv", header=TRUE, sep=",")

############################################################
#get data table

recData <- dbGetQuery(jdbc.conn, "
                      SEL * FROM P_AUB2C_T.AU_clinics_recom_eGD
                      ")

bmkData <- dbGetQuery(jdbc.conn, "
                      SEL * FROM P_AUB2C_T.AU_CLINICS_BMK_EGD
                      ")

vertData <- dbGetQuery(jdbc.conn, "
SEL * FROM P_AUB2C_T.AU_clinics_vert_output
")

#recData <- dbGetQuery(jdbc.conn, "SEL * FROM P_JUMIAO_H_T.AU_clinics_recom_eGD")

#bmkData <- dbGetQuery(jdbc.conn, "SEL * FROM P_JUMIAO_H_T.AU_CLINICS_BMK_EGD ")

#vertData <- dbGetQuery(jdbc.conn, "SEL * FROM P_JUMIAO_H_T.AU_clinics_vert_output")


invisible(dbDisconnect(jdbc.conn))

recData$month_beg_dt = as.Date(recData$month_beg_dt)
recData$month <- months(recData$month_beg_dt)

bmkData$month_beg_dt = as.Date(bmkData$month_beg_dt)
bmkData$month <- months(bmkData$month_beg_dt)

vertData$month_beg_dt = as.Date(vertData$month_beg_dt)
vertData$month <- months(vertData$month_beg_dt)

vertData$Month <- factor(vertData$month, levels = c("January","February", "March", "April", "May", "June", "July", "August", 
                                                    "September", "October", "November","December"))



vertData$Vertical <- factor(vertData$bsns_vrtcl_name, levels = c("Unknown", "Fashion","Home & Garden", "Lifestyle" , 
                                                                 "Parts & Accessories", "Business & Industrial"
                                                                 , "Media", "Vehicles", "Collectibles", "Electronics"))


vertData$gmv_lc <- round(vertData$gmv_lc)

head(recData)
head(bmkData)
head(vertData)

summary(recData)
summary(bmkData)
summary(vertData)

slrIDlist <- unique(recData$slr_id)

##########################################################
#generate outupt html file
gen_html_b<- function(slrID_i) {
  theSlrRec = recData[recData$slr_id == slrIDlist[slrID_i],]
  theSlrVert = vertData[vertData$slr_id == slrIDlist[slrID_i],]
  theSlrVert = subset(theSlrVert,Vertical != 'Vehicles')
  theSlrBmk = bmkData[bmkData$slr_id == slrIDlist[slrID_i],]
  file.copy("C:/Tasks/Demo_AU/Demo_AU_Remove_EAN_0304.Rmd", paste0("C:/Tasks/Demo_AU/Demo_AU_Remove_EAN_0304",slrID_i,".Rmd"))
  render(paste0("C:/Tasks/Demo_AU/Demo_AU_Remove_EAN_0304",slrID_i,".Rmd"),output_file = paste0("C:/Tasks/Demo_AU/html_output/", theSlrRec$ENCRYPTED_USER_ID, '.html'))
  #render(paste0("C:/Tasks/Demo_AU/Demo_AU_Remove_EAN_0304.Rmd"),output_file = paste0("C:/Tasks/Demo_AU/html_output/", theSlrRec$ENCRYPTED_USER_ID, '.html'))
  file.remove(paste0("C:/Tasks/Demo_AU/Demo_AU_Remove_EAN_0304",slrID_i,".Rmd"))
  print(paste('html file',slrID_i,'is done',sep = ' '))
  gc()
}

slrID_length<- length(slrIDlist)

#parallel computing
cl<- makeCluster(8)
clusterExport(cl,c("slrIDlist","recData","vertData","bmkData",
                   "gen_html_b","actionContent","mustShowSum"))
clusterEvalQ(cl,c(library(reshape2),library(rmarkdown),library(knitr)))
registerDoParallel(cl)
foreach(x=1:slrID_length) %dopar% gen_html_b(x)
#foreach(x=1:20) %dopar% gen_html_b(x)
stopCluster(cl)



###############################################################################
#convert html to pdf

#read names from html files
html_folder = "C:/Tasks/Demo_AU/html_output/"
pdf_folder = "C:/Tasks/Demo_AU/pdf_output/"
htmlList <- list.files(html_folder,pattern = '*.html')
dateform <- format(Sys.Date(),"_%B_%Y")

html2pdf <- function(i) {
  htmlFile <-strsplit(htmlList[i], ".", fixed = TRUE)
  slr_ID = htmlFile[[1]][1]
  html_file = paste0(html_folder, slr_ID, ".html")
  pdf_file = paste0(pdf_folder, slr_ID, dateform, ".pdf")
  system("cmd.exe", input = paste("wkhtmltopdf", html_file, pdf_file, sep = " "))
}

cl<- makeCluster(8)
clusterExport(cl,c("htmlList","html_folder","pdf_folder", "dateform"))
registerDoParallel(cl)
foreach(x=1:length(htmlList)) %dopar% html2pdf(x)
stopCluster(cl)

##############################################################################
#final upload to shared folder
# 
# pdf_folder = "C:/Tasks/Demo_AU/pdf_output/"
# pdfList <- list.files(pdf_folder,pattern = '*.pdf')
# 
# 
# finalupload <- function(i) {
#   file.copy(paste0(pdf_folder,pdfList[i]),public_folder)
# }
# 
# cl<- makeCluster(12)
# clusterExport(cl,c("pdfList","pdf_folder"))
# registerDoParallel(cl)
# foreach(x=1:length(pdfList)) %dopar% finalupload(x)
# stopCluster(cl)


# '
# for (file in pdfList) {
#   print(paste0(pdf_folder,file))
#   #file.copy(paste0(pdf_folder,file),"Z:/Marketing/Relationship_Marketing/eDM/CRM/c_C2C and B2C/SMB_seller_clinics_report/2019-01-03/pdf")
# }
# '


#jiaying monthly SMB

setwd('C:/Tasks/Demo_AU/auto')
sink("./logofcode.txt")
## Script Parameters 
suppressMessages(library(RJDBC))
suppressMessages(library(plyr))
suppressMessages(library(lubridate))
suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(sendmailR))
suppressMessages(library(dplyr))
options(warn=-1)

#change system language and location
Sys.setenv("LANGUAGE"="En")
Sys.setlocale("LC_ALL", "English")

##DB Account&password
fileCon = file("C:/Tasks/JDBC_config/account_yfw.txt", open="r")                #---------------- change the user id and password
user = readLines(fileCon, n=1)
passwd = readLines(fileCon, n=1)
close(fileCon)
drv = JDBC("com.teradata.jdbc.TeraDriver", c("C:/Tasks/JDBC_config/terajdbc4.jar", "C:/Tasks/JDBC_config/tdgssconfig.jar")) #-------------upload the drv
dbCon = dbConnect(drv,"jdbc:teradata://mozart.vip.ebay.com/TMODE=ANSI",user,passwd)

Query = "
sel max(month_beg_dt) 
from prs_restricted_v.slng_vd_b2c_slr_lvl3_sm 
where month_beg_dt GT '2019-01-01';
"
source_table_date = dbGetQuery(dbCon, Query)
source_table_date <- as.Date(source_table_date[,1],"%Y-%m-%d")
month <- month(as.POSIXlt(source_table_date))
month_today<- month(as.POSIXlt(Sys.Date()))


if (month == month_today -1){
  query = read_file("C:/Tasks/Demo_AU/auto/SMB_3TABLE_B2C.sql")
  sql_list = strsplit(query, ";")[[1]]
  for(i in 1:length(sql_list)){
    sql_list[i] = paste0(sql_list[i],';')
    print(i)
    print(sql_list[i])
    dbSendUpdate(dbCon,sql_list[i])
    }
}else{
  print("Data is not ready in upstream table")
  }

query =
  paste("CREATE MULTISET TABLE P_AUB2C_T.AU_CLINICS_BMK_EGD", month_today," AS (
        SELECT *
        FROM AU_CLINICS_BMK_EGD
  )WITH DATA
        PRIMARY INDEX(slr_id);", sep = "")
dbSendUpdate(dbCon,query)

query =
  paste("CREATE MULTISET TABLE P_AUB2C_T.AU_clinics_recom_eGD", month_today," AS (
        SELECT *
        FROM AU_clinics_recom_eGD
  )WITH DATA
        PRIMARY INDEX(slr_id);", sep = "")
dbSendUpdate(dbCon,query)

query =
  paste("CREATE MULTISET TABLE P_AUB2C_T.AU_clinics_vert_output", month_today," AS (
        SELECT *
        FROM AU_clinics_vert_output
  )WITH DATA
        PRIMARY INDEX(slr_id,bsns_vrtcl_name);", sep = "")
dbSendUpdate(dbCon,query)
dbDisconnect(dbCon)

# 
# sink()
# Sys.sleep(10)
# 
# library(sendmailR)
# 
# #####send plain email
# 
# from <- "ywang44@ebay.com"
# to <- "ywang44@ebay.com"
# subject <- "SMB_Clinic_Report_B2C_Data_refresh"
# body <- "BU zhong yao"                     
# mailControl=list(smtpServer="ATOM.CORP.EBAY.COM")
# 
# #####send same email with attachment
# 
# #needs full path if not in working directory
# attachmentPath <- "C:/Tasks/Demo_AU/auto/logofcode.txt"
# 
# #same as attachmentPath if using working directory
# attachmentName <- "logofcode.txt"
# 
# #key part for attachments, put the body and the mime_part in a list for msg
# attachmentObject <- mime_part(x=attachmentPath,name=attachmentName)
# bodyWithAttachment <- list(body,attachmentObject)
# sendmail(from=from,to=to,subject=subject,msg=bodyWithAttachment,control=mailControl)