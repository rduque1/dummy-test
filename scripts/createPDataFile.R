library("tools")
args <- commandArgs(TRUE)
uniqueID <- args[1]
email <- args[2]
filename <- args[3]
protect_from_deletion <- args[4]

#creating the pData file
timeStamp <- format(Sys.time(),"%Y-%m-%d-%H-%M")
md5sum <- md5sum(paste(uniqueID,"_raw_data.txt",sep=""))
gender <- system( "cut --delimiter=' ' -f 5 step_1.ped", intern=T )
f<-file( "pData.txt", "w")
writeLines(paste(c("uniqueID","filename","email","first_timeStamp","md5sum","gender","protect_from_deletion"),collapse="\t"),f)
writeLines(paste(c(uniqueID,filename,email,timeStamp,md5sum,gender,protect_from_deletion),collapse="\t"),f)
close(f)
