args                  <- commandArgs(TRUE)
uniqueID              <- args[1]
path_data             <- args[2]
pDataFile             <- args[3]
path_imputeme_scripts <- args[4]

require(jsonlite)
#A function that will crawl all module directories and execute the export script if present

pd <- path_data
modules <- NULL


if (is.null(modules)) {
  modules<-list.files(path_imputeme_scripts)
} else {
  if(class(modules)!="character")stop("modules must be of class character")
  if(!all(file.exists(paste(path_imputeme_scripts,modules,sep=""))))stop("Not all UniqueIDs given were found")
}



outputList <- list()
outputList[["current_date_stamp"]] <- as.character(format(Sys.time(),"%Y-%m-%d_%H-%M-%S"))
#importing standard pData stuff
pData<-try(read.table(pDataFile,header=T,stringsAsFactors=F),silent=T)
if(class(pData)=="try-error"){
  print(paste("uniqueID",uniqueID,"was skipped due to inavailability of pData file"))
  next
}
if(nrow(pData)!=1)stop("pData file must have 1 row")

#check existence of cached file
cachedFile<-paste(pd,"/",uniqueID,".cached.gz",sep="")
cachedData<-try(read.table(cachedFile,header=T,stringsAsFactors=F),silent=T)
if(class(cachedData)=="try-error"){
  print(paste("uniqueID",uniqueID,"was skipped due to inavailability of cachedData file"))
  next
}

for(imp in c("uniqueID","filename","email","first_timeStamp")){
  if(!imp %in%colnames(pData))stop(paste("pData lacked this column:",imp))
  outputList[[imp]] <-pData[1,imp]
}
names(outputList)[names(outputList)%in%"filename"] <- "original_filename"
names(outputList)[names(outputList)%in%"email"] <- "original_submission_email"


for(module in modules){
  if(!file.info(paste0(path_imputeme_scripts,module))["isdir"])next
  if("export_script.R" %in% list.files(paste0(path_imputeme_scripts,module))){
    print(paste("Running",module,"for",uniqueID))
    if(exists("export_function"))suppressWarnings(rm("export_function"))
    source(paste(paste0(path_imputeme_scripts,module,"/export_script.R")))
    if(!exists("export_function"))stop(paste("In module",module,"there was an export_script.R without and export_function"))
    exp <- export_function(uniqueID, path_data, path_imputeme_scripts)
    outputList[[module]] <-exp

  }
}

JSON<-toJSON(outputList)

filename <- paste(uniqueID,"data.json",sep="_")

f<-file(filename,"w")
writeLines(JSON,f)
close(f)


m<-paste("The module was successfully run for ",uniqueID," samples on ",length(modules)," modules")
print(m)
