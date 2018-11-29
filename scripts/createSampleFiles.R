args       <- commandArgs(TRUE)
ped_path   <- args[1]
map_path   <- args[2]
plinkExec  <- args[3]

receive_lab_file<-function(
  map_path,
  ped_path,
  sample_id,
  runDir,
  plink=paste0(path_impute_dir,"plink")
){
  if(class(map_path)!="character")      stop(paste("map_path must be of class character, not",class(map_path)))
  if(length(map_path)!=1)               stop(paste("map_path must be of length 1, not",length(map_path)))
  if(!file.exists(map_path))            stop(paste("Did not find map_path file at:",map_path))


  if(class(ped_path)!="character")      stop(paste("ped_path must be of class character, not",class(ped_path)))
  if(length(ped_path)!=1)               stop(paste("ped_path must be of length 1, not",length(ped_path)))
  if(!file.exists(ped_path))            stop(paste("Did not find ped_path file at:",ped_path))
  if(sub("\\.ped","",basename(ped_path)) != sub("\\.map","",basename(map_path)))  stop("map_path and ped_path should have the same filename except for the endings")
  plink_basename <- paste0( dirname(ped_path), "/", sub("\\.ped", "", basename(ped_path)) )
  print(plink_basename)

  if(class(runDir)!="character")        stop(paste("runDir must be character, not",class(runDir)))
  if(length(runDir)!=1)                 stop(paste("runDir must be lenght 1, not",length(runDir)))
  if(!file.exists(runDir))              stop(paste("Did not find runDir at path:",runDir))
  if(length(grep("/$",runDir))!=0)      stop("Please don't use a trailing slash in the runDir")

  if(class(sample_id)!="character")     stop(paste("sample_id must be of class character, not",class(sample_id)))
  if(length(sample_id)!=1)              stop(paste("sample_id must be of length 1, not",length(sample_id)))

  #checking samples
  cmd1      <- paste0("cut -f 1-2 ",ped_path)
  samples1  <- system(cmd1,intern=T)
  samples2  <- data.frame(matrix(unlist(strsplit(samples1,"\t")), nrow=length(samples1), byrow=T))
  if(!sample_id %in% samples2[,2]){
    if(!sample_id %in% samples2[,1]){
      stop(paste("The sample_id",sample_id,"was found in the first column of the ped file. You should give sample names from the second column, per the nxt-dx.com setup."))
    }else{
      stop(paste("The sample_id",sample_id,"was not found in the ped file"))
    }
  }
  if(sum(samples2[,2]%in%sample_id)>1)  stop(paste(sample_id,"was found more than one time in the ped file (?)"))

  keep_text <- paste(samples2[samples2[,2]%in%sample_id,1],samples2[samples2[,2]%in%sample_id,2])
  write.table(keep_text,file=paste0(runDir,"/keep_files.txt"),row.names=F,col.names=F,quote=F)

  # exclude_chr0_text<-c("0 0 1000000000 rem1","XY 0 1000000000 rem2")
  # write.table(exclude_chr0_text,file=paste0(runDir,"/exclude_chr.txt"),row.names=F,col.names=F,quote=F)
  # cmd2 <- paste0(plink," --file ",plink_basename," --noweb --recode transpose --out ",runDir,"/",sample_id,"_transposed.txt --exclude range ",runDir,"/exclude_chr.txt --keep ",runDir,"/keep_files.txt")

  cmd2 <- paste0(plink," --file ",plink_basename," --noweb --recode transpose --out ",runDir,"/",sample_id,"_transposed.txt --keep ",runDir,"/keep_files.txt")
  system(cmd2)


  write.table(
    c("#This is a pseudo 23andme file generated from nxt-dx.com lab data",
      "#rsid\tchromosome\tposition\tgenotype"),
    file=paste0(runDir,"/header.txt"),row.names=F,col.names=F,quote=F)

  cmd3<-paste0("awk '{ print $2 \"\t\" $1 \"\t\" $4 \"\t\" $5 $6}' ",runDir,"/",sample_id,"_transposed.txt.tped >",runDir,"/",sample_id,"_pos.txt")
  system(cmd3)


  cmd4 <- paste0("cat ", runDir,"/header.txt ",runDir,"/",sample_id,"_pos.txt > ",runDir,"/",sample_id,"_raw_data.txt")
  system(cmd4)

}


sample_ids <- system(paste("cut -f 2",ped_path),intern=T)

for(sample_id in sample_ids){

  runDir    <- paste0("",sample_id)
  dir.create(runDir)
  receive_lab_file(map_path=map_path, ped_path=ped_path,
                   sample_id=sample_id, runDir=runDir, plink=plinkExec)
  from_path <- paste0(runDir,"/",sample_id,"_raw_data.txt")
  to_path   <- paste0(runDir,"/id_",sample_id,"_raw_data.txt")
  system(paste0("cp ",from_path," ",to_path))
  write.table("Job is ready",file=paste0(runDir,"/job_status.txt"),col.names=F,row.names=F,quote=F)
  email     <- "lassefolkersen@gmail.com"
  filename  <- sample_id
  protect_from_deletion <- TRUE
  uniqueID  <- paste0("id_",sample_id)
  save(uniqueID,email,filename,protect_from_deletion,file=paste(runDir,"/variables.rdata",sep=""))
}
