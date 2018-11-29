source("/scripts/getGenotypes.R");


args                  <- commandArgs(TRUE)
uniqueID              <- args[1]
path_imputeme_scripts <- args[2]
gtool                 <- args[3]

#getting a list of SNPs to analyze
all_SNPs<-data.frame(SNP=vector(),chr_name=vector(),stringsAsFactors = F)
for(module in list.files(path_imputeme_scripts,full.names=T)){
  if(!file.info(module)["isdir"])next
  if("SNPs_to_analyze.txt" %in% list.files(module)){
    SNPs_to_analyze<-read.table(paste(module,"/SNPs_to_analyze.txt",sep=""),sep="\t",stringsAsFactors=F,header=T,quote="",comment="")
    if(!all(c("SNP","chr_name")%in%colnames(SNPs_to_analyze)))stop(paste("In",module,"a SNPs_to_analyze file was found that lacked the SNP and chr_name column"))
    SNPs_to_analyze[,"chr_name"]<-as.character(SNPs_to_analyze[,"chr_name"])
    if(!all(SNPs_to_analyze[,"chr_name"]%in%c(1:22,"X","input")))stop(paste("In",module,"a SNPs_to_analyze had a chr_name column that contained something else than 1:22 and X"))
    all_SNPs<-rbind(all_SNPs,SNPs_to_analyze[,c("SNP","chr_name")])

  }
}

#an extra check for non-discrepant chr info
if(any(duplicated(all_SNPs[,"SNP"]))){
  duplicates<-all_SNPs[duplicated(all_SNPs[,"SNP"]),"SNP"]
  for(duplicate in duplicates){
    if(length(unique(all_SNPs[all_SNPs[,"SNP"]%in%duplicate,"chr_name"]))!=1)stop(paste("Found a multi-entry SNP",duplicate,"with discrepant chr info"))
  }
  all_SNPs<-all_SNPs[!duplicated(all_SNPs[,"SNP"]),]
}
rownames(all_SNPs)<-all_SNPs[,"SNP"]

print(paste("Checking all requested SNPs from",uniqueID))

genotypes<-try(get_genotypes(uniqueID=uniqueID, request=all_SNPs, gtools=gtool))
if(class(genotypes)=="try-error"){
  print("Some other error happened in the extraction crawler, but probably no cause for alarm:")
  print(genotypes)
}

#getting the nonsenser SNPs if possible
e<-try(load(paste0(path_imputeme_scripts,"nonsenser/2015-12-16_all_coding_SNPs.rdata")))
if(class(e)!="try-error"){
  genotypes<-get_genotypes(uniqueID, coding_snps, gtools=gtool ,namingLabel="cached.nonsenser")
}
