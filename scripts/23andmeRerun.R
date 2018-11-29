args <- commandArgs(TRUE)
chr <- args[1]
gtools <- args[2]
plink <- args[3]

print(paste("retrying step 8-9 command for chr",chr,". Trying to split it in pieces (non-normal low memory running)"))
cmd7 <- paste("split --verbose --lines 5000000 step_8_chr",chr,".gen step_8_extra_chr",chr,".gen",sep="")
system(cmd7)
chunks<-grep(paste("step_8_extra_chr",chr,"\\.gena[a-z]$",sep=""),list.files(runDir),value=T)
for(chunk in chunks){
  ch<-sub("^.+\\.","",chunk)
  cmd8 <- paste(gtools," -G --g ",chunk," --s ",sampleFile," --chr ",chr," --snp",sep="")
  system(cmd8)
  #reform to plink fam/bim/bed file
  cmd9 <- paste(plink," --file ",chunk," --recode --transpose --noweb --out step_9_chr",chr,"_",ch,sep="")
  system(cmd9)
  #re-order to 23andme format
  cmd10<-paste("awk '{ print $2 \"\t\" $1 \"\t\"$4\"\t\" $5 $6}' step_9_chr",chr,"_",ch,".tped  > step_9_chr",chr,"_split_",ch,".txt",sep="")
  system(cmd10)
}
cmd11<-paste("cat ",paste(paste("step_9_chr",chr,"_split_",sub("^.+\\.","",chunks),".txt",sep=""),collapse=" ")," > step_10_chr",chr,".txt",sep="")
system(cmd11)
