args <- commandArgs(TRUE)
chrStrandFile <- args[1]
chrMapFile <- args[2]
chrPedFile <- args[3]
chrOutPedFileName <- args[4]
chrOutExclusionFileName <- args[5]


#Many homozygote SNPs will fail the check, because, well - of course, they don't have the ref-allele. So we make more detailed R script for sorting them
logFile<-read.table(chrStrandFile ,sep='\t', stringsAsFactors=FALSE, header=F, skip=1)
omitMissing<-logFile[logFile[,1] %in% 'Missing',3]
logStrand<-logFile[logFile[,1] %in% 'Strand',]
omitNonIdentical<-logStrand[logStrand[,5] != logStrand[,6],3]
omitBlank<-logStrand[logStrand[,5]%in%'',3]

#These are super-annoying. We have to create another (fake) person with the alternative allele just for their sake. This next command takes all the homozygotes, minus the indels (which are too complicated to lift out from 23andme)
forceHomozygoteTable<-logStrand[
  logStrand[,5] == logStrand[,6] &
    nchar(logStrand[,9])==1 &
    nchar(logStrand[,10])==1 &
    !logStrand[,5] %in% c("D","I") &
    !logStrand[,6] %in% c("D","I")
  ,]

#This removes any cases where there are more than to alleles involved
forceHomozygoteTable<-forceHomozygoteTable[sapply(apply(forceHomozygoteTable[,c(5,6,9,10)],1,unique),length)==2,]

#This removes any duplicates there might be
forceHomozygoteTable<-forceHomozygoteTable[!duplicated(forceHomozygoteTable[,4]),]
map<-read.table(chrMapFile, sep="\t", stringsAsFactors=F)
#This loads the ped file, and doubles it
ped2<-ped1<-strsplit(readLines(chrPedFile)," ")[[1]]
ped2[1]<-"Temporary"
ped2[2]<-"Non_person"
if((length(ped1)-6) / 2 !=nrow(map))stop("mismatch between map and ped")
replacementPos<-which(map[,2]%in%forceHomozygoteTable[,4])
A1_pos<-7+2*(replacementPos-1)
A2_pos<-8+2*(replacementPos-1)
ped2[A1_pos]<-forceHomozygoteTable[,9]
ped2[A2_pos]<-forceHomozygoteTable[,10]
ped<-rbind(ped1,ped2)
write.table(ped, chrOutPedFileName, sep=" ", col.names=F, row.names=F,quote=F)
omitRemaining<-logStrand[!logStrand[,4]%in%forceHomozygoteTable[,4],3]
print(paste('Omitting',length(omitMissing),'because of missing',length(omitBlank),'because they are blank, and',length(omitNonIdentical),'true strand flips'))
write.table(c(omitNonIdentical,omitBlank,omitMissing,omitRemaining),file=chrOutExclusionFileName, sep='\t', row.names=F, col.names=F, quote=F)
