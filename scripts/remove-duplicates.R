

args <- commandArgs(TRUE)
mapFile <- args[1]
outFile <- args[2]

map <- read.table(mapFile,sep='\t', stringsAsFactors=F)
exclude <- map[duplicated(map[,4]),2]
print(paste('Removed',length(exclude),'SNPs that were duplicated'))
write.table(exclude, file=outFile, sep='\t', row.names=FALSE, col.names=F, quote=F)
