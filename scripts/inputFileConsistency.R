source("/scripts/format_ancestry_com_as_23andme.R");
args <- commandArgs(TRUE)
path <- args[1]

#checking if it is a consistent file
print("checking if it is a consistent file")
testRead<-read.table(path,nrow=10,stringsAsFactors=F)
if(ncol(testRead)==5){
  #This could be an ancestry.com file. Check that first
  testRead2<-read.table(path,nrow=10,stringsAsFactors=F,header=T)
  if(unique(sub("[0-9]+$","",testRead2[,1]))!="rs")stop("testRead seemed like ancestry.com data, but didn't have rs IDs in column 1")
  #ok, this is probably an ancestry.com file. Let's reformat.
  format_ancestry_com_as_23andme(path)
}

testRead2<-read.table(path,nrow=10,stringsAsFactors=F)
if(ncol(testRead2)!=4)stop("testRead2 didn't have 4 columns (or 5 for ancestry.com data)")
if(unique(sub("[0-9]+$","",testRead2[,1]))!="rs")stop("testRead2 didn't have rs IDs in column 1")
