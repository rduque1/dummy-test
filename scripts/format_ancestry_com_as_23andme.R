
format_ancestry_com_as_23andme<-function(path){
  #this is a function to be called whenever a text file with 5 columns and header row needs to be reformatted to a text file with 4 columns and no header rows (and 20 commented out lines at the top). I.e. when reforming from ancestry.com to 23andme format.

  if(class(path)!="character")stop(paste("path must be character, not",class(path)))
  if(length(path)!=1)stop(paste("path must be lengh 1, not",length(path)))
  if(!file.exists(path))stop(paste("Did not find file at path:",path))

  testRead<-read.table(path,nrow=10,stringsAsFactors=F,header=T)
  if(ncol(testRead)!=5){stop("testRead of file didn't have 5 columns (as it should have when invoking ancestry.com conversion)")}
  if(unique(sub("[0-9]+$","",testRead[,1]))!="rs")stop("testRead seemed like ancestry.com data, but didn't have rs IDs in column 1")

  #inserting # at first rsid palce
  cmd1<-paste("sed 's/^rsid/#rsid/' ",path," > tempOut0.txt",sep="")
  system(cmd1)


  #retain only non-commented lines
  cmd2<-paste("awk 'NF && $1!~/^#/' tempOut0.txt > tempOut1.txt",sep="")
  system(cmd2)

  #merge column 4 and 5
  cmd3<-paste("awk '{ print $1 \"\t\" $2 \"\t\"$3\"\t\" $4 $5}' tempOut1.txt  > tempOut2.txt",sep="")
  system(cmd3)

  #Saving header and insert the right number of commented out lines (20)
  linestoinsert<-20
  cmd4<-paste("sed  -n '/^\\s*#/!{=;q}' tempOut0.txt",sep="")
  commentLineCount<-as.numeric(system(cmd4,intern=T))-1
  header<-readLines("tempOut0.txt",n=commentLineCount)

  f<-file("spacer","w")
  headerFull<-c(rep("#",linestoinsert-length(header)),header)
  writeLines(paste(headerFull,collapse="\n"),f)
  close(f)

  cmd5<-paste("cat spacer tempOut2.txt > tempOut3.txt")
  system(cmd5)

  #Replace chromosome 23 with X, 24 with Y, 25 with XY, 26 with MT
  system("sed -E -i 's/([a-z]+[0-9]+)\\t23\\t/\\1\\tX\\t/g' tempOut3.txt")
  system("sed -E -i 's/([a-z]+[0-9]+)\\t24\\t/\\1\\tY\\t/g' tempOut3.txt")
  system("sed -E -i 's/([a-z]+[0-9]+)\\t25\\t/\\1\\tXY\\t/g' tempOut3.txt")
  system("sed -E -i 's/([a-z]+[0-9]+)\\t26\\t/\\1\\tMT\\t/g' tempOut3.txt")

  file.rename("tempOut3.txt",path)

  unlink("spacer")
  unlink("tempOut2.txt")
  unlink("tempOut1.txt")
  unlink("tempOut0.txt")

}
