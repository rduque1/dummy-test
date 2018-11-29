source("/scripts/getGenotypes.R");

export_function <- function(uniqueID, path_data, path_imputeme_scripts){
  #start check ups

  BRCA_table_file <-paste0(path_imputeme_scripts,"/BRCA/SNPs_to_analyze.txt")
  BRCA_table<-read.table(BRCA_table_file,sep="\t",header=T,stringsAsFactors=F)

  rownames(BRCA_table)<-BRCA_table[,"SNP"]
  BRCA_table[BRCA_table[,"chr_name"]%in%13,"gene"]<-"BRCA2"
  BRCA_table[BRCA_table[,"chr_name"]%in%17,"gene"]<-"BRCA1"

  BRCA_table["i4000377","gene"]<-"BRCA1"
  BRCA_table["i4000378","gene"]<-"BRCA1"
  BRCA_table["i4000379","gene"]<-"BRCA2"

  BRCA_table["i4000377","consequence_type_tv"]<-"Direct from 23andme"
  BRCA_table["i4000378","consequence_type_tv"]<-"Direct from 23andme"
  BRCA_table["i4000379","consequence_type_tv"]<-"Direct from 23andme"


  #get genotypes and calculate gheight
  genotypes <- get_genotypes(uniqueID=uniqueID,request=BRCA_table)

  BRCA_table[,"Your genotype"]<-genotypes[rownames(BRCA_table),]

  BRCA_table<-BRCA_table[,c("SNP","gene","Your genotype","normal","polyphen_prediction","sift_prediction","consequence_type_tv")]




  reds<-which(BRCA_table[,"consequence_type_tv"]%in%c("Direct from 23andme","stop_gained") | BRCA_table[,"polyphen_prediction"]%in%"probably damaging" | BRCA_table[,"sift_prediction"]%in%"deleterious")

  yellows <-which(!(1:nrow(BRCA_table))%in%reds)

  greens<-which(is.na(BRCA_table[,"Your genotype"]) | BRCA_table[,"Your genotype"]== BRCA_table[,"normal"])


  BRCA_table[reds,"colour"] <- "Red"
  BRCA_table[yellows,"colour"] <- "Yellow"
  BRCA_table[greens,"colour"] <- "Green"

  BRCA_table[,"sift_prediction"]<-NULL
  BRCA_table[,"consequence_type_tv"]<-NULL
  BRCA_table[,"polyphen_prediction"]<-NULL


  return(BRCA_table)

}
