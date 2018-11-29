source("/scripts/getGenotypes.R");

export_function <- function(uniqueID, path_data, path_imputeme_scripts){
  #start check ups
  SNPs_to_analyze_file<-paste0(path_imputeme_scripts,"leukemia/SNPs_to_analyze_SOURCE.txt")

  means_file<-paste0(path_imputeme_scripts,"leukemia/2016-05-22_means.txt")
  means<-suppressWarnings(read.table(means_file,sep="\t",header=T,row.names=1,stringsAsFactors=F))

  diseaseNames<-rbind(
    c("CLL","Chronic Lymphoblastic Leukemia","Berndt-2015","26956414"),
    c("ALL","Acute Lymphoblastic Leukemia","Xu-2013","23512250")
  )
  colnames(diseaseNames)<-c("Acronym","Disease","Source","PMID")
  rownames(diseaseNames)<-diseaseNames[,"Acronym"]

  output<-list()

  for(disease in diseaseNames[,"Acronym"]){
    output[[disease]]<-list()
    SNPs_to_analyze<-read.table(sub("SOURCE",disease,SNPs_to_analyze_file),sep="\t",stringsAsFactors=F,header=T,row.names=1)
    genotypes<-get_genotypes(uniqueID=uniqueID,request=SNPs_to_analyze)

    #get risk score
    if(disease == "CLL"){
      or_column<-"OR.M1"
    }else if(disease=="ALL"){
      or_column<-"OR"
    }else{stop("!!!")}

    SNPs_to_analyze[,"Beta"]<-log10(SNPs_to_analyze[,or_column])
    GRS_beta <-get_GRS(genotypes=genotypes,betas=SNPs_to_analyze)
    output[[disease]][["GRS_beta"]] <- GRS_beta

    output[[disease]][["case_mean"]]<-means[disease,"case_mean"]
    output[[disease]][["case_sd"]]<-means[disease,"case_sd"]
    output[[disease]][["control_mean"]]<-means[disease,"control_mean"]
    output[[disease]][["control_sd"]]<-means[disease,"control_sd"]

    output[[disease]][["control_prob"]]<-signif(100*pnorm(GRS_beta,mean=output[[disease]][["control_mean"]],sd=output[[disease]][["control_sd"]]),4)

    output[[disease]][["case_prob"]]<-signif((1-pnorm(GRS_beta,mean=output[[disease]][["case_mean"]],sd=output[[disease]][["case_sd"]]))*100,4)




  }



  return(output)

}
