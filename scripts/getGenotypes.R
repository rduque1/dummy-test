get_genotypes <- function(
  uniqueID,
  request,
  path_data=getwd(),
  gtools="/install/gtool",
  namingLabel="cached", #should default to cached, but it's a way of separately saving larger cached sets in a different file
  mode="remote"
) {

  #checking
  if(class(namingLabel)!="character")stop(paste("namingLabel must be character, not",class(namingLabel)))
  if(length(namingLabel)!=1)stop(paste("namingLabel must be lengh 1, not",length(namingLabel)))


  #checking data in uniqueID's home folder
  if(class(uniqueID)!="character")stop(paste("uniqueID must be character, not",class(uniqueID)))
  if(length(uniqueID)!=1)stop(paste("uniqueID must be lengh 1, not",length(uniqueID)))
  genZipFile<-paste(path_data,"/",uniqueID,".gen.zip",sep="")
  if(!file.exists(genZipFile))stop(paste("Did not find a .gen file in path_data at ",path_data))
  inputZipFile<-paste(path_data,"/",uniqueID,".input_data.zip",sep="")
  if(!file.exists(inputZipFile))stop(paste("Did not find a .input_data file in path_data at ",path_data))
  cachedGenotypeFile<-paste(path_data,"/",uniqueID,".",namingLabel,".gz",sep="")
  if(!file.exists(cachedGenotypeFile))print(paste("Did not find a chachedGenotypeFile file in path_data at ",path_data," but that's no problem"))

  #creating a temp folder to use
  idTempFolder<-paste(path_data,uniqueID,"temp",sep="/")
  if(file.exists(idTempFolder))stop(paste("Temp folder exists, this could indicate that",uniqueID,"is already worked on. Wait a little, or write administrators if you think this is a mistake"))


  #checking other variables
  if(class(gtools)!="character")stop(paste("gtools must be character, not",class(gtools)))
  if(length(gtools)!=1)stop(paste("gtools must be lengh 1, not",length(gtools)))
  if(!file.exists(gtools))stop(paste("Did not find gtools at path:",gtools))

  if(class(request)!="data.frame")stop(paste("request must be data.frame, not",class(request)))
  if(!"chr_name"%in%colnames(request))stop("request object must have a column 'chr_name'")
  if("already_exists"%in%colnames(request))print("request object had a column 'already_exists', this will be overwritten")
  if(!any(substr(rownames(request),1,2)%in%"rs")){
    if(!any(substr(rownames(request),1,1)%in%"i")){
      stop("Not a single rs id was found among the rownames of the request. Really?")
    }
  }


  #checking existence of already cached genotypes
  if(file.exists(cachedGenotypeFile)){
    cachedGenotypes<-read.table(cachedGenotypeFile,header=T,stringsAsFactors=F,row.names=1)
    snpsAlreadyCached<-rownames(cachedGenotypes)
    requestDeNovo<-request[!rownames(request)%in%snpsAlreadyCached,,drop=F]
  }else{
    requestDeNovo<-request
  }


  #If there are anything novel, extract it from zip (takes a long time)
  if(nrow(requestDeNovo)>0){
    dir.create(idTempFolder, recursive = TRUE)
    chromosomes<-unique(requestDeNovo[,"chr_name"])
    contents<-unzip(genZipFile,list=T)

    #if input is in as a chromosome, use the 23andmefile as input
    if("input"%in%chromosomes){
      snpsFromInput<-requestDeNovo[requestDeNovo[,"chr_name"]%in%"input","SNP"]
      outZip<-unzip(inputZipFile, overwrite = TRUE,exdir = idTempFolder, unzip = "internal")
      cmd0 <- paste("grep -E '",paste(paste(snpsFromInput,"\t",sep=""),collapse="|"),"' ",outZip,sep="")
      input_genotypes<-system(cmd0,intern=T)
      input_genotypes<-do.call(rbind,strsplit(input_genotypes,"\t"))
      input_genotypes[,4]<-sub("\r$","",input_genotypes[,4])


      male_x_chr <- which(input_genotypes[,2]%in%"X" & nchar(input_genotypes[,4])==1)
      if(length(male_x_chr) > 0){
        input_genotypes[male_x_chr,4] <- paste(input_genotypes[male_x_chr,4]," ",sep="")
      }



      if(any(nchar(input_genotypes[,4])!=2))stop("input data must have length 2 genotypes")

      input_genotypes[,4]<-paste(substr(input_genotypes[,4],1,1),substr(input_genotypes[,4],2,2),sep="/")
      genotypes<-data.frame(row.names=input_genotypes[,1],genotype=input_genotypes[,4],stringsAsFactors=F)
    }else{
      genotypes<-data.frame(genotype=vector(),stringsAsFactors=F)
    }

    #if any normal style chromosome names are in use the gen files
    if(any(c(as.character(1:22),"X")%in%chromosomes)){
      chromosomes<-chromosomes[chromosomes%in%c(as.character(1:22),"X")]
      gensToExtract<-paste(uniqueID,"_chr",chromosomes,".gen",sep="")
      if(!all(gensToExtract%in%contents[,"Name"])){
        missing<-gensToExtract[!gensToExtract%in%contents[,"Name"]]
        stop(paste("These were missing in the zip-gen file:",paste(missing,collapse=", ")))
      }
      outZip<-unzip(genZipFile, files = gensToExtract, overwrite = TRUE,exdir = idTempFolder, unzip = "internal")

      f<-file(paste(idTempFolder,"/samples.txt",sep=""),"w")
      writeLines("ID_1 ID_2 missing sex",f)
      writeLines("0 0 0 D",f)
      writeLines("John Doe 0.0 2 ",f)#gender probably doesn't matter here
      close(f)


      #looping over all chromosomes and extracting the relevant genotypes in each using gtools
      for(chr in chromosomes){

        #This is wrapped in a try block, because it has previously failed from unpredictble memory issues, so it's better to give a few tries
        for(tryCount in 1:5){
          print(paste("Getting ped and map file at chr",chr," - try",tryCount))
          gen<-paste(idTempFolder,"/",uniqueID,"_chr",chr,".gen",sep="")
          snpsHere<-rownames(requestDeNovo)[requestDeNovo[,"chr_name"]%in%chr]
          write.table(snpsHere,file=paste(idTempFolder,"/snps_in_chr",chr,".txt",sep=""),quote=F,row.names=F,col.names=F)
          cmd1<-paste(gtools," -S --g " , gen, " --s ",idTempFolder,"/samples.txt --inclusion ",idTempFolder,"/snps_in_chr",chr,".txt",sep="")
          system(cmd1)
          subsetFile<-paste(idTempFolder,"/",uniqueID,"_chr",chr,".gen.subset",sep="")
          if(!file.exists(subsetFile)){
            print(paste("Did not find any of the SNPs on chr",chr))
            next
          }
          cmd2<-paste(gtools," -G --g " ,subsetFile," --s ",idTempFolder,"/samples.txt --snp --threshold 0.7",sep="")
          system(cmd2)


          ped<-try(strsplit(readLines(paste(idTempFolder,"/",uniqueID,"_chr",chr,".gen.subset.ped",sep="")),"\t")[[1]],silent=T)
          map<-try(read.table(paste(idTempFolder,"/",uniqueID,"_chr",chr,".gen.subset.map",sep=""),stringsAsFactors=FALSE),silent=T)

          if(class(ped)!="try-error" & class(map)!="try-error"){
            ped<-ped[7:length(ped)]
            genotypes_here<-data.frame(row.names=map[,2],genotype=sub(" ","/",ped),stringsAsFactors=F)
            break
          }else{
            genotypes_here<-data.frame(row.names=vector(),genotype=vector(),stringsAsFactors=F)
          }
        }
        genotypes<-rbind(genotypes,genotypes_here)
      }
    }


    genotypes[genotypes[,"genotype"]%in%"N/N","genotype"]<-NA
    stillMissing<-rownames(requestDeNovo)[!rownames(requestDeNovo)%in%rownames(genotypes)]
    genotypes<-rbind(genotypes,data.frame(row.names=stillMissing,genotype=rep(NA,length(stillMissing),stringsAsFactors=F)))

    #removing temporary folder
    unlink(idTempFolder,recursive=T)
  }

  #merge with cachedGenotypes
  if(nrow(requestDeNovo)>0) {
    if(file.exists(cachedGenotypeFile)){
      genotypes<-rbind(cachedGenotypes,genotypes)
      unlink(cachedGenotypeFile)
    }
    f<-gzfile(cachedGenotypeFile,"w")
    write.table(genotypes,file=f,sep="\t",col.names=NA)
    close(f)
  } else {
    genotypes<-cachedGenotypes
  }


  return(genotypes[rownames(request),,drop=FALSE])

}

get_GRS<-function(genotypes, betas){

  if(class(genotypes)!="data.frame")stop(paste("genotypes must be data.frame, not",class(genotypes)))
  if(!"genotype"%in%colnames(genotypes))stop(paste("genotypes must have a column genotype"))
  if(!all(unique(sub("[0-9].+$","",rownames(genotypes)))%in%c("i","rs"))){

    stop(paste("genotypes must have rownames starting with rs. You had these:",paste(unique(sub("[0-9].+$","",rownames(genotypes))),collapse=", ")))

  }

  if(class(betas)!="data.frame")stop(paste("genotypes must be data.frame, not",class(betas)))
  necessary_columns<-c("effect_allele","non_effect_allele","Beta")
  if(!all(necessary_columns%in%colnames(betas)))stop(paste("betas must have a column",paste(necessary_columns,collapse=", ")))
  if(!all(unique(sub("[0-9].+$","",rownames(betas)))%in%c("i","rs")))stop("betas must have rownames starting with rs")


  # if(!all(rownames(genotypes)%in%rownames(betas)))stop("all SNPs in genotypes must be present in betas")
  if(!all(rownames(betas)%in%rownames(genotypes)))stop("all SNPs in betas must be present in genotypes")



  geneticRiskScore<-0
  for(snp in rownames(betas)){
    if(is.na(genotypes[snp,"genotype"])){
      warning(paste("Note, for",snp,"we found missing genotypes. This can cause errors particularly if the data is not mean centered."))
      next
    }

    genotype<-strsplit(genotypes[snp,],"/")[[1]]
    effect_allele<-betas[snp,"effect_allele"]
    non_effect_allele<-betas[snp,"non_effect_allele"]

    if(!all(genotype%in%c(effect_allele,non_effect_allele))){
      print(paste("Note, for",snp,"we found wrong alleles:",paste(genotype,collapse=""),"and should find",effect_allele,"or",non_effect_allele))
      next
    }

    beta<-betas[snp,"Beta"]
    geneticRiskScore <- geneticRiskScore + sum(genotype%in%effect_allele) * beta
  }
  return(geneticRiskScore)

}
