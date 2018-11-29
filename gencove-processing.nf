params.rawdata    = 'TODO'
params.datasetId  = "placeholder"
params.webhook    = false
params.publishDir = "/tmp"
params.genomeAnnotationFile      = "/root/All_20170710.vcf.gz" //ftp: //ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b150_GRCh37p13/VCF/
params.genomeAnnotationIndexFile = "/root/All_20170710.vcf.gz.tbi"
params.twentythreeandmeSnps      = "/reference/23andme.snps.txt"

genomeAnnotationFile      = file(params.genomeAnnotationFile)
genomeAnnotationIndexFile = file(params.genomeAnnotationIndexFile)
twentythreeandmeSnpsFile  = file(params.twentythreeandmeSnps)

datasetId = params.datasetId

if (datasetId == "placeholder") {
  datasetId = (params.rawdata =~ /([^\/.]*)+$/)[ 0 ][ 1 ]
}
println datasetId
println params.rawdata

getInputFileFromPathOutChan = Channel.fromPath( params.rawdata )

process unzip {
  echo true

  input:
  file inputFile from getInputFileFromPathOutChan

  output:
  file("*.vcf") into unzipOutChan

  script:
  if ( "${inputFile}".endsWith(".zip") ) {
    """
    unzip ${inputFile}
    """
  } else if ( "${inputFile}".endsWith(".gz") ) {
    """
    gunzip -f ${inputFile}
    """
  } else if ( "${inputFile}".endsWith(".vcf") ) {
    """
    cp ${inputFile} rawdata.txt
    """
  } else {
    error "Unsupported file extension ${inputFile}"
  }
}

process split {
  echo true

  input:
  file inputFile from unzipOutChan

  output:
  file("*.vcf") into splitOutChan
  file("*.vcf") into splitOutChan1

  script:
  """
  bgzip -c ${inputFile} > ${inputFile}.gz
  tabix -p vcf ${inputFile}.gz
  for i in `tabix -l ${inputFile}.gz`; do
    tabix -h ${inputFile}.gz \$i > \$i.vcf;
  done
  """
}

convertInChan = splitOutChan.flatMap();

splitOutChan1.flatMap().subscribe { println "File: ${it.name}" };

process annotate {
  //INFO https://gtamazian.com/2016/05/29/adding-rs-numbers-to-vcf-file/
  maxForks 4

  input:
  file inputFile from convertInChan
  file annotationFile from genomeAnnotationFile
  file annotationIndexFile from genomeAnnotationIndexFile

  output:
  set val(chromosome), file("${chromosome}.rsnum.vcf") into annotOutChan

  script:
  vcfFile    = file(inputFile)
  chromosome = vcfFile.getBaseName()

  """
  #remove ID column
  bcftools annotate --output ${chromosome}.noids.vcf.gz --output-type z --remove ID ${inputFile}

  #create index for next step
  tabix -p vcf ${chromosome}.noids.vcf.gz

  #add id column with rs ids
  bcftools annotate --annotations ${annotationFile} --columns ID --output ${chromosome}.rsnum.vcf --output-type v ${chromosome}.noids.vcf.gz

  ls -l
  """
}

process GpToGl {
  maxForks 4

  input:
  set val(chromosome), file( vcfFile ) from annotOutChan

  output:
  set val(chromosome), file(ouputFile) into gpOutChannel

  script:
  ouputFile  = "${chromosome}.rsnum.gp.vcf"

  """
  #!/usr/bin/env python
  import math

  with open("${vcfFile}", 'r') as infile:
      with open('${ouputFile}', 'a') as outfile:
          for line in infile:
              if line.startswith( '##FORMAT=<ID=GL,' ):
                  line = line.replace("ID=GL,", "ID=GP,").replace('Description="Genotype Likelihoods"', 'Description="Genotype Probabilities"')
              elif line.startswith( '#' ):
                  line = line
              else:
                  parts      = line.split("\\t")
                  formatDefs = parts[-2]
                  formatDefs = formatDefs.split(":")
                  GLidx      = formatDefs.index("GL") if "GL" in formatDefs else -1
                  if GLidx and GLidx > -1 :
                      formatDefs[GLidx] = "GP"
                      parts[-2]         = ":".join(formatDefs)
                      formatParts       = parts[-1]
                      formatParts       = formatParts.split(":")
                      gls               = formatParts[GLidx]
                      gls               = gls.split(",")
                      sumOfGL           = 0;
                      for idx,g in enumerate(gls):
                          g        = float(g)
                          gls[idx] = math.pow(10,g)
                          sumOfGL  = sumOfGL + gls[idx]
                      for idx,g in enumerate(gls):
                          gls[idx] = gls[idx]/sumOfGL
                          gls[idx] = str(gls[idx])
                      formatParts[GLidx] = ",".join(gls)
                      formatParts        = ":".join(formatParts)
                      parts[-1]          = formatParts
                      line               = "\\t".join(parts)
              outfile.write( "%s\\n"%line.rstrip() )
  """
}

process convert {
  maxForks 4

  input:
  set val(chr), file( vcfFile ) from gpOutChannel
  file twentythreeandmeSnps from twentythreeandmeSnpsFile

  output:
  set val(chr), file("${genOutputPrefix}.gen.gz"), file("${genOutputPrefix}.samples"), file("${twentythreeandmeOutputPrefix}.txt") into convertChanOut

  script:
  genOutputPrefix              = "${datasetId}_chr${chr}"
  twentythreeandmeOutputPrefix = "${chr}_23andme"

  """
  head -n 100 ${vcfFile}

  ## convert to gen file using GP(genotype probability)
  bcftools convert --chrom  --vcf-ids --gensample ${genOutputPrefix} --tag GP ${vcfFile}

  ## filter 23andme snps
  vcftools --vcf ${vcfFile} --snps ${twentythreeandmeSnps} --recode --out ${twentythreeandmeOutputPrefix}

  ## convert to 23andme format
  plink-1.9 --noweb --vcf ${twentythreeandmeOutputPrefix}.recode.vcf --snps-only --recode 23 --out ${twentythreeandmeOutputPrefix}
  """
}

process summarize {

  echo true

  input:
  set val(chr), file(genFileIn), file(sampleFile), file( "in_${chr}_23andme.txt" ) from convertChanOut

  output:
  set file("id_${datasetId}_chr${chr}.23andme.txt"), file(genFile), file(chr23andmeFile) into summarizeChan

  script:
  genFile          = "id_${datasetId}_chr${chr}.gen"
  chr23andmeFile   = "${chr}_23andme.txt"

  """
  echo $chr
  echo $sampleFile
  echo $genFile
  echo chr23andmeFile

  cp ${genFileIn} ${genFile}.gz
  gunzip ${genFile}.gz

  cp in_${chr}_23andme.txt $chr23andmeFile

  #make list of non-indels
  awk -F' ' '{ if ((length(\$4) > 1 ) || (length(\$5) > 1 )) print \$2 }' ${genFile}  > step_8_chr${chr}_snps_to_exclude

  /install/gtool -S --g ${genFile} --s $sampleFile --exclusion step_8_chr${chr}_snps_to_exclude --og step_8_chr${chr}.gen

  set +e
  #Convert to ped format
  /install/gtool -G --g step_8_chr${chr}.gen --s $sampleFile --chr ${chr} --snp

  #reform to plink fam/bim/bed file
  plink-1.07 --file step_8_chr${chr}.gen --recode --transpose --noweb --out step_9_chr${chr}

  awk '{ print \$2 \"\t\" \$1 \"\t\"\$4\"\t\" \$5 \$6}' step_9_chr${chr}.tped  > step_10_chr${chr}.txt
  set -e

  #The step 8 and also 9 sometime fails for no apparent reason. Probably memory. We therefore make a checkup, where
  #it is checked if the file actually exists and if not - a more complicated step splits it up in chunks.
  #It's not looking nice, but at least the split-up only needs to run in very low memory settings

  if [ -f step_10_chr${chr}.txt ]; then
    FILESIZE=\$(stat -c%s "step_10_chr${chr}.txt")
  else
    FILESIZE=0
  fi

  # arbitraly re-run if it's less than 100 bytes (fair to assume something was wrong then)
  if [ \$FILESIZE -lt 100]; then
   Rscript '/scripts/23andmeRerun.R' ${chr} "/install/gtool" "plink-1.07"
  fi

  #remove NN
  awk '{ if(\$4 != \"NN\") print}' step_10_chr${chr}.txt  > id_${datasetId}_chr${chr}.23andme.txt

  """

}

summarizeConcatChan = summarizeChan.reduce { a, b -> a + b }

process zipping {
  echo true
  publishDir params.publishDir, mode: 'copy'

  input:
  file toZipFiles from summarizeConcatChan

  output:
  set file( zipFileGen ), file( zipFile23andme ), file( zipFile23andmeSmall ) into zippingOutChan

  script:
  zipFile23andme          = "id_${datasetId}.23andme.zip"
  zipFileGen              = "id_${datasetId}.gen.zip"
  zipFile23andmeSmall     = "id_${datasetId}.input_data.zip"

  """
  #zipping gen files
  zip -r9X ${zipFileGen} *.gen

  #zipping and moving 23andme files
  zip -r9X ${zipFile23andme} *.23andme.txt

  for i in {1..22}; do
    wc -l \${i}_23andme.txt
    cat \${i}_23andme.txt >> id_${datasetId}.input_data.txt
  done
  zip ${zipFile23andmeSmall} id_${datasetId}.input_data.txt

  """
}

// zippingOutChan.subscribe { println "File: ${it}" };

workflow.onComplete {
  if (params.webhook && !workflow.success) {
    proc = ["curl", "-X", "POST", "-H", "Content-type: application/json", "--data", "'{\"text\": \"Gencove pipeline ${datasetId} failed\"}'", params.webhook].execute()
    proc.waitForProcessOutput(System.out, System.err)
  }
  def fileName = workflow.success ? 'success.txt' : 'fail.txt';
  def filePath = "${params.publishDir}/${fileName}";
  proc2 = ["touch", filePath].execute()
  proc2.waitForProcessOutput(System.out, System.err)
}
