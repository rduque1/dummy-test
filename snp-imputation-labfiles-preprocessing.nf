params.labdatazip                  = 'TODO'
params.publishPath                 = false
params.webhook                     = false
params.nodeLabFileMappingLogConfig = null
params.dryRun                      = false

println params.labdatazip

process getInputFileFromPath {

  echo true

  output:
  file("*.zip") into getInputFileFromPathOutChan

  script:
  if ( params.labdatazip.startsWith("gs://") ) {
    """
    gsutil cp ${params.labdatazip} .
    """
  } else if ( params.labdatazip.startsWith("s3://") ) {
    """
    aws s3 cp ${params.labdatazip} .
    """
  } else {
    """
    cp ${params.labdatazip} .
    """
  }
}

process unzip {

  echo true

  input:
  file rawdata from getInputFileFromPathOutChan

  output:
  set file("*.ped"), file("*.map"), file("SampleAnnotation*.txt") into unzipOutChan

  """
  unzip ${rawdata}
  find . -name "*.ped" -exec mv {} .  \\;
  find . -name "*.map" -exec mv {} .  \\;
  find . -name "SampleAnnotation*.txt" -exec mv {} .  \\;
  """
}

process mapSampleFilesToDatasets {

  echo true;

  input:
  set file(pedFilePath), file(mapFilePath), file(sampleAnnotationFilePath) from unzipOutChan

  output:
  set file("${pedFilePath}"), file("${mapFilePath}") into mapSampleFilesToDatasetsOutChan

  """
  mv ${pedFilePath} original_${pedFilePath}
  node /scripts/lab-files/mapBarcodesToDatasets.js original_${pedFilePath} ${pedFilePath} ${sampleAnnotationFilePath} ${params.nodeLabFileMappingLogConfig} ${params.dryRun}
  """

}


process makeSampleFiles {

  echo true

  input:
  set file(pedFilePath), file(mapFilePath) from mapSampleFilesToDatasetsOutChan

  when:
  !params.dryRun

  script:
  if (params.publishPath && params.publishPath.startsWith("gs://") ) {
    """
    echo "${pedFilePath} ${mapFilePath}"
    Rscript '/scripts/createSampleFiles.R' ${pedFilePath} ${mapFilePath} plink-1.9
    ls -l
    for d in `cut -f 2 ${pedFilePath}`;
    do
      gsutil cp -r \$d ${params.publishPath}/id_\$d
    done
    """
  } else if ( params.publishPath && params.publishPath.startsWith("s3://") ) {
    """
    echo "${pedFilePath} ${mapFilePath}"
    Rscript '/scripts/createSampleFiles.R' ${pedFilePath} ${mapFilePath} plink-1.9
    ls -l
    for d in `cut -f 2 ${pedFilePath}`;
    do
      aws s3 cp --recursive \$d ${params.publishPath}/id_\$d/
    done
    """
  } else {
    """
    echo "${pedFilePath} ${mapFilePath}"
    Rscript '/scripts/createSampleFiles.R' ${pedFilePath} ${mapFilePath} plink-1.9
    ls -l
    for d in `cut -f 2 ${pedFilePath}`;
    do
      cp -r \$d ${params.publishPath}
    done
    """
  }

}

workflow.onComplete {
  if (params.webhook && !workflow.success) {
    proc = ["curl", "-X", "POST", "-H", "Content-type: application/json", "--data", "'{\"text\": \"Error in labfile imputation preprocessing for input ${params.labdatazip}\"}'", params.webhook].execute()
    proc.waitForProcessOutput(System.out, System.err)
  }
}
