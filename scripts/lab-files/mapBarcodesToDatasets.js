`use strict`

const csv                = require('fast-csv');
const fs                 = require('fs');
const mysql              = require('mysql');
const Promise            = require("bluebird");
const GpDevSDK           = require("gp-dev-sdk");
const path               = require('path');
const Slack              = require('slack-node');

const pedFile            = process.argv[2];
const pedFileEndName     = process.argv[3];
const sampleTxtFile      = process.argv[4];
const configFileLocation = process.argv[5];
const dryRun             = process.argv[6] && process.argv[6] === "true" ? true : false;
const config             = require(configFileLocation);

const sedExecutable      = config.sedExecutable || "sed";

const slack              = new Slack();
slack.setWebhook(config.slackUri);

// console.log(config.devApi.baseUrl, config.devApi.clientName, config.devApi.clientSecret);

const gpSdk = new GpDevSDK(config.devApi.baseUrl, config.devApi.clientName, config.devApi.clientSecret);
//create custom error
function ValidationError(message) {this.message=message;}
ValidationError.prototype = Object.create(Error.prototype);

// console.log(config);

var stream = fs.createReadStream(sampleTxtFile);

var promiseContext = {
  pedFile,
  config,
  barcodes:{},
  slack
};
var promiseChain = Promise.resolve()
                          .then( function(){
                            promiseContext.connection = mysql.createConnection(promiseContext.config.db);
                            return promiseContext.connection.connect();
                          });

var counter = 0;

csv
  .fromStream(stream, {ignoreEmpty: true, delimiter: '\t', headers: true})
  .on("data", data => {
    let uniqueId = getUniqueIdentifier(data);
    promiseChain = promiseChain.then( () => {
        promiseContext.barcodes[uniqueId] = { data: data, errors:[] };
        console.log(uniqueId,'getUserInfo for barcode', data.Sample_Name);
        return getUserInfo(promiseContext.connection,  data.Sample_Name);
      })
      .then( userSqlData => {
        // console.log(userSqlData);
        console.log(uniqueId,'is sql returned data valid', userSqlData);
        promiseContext.barcodes[uniqueId].userSqlData = userSqlData;
        return isProductPackageValid(userSqlData, promiseContext.barcodes[uniqueId]);
      });
    if ( !dryRun ) {
      promiseChain = promiseChain
        // .then(() => console.log("in data") )
        .then( userSqlData => {
          console.log(uniqueId, `get accessToken`);
          return gpSdk.getAdminToken();
        })
        .then( body => {
          console.log(uniqueId, `create dataset with token`, body.data.attributes.accessToken);
          promiseContext.barcodes[uniqueId].token = body.data.attributes.accessToken;
          return gpSdk.createDataset(promiseContext.barcodes[uniqueId].token, promiseContext.barcodes[uniqueId].userSqlData.user_id);
        })
        .then( body => {
          let datasetId = body.data.id;
          console.log(uniqueId, `create dataset import for dataset`, body.data.id);
          promiseContext.barcodes[uniqueId].datasetId = datasetId;
          return gpSdk.createLocalArchiveDatasetImport(promiseContext.barcodes[uniqueId].token, datasetId, `/id_${datasetId}/id_${datasetId}.gen.zip`, false);
        })
        .then( body => {
          console.log(uniqueId, `mark dataset import ${body.data.id} as `, "wait-for-imputed-data");
          promiseContext.barcodes[uniqueId].datasetImportId = body.data.id;
          return gpSdk.markDatasetImportAsWaitForImputedData(promiseContext.barcodes[uniqueId].token, promiseContext.barcodes[uniqueId].datasetImportId);
        })
        .then( body => {
          return setUserDataset(promiseContext.connection, promiseContext.barcodes[uniqueId].datasetId, promiseContext.barcodes[uniqueId].userSqlData.user_id);
        })
        .then( body => {
          promiseContext.copiedPedFile = `${path.dirname(promiseContext.pedFile)}/${pedFileEndName}`;
          console.log(uniqueId, `check if file exists`, promiseContext.copiedPedFile);
          return fileExists(promiseContext.copiedPedFile);
        })
        .then( fileExistsResult => {
          if (fileExistsResult) {
            console.log(uniqueId, promiseContext.copiedPedFile, `does exists`);
            return fileExistsResult;
          } else {
            console.log(uniqueId, promiseContext.copiedPedFile, `does NOT exists so copy from`, promiseContext.copiedPedFile);
            return copyFile(promiseContext.pedFile, promiseContext.copiedPedFile);
          }
        })
        .then( body => {
          var searchString = promiseContext.barcodes[uniqueId].searchString = uniqueId;
          var replaceString = promiseContext.barcodes[uniqueId].replaceString = promiseContext.barcodes[uniqueId].datasetId;
          console.log(uniqueId, sedExecutable, ["-i", `s/${searchString}/${replaceString}/g`, promiseContext.copiedPedFile] );
          return exec(sedExecutable, ["-i", `s/${searchString}/${replaceString}/g`, promiseContext.copiedPedFile] );
        });
    }
    promiseChain = promiseChain
      .catch( ValidationError, function(e) {
        // console.log(e);
        console.log(uniqueId, "VALIDATION_ERROR: ", e.message);
        // console.log(promiseContext.barcodes);
        // console.log(e.message);
        promiseContext.barcodes[uniqueId].errors.push(e.message);
      })
      .then( () => console.log() );
  })
  .on("end", function(){
    promiseChain.then( () => {
      try {
        promiseContext.connection.end();
      } catch(e) {
        console.log(e);
      }
      removeUnMappedLines(promiseContext);
      // notifySlackHandler(promiseContext);
    });
  });

function getUniqueIdentifier (data) {
  return `${data.SentrixBarcode_A}_${data.SentrixPosition_A}`;
}

function getUserInfo(connection, barcode) {
  return new Promise((resolve,reject) => {
    var queryString = `SELECT pp.id, pp.user_id, u.developer_dataset_id FROM product_packages pp
                      LEFT JOIN users u ON pp.user_id=u.id
                      WHERE pp.barcode = '${barcode.trim()}'`;

    // console.log(queryString);
    connection.query(queryString, function(err, rows, fields) {
      if (err) reject(err);
      else     resolve(rows[0]|| {});
    });
  });
}

function isInt(n) {
  return Number(n) === n && n % 1 === 0;
}

function setUserDataset( connection, datasetId, user_id ){
  if ( !isInt(user_id) ) {
    throw new Error("user_id must be a int");
  }
  return new Promise((resolve,reject) => {
    var queryString = `UPDATE users SET developer_dataset_id=${datasetId} WHERE id=${user_id}`;
    connection.query(queryString, function(err, rows, fields) {
      if (err) reject(err);
      else     resolve(rows[0]|| {});
    });
  });
}

function isProductPackageValid(productPackageSqlData, barcodeObj) {
  if( !productPackageSqlData.id ) {
    throw new ValidationError(`No product_package for barcode ${barcodeObj.data.Sample_Name}` );
  }
  if( !productPackageSqlData.user_id ) {
    throw new ValidationError(`No user_id for product_package ${productPackageSqlData.id} and with barcode ${barcodeObj.data.Sample_Name}` );
  }
  if( productPackageSqlData.developer_dataset_id ) {
    throw new ValidationError(`User ${productPackageSqlData.user_id} for barcode ${barcodeObj.data.Sample_Name} has already a developer_dataset_id ${productPackageSqlData.developer_dataset_id}`);
  }
  return productPackageSqlData;
}

function deleteLineFromFileContaining(text, file) {
  console.log(text, sedExecutable, ["-i", `/${text}/d`, file] );
  return exec(sedExecutable, ["-i", `/${text}/d`, file] );
}

function removeUnMappedLines(contextObj) {
  var deleteLinePromiseChain = Promise.resolve();
  for (var barcode in contextObj.barcodes) {
    var dataObj = contextObj.barcodes[barcode];
    if (contextObj.copiedPedFile && (!dataObj.userSqlData || !dataObj.userSqlData.user_id || !dataObj.datasetId || !dataObj.datasetImportId || dataObj.errors.length>0) ) {
      deleteLinePromiseChain = deleteLinePromiseChain.then( () => fileExists(contextObj.copiedPedFile) )
        .then(fileExistsResult => {
          if (fileExistsResult) {
            return deleteLineFromFileContaining(barcode, contextObj.copiedPedFile);
          }
        });
    }
  }
  deleteLinePromiseChain = deleteLinePromiseChain.then( () => contextObj.copiedPedFile ? fileExists(contextObj.copiedPedFile) : false)
    .then(fileExistsResult => {
      if (fileExistsResult) {
        return exec(`cut`, ["-f", 2, contextObj.copiedPedFile], true );
      } else {
        console.log("NO Remaining samples, NO PED file created" );
      }
    })
    .then( result => {
      var datasetIds = Object.keys(contextObj.barcodes).map( code => contextObj.barcodes[code].datasetId ).filter( val => val ).map( datasetId => datasetId.toString());
      console.log(datasetIds);
      var linesToDelete = [];
      if ( typeof result === 'object' && result.output) {
        var linesArray = mapProcessOutputToLinesArray(result.output);
        // console.log(result.output, datasetIds, linesArray);
        linesArray.forEach( sampleInPed => {
          sampleInPed = sampleInPed.trim();
          console.log(sampleInPed, sampleInPed && sampleInPed.length>0 && datasetIds.indexOf(sampleInPed)==-1, datasetIds.indexOf(sampleInPed));
          if ( sampleInPed && sampleInPed.length>0 && datasetIds.indexOf(sampleInPed)==-1 ) {
            console.log(sampleInPed, "!contextObj.barcodes[sampleInPed]", !contextObj.barcodes[sampleInPed]);
            console.log(sampleInPed, "contextObj.barcodes[sampleInPed].errors && contextObj.barcodes[sampleInPed].errors.length>0", contextObj.barcodes[sampleInPed] && contextObj.barcodes[sampleInPed].errors && contextObj.barcodes[sampleInPed].errors.length>0);
            linesToDelete.push(sampleInPed);
          }
        });
        console.log(linesToDelete);
        console.log("Remaining samples:" );
        return chainLineDelete(linesToDelete, contextObj.copiedPedFile).then(() => exec(`cut`, ["-f", 2, contextObj.copiedPedFile] ));
      }
    });
}

function mapProcessOutputToLinesArray( processOutput ) {
  var linesArray = [];
  processOutput.forEach( output => output.split("\n").forEach( line => linesArray.push(line)) );
  return linesArray;
}

function chainLineDelete(lines, file) {
  var chain = Promise.resolve();
  lines.forEach( line => {
    chain = chain.then( () => deleteLineFromFileContaining(line, file) )
  });
  return chain;
}

function notifySlackHandler(contextObj) {
  for (var barcode in contextObj.barcodes) {
    var dataObj = contextObj.barcodes[barcode];
    if (dataObj.userSqlData && dataObj.userSqlData.user_id && dataObj.datasetId && dataObj.datasetImportId) {
      notifySlackChannel(contextObj.slack, `Mapped barcode *${dataObj.data.Sample_Name}* and labreference *${dataObj.searchString}* to user`, `*${dataObj.userSqlData.user_id}* and datasetId: *${dataObj.datasetId}*`);
    } else {
      notifySlackChannel(contextObj.slack, `Mapping *FAILED* *${dataObj.data.Sample_Name}* and *${dataObj.searchString}*`, "Missing user_id or datasetId or datasetImportId");
    }
    dataObj.errors.forEach( err => {
      notifySlackChannel(contextObj.slack, `Mapping *FAILED* *${dataObj.data.Sample_Name}* and *${dataObj.searchString}*`, err);
    });
  }
}

function notifySlackChannel(slackInstance, title, message) {
  slackInstance.webhook({
    username: "preprocessing-labfiles-bot",
    text: `${title}: ${message}`
  }, function(err, response) {
    if (err) console.log(err);
    // else     console.log(response);
  });
}

function fileExists(path) {
  return new Promise( (resolve, reject) => {
    fs.stat(path, (err, stat) => {
      if(err == null) {
        resolve( true );
      } else if(err.code == 'ENOENT') {
        resolve( false );
      } else {
        reject(err);
      }
    });
  });
}

function copyFile(source, target) {

  return new Promise( (resolve, reject) => {
    var rd = fs.createReadStream(source);
    var wr = fs.createWriteStream(target);
    function rejectCleanup(err) {
      rd.destroy();
      wr.end();
      reject(err);
    }
    rd.on('error', rejectCleanup);
    wr.on('error', rejectCleanup);
    wr.on('finish', resolve);
    rd.pipe(wr);
  });
}


function exec(command, argsArr, caputureOuput) {
  const spawn = require('child_process').spawn;
  const cli = spawn(command, argsArr);
  const output = [];
  return new Promise( (resolve, reject) => {
    cli.stdout.on('data', (data) => caputureOuput ? output.push(data.toString()) : console.log(`stdout: ${data}`) );

    cli.stderr.on('data', (data) => console.log(`stderr: ${data}`) );

    cli.on('close', (code) => {
      console.log(`child process exited with code ${code}`);
      if (code == 0) {
        caputureOuput ? resolve( {code, output} ) : resolve(code);
      } else {
        reject(code);
      }
    });
  });
}
