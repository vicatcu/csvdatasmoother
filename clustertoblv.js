const moment = require('moment');
const argv = require('minimist')(process.argv.slice(2));
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify');
const jStat = require('jStat');
const fs = require('fs');

// expects "smoothed" input data, with ppb columns ignored

const outputFilename = argv.output || "output.json";
const inputFilename = argv.input || "input.csv"
const clusterTemperatureEpsilon = argv.epsilon || 1.0;
const temperatureColumn = argv.temperatureCol || 2;
const voltageColumn1 = argv.voltageCol1 || 10;
const voltageColumn2 = argv.voltageCol2 || 12;

let clusterTemperatures = [];
let clusteredResults = {};

var records = parse(fs.readFileSync(inputFilename));

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function cluster(row){
  
  if(!isNumeric(row[temperatureColumn])){
    return;
  }

  for(let ii = 0; ii < clusterTemperatures.length; ii++){
    if(Math.abs(clusterTemperatures[ii] - row[temperatureColumn]) < clusterTemperatureEpsilon){
      clusteredResults[clusterTemperatures[ii]].push(row);
      return;
    }
  }

  clusterTemperatures.push(row[temperatureColumn]);
  clusteredResults[row[temperatureColumn]] = [ row ];
}

records.forEach((row) => {
  cluster(row);
});

// now for each of the clusters, calculate the average temperature and voltage stats
let stats = Object.keys(clusteredResults).map((clusterKey) => {
  let objKeyPrefixes = ['temperature', 'voltage1', 'voltage2'];
  let objKeyIndex = {
    temperature: temperatureColumn,
    voltage1: voltageColumn1,
    voltage2: voltageColumn2
  }
  let ret = {};

  console.log(clusteredResults[clusterKey].map(r => r[voltageColumn2]))

  objKeyPrefixes.forEach((ok) => {
    ret[ok+'_mean'] = jStat.mean(clusteredResults[clusterKey].map(r => +r[objKeyIndex[ok]]).filter(v => isNumeric(v)));
    ret[ok+'_stdev'] = jStat.stdev(clusteredResults[clusterKey].map(r => +r[objKeyIndex[ok]]).filter(v => isNumeric(v)), true);
  });

  return ret;
});

console.log("Clusters: ", clusterTemperatures);
console.log("Stats: ", stats);
