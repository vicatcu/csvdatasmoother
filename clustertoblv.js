const moment = require('moment');
const argv = require('minimist')(process.argv.slice(2));
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify');
const jStat = require('jStat');
const fs = require('fs');

// expects "smoothed" input data, with ppb columns ignored

const inputFilename = argv.input || "input.csv"
const outputFilename = argv.output || inputFilename;
const clusterTemperatureEpsilon = argv.epsilon || 0.5;
const temperatureColumn = argv.temperatureCol || 2;
const voltageColumn1 = argv.voltageCol1 || 14;
const voltageColumn2 = argv.voltageCol2 || 17;

let clusterTemperatures = [];
let clusteredResults = {};

let records = parse(fs.readFileSync(inputFilename));

function slope_intercept(x1, y1, x2, y2){
  let ret = {
    slope: (y1 - y2) / (x1 - x2),
  };
  ret.intercept = y1 - ret.slope * x1;
  return ret;
}

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

  objKeyPrefixes.forEach((ok) => {
    ret[ok+'_mean'] = jStat.mean(clusteredResults[clusterKey].map(r => +r[objKeyIndex[ok]]).filter(v => isNumeric(v)));
    ret[ok+'_stdev'] = jStat.stdev(clusteredResults[clusterKey].map(r => +r[objKeyIndex[ok]]).filter(v => isNumeric(v)), true);
  });

  return ret;
});

// first sort the stats data by mean temperature
stats = stats.sort((a, b) => {
  if(a.temperature_mean < b.temperature_mean){
    return -1;
  }
  return +1;
});

// ok now that you have temperature ordered cluster data,
// compute slopes and intercepts for adjacent bins implied by them
let linest = stats.map((row, idx) => {
  if(idx > 0){
    let ret = { temperature: stats[idx-1].temperature_mean };
    let tmp = slope_intercept(row.temperature_mean, row.voltage1_mean,
      stats[idx-1].temperature_mean, stats[idx-1].voltage1_mean);
    ret.voltage1_slope = tmp.slope;
    ret.voltage1_intercept = tmp.intercept;
    tmp = slope_intercept(row.temperature_mean, row.voltage2_mean,
      stats[idx-1].temperature_mean, stats[idx-1].voltage2_mean);
    ret.voltage2_slope = tmp.slope;
    ret.voltage2_intercept = tmp.intercept;
    return ret;
  }
  return null;
}).filter(v => v !== null);

let prefix = records[0][voltageColumn1].split("[")[0];
let obj = {};

try{ fs.unlinkSync(outputFilename+'.txt'); } catch (e) { }

obj[prefix] = {commands: []};
obj[prefix].commands.push(`${prefix}_blv clear`);
linest.forEach((r) => {
  obj[prefix].commands.push(`${prefix}_blv add ${r.temperature.toFixed(8)} ${r.voltage1_slope.toFixed(8)} ${r.voltage1_intercept.toFixed(8)}`);
});
obj[prefix].commands.forEach((c) => {
  fs.appendFileSync(outputFilename+'.txt', c+'\n');
  console.log(c);
});
fs.appendFileSync(outputFilename+'.txt', "\n");
console.log();

prefix = records[0][voltageColumn2].split("[")[0];
obj[prefix] = {commands: []};
obj[prefix].commands.push(`${prefix}_blv clear`);
linest.forEach((r) => {
  obj[prefix].commands.push(`${prefix}_blv add ${r.temperature.toFixed(8)} ${r.voltage2_slope.toFixed(8)} ${r.voltage2_intercept.toFixed(8)}`);
})
obj[prefix].commands.forEach((c) => {
  fs.appendFileSync(outputFilename+'.txt', c+'\n');
  console.log(c);
});

console.log();

// console.log("Clusters: ", clusterTemperatures);
// console.log("Stats: ", stats);
// console.log("Linest: ", linest);
fs.writeFileSync(outputFilename+'.json', JSON.stringify(obj, null, 2));
