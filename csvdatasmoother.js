const moment = require('moment');
const argv = require('minimist')(process.argv.slice(2));
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify');
const jStat = require('jStat');
const fs = require('fs');

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

const derivativeFunctions = argv.functions ? argv.functions.split(",") : ['mean'];
const averageDuration = moment.duration(argv.duration || 'PT10M');
const ignoreColumns = argv.ignoreColumns ? argv.ignoreColumns.split(",").map(v => +v).filter(v => isNumeric(v)) : [];
const outputFilename = argv.output || "output.csv";
const inputFilename = argv.input || "input.csv"
const augmented = argv.augmented || false;
const dropDateRanges = argv.drop ? argv.drop.split(",").map((v) => {
  let range = v.split("-");
  if(range.length !== 2){
    return null;
  }
  let start = moment(range[0].trim(), "MM/DD/YYYY HH:mm:ss");
  let end = moment(range[1].trim(), "MM/DD/YYYY HH:mm:ss");
  if(!start.isValid() || !end.isValid()){
    return null;
  }
  return {start, end};
}).filter(v => v !== null) : [];

if(argv.help){
  console.log(`
  optional arguments:
    --input="input.csv"
    --ouput="output.csv"
    --duration="PT10M"      <-- ISO8601 duration
    --ignoreColumns="7,8,9" <-- comma separated list of (0-based) column indexes to ignore in averaging
    --augmented             <-- keeps the unaveraged data alongside the averaged data
    --drop="MM/DD/YYYY HH:mm:ss - MM/DD/YYYY HH:mm:ss, MM/DD/YYYY HH:mm:ss - MM/DD/YYYY HH:mm:ss, etc"
`);
  process.exit(0);
}

console.log(inputFilename, outputFilename, averageDuration.toString(), ignoreColumns, augmented);

// convert timestamps to moments, reject rows with unparseable timestamps
// convert other fields to numeric where possible
var records = parse(fs.readFileSync(inputFilename)).map((r, idx) => {
  if(idx === 0){
    return r;
  }

  try{

    let m = moment(r[0], 'MM/DD/YYYY HH:mm:ss');
    if(m.isValid()){
      r[0] = m;
    }
    else{
      throw new Error("first col is not a valid date");
    }

    // coerce numberst to be actual numbers
    for(let ii = 1; ii < r.length; ii++){
      if(isNumeric(r[ii])){
        r[ii] = +r[ii];
      }
    }
  }
  catch(e){
    console.log(e.message);
    return null;
  }

  return r;
})
.filter(r => r !== null); // remove entries with invalid dates

let last_index = 1;

if(argv.ignoreColumns){
  let originalIgnoreColumns = argv.ignoreColumns.split(',');
  if((originalIgnoreColumns.length >= 2) && (originalIgnoreColumns.slice(-1)[0] === '+')){
    for(let jj = +(originalIgnoreColumns.slice(-2)[0]) + 1; jj < records[0].length; jj++){
      ignoreColumns.push(jj);
    }
  }
  console.log(ignoreColumns);
  console.log();
}

// crunch the numbers
let averages = records.map((r, idx) => {
  if(idx % 100){
    process.stdout.clearLine();  // clear current text
    process.stdout.cursorTo(0);  // move cursor to beginning of line
    process.stdout.write(`${idx} of ${records.length} (${(100*idx/records.length).toFixed(1)}%)`);
  }

  if( idx === 0 ) {
    if(augmented){
      let newR = [r[0]];
      for(let jj = 1; jj < r.length; jj++){
        newR.push(r[jj]);
        if(ignoreColumns.indexOf(jj) < 0){

          if(derivativeFunctions.indexOf('mean') >= 0){
            newR.push(r[jj]+'_avg');
          }

          if(derivativeFunctions.indexOf('stdev') >= 0){
            newR.push(r[jj]+'_stdev');
          }
        }
      }
      return newR;
    }
    return r;
  }

  let rowMoment = moment(r[0]);

  let taboo = false;
  dropDateRanges.forEach((range) => {
    if(rowMoment.isSameOrAfter(range.start) && rowMoment.isSameOrBefore(range.end)){
      taboo = true;
    }
  });

  if(taboo){
    return null;
  }

  let oldestDateInRange = rowMoment.subtract(averageDuration);
  let record_subset = records.slice(last_index);
  let ii = record_subset.find(rr =>  rr[0].isSameOrAfter(oldestDateInRange));
  // console.log(idx, last_index, oldestDateInRange.format(), ii[0].format());

  if(!ii){
    ii = -1;
  }
  else{
    ii = record_subset.indexOf(ii) + last_index;
    last_index = ii;
  }
  let newRow = [r[0]];

  // console.log(ii, idx + 1);

  let rows = records.slice(ii, idx + 1);

  if(rows.length == 0){
    return null;
  }

  for(let col = 1; col < r.length; col++){
    if(ignoreColumns.indexOf(col) >= 0){
      newRow.push(records[idx][col]);
    }
    else{
      let nontrivial_data = rows.filter(rr => isNumeric(rr[col]));
      if(!nontrivial_data.length){
        if(augmented){
          newRow.push(records[idx][col]);
        }
        newRow.push("---");
      }
      else{
        if(augmented){
          newRow.push(records[idx][col]);
        }

        if(derivativeFunctions.indexOf('mean') >= 0){
          newRow.push(jStat.mean(nontrivial_data.map(rr => rr[col])));
        }

        if(derivativeFunctions.indexOf('stdev') >= 0){
          newRow.push(jStat.stdev(nontrivial_data.map(rr => rr[col]), true));
        }

      }
    }
  }
  return newRow;
})
.filter(rr => rr !== null)
.map((rr, idx) => {
  if(idx === 0){
    return rr;
  }

  rr[0] = rr[0].format("MM/DD/YYYY HH:mm:ss");
  return rr;
});

stringify(averages, function(err, output){
  fs.writeFileSync(outputFilename, output);
});
