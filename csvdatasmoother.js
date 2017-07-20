const moment = require('moment');
const argv = require('minimist')(process.argv.slice(2));
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify');
const jStat = require('jStat');
const fs = require('fs');

const averageDuration = moment.duration(argv.duration || 'PT10M');
const ignoreColumns = argv.ignoreColumns ? argv.ignoreColumns.split(",").map(v => +v) : [];
const outputFilename = argv.output || "output.csv";
const inputFilename = argv.input || "input.csv"
if(argv.help){
  console.log(`
  optional arguments:
    --input="input.csv"
    --ouput="output.csv"
    --duration="PT10M"      <-- ISO8601 duration
    --ignoreColumns="7,8,9" <-- comma separated list of (0-based) column indexes to ignore in averaging
`);
  process.exit(0);
}

console.log(inputFilename, outputFilename, averageDuration.toString(), ignoreColumns);

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

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

let averages = records.map((r, idx) => {
  if(idx % 100){
    process.stdout.clearLine();  // clear current text
    process.stdout.cursorTo(0);  // move cursor to beginning of line
    process.stdout.write(`${idx} of ${records.length} (${(100*idx/records.length).toFixed(1)}%)`);
  }

  if( idx === 0 ) {
    return r;
  }
  let oldestDateInRange = moment(r[0]).subtract(averageDuration);
  let ii = records.find((rr, index) => {
    return (index > 0) && rr[0].isSameOrAfter(oldestDateInRange)
  });
  if(!ii){
    ii = -1;
  }
  else{
    ii = records.indexOf(ii);
  }
  let newRow = [r[0]];

  // console.log(ii + 1, idx + 1);

  let rows = records.slice(ii + 1, idx + 1);
  for(let col = 1; col < r.length; col++){
    if(ignoreColumns.indexOf(col) >= 0){
      newRow.push(records[idx][col]);
    }
    else{
      newRow.push(jStat.mean(rows.filter(rr => isNumeric(rr[col])).map(rr => rr[col])));
    }
  }
  return newRow;
}).map((rr, idx) => {
  if(idx === 0){
    return rr;
  }

  rr[0] = rr[0].format("MM/DD/YYYY HH:mm:ss");
  return rr;
});

stringify(averages, function(err, output){
  fs.writeFileSync(outputFilename, output);
});
