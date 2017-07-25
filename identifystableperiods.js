const moment = require('moment');
const argv = require('minimist')(process.argv.slice(2));
const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify');
const jStat = require('jStat');
const fs = require('fs');

const nostatus = !!argv.nostatus;
const inputFilename = argv.input || "input.csv"
const outputFilename = argv.output || inputFilename;
const temperatureColumn = argv.temperatureCol || 6; //stdev
const temperatureStdevAboveThreshold = argv.abovethreshold || 0.09;
const temperatureStdevBelowThreshold = argv.belowthreshold || 0.05;
let changingDurationLimit = argv.limit || 'PT60M';
let requiredDurationAboveThreshold = argv.overduration || 'PT10M';
let requiredDurationBelowThreshold = argv.underduration || 'PT10M';
const ignoreInitialStability = argv.ignoreInitialStability || false;
const debug = argv.debug;

requiredDurationAboveThreshold = moment.duration(requiredDurationAboveThreshold);
requiredDurationBelowThreshold = moment.duration(requiredDurationBelowThreshold);
changingDurationLimit = moment.duration(changingDurationLimit);

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

// momentification of timestamps
var records = parse(fs.readFileSync(inputFilename)).map((r, idx, all) => {
  if(((idx % 100) == 0) && !nostatus){
    process.stdout.clearLine();  // clear current text
    process.stdout.cursorTo(0);  // move cursor to beginning of line
    process.stdout.write(`${idx} of ${all.length} (${(100*idx/all.length).toFixed(1)}%)`);
  }

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

// what we are looking for is times when the temperature standard deviation is
// above a threshold setting for a period of time,
// and then below the threshold setting for a period of time
let transition_points = [];
let lastStateChangeMoment = null;
const State = {
    ABOVE: 0,
    CHANGING: 1,
    MAYBE_BELOW: 2,
    BELOW: 3
};

let state = State.ABOVE; // initial state
let lastPeakStdev = 0;
let momentOfLastPeakStdev = null;
let lastStateTransitionMomentFromBelowToAbove = null;
records.forEach((row, idx) => {
  if(((idx % 100) == 0) && !nostatus){
    process.stdout.clearLine();  // clear current text
    process.stdout.cursorTo(0);  // move cursor to beginning of line
    process.stdout.write(`${idx} of ${records.length} (${(100*idx/records.length).toFixed(1)}%)`);
  }

  let currentMoment = moment(row[0], "MM/DD/YYYY HH:mm:ss");
  let currentValue = row[temperatureColumn];

  if(!isNumeric(currentValue)){
    return;
  }

  if(!lastStateChangeMoment){
    lastStateChangeMoment = moment(currentMoment);
  }

  if(!momentOfLastPeakStdev){
    momentOfLastPeakStdev = moment(currentMoment);
    lastPeakStdev = currentValue;
  }

  let aboveThreshold = currentValue > temperatureStdevAboveThreshold;
  let belowThreshold = currentValue < temperatureStdevBelowThreshold;
  let stateChanged = false;
  let previousState = state;
  switch(state){
    case State.ABOVE:
      if(!ignoreInitialStability && belowThreshold){
        state = State.MAYBE_BELOW;
        stateChanged = true;
      }
      else if(aboveThreshold){
        let m = moment(currentMoment);
        m.subtract(requiredDurationAboveThreshold);
        if(lastStateChangeMoment.isBefore(m)){
          state = State.CHANGING;
          stateChanged = true;
        }
      }
      else{
        stateChanged = true; // still have to reset the clock in this case
      }
      break;
    case State.CHANGING:
      if(currentValue > lastPeakStdev){
        lastPeakStdev = currentValue;
        momentOfLastPeakStdev = moment(currentMoment);
      }

      if(belowThreshold){
        state = State.MAYBE_BELOW;
        stateChanged = true;
      }
      else{

        let m = moment(currentMoment);
        m.subtract(changingDurationLimit);
        if(lastStateChangeMoment.isBefore(m)){
          state = State.ABOVE;
          stateChanged = true;
          lastPeakStdev = 0;
        }
      }
      break;
    case State.MAYBE_BELOW:
      if(aboveThreshold){
        state = State.ABOVE;
        stateChanged = true;
      }
      else{
        let m = moment(currentMoment);
        m.subtract(requiredDurationBelowThreshold);
        if(lastStateChangeMoment.isBefore(m)){
          state = State.BELOW;
          stateChanged = true;

          transition_points.push(momentOfLastPeakStdev);
          momentOfLastPeakStdev = null;
          lastPeakStdev = 0;
        }
      }
      break;
    case State.BELOW:
      if(aboveThreshold){
        state = State.ABOVE;
        stateChanged = true;
        lastStateTransitionMomentFromBelowToAbove = moment(currentMoment);
      }
      break;
  }

  if(stateChanged){
    lastStateChangeMoment = moment(currentMoment);
    if(previousState != state){
      if(debug) console.log(`State Changed from ${previousState} to ${state} @ ${lastStateChangeMoment.format("MM/DD/YYYY HH:mm:ss")}`)
    }
  }
});

transition_points.push(lastStateTransitionMomentFromBelowToAbove);

console.log();
transition_points.forEach((m) => {
  console.log(m.format("MM/DD/YYYY HH:mm:ss"));
})
