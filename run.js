const moment = require('moment');
const parse = require('csv-parse/lib/sync');
const exec = require('child_process').exec;
const argv = require('minimist')(process.argv.slice(2));
const input = argv.input || "input";
const dropPercentAfter = (argv.percentAfter || 45) / 100;
const dropPercentBefore = (argv.percentBefore || 25) / 100;
const plot = !!argv.plot;
const fs = require('fs');
let ChartjsNode = null;
if(plot){
  ChartjsNode = require('chartjs-node');
}

let drop = argv.drop;
if(drop){
  drop = `--drop="${drop}"`;
}

function runCommand(cmd, options){
  return new Promise((resolve, reject) => {
    if(!options) options = {};
    exec(cmd, options, (error, stdout, stderr) => {
      if(error){
        reject(error);
      }
      else{
        resolve({stdout, stderr});
      }
    });
  });
}

// EXAMPLE
// runCommand('node identifystableperiods.js --nostatus --input output.csv')
// .then((result) => {
//   console.log(JSON.stringify(result.stdout.split("\n").filter(l => l.trim()), null, 2));
// })
// .catch((error) => {
//   console.log(error.message, error.stack);
// });

function generateScatterChart(data, type){
  if(!plot) return;

  // console.log(JSON.stringify(data, null, 2));
  // console.log("Type: ", type);

  let plugins = {
    beforeDraw: function (chartInstance, easing) {
        let self = chartInstance.config;    /* Configuration object containing type, data, options */
        let ctx = chartInstance.chart.ctx;  /* Canvas context used to draw with */
        ctx.fillStyle = "white";
        ctx.fillRect(0, 0, chartInstance.chart.width, chartInstance.chart.height);
    }
  };

  // 600x600 canvas size
  let chartNode = new ChartjsNode(600, 600);
  return chartNode.drawChart({
      type: 'scatter',
      data: {
          datasets: [{
              label: `${type.toUpperCase()} BLV vs Temperature`,
              data
          }]
      },
      options: {
          scales: {
              xAxes: [{
                  type: 'linear',
                  position: 'bottom'
              }]
          },
          plugins
      }
  })
  .then(() => {
      // chart is created

      // get image as png buffer
      return chartNode.getImageBuffer('image/png');
  })
  .then((buffer) => {
      Array.isArray(buffer) // => true
      // as a stream
      return chartNode.getImageStream('image/png');
  })
  .then((streamResult) => {
      // using the length property you can do things like
      // directly upload the image to s3 by using the
      // stream and length properties
      streamResult.stream // => Stream object
      streamResult.length // => Integer length of stream
      // write to a file
      return chartNode.writeImageToFile('image/png', `./${input}.${type}.scatter.png`);
  })
  .then(() => {
    return chartNode.destroy();
  });
}

console.log("Running csvdatasmoother with 10M data smoothing...");
runCommand(`node csvdatasmoother.js --ignoreColumns=7,8,9 --augmented ${drop} --input="${input}.csv" --output="${input}.smoothed.csv" --nostatus --functions="mean,stdev" --duration="PT10M"`)
.then((result) => {
  console.log("Running csvdatasmoother with 1H stdev smoothing...");
  return runCommand(`node csvdatasmoother.js --ignoreColumns=1,2,4,+ --augmented --input="${input}.smoothed.csv" --output="${input}.smoothed-heavy.csv" --nostatus --functions="mean" --duration="PT1H"`)
})
.then((result) => {
  console.log("Running identifystableperiods...");
  return runCommand(`node identifystableperiods.js --input="${input}.smoothed-heavy.csv" --nostatus`)
})
.then((result) => {
  let records = parse(fs.readFileSync(`${input}.smoothed.csv`));
  let earliestMoment = moment(records[1][0],'MM/DD/YYYY HH:mm:ss');
  let latestMoment = moment(records[records.length-1][0],'MM/DD/YYYY HH:mm:ss');
  console.log(earliestMoment.format("MM/DD/YYYY HH:mm:ss"), "--", latestMoment.format("MM/DD/YYYY HH:mm:ss"));

  let edgeDates = result.stdout.split("\n").filter(l => l.trim());
  console.log(JSON.stringify(edgeDates, null, 2));
  // generate a drop zones based on results and percent knobs
  let drop_ranges = [];
  edgeDates = edgeDates.map((l) => {
    let mm = moment(l, "MM/DD/YYYY HH:mm:ss");
    mm.subtract(60, 'minutes'); // shift back to undo averaging lag
    return mm;
  });
  edgeDates.forEach((m, idx) => {
    if(idx === 0){
      let diff = edgeDates[1].diff(m, "seconds");
      let mm = moment(m);
      mm.add(diff * dropPercentAfter, "seconds");
      drop_ranges.push({start: '1/1/1970 00:00:00', end: mm.format("MM/DD/YYYY HH:mm:ss")})
    }
    else if(idx === (edgeDates.length - 1)){
      let diff = latestMoment.diff(m, "seconds");
      let mm = moment(m);
      mm.subtract(diff * dropPercentBefore, "seconds");
      drop_ranges.push({start: mm.format("MM/DD/YYYY HH:mm:ss"), end: '12/31/2999 23:59:59'});
    }
    else{
      let diffBefore = -edgeDates[idx-1].diff(m, "seconds");
      let diffAfter = edgeDates[idx+1].diff(m, "seconds");
      let mm1 = moment(m);
      let mm2 = moment(m);
      mm1.subtract(diffBefore * dropPercentBefore, "seconds");
      mm2.add(diffAfter * dropPercentAfter, "seconds");
      drop_ranges.push({start: mm1.format("MM/DD/YYYY HH:mm:ss"), end: mm2.format("MM/DD/YYYY HH:mm:ss")});
    }
  });

  if(argv.drop){
    drop_ranges = drop_ranges.concat(
      argv.drop.split(",").map((r) => {
        let x = r.split("-");
        return {start: x[0], end: x[1]};
      })
    );
  }

  console.log(JSON.stringify(drop_ranges, null, 2));
  drop = drop_ranges
    .map(r => `${r.start}-${r.end}`)
    .join(",");
  drop = `--drop="${drop}"`;

  console.log("Running csvdatasmoother with drop durations...");
  return runCommand(`node csvdatasmoother.js --ignoreColumns=7,8,9 --augmented ${drop} --input="${input}.csv" --output="${input}.smoothed-culled.csv" --nostatus --functions="mean,stdev" --duration="PT10M"`);
})
.then((result) => {
  console.log("Running clustertoblv...");
  return runCommand(`node clustertoblv.js --input ${input}.smoothed-culled.csv`);
})
.then((result) => {
  let lines = result.stdout.split("\n").filter(l => l.trim());
  let data = "["+(/Stats:  \[(.*)\]/g).exec(lines.join(""))[1]+"]";
  data = JSON.parse(data);
  console.log(JSON.stringify(data, null, 2));
  let otherLines = (/Stats:[\s\S]*\[[\s\S]*\]([\s\S]*)/g).exec(lines.join("\n"))[1].split('\n').filter(l => l.trim());
  otherLines.forEach((l) => {
    console.log(l);
  })

  let first_type = otherLines[0].split("_")[0];
  let second_type = otherLines.slice(-1)[0].split("_")[0];

  return generateScatterChart(data.map((r) => {
    return {
      x: +r.temperature_mean,
      y: +r.voltage1_mean
    };
  }), first_type)
  .then(() => {
    return generateScatterChart(data.map((r) => {
      return {
        x: +r.temperature_mean,
        y: +r.voltage2_mean
      };
    }), second_type);
  });

})
.catch((error) => {
  console.log(error.message, error.stack);
});
