const exec = require('child_process').exec;

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

runCommand('node identifystableperiods.js --nostatus --input output.csv')
.then((result) => {
  console.log(JSON.stringify(result.stdout.split("\n").filter(l => l.trim()), null, 2));
})
.catch((error) => {
  console.log(error.message, error.stack);
});
