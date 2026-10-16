// Auxiliar offline; somente fontes Mermaid em /input e SVG/relatório em /output.
const fs = require('fs');
const cp = require('child_process');
const crypto = require('crypto');
const start = Date.now();
const report = {date_utc:new Date().toISOString(), node:process.version, uid:process.getuid(), results:[], versions:{}, script_sha256:crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex')};
for (const pkg of ['@mermaid-js/mermaid-cli','mermaid','puppeteer']) {
  try {report.versions[pkg]=JSON.parse(fs.readFileSync('/home/mermaidcli/node_modules/'+pkg+'/package.json')).version;} catch {report.versions[pkg]='não localizado';}
}
report.chromium=cp.execFileSync('/usr/bin/chromium',['--version'],{encoding:'utf8',timeout:10000}).trim();
const config={executablePath:'/usr/bin/chromium',args:['--no-sandbox','--disable-dev-shm-usage'],headless:true};
fs.writeFileSync('/tmp/r1-puppeteer.json',JSON.stringify(config));
const input=JSON.parse(fs.readFileSync('/input/manifest.json'));
for (const item of input) {
  if (!/^[A-Za-z0-9._-]+\.mmd$/.test(item.file)) throw Error('Nome inválido');
  const begin=Date.now(); let code=0, error='';
  try {cp.execFileSync('/home/mermaidcli/node_modules/.bin/mmdc',['-p','/tmp/r1-puppeteer.json','-i','/input/'+item.file,'-o','/output/'+item.file+'.svg'],{encoding:'utf8',timeout:45000,env:{...process.env,HOME:'/tmp',XDG_CONFIG_HOME:'/tmp',XDG_CACHE_HOME:'/tmp'}});}
  catch(e){code=e.status??1;error=String(e.stderr||e.message).slice(0,2000);}
  report.results.push({...item,code,error,duration_seconds:(Date.now()-begin)/1000,source_sha256:crypto.createHash('sha256').update(fs.readFileSync('/input/'+item.file)).digest('hex')});
  fs.writeFileSync('/output/mermaid-report.json',JSON.stringify(report,null,2));
  if (Date.now()-start>840000) break;
}
report.duration_seconds=(Date.now()-start)/1000;
report.complete=report.results.length===input.length;
fs.writeFileSync('/output/mermaid-report.json',JSON.stringify(report,null,2));
console.log(JSON.stringify({versions:report.versions,node:report.node,chromium:report.chromium,count:report.results.length,failures:report.results.filter(x=>x.code),complete:report.complete,duration_seconds:report.duration_seconds}));
process.exit(report.complete && report.results.every(x=>x.code===0)?0:1);
