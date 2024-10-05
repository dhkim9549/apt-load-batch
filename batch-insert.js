import { readFileSync, writeFileSync, unlinkSync, readdirSync } from 'node:fs';
import iconv from 'iconv-lite';
import { insertTrd } from './insert-trd.js';
import { updateAptInfo } from './update-apt-info.js';
import { updateAptInfoPrc } from './update-apt-info-prc.js';

const srcDir = '/data/apt/temp';
const dstDir = '/data/apt/result';

function convert(fileNm) {
  const content = readFileSync(srcDir + '/' + fileNm); 
  const utf8Str = iconv.decode(content, 'euc-kr');
  writeFileSync(dstDir + '/' + fileNm, utf8Str, { encoding: 'utf8' });
  unlinkSync(srcDir + '/' + fileNm);
}

async function batchInsert() {

  console.log('batchInsert() start...');

  for(const file of readdirSync(srcDir)) {
    console.log(file);
    convert(file);
    await insertTrd(dstDir + '/' + file);
  }

  console.log('batchInsert() end...');
}

async function main() {
  await batchInsert();
  await updateAptInfo();
  await updateAptInfoPrc();
}

main();
