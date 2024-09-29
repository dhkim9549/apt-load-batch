import { readFileSync, writeFileSync, unlinkSync, readdirSync } from 'node:fs';
import iconv from 'iconv-lite';
import { insertTrd, clientClose } from './insert-trd.js';

const srcDir = '/data/apt/temp';
const dstDir = '/data/apt/result';

function convert(fileNm) {

  const content = readFileSync(srcDir + '/' + fileNm); 
  const utf8Str = iconv.decode(content, 'euc-kr');
  writeFileSync(dstDir + '/' + fileNm, utf8Str, { encoding: 'utf8' });
  unlinkSync(srcDir + '/' + fileNm);
}

async function main() {
  for(const file of readdirSync(srcDir)) {
    console.log(file);
    convert(file);
    await insertTrd(dstDir + '/' + file);
  }
  clientClose();
}

main();


