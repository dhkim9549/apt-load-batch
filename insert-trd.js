import { open } from 'node:fs/promises';
import { MongoClient } from 'mongodb'
import 'dotenv/config'

const client = new MongoClient(process.env.MONGODB_URI);
await client.connect();
console.log('Connected successfully to server');
const db = client.db('dbApt');
const collection = db.collection('colAptTrd');

export async function insertTrd(fileNm) {

  await main(fileNm);
}

export function clientClose() {
  client.close();
}

async function main(fileNm) {

  console.log("fileNm = " + fileNm);

  const file = await open(fileNm);
  let i = 0;
  for await (const line of file.readLines()) {
    i++;
    if(line.startsWith('"NO"')) {
      continue;
    }
    if((line.match(/,/g) || []).length < 5) continue; 

    let s = new String(line);
    s = line.substring(1, s.length - 1);
    s = s.replaceAll('","', '|');
    const w = s.split('|');
    let apt = {};
    apt.sggu = w[1];
    apt.bunji = w[2];
    apt.aptNm = w[5];
    apt.area = Number(w[6]);
    apt.ctrtDy = w[7] + w[8];
    apt.prc = Number(w[9].replaceAll(",", ""));
    apt.floor = w[11];
    apt.cnstYr = w[14];
    apt.stnmAddr= w[15];
    apt.cnclDy = w[16];

    let aptFilter = structuredClone(apt);
    delete aptFilter.stnmAddr;
    delete aptFilter.cnclDy;

    await collection.updateOne(aptFilter, {$set: apt}, {upsert: true});

    if(i % 1000 == 0) {
      console.log("i = " + i);
      console.log(s);
      console.log("apt = " + JSON.stringify(apt, null, 2));
      console.log(new Date());
    }
  }

  return 'done.';
}
