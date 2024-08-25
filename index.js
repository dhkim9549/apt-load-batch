import { open } from 'node:fs/promises';
import { MongoClient } from 'mongodb'
import 'dotenv/config'

const client = new MongoClient(process.env.MONGODB_URI);

async function main() {
  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db('dbTest');
  const collection = db.collection('aptTest');

  let msg = await collection.deleteMany({"ctrtYm": {"$regex": "20.*"}});
  console.log("msg = " + JSON.stringify(msg));

  const file = await open('/data/apt/apt-seoul-2024.csv');
  let i = 0;
  for await (const line of file.readLines()) {
    let s = new String(line);
    s = line.substring(1, s.length - 1);
    s = s.replaceAll('","', '|');
    s = s.replaceAll(",", "");
    const w = s.split('|');
    let apt = {};
    apt.i = Number(w[0]);
    apt.sggu = w[1];
    apt.aptNm = w[5];
    apt.area = Number(w[6]);
    apt.ctrtYm = w[7];
    apt.ctrtDy = w[8];
    apt.prc = Number(w[9]);
    apt.cnstYr = w[14];
    apt.addr = w[15];

    await collection.insertOne(apt);

    if(i % 1000 == 0) {
      console.log("i = " + i);
      console.log(s);
      console.log("apt = " + JSON.stringify(apt, null, 2));
    }
    i++;
  }

  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
