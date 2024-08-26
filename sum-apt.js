import { open } from 'node:fs/promises';
import { MongoClient } from 'mongodb'
import 'dotenv/config'

const client = new MongoClient(process.env.MONGODB_URI);

async function main() {
  for(let year = 2006; year <= 2024; year++) {
    console.log("year = " + year);
    await main2({"year": "" + year});
  }
}

async function main2({year}) {
  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db('dbApt');
  const collection = db.collection('cltAptSum');

  let msg = await collection.deleteMany({"year": year});

  let map = new Map();

  let i = 0;
  for await (const doc of
    db.collection('cltAptTrd')
      .find({"ctrtYm": {"$regex": year + ".*"}})
      .project({_id: 0, sggu:1, aptNm:1, cnstYr: 1, prc: 1})
    ) {

    let keyStr = doc.sggu + " " + doc.aptNm;
    if(i % 10000 == 0) {
      console.log("i = " + i);
      console.log("keyStr = " + keyStr);
      console.log("doc = " + JSON.stringify(doc, null, 2));
    }

    if(map.has(keyStr)) {
      let value = map.get(keyStr);
      value.prc += doc.prc;
      value.cnt += 1;
    } else {
      doc.cnt = 1;
      map.set(keyStr, doc);
    }

    i++;
  }

  for (let [key, value] of map) {
    value.year = year;
    value.keyStr = key;

    await collection.insertOne(value);

    if(value.cnt > 100) {
      console.log(key + ': ' + JSON.stringify(value, null, 2));
    }
  }

  console.log("size = " + map.size);

  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
