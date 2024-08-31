import { open } from 'node:fs/promises';
import { MongoClient } from 'mongodb'
import 'dotenv/config'

const client = new MongoClient(process.env.MONGODB_URI);

async function main() {

  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db('dbApt');
  const collection = db.collection('cltAptInfo');

  let i = 0;
  for await (const doc of
    db.collection('cltAptTrd')
      .find()
    ) {

    let keyStr = doc.sggu + ' ' + doc.aptNm;

    let aptInfo = await collection.findOne({keyStr: keyStr});
    if(aptInfo == null) {
      aptInfo = {};
      aptInfo.keyStr = keyStr;
      aptInfo.areas = [doc.area];
    } else {
      if(!aptInfo.areas.includes(doc.area)) {
        aptInfo.areas.push(doc.area);
      }
    }

    await collection.updateOne({keyStr: keyStr}, {$set: aptInfo}, {upsert: true});





    if(i % 1000 == 0) {
      console.log('i = ' + i);
      console.log({doc});
      console.log({keyStr});
      console.log({aptInfo});
    }

    i++;
  }

  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
