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
    db.collection('cltAptTrd').aggregate([
        {
          $group: { _id: ["$sggu", "$aptNm"], prc: { $sum: "$prc" }, cnt: { $sum: 1 } }
        },
        {
          $sort:{ prc : -1 }
        },
      ])
    ) {

    let [sggu, aptNm] = doc._id;

    let aptInfo = await collection.findOne({sggu: sggu, aptNm: aptNm});
    if(aptInfo == null) {
      aptInfo = {};
      aptInfo.sggu = sggu;
      aptInfo.aptNm = aptNm;
      aptInfo.sgguAptNm = sggu + ' ' + aptNm;
    }
    aptInfo.prc = doc.prc;
    aptInfo.cnt = doc.cnt;

    await collection.updateOne({sggu: sggu, aptNm: aptNm}, {$set: aptInfo}, {upsert: true});

    if(i % 1000 == 0) {
      console.log('i = ' + i);
      console.log({doc});
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
