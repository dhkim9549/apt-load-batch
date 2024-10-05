import { open } from 'node:fs/promises';
import { MongoClient } from 'mongodb'
import 'dotenv/config'

export async function updateAptInfo() {

  console.log('updateAptInfo() start...');

  const client = new MongoClient(process.env.MONGODB_URI);

  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db('dbApt');
  const collection = db.collection('colAptInfo');

  let i = 0;
  for await (const doc of
    db.collection('colAptTrd')
      .aggregate(
        [
          {
            $group: { _id: ["$sggu", "$aptNm", "$area", "$cnstYr", "$bunji", "$stnmAddr"] }
          },
          {
            $sort: { _id : 1 }
          }
        ]
      )
    ) {

    let [sggu, aptNm, area, cnstYr, bunji, stnmAddr] = doc._id;

    let aptInfo = await collection.findOne({sggu: sggu, aptNm: aptNm});
    if(aptInfo == null) {
      aptInfo = {};
      aptInfo.sgguAptNm = sggu + ' ' + aptNm;
      aptInfo.sggu = sggu;
      aptInfo.aptNm = aptNm;
      aptInfo.areas = [area];
      aptInfo.cnstYr = cnstYr;
      aptInfo.bunji = bunji;
      aptInfo.stnmAddr = stnmAddr;
    } else {
      if(!aptInfo.areas.includes(area)) {
        aptInfo.areas.push(area);
        aptInfo.areas.sort((a, b) => a - b);
      }
    }

    await collection.updateOne({sggu: sggu, aptNm: aptNm}, {$set: aptInfo}, {upsert: true});

    if(i % 1000 == 0) {
      console.log('i = ' + i);
      console.log({doc});
      console.log({aptInfo});
    }

    i++;
  }

  client.close();

  console.log('updateAptInfo() end...');
}

// updateAptInfo();
