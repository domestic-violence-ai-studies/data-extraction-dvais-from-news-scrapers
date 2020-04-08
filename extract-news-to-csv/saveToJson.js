const MongoClient = require('mongodb').MongoClient;
require('dotenv').config()
const url = process.env["database_url"];
const fs = require("fs");
const { convertArrayToCSV } = require('convert-array-to-csv');

const pathoutbundle = "../data/"

MongoClient.connect(url, async function (err, db) {
  if (err) throw err;
  var dbo = db.db("news-scraped-with-tags");

  //db.getCollection('NewsContentScraped').aggregate([
  //  { $match : { newspaper : "elpais" } },
  //  { $sample : { size: 3 } }
  //])

  console.log("Starting search with violencia de genero words")

  const words =["violencia género", "violencia machista", "violencia doméstica", "discriminación mujer", "sexismo", "violencia contra las mujeres", "acoso sexual", "violación"]

  const queryFromWord = (word) => {
    return {"$text":{"$search": '\"' + word + '\"'}}
  }

  const searchPromisified = (query) => {
    return new Promise((resolve, reject)=>{
      dbo.collection("NewsContentScraped").find(query).toArray(function (err, result) {
        if (err) return reject(err)
        return resolve(result);
      })
    })
  }

  const results = []
  
  for (const word of words) {
    const resultsSearch = await searchPromisified(queryFromWord(word))
    console.log("---")
    console.log(word + " " + resultsSearch.length)
    results.push(...resultsSearch)
  }

  const resultsMap = new Map()
  for (const result of results) {
    result["about_domestic_violence"]=1
    resultsMap.set(result._id, result)
  }

  const noRepetitionResults = Array.from(resultsMap.values())
  const noRepertitionLength = noRepetitionResults.length
  

  let data = JSON.stringify(results);
  let dataNoRepetitionDomesticViolence = JSON.stringify(noRepetitionResults);
  fs.writeFileSync(pathoutbundle + "no_rep_news-domestic-violence.json", dataNoRepetitionDomesticViolence);

  console.log("----------------")
  console.log("Starting search with no domestic violence words")


  //  { $match : { newspaper : "elpais" } },
  //  { $sample : { size: 3 } }
  //]
  const query = [{ $match : {} }, { $sample : { size: noRepertitionLength } }]

  const searchAggragatePromisified = () => {
    return new Promise((resolve, reject)=>{
      dbo.collection("NewsContentScraped").aggregate(query).toArray(function (err, result) {
        if (err) reject(err)
        return resolve(result);
      })
    })
  }

  resultsNoViolencia = await searchAggragatePromisified()
  for (const result of resultsNoViolencia) {
    result["about_domestic_violence"]=0
  }

  console.log("found " + resultsNoViolencia.length)
  let dataNoViolence = JSON.stringify(resultsNoViolencia);
  fs.writeFileSync(pathoutbundle + "no_rep_news-domestic-no-violence.json", dataNoViolence);


  db.close();

});