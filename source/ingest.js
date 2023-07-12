const { Client } = require("@opensearch-project/opensearch");
const fs = require('fs');
const CLUSTER_URL = "https://admin:admin@localhost:9200";
const INDEX_NAME = "opensearch_content"


const os_client = new Client({
  node: CLUSTER_URL,
  ssl: {
    rejectUnauthorized: false
  }
})


async function index_exists(index_name) {
  /**
   * @param 
   */
  return (await os_client.indices.exists({ index: index_name })).body;
}


async function create_index(index_name) {
  try {
    if (!index_exists(index_name)) {
      const result = await os_client.indices.create({
        index: index_name
      })
    }
  } catch (error) {
    console.log(error)
  } 
}


function collect_data(dir, read_data) {
  /**
   * @param {String} dir The directory that you would like to pull the data from
   * @param {Function} read_data A function for parsing the file data
   * @returns {Promise} A promise that resolves to the data from the file
   */
  return new Promise ((resolve, reject) => {
    fs.promises.readdir(dir)
      .then(filenames => {
        let prom_list = []
        for (let filename of filenames){
          prom_list.push(read_data(dir + "/" + filename))
        }
        Promise.all(prom_list).then(files => {
          let data = []
          for (let f of files) {
            data.push(...f)
          }
          resolve(data)
        })
      })
    });
}


function read_json_data(file){
  /**
   * @param {String} file A path to the file whos JSON you'd like parsing
   * @returns {Promise} Returns a promise that resolves to the file's json content
   */
  return new Promise ((resolve, reject) => {
    fs.promises.readFile(file, 'utf-8').then(data => {
      resolve(JSON.parse(data));
    })
  })
}

async function send_to_opensearch(data, index_name) {
  let result = await os_client.helpers.bulk({
    datasource: data,
    onDocument (doc) {
      return {
          index: { _index: index_name , _id: doc.url}
        }
    }
  })
}

function ingest(){
  create_index(INDEX_NAME).then(() => {
    collect_data("data", read_json_data).then(data => {
      console.log(data)
      send_to_opensearch(data, INDEX_NAME)
    })
  })
}

ingest()