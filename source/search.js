const { Client } = require("@opensearch-project/opensearch");
const CLUSTER_URL = "https://admin:admin@localhost:9200";
const INDEX_NAME = "opensearch_content"


const os_client = new Client({
  node: CLUSTER_URL,
  ssl: {
    rejectUnauthorized: false
  }
})

async function search(query_text) {
  const query = {
    query: {
      multi_match: {
          query: query_text,
          fields: ["title^1", "content"]
      },
    },
  };

  return response = await os_client.search({
    index: INDEX_NAME,
    body: query,
  });
}

search("Segement replication documentation").then(result => {
  result.body.hits.hits.forEach(element => {
    console.log(element._source.content)
})
})