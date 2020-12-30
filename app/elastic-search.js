const fakeSendToElasticSearch = () =>
  new Promise(resolve => resolve({dbName: 'model'}));

module.exports = {fakeSendToElasticSearch};
