const {processKryptonopolis} = require('./app/kraken');
const {processElasticSearch} = require('./app/kraken');
const {processHelheim} = require('./app/kraken');
const {startConsumer} = require('./app/consumer-starter');

const AllConsumer = [
  {
    topic: 'Kryptonopolis',
    consumerType : 'consumerWithKey',
    function: processKryptonopolis
  },
  {
    topic: 'Monitor',
    consumerType : 'consumerGroupStreamWithKey',
    function: processKryptonopolis
  },
  {
    topic: 'ElasticSearch',
    consumerType : 'consumerGroupStream',
    function: processElasticSearch
  },
  {
    topic: 'Vanaheim',
    consumerType : 'consumer',
    function: processKryptonopolis
  },
  {
    topic: 'Helheim',
    consumerType : 'consumerGroupStream',
    function: processHelheim
  }
];
startConsumer(AllConsumer);
