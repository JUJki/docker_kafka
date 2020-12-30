const {processKryptonopolis} = require('./app/kraken');
const {processElasticSearch} = require('./app/kraken');
const {processHelheim} = require('./app/kraken');
const {startConsumer} = require('./app/consumer-starter');

const AllConsumer = [
  {
    topic: 'Kryptonopolis',
    function: processKryptonopolis
  },
  {
    topic: 'ElasticSearch',
    function: processElasticSearch
  },
  {
    topic: 'Vanaheim',
    function: processKryptonopolis
  },
  {
    topic: 'Helheim',
    function: processHelheim
  }
];
startConsumer(AllConsumer);
