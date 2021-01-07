const {processKryptonopolis} = require('./app/kraken');
const {processElasticSearch} = require('./app/kraken');
const {processHelheim} = require('./app/kraken');
const {startConsumer} = require('./app/consumer-starter');

const AllConsumer = [
  {
    topic: 'Kryptonopolis',
    consumerType: 'consumerGroup',
    config: {
      partitions: 2,
      replication: 1,
      entries: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        },
        {
          name: 'min.cleanable.dirty.ratio',
          value: '0.01'
        },
        {
          name: 'segment.ms',
          value: '10000'
        }
      ]
    },
    function: processKryptonopolis
  },
  {
    topic: 'Monitor',
    consumerType: 'consumerGroupStreamWithKey',
    config: {
      partitions: 3,
      replication: 1
    },
    function: processKryptonopolis
  },
  {
    topic: 'ElasticSearch',
    consumerType: 'consumerGroupStream',
    function: processElasticSearch
  },
  {
    topic: 'Vanaheim',
    consumerType: 'consumer',
    function: processKryptonopolis
  },
  {
    topic: 'Helheim',
    consumerType: 'consumerGroupStream',
    function: processHelheim
  }
];
startConsumer(AllConsumer);
