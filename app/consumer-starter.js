const {
  Consumer,
  ConsumerStream,
  // ConsumerGroup,
  ConsumerGroupStream,
  KafkaClient,
  Admin
} = require('kafka-node');


const R = require('ramda');
const config = require('config');
const {logger} = require('../app/utils/logger');

const getKafkaUrl_ = () => R.prop('kafka', config.get('urlService'));

const topicConsumerGroupStream = [
  'ElasticSearch',
  'SentenceEncoder',
  'Helheim'
];
const topicConsumerGroupStreamWithKey = ['Monitor'];
const topicConsumerStreamWithKey = [];
const topicConsumerSimpleWithKey = ['Kryptonopolis'];

const getListOfTopics_ = () =>
  new Promise(resolve => {
    const admin = new Admin(new KafkaClient({kafkaHost: getKafkaUrl_()}));
    admin.listTopics((error, response) => {
      logger.log(
        'info',
        `response getListOfTopics_ : ${JSON.stringify(response)}`
      );
      return R.ifElse(
        R.not,
        () => resolve(_formatResponseListTopic(response)),
        () => {
          logger.log(
            'error',
            `error getListOfTopics_ : ${JSON.stringify(error)}`
          );
          resolve([]);
        }
      )(error);
    });
  });
const _formatResponseListTopic = R.pipe(R.last, R.prop('metadata'));

const _getPartitionFromListConfig = topicConfig => R.values(R.map(itemTopic => {
  return R.prop('partition', itemTopic)
}, topicConfig))

const createConsumerIfTopicExist_ = (keyTopic, listTopic, topic) =>
  R.ifElse(
    () => R.includes(R.prop('topic', topic), keyTopic),
    () =>
      connectAndStartConsumer_(
        R.prop('topic', topic),
        _getPartitionFromListConfig(R.prop(R.prop('topic', topic), listTopic)),
        R.prop('function', topic)
      ),
    R.always(false)
  )(listTopic);

const parseMissive_ = (message, fn) =>
  R.pipe(
    R.prop('value'),
    data => JSON.parse(data),
    R.tap(dataParse =>
      logger.log(
        'info',
        `Consumer data kafka  topic ${R.prop('topic', message)}`,
        dataParse
      )
    ),
    fn
  )(message);

const consumerGroupStream_ = (topic, fn) => {
  const options = {
    kafkaHost: getKafkaUrl_(),
    id: `consumer_${topic}`,
    groupId: `kafka-node-${topic}`,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640
  };
  const consumerGroupStream = new ConsumerGroupStream(options, topic);
  consumerGroupStream.on('connect', () => {
      logger.log('info', `consumer group stream of ${topic} created and connected`);
    }
  );
  consumerGroupStream.on('pause', () => {
      logger.log('info', `consumer group stream of ${topic} pause`);
    }
  );
  consumerGroupStream.on('resume', () => {
      logger.log('info', `consumer group stream of ${topic} resume`);
    }
  );
  consumerGroupStream.on('data', async chunk => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', chunk)} ${JSON.stringify({
        offset: R.prop('offset', chunk),
        highWaterOffset: R.prop('highWaterOffset', chunk),
        partition: R.prop('partition', chunk)
      })}`
    );
    consumerGroupStream.pause();
    await parseMissive_(chunk, fn);
    consumerGroupStream.resume();
  });
  consumerGroupStream.on('error', error =>
    logger.log(
      'error',
      `error on consumerGroupStream when kraken consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};
const consumerGroupStreamWithKey_ = (topic, fn) => {
  const options = {
    kafkaHost: getKafkaUrl_(),
    id: `consumer_${topic}`,
    groupId: `kafka-node-${topic}`,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640
  };
  const consumerGroupStream = new ConsumerGroupStream(options, topic);
  consumerGroupStream.on('connect', () => {
      logger.log('info', `consumer group stream of ${topic} created and connected`);
    }
  );
  consumerGroupStream.on('pause', () => {
      logger.log('info', `consumer group stream of ${topic} pause`);
    }
  );
  consumerGroupStream.on('resume', () => {
      logger.log('info', `consumer group stream of ${topic} resume`);
    }
  );
  consumerGroupStream.on('data', async chunk => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', chunk)} ${JSON.stringify({
        offset: R.prop('offset', chunk),
        highWaterOffset: R.prop('highWaterOffset', chunk),
        partition: R.prop('partition', chunk),
        key: R.prop('key', chunk)
      })}`
    );
    consumerGroupStream.pause();
    await parseMissive_(chunk, fn);
    consumerGroupStream.resume();
  });
  consumerGroupStream.on('error', error =>
    logger.log(
      'error',
      `error on consumerGroupStream when consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};
const partitionConfig_ = (topic, partitions) => R.map(partition => ({topic, partition}), partitions);
const consumerStreamWithKey_ = (topic, partitions, fn) => {
  const options = {
    groupId: `kafka-node-${topic}`,
    fetchMaxBytes: 15728640,
    fromOffset: 'earliest',
    autoCommit: true,
  };

  const client = new KafkaClient({kafkaHost: getKafkaUrl_()});
  const consumerStream = new ConsumerStream(client, partitionConfig_(topic, partitions), options);
  logger.log('info', `consumer stream with key of ${topic} created`);
  consumerStream.init();
  consumerStream.on('data', async message => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', message)} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message),
        key: R.prop('key', message),
      })}`
    );
    consumerStream.pause();
    await parseMissive_(message, fn);
    consumerStream.resume();
  });
  consumerStream.on('error', error =>
    logger.log(
      'error',
      `error on consumerStream when consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};
const consumerSimpleWithKey_ = (topic, partitions, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: getKafkaUrl_()}),
    partitionConfig_(topic, partitions),
    {
      groupId: `kafka-node-${topic}`,
      autoCommit: true,
      fetchMaxBytes: 15728640
    }
  );
  consumer.connect();
  consumer.init();
  logger.log('info', `consumer simple of ${topic} created and connected`);
  consumer.on('message', async message => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', message)} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message),
        key: R.prop('key', message)
      })}`
    );
    consumer.pause();
    await parseMissive_(message, fn);
    consumer.resume();
  });
  consumer.on('error', error =>
    logger.log(
      'error',
      `error on consumerSimple when kraken consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};
const consumerSimple_ = (topic, partitions, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: getKafkaUrl_()}),
    partitionConfig_(topic, partitions),
    {
      groupId: `kafka-node-${topic}`,
      autoCommit: true,
      fetchMaxBytes: 15728640
    }
  );
  consumer.connect();
  consumer.init();
  logger.log('info', `consumer simple of ${topic} created and connected`);
  consumer.on('message', message => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', message)} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message)
      })}`
    );
    return parseMissive_(message, fn);
  });
  consumer.on('error', error =>
    logger.log(
      'error',
      `error on consumerSimple when kraken consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};

const connectAndStartConsumer_ = (topic, partition, fn) =>
  R.cond([
    [
      topic => R.includes(topic, topicConsumerGroupStream),
      () => consumerGroupStream_(topic, fn)
    ],
    [
      topic => R.includes(topic, topicConsumerGroupStreamWithKey),
      () => consumerGroupStreamWithKey_(topic, fn)
    ],
    [
      topic => R.includes(topic, topicConsumerStreamWithKey),
      () => consumerStreamWithKey_(topic, partition, fn)
    ],
    [
      topic => R.includes(topic, topicConsumerSimpleWithKey),
      () => consumerSimpleWithKey_(topic, partition, fn)
    ],
    [R.T, () => consumerSimple_(topic, partition, fn)]
  ])(topic);

const startConsumer = data => {
  const kafkaClient_ = new KafkaClient({kafkaHost: getKafkaUrl_()});
  kafkaClient_.on('ready', async () => {
    logger.log('info', 'client kafka ready');
    const listTopic = await getListOfTopics_();
    const keyTopic = R.keys(listTopic);
    if (R.length(keyTopic) === 0) {
      logger.log('error', 'no topic has been created');
      return;
    }

    R.map(item => createConsumerIfTopicExist_(keyTopic, listTopic, item), data);
  });
  kafkaClient_.on('error', err => {
    logger.log('error', `client kafka error: ${JSON.stringify(err)}`);
  });
};

module.exports = {startConsumer};
