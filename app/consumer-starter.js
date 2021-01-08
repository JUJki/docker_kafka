const {
  Consumer,
  ConsumerStream,
  ConsumerGroup,
  ConsumerGroupStream,
  KafkaClient,
  Admin
} = require('kafka-node');

const R = require('ramda');
const config = require('config');
const {logger} = require('../app/utils/logger');

const getKafkaUrl_ = () => R.prop('kafka', config.get('urlService'));

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

const getPartitionFromListConfig_ = topicConfig =>
  R.values(
    R.map(itemTopic => {
      return R.prop('partition', itemTopic);
    }, topicConfig)
  );

const createConsumerIfTopicExist_ = (keyTopic, listTopic, topic) =>
  R.ifElse(
    () => R.includes(R.prop('topic', topic), keyTopic),
    () =>
      connectAndStartConsumer_(
        R.prop('topic', topic),
        R.prop('consumerType', topic),
        R.prop('function', topic),
        getPartitionFromListConfig_(R.prop(R.prop('topic', topic), listTopic))
      ),
    R.always(false)
  )(listTopic);

const parseMissive_ = (message, fn) =>
  R.pipe(
    R.tap(message =>
      logger.log(
        'info',
        `${R.prop('key', message)} - ${R.prop('offset', message)}`
      )
    ),
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
const partitionConfig_ = (topic, partitions) =>
  R.map(partition => ({topic, partition}), partitions);
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
    logger.log('info', `consumerGroupStream of ${topic} created and connected`);
  });
  consumerGroupStream.on('pause', () => {
    logger.log('info', `consumerGroupStream of ${topic} pause`);
  });
  consumerGroupStream.on('resume', () => {
    logger.log('info', `consumerGroupStream of ${topic} resume`);
  });
  consumerGroupStream.on('data', async chunk => {
    logger.log(
      'info',
      `consumerGroupStream data topic ${R.prop(
        'topic',
        chunk
      )} ${JSON.stringify({
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
      `error on consumerGroupStream when consume ${topic} : ${JSON.stringify(
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
    fetchMaxBytes: 15728640,
    maxTickMessages: 1
  };
  const consumerGroupStream = new ConsumerGroupStream(options, topic);
  consumerGroupStream.on('connect', () => {
    logger.log(
      'info',
      `consumerGroupStream with key of ${topic} created and connected`
    );
  });
  consumerGroupStream.on('pause', () => {
    logger.log('info', `consumerGroupStream with key of ${topic} pause`);
  });
  consumerGroupStream.on('resume', () => {
    logger.log('info', `consumerGroupStream with key of ${topic} resume`);
  });
  consumerGroupStream.on('data', async chunk => {
    logger.log(
      'info',
      `consumerGroupStream with key data topic ${R.prop(
        'topic',
        chunk
      )} ${JSON.stringify({
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
      `error on consumerGroupStream with key when consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};

const queueCompact = new Map();

const consumerGroup_ = (topic, fn) => {
  const options = {
    kafkaHost: getKafkaUrl_(),
    id: `consumer_${topic}`,
    groupId: `kafka-node-${topic}`,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640,
    maxTickMessages: 1
  };
  const consumerGroup = new ConsumerGroup(options, topic);
  logger.log('info', `consumerGroup of ${topic} created`);

  consumerGroup.on('message', async message => {
    logger.log(
      'info',
      `consumerGroup data topic ${R.prop('topic', message)} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message),
        key: R.prop('key', message)
      })}`
    );
    logger.log(
      'info',
      `consumerGroup value message ${R.prop('value', message)}`
    );
    consumerGroup.pause();
    await parseMissive_(message, fn);
    consumerGroup.resume();
  });
  consumerGroup.on('error', error =>
    logger.log(
      'error',
      `error on consumerGroup when consume ${topic} : ${JSON.stringify(error)}`
    )
  );
};

const consumerGroupCompact_ = (topic, partitions, fn) => {
  const options = {
    kafkaHost: getKafkaUrl_(),
    id: `consumer_${topic}`,
    groupId: `kafka-node-${topic}`,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640,
    maxTickMessages: 1
  };
  const consumerGroup = new ConsumerGroup(options, topic);
  logger.log('info', `consumerGroup of ${topic} created`);

  R.forEach(
    partition => consumeQueueCompact(`${topic}-${partition}`),
    partitions
  );

  consumerGroup.on('message', async message => {
    logger.log(
      'info',
      `consumerGroup data topic ${R.prop('topic', message)} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message),
        key: R.prop('key', message)
      })}`
    );
    logger.log(
      'info',
      `consumerGroup value message ${R.prop('value', message)}`
    );
    const queue =
      queueCompact.get(
        R.prop('topic', message) + '-' + R.prop('partition', message)
      ) || [];
    /* If (!queue) {
      queue = [];
      queueGRoup.set(R.prop('topic', message) + '-' + R.prop('partition', message), queue);
    } */

    queue.push({
      key: R.prop('key', message),
      offset: R.prop('offset', message),
      message,
      function: fn
    });
    queueCompact.set(
      R.prop('topic', message) + '-' + R.prop('partition', message),
      queue
    );
  });
  consumerGroup.on('error', error =>
    logger.log(
      'error',
      `error on consumerGroup when consume ${topic} : ${JSON.stringify(error)}`
    )
  );
};

async function consumeQueueCompact(topicPartition) {
  const data = queueCompact.get(topicPartition);
  if (R.type(data) === 'Array') {
    const goodItem = filterGoodItemQueueCompact_(data);
    if (R.length(goodItem) > 0) {
      const firstElement = R.head(goodItem);
      const keyFirstElement = R.prop('key', firstElement);
      const filterByKey = R.filter(
        item => R.equals(keyFirstElement, R.prop('key', item)),
        goodItem
      );
      const last = R.last(filterByKey);
      await parseMissive_(R.prop('message', last), R.prop('function', last));
      queueCompact.set(
        topicPartition,
        getLastItemsQueueCompact(
          filterGoodItemQueueCompact_(queueCompact.get(topicPartition)),
          last
        )
      );
    } else {
      await sleep(3000);
    }
  } else {
    await sleep(3000);
  }

  consumeQueueCompact(topicPartition);
}

const filterGoodItemQueueCompact_ = data =>
  R.filter(
    item =>
      R.has('function', item) &&
      R.has('message', item) &&
      R.has('key', item) &&
      R.has('offset', item),
    data
  );

const condGetLastItemQueueCompact = (elem, last) =>
  R.anyPass([
    () => R.not(R.equals(R.prop('key', elem), R.prop('key', last))),
    () => R.lt(R.prop('offset', last), R.prop('offset', elem))
  ])(elem);

const getLastItemsQueueCompact = (arrayInitial, last) =>
  R.filter(elem => condGetLastItemQueueCompact(elem, last), arrayInitial);

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const consumerStreamWithKey_ = (topic, partitions, fn) => {
  const options = {
    groupId: `kafka-node-${topic}`,
    fetchMaxBytes: 15728640,
    fromOffset: 'earliest',
    autoCommit: true
  };

  const client = new KafkaClient({kafkaHost: getKafkaUrl_()});
  const consumerStream = new ConsumerStream(
    client,
    partitionConfig_(topic, partitions),
    options
  );
  logger.log('info', `consumerStream with key of ${topic} created`);
  consumerStream.connect();
  consumerStream.init();
  consumerStream.on('data', async message => {
    logger.log(
      'info',
      `consumerStream with key data topic ${R.prop(
        'topic',
        message
      )} ${JSON.stringify({
        offset: R.prop('offset', message),
        highWaterOffset: R.prop('highWaterOffset', message),
        partition: R.prop('partition', message),
        key: R.prop('key', message)
      })}`
    );
    consumerStream.pause();
    await parseMissive_(message, fn);
    consumerStream.resume();
  });
  consumerStream.on('error', error =>
    logger.log(
      'error',
      `error on consumerStream with key when consume ${topic} : ${JSON.stringify(
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
  logger.log('info', `Consumer with key of ${topic} created and connected`);
  consumer.on('message', async message => {
    logger.log(
      'info',
      `Consumer with key data topic ${R.prop(
        'topic',
        message
      )} ${JSON.stringify({
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
      `error on Consumer with key when consume ${topic} : ${JSON.stringify(
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
  logger.log('info', `Consumer simple of ${topic} created and connected`);
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
      `error on Consumer when consume ${topic} : ${JSON.stringify(error)}`
    )
  );
};

const connectAndStartConsumer_ = (topic, consumer, fn, partition) =>
  R.cond([
    [
      consumer => R.equals('consumerGroupStream', consumer),
      () => consumerGroupStream_(topic, fn)
    ],
    [
      consumer => R.equals('consumerGroupStreamWithKey', consumer),
      () => consumerGroupStreamWithKey_(topic, fn)
    ],
    [
      consumer => R.equals('consumerGroup', consumer),
      () => consumerGroup_(topic, partition, fn)
    ],
    [
      consumer => R.equals('consumerGroupCompact', consumer),
      () => consumerGroupCompact_(topic, partition, fn)
    ],
    [
      consumer => R.equals('consumerStreamWithKey', consumer),
      () => consumerStreamWithKey_(topic, partition, fn)
    ],
    [
      consumer => R.equals('consumerWithKey', consumer),
      () => consumerSimpleWithKey_(topic, partition, fn)
    ],
    [
      consumer => R.equals('consumer', consumer),
      () => consumerSimple_(topic, partition, fn)
    ],
    [
      R.T,
      () =>
        logger.log(
          'error',
          `Consumer type config "${consumer}" was not defined`
        )
    ]
  ])(consumer);

const formatObjectTopicForCreate_ = topic =>
  R.pipe(
    () => R.objOf('topic', R.prop('topic', topic)),
    R.assoc('partitions', R.path(['config', 'partitions'], topic) || 1),
    R.assoc('replicationFactor', R.path(['config', 'replication'], topic) || 1),
    R.assoc('configEntries', R.path(['config', 'entries'], topic) || [])
  )();

const startConsumer = data => {
  const kafkaClient_ = new KafkaClient({kafkaHost: getKafkaUrl_()});
  kafkaClient_.on('ready', () => {
    logger.log('info', 'client kafka ready');
    const allTopicsToCreate = R.map(
      item => formatObjectTopicForCreate_(item),
      data
    );
    kafkaClient_.createTopics(allTopicsToCreate, async error => {
      if (error) {
        logger.log(
          'error',
          `error when create topics ${JSON.stringify(error)}`
        );
        return;
      }

      logger.log('info', 'client kafka all topics created');
      const listTopic = await getListOfTopics_();
      const keyTopic = R.keys(listTopic);
      if (R.length(keyTopic) === 0) {
        logger.log('error', 'no topic has been created');
        return;
      }

      R.map(
        item => createConsumerIfTopicExist_(keyTopic, listTopic, item),
        data
      );
    });
  });
  kafkaClient_.on('error', err => {
    logger.log('error', `client kafka error: ${JSON.stringify(err)}`);
  });
  kafkaClient_.on('socket_error', err => {
    logger.log('error', `client kafka socket_error: ${JSON.stringify(err)}`);
  });
  kafkaClient_.on('brokersChanged', () => {
    logger.log('info', `client kafka brokersChanged`);
  });
  kafkaClient_.on('close', () => {
    logger.log('info', `client kafka close`);
  });
  kafkaClient_.on('connect', () => {
    logger.log('info', `client kafka connect`);
  });
  kafkaClient_.on('reconnect', () => {
    logger.log('info', `client kafka reconnect`);
  });
  kafkaClient_.on('zkReconnect', () => {
    logger.log('info', `client kafka zkReconnect`);
  });
};

module.exports = {startConsumer};
