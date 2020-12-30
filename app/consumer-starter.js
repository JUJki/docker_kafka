const {
  Consumer,
  // ConsumerStream,
  // ConsumerGroup,
  ConsumerGroupStream,
  KafkaClient,
  Admin
} = require('kafka-node');
const R = require('ramda');
const config = require('config');
const {logger} = require('../app/utils/logger');

/* Const kafkaClient_ = new KafkaClient({kafkaHost: 'localhost:9092'});

kafkaClient_.on('ready', () => {
  log('info', `client kafka ready`);
});

kafkaClient_.on('error', err => {
  log('error', `client kafka error: ${err}`);
});

const getListOfTopics_ = () =>
  new Promise(resolve => {
    const admin = new Admin(
      new KafkaClient({kafkaHost: 'localhost:9092'})
    );
    admin.listTopics((err, res) => {
      R.when(R.not(err), resolve(res));
    });
  });

const checkTopicCreated_ = (resolve, topic) =>
  R.then(
    R.pipe(
      R.last,
      R.prop('metadata'),
      R.keys,
      R.ifElse(R.includes(topic), resolve, () =>
        setTimeout(
          () => checkTopicCreated_(resolve, topic),
          Number(10000)
        )
      )
    )
  )(getListOfTopics_());

const waitForTopics_ = topic =>
  new Promise(resolve => checkTopicCreated_(resolve, topic));

const parseMissive_ = (message, fn) =>
  R.pipe(
    R.prop('value'),
    JSON.parse,
    fn
  )(message);

const consumerGroup_ = (topic, fn) => {
  const consumerOptions = {
    kafkaHost: 'localhost:9092',
    id: `consumer${topic}`,
    groupId: `kafka-node-${topic}`,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    //fetchMaxBytes: 2048 * 2048
  };
  const consumerGroup = new ConsumerGroup(consumerOptions, [topic]);
  consumerGroup.on('error', error => {
    console.log('fff', consumerGroup.getOffset());
    consumerGroup.pause();
    console.log('fff');
    console.log(JSON.stringify(error));
    consumerGroup.resume();
    return false;
  });
  consumerGroup.on('message', async message => {
    consumerGroup.pause();
    console.log('element', {offset: message.offset, highWaterOffset: message.highWaterOffset});
    await parseMissive_(message, fn)
    consumerGroup.resume();
  });
};

const consumerGroupStream_ = (topic, fn) => {
  const consumerOptions = {
    kafkaHost: 'localhost:9092',
    id: 'consumer_'+topic,
    groupId: 'kafka-node-'+topic,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640
  };
  const consumerGroup = new ConsumerGroupStream(consumerOptions, [topic]);
   consumerGroup.on('error', async error => {
     console.log('error');
    console.log(JSON.stringify(error));
   // console.log('ConsumerGroupStream.consumerGroup', consumerGroup.consumerGroup);
    consumerGroup.pause();
    //consumerGroup.consumerGroup.fetchOffset(error.partition);
     await new Promise(resolve => resolve('test'));
    consumerGroup.resume();
  });
  consumerGroup.on('data', async message => {
    consumerGroup.pause();
    console.log('element', {
      offset: message.offset,
      highWaterOffset: message.highWaterOffset,
      partition: message.partition
    });
    await parseMissive_(message, fn)
    consumerGroup.resume();
  });
};
const consumerGroupStreamKryptonopolis_ = (topic, fn) => {
  const consumerOptions = {
    kafkaHost: 'localhost:9092',
    id: 'consumer_'+topic,
    groupId: 'kafka-node-'+topic,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640
  };
  const consumerGroup = new ConsumerGroupStream(consumerOptions, [topic]);
  consumerGroup.on('error', async error => {
    console.log('error');
    console.log(JSON.stringify(error));
    // console.log('ConsumerGroupStream.consumerGroup', consumerGroup.consumerGroup);
    consumerGroup.pause();
    //consumerGroup.consumerGroup.fetchOffset(error.partition);
    await new Promise(resolve => resolve('test'));
    consumerGroup.resume();
  });
  consumerGroup.on('data', async message => {
    consumerGroup.pause();
    console.log('element', {
      offset: message.offset,
      highWaterOffset: message.highWaterOffset,
      partition: message.partition,
      key: message.key,
    });
    await parseMissive_(message, fn)
    consumerGroup.resume();
  });
};


const consumerStreamKrypto_ = (topic, fn) => {
  const consumer = new ConsumerStream(
    new KafkaClient({kafkaHost: 'localhost:9092'}),
    [{topic}],
    {
      // bufferRefetchThreshold: 1
      id: 'consumerKryptonopolis',
      // groupId: 'kafka-node-krypto',
      sessionTimeout: 15000,
      autoCommit: true,
      fetchMaxBytes: 2048 * 2048
//      protocol: ['roundrobin'],
      // fromOffset: 'earliest',
    }
  );
  consumer.on('data', async chunk => {
    console.log('elements', {
      offset: chunk.offset,
      highWaterOffset: chunk.highWaterOffset,
      partition: chunk.partition
    });
    consumer.pause();
    await parseMissive_(chunk, fn);
    consumer.resume();
  });
  consumer.on('error', log('error'));
};

const consumerKrypto_ = (topic, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: 'localhost:9092'}),
    [{topic}],
    {
      // bufferRefetchThreshold: 1
      autoCommit: false,
      fetchMaxBytes: 2048 * 2048
    }
  );
  consumer.on('message', async chunk => {
    console.log('elements', {
      offset: chunk.offset,
      highWaterOffset: chunk.highWaterOffset,
      partition: chunk.partition
    });
    consumer.pause();
    await parseMissive_(chunk, fn);
    consumer.commit(() => {
      consumer.resume();
    })
  });
  consumer.on('error', log('error'));
};

const consumerSimple_ = (topic, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: 'localhost:9092'}),
    [{topic}],
    {
      id: "julien",
      autoCommit: true,
      fetchMaxBytes: 2048 * 2048
    }
  );
  consumer.on('message', message => {
    console.log('elements');
    return parseMissive_(message, fn)
  });
  consumer.on('error', error => {
    console.log(JSON.stringify(error));
    //consumer.resume();
    //consumer.commit(true,() => { console.log('auto commit when error')})
    //consumer.setOffset(error.topic, error.partition, 1);
    return true;
  });
};

const connectAndStartConsumer_ = (topic, fn) => {
  if (['Kryptonopolis'].includes(topic)) {
    consumerGroupStreamKryptonopolis_(topic, fn);
  } else if (['ElasticSearch', 'Helheim'].includes(topic)) {
    consumerGroupStream_(topic, fn);
  } else {
    consumerSimple_(topic, fn);
  }
  log('info', `consumer of ${topic} created`);
};

const startConsumer = (topic, fn) =>
  R.pipe(
    waitForTopics_,
    R.then(() => connectAndStartConsumer_(topic, fn))
  )(topic); */

const getKafkaUrl_ = () => R.prop('kafka', config.get('urlService'));

const topicConsumerGroupStream = [
  // 'Kryptonopolis',
  'ElasticSearch',
  'SentenceEncoder',
  'Helheim',
  'Monitor'
];

const getListOfTopics_ = () =>
  new Promise(resolve => {
    const admin = new Admin(new KafkaClient({kafkaHost: getKafkaUrl_()}));
    admin.listTopics((error, response) => {
      /* Logger.log(
        'info',
        `response getListOfTopics_ : ${JSON.stringify(response)}`
      ); */
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

const _formatResponseListTopic = R.pipe(R.last, R.prop('metadata'), R.keys);

const createConsumerIfTopicExist_ = (listTopic, topic) =>
  R.ifElse(
    () => R.includes(R.prop('topic', topic), listTopic),
    () =>
      connectAndStartConsumer_(
        R.prop('topic', topic),
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

const consumerGroupKryptonopolis_ = (topic, fn) => {
  const options = {
    kafkaHost: getKafkaUrl_(),
    id: `consumer_${topic}`,
    groupId: `kafka-node-${topic}`,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
    fetchMaxBytes: 15728640
  };
  const consumerGroup = new ConsumerGroupStream(options, topic);
  logger.log('info', `consumer group Krypto of ${topic} created`);
  consumerGroup.on('data', async chunk => {
    logger.log(
      'info',
      `Consumer data topic ${R.prop('topic', chunk)} ${JSON.stringify({
        offset: R.prop('offset', chunk),
        highWaterOffset: R.prop('highWaterOffset', chunk),
        partition: R.prop('partition', chunk),
        key: R.prop('key', chunk)
      })}`
    );
    consumerGroup.pause();
    await parseMissive_(chunk, fn);
    consumerGroup.resume();
  });
  consumerGroup.on('error', error =>
    logger.log(
      'error',
      `error on consumerGroupStream when kraken consume ${topic} : ${JSON.stringify(
        error
      )}`
    )
  );
};

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
  logger.log('info', `consumer group stream of ${topic} created`);
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

const consumerSimple_ = (topic, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: getKafkaUrl_()}),
    [{topic}],
    {
      id: `consumer_${topic}`,
      groupId: `kafka-node-${topic}`,
      autoCommit: true,
      fetchMaxBytes: 15728640
    }
  );
  logger.log('info', `consumer simple of ${topic} created`);
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

const connectAndStartConsumer_ = (topic, fn) =>
  R.cond([
    [
      topic => R.includes(topic, topicConsumerGroupStream),
      () => consumerGroupStream_(topic, fn)
    ],
    [R.equals('Kryptonopolis'), () => consumerGroupKryptonopolis_(topic, fn)],
    [R.T, () => consumerSimple_(topic, fn)]
  ])(topic);

const startConsumer = data => {
  const kafkaClient_ = new KafkaClient({kafkaHost: getKafkaUrl_()});
  kafkaClient_.on('ready', async () => {
    logger.log('info', 'client kafka ready');
    const listTopic = await getListOfTopics_();
    if (R.length(listTopic) === 0) {
      logger.log('error', 'no topic has been created');
      return;
    }

    R.map(item => createConsumerIfTopicExist_(listTopic, item), data);
  });
  kafkaClient_.on('error', err => {
    logger.log('error', `client kafka error: ${JSON.stringify(err)}`);
  });
};

module.exports = {startConsumer};
