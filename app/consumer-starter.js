const {Consumer, ConsumerStream, ConsumerGroup, ConsumerGroupStream, KafkaClient, Admin} = require('kafka-node');
const R = require('ramda');
const {log} = require('loggy-log');

const kafkaClient_ = new KafkaClient({kafkaHost: 'localhost:9092'});

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

const consumerGroupKryptonopolis = (topic, fn) => {
  const consumerOptions = {
    kafkaHost: 'localhost:9092',
    id: 'consumerKryptonopolis',
    groupId: 'kafka-node-krypto',
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

const consumerGroupStreamKryptonopolis = (topic, fn) => {
  const consumerOptions = {
    kafkaHost: 'localhost:9092',
    id: 'consumerKryptonopolis',
    groupId: 'kafka-node-krypto',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest',
   // fetchMaxBytes: 2048 * 2048
  };
  const consumerGroup = new ConsumerGroupStream(consumerOptions, [topic]);
   consumerGroup.on('error', async error => {
    console.log(JSON.stringify(error));
   // console.log('ConsumerGroupStream.consumerGroup', consumerGroup.consumerGroup);
    consumerGroup.pause();
    //consumerGroup.consumerGroup.fetchOffset(error.partition);
     await new Promise(resolve => resolve('test'));
    consumerGroup.resume();
  });
  consumerGroup.on('data', async message => {
    consumerGroup.pause();
    console.log('element', {offset: message.offset, highWaterOffset: message.highWaterOffset});
    await parseMissive_(message, fn)
    consumerGroup.resume();
  });
};

const consumerStreamKrypto = (topic, fn) => {
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
    console.log('elements', {offset: chunk.offset, highWaterOffset: chunk.highWaterOffset});
    consumer.pause();
    await parseMissive_(chunk, fn);
    consumer.resume();
  });
  consumer.on('error', log('error'));
};

const consumerKrypto = (topic, fn) => {
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
    console.log('elements', {offset: chunk.offset, highWaterOffset: chunk.highWaterOffset});
    consumer.pause();
    await parseMissive_(chunk, fn);
    consumer.commit(() => {

      consumer.resume();
    })
  });
  consumer.on('error', log('error'));
};

const consumerSimple = (topic, fn) => {
  const consumer = new Consumer(
    new KafkaClient({kafkaHost: 'localhost:9092'}),
    [{topic}],
    {
      autoCommit: true,
      //fetchMaxBytes: 2048 * 2048
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
  if (topic === 'Kryptonopoliss') {
    consumerGroupStreamKryptonopolis(topic, fn);
  } else {
    consumerGroupStreamKryptonopolis(topic, fn);
  }
  log('info', `consumer of ${topic} created`);
};

const startConsumer = (topic, fn) =>
  R.pipe(
    waitForTopics_,
    R.then(() => connectAndStartConsumer_(topic, fn))
  )(topic);

module.exports = {startConsumer};
