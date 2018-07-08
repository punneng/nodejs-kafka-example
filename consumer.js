require('dotenv').config();
var Kafka = require("node-rdkafka");

const topics = process.env.TOPICS.split(",");
const outgoingTopic = "json-failed-test";
const bootstrapServers = process.env.BOOTSTRAP_SERVERS.split(",");

const producerConf = {
  "bootstrap.servers": bootstrapServers,
  "group.id": "producer-kafka"
};

const producer = new Kafka.Producer(producerConf);
producer.connect();

const consumerConfig = {
  "bootstrap.servers": bootstrapServers,
  "group.id": "consumer-kafka",
  "socket.keepalive.enable": true,
  'enable.auto.offset.store': false
};
  
const consumer = new Kafka.KafkaConsumer(consumerConfig, {
  "enable.auto.commit": false,
  "auto.offset.reset": "earliest"
});

consumer.on("error", function(err) {
  console.error(err);
});
consumer.on("ready", function(arg) {
  console.log(`Consumer ${arg.name} ready`);
  consumer.subscribe(topics);
  consumer.consume();
});
consumer.on("data", function(m) {
  console.log(m.value.toString())
  try {
    JSON.parse(m.value.toString());
  } catch (err) {
    console.log("failed")
    producer.produce(outgoingTopic, -1, new Buffer(m.value.toString()));
  }
  consumer.commit(m);
});
consumer.on("disconnected", function(arg) {
  process.exit();
});
consumer.on('event.error', function(err) {
  console.error(err);
  process.exit(1);
});
consumer.on('event.log', function(log) {
  console.log(log);
});
consumer.connect();

// setTimeout(function() {
//   consumer.disconnect();
// }, 3000);