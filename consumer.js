require('dotenv').config();
var Kafka = require("node-rdkafka");

const topics = process.env.TOPICS.split(",");
const bootstrapServers = process.env.BOOTSTRAP_SERVERS.split(",");

var kafkaConf = {
  "group.id": "consumer-kafka",
  "bootstrap.servers": bootstrapServers,
  "socket.keepalive.enable": true, // only node lib
};

const consumer = new Kafka.KafkaConsumer(kafkaConf, {
  "auto.offset.reset": "beginning"
});
const numMessages = 5;
let counter = 0;
consumer.on("error", function(err) {
  console.error(err);
});
consumer.on("ready", function(arg) {
  console.log(`Consumer ${arg.name} ready`);
  consumer.subscribe(topics);
  consumer.consume();
});
consumer.on("data", function(m) {
  counter++;
  if (counter % numMessages === 0) {
    console.log("calling commit");
    consumer.commit(m);
  }
  console.log(m.value.toString());
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

setTimeout(function() {
  consumer.disconnect();
}, 300000);