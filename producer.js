require('dotenv').config();
const Kafka = require("node-rdkafka");

const topic = process.env.TOPICS;
const bootstrapServers = process.env.BOOTSTRAP_SERVERS.split(",");
const kafkaConf = {
  'group.id': "producer-kafka",
  'bootstrap.servers': bootstrapServers,
  'dr_cb': true  //delivery report callback
};
console.log('configuration: ', kafkaConf)
const producer = new Kafka.Producer(kafkaConf);

producer.on("ready", function(arg) {
  console.log(`producer ${arg.name} ready.`);
  // args:
  // - topic
  //   Topic to send the message to
  // - partition
  //   specify a partition for the message
  //   this defaults to -1 - which will use librdkafka's default partitioner 
  //   (consistent random for keyed messages, random for unkeyed messages)
  // - message
  //   Message to send. Must be a buffer
  // - key
  //   The key associated with the message.
  // - timestamp
  //   Timestamp to send with the message.
  // - opaque 
  //   you can send an opaque token here, which gets passed along to your delivery reports
  // return true/false
  const result = producer.produce(topic, -1, new Buffer("test"));
  console.log('produce result', result)
  setTimeout(() => producer.disconnect(), 1000);
});

producer.on("disconnected", function(arg) {
  console.log('disconnected')
  process.exit();
});

producer.on('event.error', function(err) {
  console.error(err);
  process.exit(1);
});
producer.on('event.log', function(log) {
  console.log(log);
});

// 'dr_cb': true  //delivery report callback
producer.on('delivery-report', function(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
});
producer.connect();