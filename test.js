var amqp = require('amqplib/callback_api');
amqp.connect('amqp://localhost', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'hello';

    ch.assertQueue(q, {durable: false});
    // Note: on Node 6 Buffer.from(msg) should be used
    ch.consume(q, function(msg) {
      console.log(" [x] Received %s", msg.content.toString());
    }, {noAck: true});

    ch.sendToQueue(q, new Buffer('Hello World!'));
    console.log(" [x] Sent 'Hello World!'");
  });
});

// amqp.connect('amqp://localhost', function(err, conn) {
//   conn.createChannel(function(err, ch) {
//     var q = 'hello';

//     ch.assertQueue(q, {durable: false});
//     console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
//     ch.consume(q, function(msg) {
//       console.log(" [x] Received %s", msg.content.toString());
//     }, {noAck: true});
//   });
// });
