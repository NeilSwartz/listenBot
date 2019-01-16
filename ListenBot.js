/*Neil Swartz 
15/01/2019
Project using Envirophat on pi to store measurements in a mongo DB
*/

var amqp = require('amqplib/callback_api');
const mongodb = require('mongodb');

var pubChannel = null;
var offlinePubQueue = [];
var clientDB = null;
var db = null;
var amqpConn = null;
let urlDB = 'mongodb://admin:admin987@ds062818.mlab.com:62818/neil_s_db';
let urlMB = "amqp://jrbbqjwq:g1uvK2MTYiBfvH_JEu9QB5ok6It3oYhJ@flamingo.rmq.cloudamqp.com/jrbbqjwq"

// if the connection is closed or fails to be established at all, we will reconnect

function start() {
  amqp.connect(urlMB, function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });

    console.log("[AMQP] connected");
    amqpConn = conn;

    ConnectDB();
  });
}

function ConnectDB() {
    mongodb.MongoClient.connect(urlDB, function(err, client) {

        if (err) {
            console.error("[MDB]", err.message);
            return setTimeout(start, 1000);
          }
        clientDB = client;
        db = client.db('neil_s_db');
        console.log("[MDB] connected");
        whenConnected();

    });
}

function whenConnected() {
  startPublisher();
  startReader();
//   startReaderPi();
}


function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    pubChannel = ch;
    while (true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

// method to publish a message, will queue messages internally if the connection is down and resend later
function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                       function(err, ok) {
                         if (err) {
                           console.error("[AMQP] publish", err);
                           offlinePubQueue.push([exchange, routingKey, content]);
                           pubChannel.connection.close();
                         }
                       });
  } catch (e) {
    console.error("[AMQP] publish", e.message);
    offlinePubQueue.push([exchange, routingKey, content]);
  }
}

// A worker that acks messages only if processed succesfully
function startReader() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });
    ch.prefetch(10);
    ch.assertQueue("Go", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("Go", processMsg, { noAck: false });
      console.log("App worker has started");
    });

    function processMsg(msg) {
        console.log("received message from App")
      read(msg, function(ok) {
        try {
          if (ok)
            ch.ack(msg);
          else
            ch.reject(msg, true);
        } catch (e) {
          closeOnErr(e);
        }
      });
    }
  });

  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });
    ch.prefetch(10);
    ch.assertQueue("measurement", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("measurement", processMsg2, { noAck: false });
      console.log("Pi worker has started");
    });

    function processMsg2(msg) {
      sendDB(msg, function(ok) {
        try {
          if (ok)
            ch.ack(msg);
          else
            ch.reject(msg, true);
        } catch (e) {
          closeOnErr(e);
        }
      });
    }
  });
}

function read(msg, cb) {
   console.log("Message Received from App:", msg.content.toString());
   publish("", "Read", new Buffer("read"));
   cb(true);
}

function sendDB(info, cb) {
    let measurements = db.collection("Measurements");
    measurements.insert(JSON.parse(info.content.toString()), function(err){
        if (closeOnErr(err)) return;
    });
    cb(true);
    console.log("Message sent to DB");
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("error", err);
  amqpConn.close(function (err) {if(err) throw err;});
  clientDB.close(function (err) {if(err) throw err;});
  return true;
}

start();