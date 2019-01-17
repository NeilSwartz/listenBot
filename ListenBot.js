/*Neil Swartz 
15/01/2019
Project using Envirophat on pi to store measurements in a mongo DB
*/

var amqp = require('amqplib/callback_api')
const mongodb = require('mongodb')
const express = require('express')

var pubChannel = null
var offlinePubQueue = []
var clientDB = null
var db = null
var amqpConn = null
var dBConn = false
var mBConn = false
let urlDB = 'mongodb://admin:admin987@ds062818.mlab.com:62818/neil_s_db'
let urlMB = "amqp://jrbbqjwq:g1uvK2MTYiBfvH_JEu9QB5ok6It3oYhJ@flamingo.rmq.cloudamqp.com/jrbbqjwq"
const PORT = process.env.PORT || 5000

const app = express()



// if the connection is closed or fails to be established at all, we will reconnect


// CONNECTION PROCESS 
function start() {
  amqp.connect(urlMB, function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message)
      return setTimeout(start, 1000)
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message)
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000)
    });

    console.log("[AMQP] connected")
    amqpConn = conn

    ConnectDB()
  });
}

function ConnectDB() {
    mongodb.MongoClient.connect(urlDB, function(err, client) {
        if (err) {
            console.error("[MDB]", err.message)
            return setTimeout(start, 1000)
          }
        clientDB = client;
        db = client.db('neil_s_db')
        console.log("[MDB] connected")
        app.listen(PORT, (err) =>{
            if(err){throw err}
            console.log('Listening on port: ', PORT)
        })
        whenConnected()
    });
}

//CHANNEL CREATION

function whenConnected() {
  startPublisher()
  startReader()
}

//publish channels
function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message)
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    });

    pubChannel = ch
    while (true) {
      var m = offlinePubQueue.shift()
      if (!m) break
      publish(m[0], m[1], m[2])
    }
  });
}

// read channels - only ack if successful
function startReader() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message)
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    });
    ch.prefetch(10)
    ch.assertQueue("Checked", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return
      ch.consume("Checked", processMsg, { noAck: false })
      console.log("Check worker has started")
    });

    function processMsg(msg) {
        console.log("received message from pi:", msg.content.toString())
        try {
          if (msg !== "")
            ch.ack(msg)
          else
            ch.reject(msg, true)
        } catch (e) {
          closeOnErr(e)
        }
    }
  });

  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message)
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    });
    ch.prefetch(10)
    ch.assertQueue("measurement", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return
      ch.consume("measurement", processMsg2, { noAck: false })
      console.log("Pi worker has started")
    });

    function processMsg2(msg) {
      sendDB(msg, function(ok) {
        try {
          if (ok)
            ch.ack(msg)
          else
            ch.reject(msg, true)
        } catch (e) {
          closeOnErr(e)
        }
      });
    }
  });
}

//FUNCTIONS 

// method to publish a message, will queue messages internally if the connection is down and resend later
function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                       function(err, ok) {
                         if (err) {
                           console.error("[AMQP] publish", err)
                           offlinePubQueue.push([exchange, routingKey, content])
                           pubChannel.connection.close()
                         }
                       })
  } catch (e) {
    console.error("[AMQP] publish", e.message)
    offlinePubQueue.push([exchange, routingKey, content])
  }
}

function read(msg, cb) {
   console.log("Message Received from App:", msg.content.toString())
   publish("", "Read", new Buffer("read"))
   cb(true)
}

function sendDB(info, cb) {
    let measurements = db.collection("Measurements")
    measurements.insert(JSON.parse(info.content.toString()), function(err){
        if (closeOnErr(err)) return
    });
    cb(true);
    console.log("Message sent to DB")
}

function closeOnErr(err) {
  if (!err) return false
  console.error("error", err)
  amqpConn.close(function (err) {if(err) throw err;})
  clientDB.close(function (err) {if(err) throw err;})
  return true
}

//CODE START

start()

app.get('/data', (req ,res) => {
  db.collection("Measurements").find({}).toArray((err,docs) => {
    if(err){
      console.log("There was error sending to Port: ", PORT)
      res.status(400).send(err)
    }else{
      console.log("Data sent to Port: ", PORT)
      console.log(docs)
      res.status(200).send(docs)
    }
    
  })
})
 

// setInterval(() =>{
//   publish("", "Check", new Buffer("You Good?"))
//   console.log("Check send")
// } ,180000)

setInterval(() =>{
  publish("", "Read", new Buffer("read"))
  console.log("Read Ask")
} ,60000)