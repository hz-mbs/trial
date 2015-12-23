var assert     = require('assert');
var thrift     = require('thrift');
var transport  = require('thrift/lib/nodejs/lib/thrift/transport');
var protocol  = require('thrift/lib/nodejs/lib/thrift/protocol');
var ThriftHive = require('thrift-hive/lib/0.7.1-cdh3u1/ThriftHive');

//var protocol = ThriftProtocols.TBinaryProtocol();

// Client connection 
var options = {transport: transport.TMemoryTransport , protocol: protocol.TJSONProtocol(), timeout: 1000};

var connection = thrift.createConnection('10.212.2.143', 10000, options);
var client = thrift.createClient(ThriftHive, connection);
// Execute query 
/*
client.execute('use frauddb', function(err){
  client.execute('show tables', function(err){
    assert.ifError(err);
    client.fetchAll(function(err, databases){
      if(err){
        console.log(err.message);
      }else{
        console.log(databases);
      }
      connection.end();
    });
  });
});
*/
//tcg_detections detection_details


var express = require('express');
var app = express();
var fs = require("fs");

app.get('/listtables', function (req, res) {
  
	client.execute('use frauddb', function(err){
		client.execute('show tables', function(err){
			assert.ifError(err);
			client.fetchAll(function(err, databases){
			  if(err){
				console.log(err.message);
			  }else{
				console.log(databases);
				//vat recs = databases.to_array;
				console.log(JSON.stringify(databases));
				res.end( JSON.stringify(databases) );
			  }
			  connection.end();
			});
		});
	});
   
});

app.get('/listtest', function (req, res) {
  
	client.execute('use default', function(err){
		client.execute('select concat("id:",id),concat("value:",value) from test', function(err){
			assert.ifError(err);
			client.fetchAll(function(err, databases){
			  if(err){
				console.log(err.message);
			  }else{
				console.log(databases);
				//vat recs = databases.to_array;
				console.log(JSON.stringify(databases));
				res.end( JSON.stringify(databases) );
			  }
			  connection.end();
			});
		});
	});
   
});

app.get('/list', function (req, res) {
  
	res.end("List");

});

var server = app.listen(8081, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

});

