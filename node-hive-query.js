var assert     = require('assert');
var hive = require('node-hive').for({ server:"10.212.2.143" });
// var hive = require('node-hive').for({ server:"127.0.0.1" });
/*
 * Hive instances currently support the following functions
 * 
 * hive.fetch(query, callback); hive.fetchInBatch(batchSize, query, callback);
 * hive.execute(query, [callback]);
 */
var dateFormat = require('dateformat');

var express = require('express');
var app = express();
// var fs = require("fs");
app.get('/listtables', function (req, res) {
  
	hive.fetch("show tables from frauddb", function(err, data) {
	  data.each(function(record) {
		console.log(JSON.stringify(record));
	  });
	  res.end(JSON.stringify(data));
	});
   
})

// var dateFormat(now, "dddd, mmmm dS, yyyy, h:MM:ss TT");

app.get('/listtest', function (req, res) {
  
	hive.fetch("SELECT * FROM default.test limit 100", function(err, data) {
	  console.log(JSON.stringify(data));
	  	  res.setHeader("Content-Type", "application/json");
	  data.each(function(record) {
		console.log(JSON.stringify(record));
// res.write(JSON.stringify(record));

	  });
	  
	  res.end(JSON.stringify(data.rows));
	});
   
})

app.get('/listcallsnodup', function (req, res) {
	var day = req.param('day');
	var hour = req.param('hour');
	console.log("day=", day);
	console.log("hour=", hour);
	var sql = "SELECT * FROM frauddb.calls_no_dup ";
	if (day){
		sql += " where call_date = '" + day +"'";
		if (hour){
			sql += " and call_hour = '" + hour+"'";
		}
	}
	sql += " limit 2000";
	console.log("sql=", sql);
	console.log("start time=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
	hive.fetch(sql, function(err, data) {
// console.log(JSON.stringify(data));
		console.log("end time=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
		console.log("data.rows.length:", data.rows.length);
	  res.setHeader("Content-Type", "application/json");
// res.write("[");
// data.each(function(record) {
// console.log(JSON.stringify(record));
// res.write("[{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"0\",},{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"605030566844897\"}]"
// );
// });
// res.end("]");
	var re = new RegExp("calls_no_dup.", 'g');


      var padding = "";
	  res.write("[");
	  data.each(function(record) {
		res.write(padding);
		padding = ",";
		var str = JSON.stringify(record);
// console.log("--------------------------------------------"+ str);
// str = str.replaceAll("calls_no_dup.", "");
		str = str.replace(re, '');
		res.write(str);
// console.log("--------------------------------------------"+ str);
		// res.write("[{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"0\",},{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"605030566844897\"}]"
		// );
	  });
	res.end("]");
	console.log("finished json response=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
// res.end("[{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"0\"},{\"calls_no_dup.sw_id\":1,\"calls_no_dup.s_imsi\":\"605030566844897\"}]"
// );

	});
   
})

app.get('/list', function (req, res) {
  
	res.end("List");

})


app.get('/hourlyFraud', function (req, res) {
	var day = req.query.day;
	var hour = req.query.hour;
	var illegalOdds = req.query.illegalOdds;
	console.log("day=", day);
	console.log("hour=", hour);
	console.log("illegalOdds=", illegalOdds);
	var now = new Date();
	if (!day){
		day = dateFormat(now, "yyyy-mm-dd");
	}
	if (!hour){
		hour = dateFormat(now, "hh");
	}
	
//	SELECT * FROM frauddb.hourlysuspect_test  where from_unixtime(CAST(traffic_date/1000 as BIGINT), 'yyyy-MM-dd') = '2015-12-07' and traffic_hour = '0' limit 2000
//	SELECT * FROM frauddb.hourlysuspect_test  where traffic_date = UNIX_TIMESTAMP('2015-12-07', 'yyyy-MM-dd' )*1000 and traffic_hour = '0' limit 2000
	var sql = "SELECT * FROM frauddb.hourlysuspect_test ";
	if (day){
		sql += " where traffic_date = " + " unix_timestamp('"+day +"', 'yyyy-MM-dd' )*1000 ";
		if (hour){
			sql += " and traffic_hour = '" + hour+"'";
		}
	}
	if (illegalOdds){
		sql += " and illegalOdds >= " + illegalOdds;
	}
	
	sql += " limit 2000";
	console.log("sql=", sql);
	console.log("start time=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
	hive.fetch(sql, function(err, data) {
		console.log("end time=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
		if (err){
		     console.log('Error while performing Query.', err);
		     res.send(err);
		}
		if (!data || !data.rows){
			console.log("data error:", "no data");
			res.send("no data");
		}
		
	console.log("data.rows.length:", data.rows.length);
	res.setHeader("Content-Type", "application/json");

	var re = new RegExp("hourlysuspect_test.", 'g');
      var padding = "";
	  res.write("[");
	  data.each(function(record) {
		res.write(padding);
		padding = ",";
		var str = JSON.stringify(record);
		str = str.replace(re, '');
		res.write(str);
	  });
	res.end("]");
	console.log("finished json response=", dateFormat(Date.now(), "dddd, mmmm dS, yyyy, h:MM:ss TT"));
	});
   
})

// Use this if bind to specific IP
// var server = app.listen(8081, '127.0.0.1', function () {
// Binds to all IPs. Will show host as :::
var server = app.listen(8380, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Listening at http://%s:%s", host, port)

})
