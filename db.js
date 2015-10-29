"use strict";

var mysql = require('mysql');
var settings = require('./conf/settings')

module.exports = function() {
	var service = {};
	var pool  = mysql.createPool(settings);
	

	var getServerConnection = function(connectHandler) {
		pool.getConnection(function(err, connection) {
			if (err) {
				console.log('Error While connecting to the database', err);
            	return connectHandler(err, null);
			}
			return connectHandler(null, connection);
		});
	};

	service.queryServer = function(params) {
		var sql = params.sql;
		var values = params.values;
		var queryHandler = params.callback;

		getServerConnection(function(err, connection) {
			if (err) return queryHandler(err, null);
			
			connection.query(sql, values, function(err, rows, fields) {
				queryHandler(err, rows);
				connection.release();
			});
		});
	};

	return service;	
}();