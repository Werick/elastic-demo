"use strict";

var es = require('elasticsearch');
var ejs = require('elastic.js');
var _ = require('lodash');


// Set ElasticSearch location and port
var client = new es.Client({
    host : 'localhost:9200'
});

var index = 'openmrs',
    type = 'obs_report';

function _handlePeriodCondition(params, conditionsArray) {
    var includeCondition = false;
    var period = ejs.RangeQuery('obs_datetime');
    
    if(params.startDate) {
        period = ejs.RangeQuery('obs_datetime').gte(params.startDate);
        includeCondition = true;
    }
    
    if(params.endDate) {
        period = ejs.RangeQuery('obs_datetime').lte(params.endDate);
        includeCondition = true;
    }
    
    if(includeCondition) {
        conditionsArray.push(period);
    }
}
    
function _handleAgeCondition(params, conditionsArray) {
    var age = ejs.RangeQuery('birthdate');

    var includeCondition = false;
    
    if(params.lowerAgeLimit) {
        if(params.endDate) {
            age =  ejs.RangeQuery('birthdate').lte(params.endDate + '||-' + params.lowerAgeLimit + 'y');
        } else {
            age =  ejs.RangeQuery('birthdate').lte('now-' + params.lowerAgeLimit + 'y');
        }
        includeCondition = true;
    } 
    
    if(params.upperAgeLimit) {
        if(params.endDate) {
            age =  ejs.RangeQuery('birthdate').gte(params.endDate + '||-' + params.upperAgeLimit + 'y');
        } else {
            age =  ejs.RangeQuery('birthdate').gte('now-' + params.upperAgeLimit + 'y');
        }
        includeCondition = true;
    }
    
    if(includeCondition) {
        conditionsArray.push(age);
    }
}
    
function _handleLocationCondition(params, conditionsArray) {
    if(params.location) {
    	var locations = [];
        var location = ejs.TermsQuery('location_id', []);
        
        if(typeof params.location === 'array') {
            for(var loc in params.location) {
                locations.push(loc);
            }                

        } else {
            locations.push(params.location);
        }
        location = ejs.TermsQuery('location_id', locations);
        conditionsArray.push(location);
    }
}
   
function _handleGenderCondition(params, conditionsArray) {
    if(params.gender) {
        var gender = ejs.TermQuery('gender', params.gender);
        conditionsArray.push(gender);
    }
}
   
var test = function(result, callback){
	console.log('THIS FUNCTION IS AWESOME');
}

var searchData = function(query, callback){
	var Results = [];
	
	var queryObj = getPatientsOnProphylaxis(4, '2007-01-01', '2007-12-31');;

	client.search(queryObj).then(function (resp) {
		var hits = resp.aggregations.patient_bucket.buckets;		
		console.log('Results CTX: ', hits.length);
		getDisaggregation(hits, 'OnCTX', function(_feedBack){
			Results.push(_feedBack);	
			callback(Results);		
		});
		
	}, function (err) {
    console.trace(err.message);
	});	

	queryObj = getPregantPatients(4, '2007-01-01', '2007-12-31');

	client.search(queryObj).then(function (resp) {
		var hits = resp.aggregations.patient_bucket.buckets;		
		console.log('Results Preg: ', hits.length);
		getDisaggregation(hits, 'Pregnant', function(_feedBack){
			Results.push(_feedBack);			
		});
		
	}, function (err) {
    console.trace(err.message);
	});	

	queryObj = getStartingART(4, '2007-01-01', '2007-12-31');

	client.search(queryObj).then(function (resp) {
		var hits = resp.aggregations.patient_bucket.buckets;		
		console.log('Results ART Start: ', hits.length);
		getDisaggregation(hits, 'StartingART', function(_feedBack){
			Results.push(_feedBack);			
		});
		
	}, function (err) {
    console.trace(err.message);
	});


}

var getYears = function(dob, reportEndDate ) {
  //Get 1 day in milliseconds
  var one_day=1000*60*60*24;
  var one_year = one_day * 365.25

  // Convert both dates to milliseconds
  var date1_ms = dob.getTime();
  var date2_ms = reportEndDate.getTime();

  // Calculate the difference in milliseconds
  var difference_ms = date2_ms - date1_ms;
    
  // Convert back to years and return
  return Math.round(difference_ms/one_year); 
  
}

var getDisaggregation = function(result, indicatorName, callback)
{
	var childMale = 0;
	var childFemale = 0;
	var adultMale = 0;
	var adultFemale = 0;
	var totalCount = result.length;

	var male = 0;
	var female = 0;
	var patientList = [];
	_.each(result, function(_bucket){
		var patient = {};
		patient.id = _bucket.key;
		patient.gender = _bucket.gender.buckets[0].key;
		patient.dob = new Date(_bucket.gender.buckets[0].dob.buckets[0].key_as_string.substring(0, 10));
		patient.age = getYears(patient.dob,new Date('2007-12-31'));
		// console.log(_bucket.gender.buckets[0].dob.buckets[0].key_as_string.substring(0, 10))
		// console.log('getUTCFullYear', getYears(patient.dob,new Date('2007-12-31')))
		// console.log('Patient Obj', JSON.stringify(patient))
		patientList.push(patient);
	});

	_.each(patientList, function(patient){
		//below 15yrs
		if(patient.age < 15)
		{
			//children
			if (patient.gender = 'M') childMale = childMale + 1;
			else childFemale = childFemale + 1;
		} 
		else {
			//adults
			if(patient.gender = 'F') adultFemale = adultFemale + 1;
			else adultMale = adultMale +1;
		}
		
	})


	result = {
		indicatorName:indicatorName,
		ageCategory: {
			"adult":{
				male: adultMale,
				female: adultFemale
			},
			"child":{
				male:childMale,
				female:childFemale
			}
		},
		total:totalCount
	};

	callback(result);
}
//test multi-search

var getTestMultiSearch = function(callback){
	var queries = [];
	var queryObj1 = {
		index: 'openmrs',
  		type: 'obs_report'
	};

	//using  elasticjs to build query body
	var _body = ejs.Request().filter(ejs.TermFilter('location_id', 1));
	queryObj1.body = _body;

	var queryObj2 = {
		index: 'openmrs',
  		type: 'obs_report'
	};

	//using  elasticjs to build query body
	_body = ejs.Request().filter(ejs.TermFilter('location_id', 2));
	queryObj2.body = _body;
	var header = {index: 'openmrs', type: 'obs_report'}

	queries.push(header);
	queries.push(queryObj1.body);
	queries.push(header);
	queries.push(queryObj2.body);

	client.msearch({
		body: queries
	}).then(function (resp) {
		
		var hits = resp;		
		_.each(resp.responses, function(data){
			console.log('Results multi-search: ', data.hits.hits.length);
		});
		
		callback(hits)
	}, function (err) {
    console.trace(err.message);
	});	

}

var multiSearchQuery = function(params, callback){

	var queries = [];
	var header = {index: 'openmrs', type: 'obs_report'};

	var params = params || {};
	params.index = index;
	params.type = type;
	params.location = 1
    params.upperAgeLimit = 1;
    params.aggsName = 'belowOne';	

    console.log('Params before call++', params);
	var belowOne = getEnrolledInCare(params);

	params.upperAgeLimit = 14;
	params.gender = 'M';
    params.aggsName = 'below15Male';
	var below15Male = getEnrolledInCare(params);

	params.gender = 'F';
    params.aggsName = 'below15Female';
	var below15Female = getEnrolledInCare(params);

	delete params['upperAgeLimit'];
	params.lowerAgeLimit = 15;
	params.gender = 'M';
    params.aggsName = 'above15Male';
	var above15Male = getEnrolledInCare(params);

	params.gender = 'F';
    params.aggsName = 'above15Female';
	var above15Female = getEnrolledInCare(params);

	//build queries array
	queries.push(header);
	queries.push(belowOne.body);
	queries.push(header);
	queries.push(below15Male.body);
	queries.push(header);
	queries.push(below15Female.body);
	queries.push(header);
	queries.push(above15Male.body);
	queries.push(header);
	queries.push(above15Female.body);


	client.msearch({
		body: queries
	}).then(function (resp) {
		
		var hits = resp;	
		var belowOneCount = 0, below15FemaleCount =0, below15MaleCount = 0, 
			above15FemaleCount =0, above15MaleCount = 0, totalCount = 0;	
		_.each(resp.responses, function(data){
			console.log('iteration data ', data);
		
			if(data.aggregations.hasOwnProperty('belowOne')) {
				belowOneCount = data.aggregations.belowOne.buckets.length;
			} else if(data.aggregations.hasOwnProperty('below15Female')) {
				below15FemaleCount = data.aggregations.below15Female.buckets.length;
			} else if(data.aggregations.hasOwnProperty('below15Male')) {
				below15MaleCount = data.aggregations.below15Male.buckets.length;
			} else if(data.aggregations.hasOwnProperty('above15Female')) {
				above15FemaleCount = data.aggregations.above15Female.buckets.length;
			} else if(data.aggregations.hasOwnProperty('above15Male')) {
				above15MaleCount = data.aggregations.above15Male.buckets.length;
			}
			
		});

		totalCount = below15MaleCount + below15FemaleCount + above15MaleCount + above15FemaleCount;

		var result = {
		indicatorName:'Testing EnrolledInCareMOH',
		ageCategory: {
			'belowOne':belowOneCount,
			'adult':{
				male: above15MaleCount,
				female: above15FemaleCount
			},
			'child':{
				male:below15MaleCount,
				female:below15FemaleCount
			}
		},
		total:totalCount
		};
		console.log('Results multi-search: ', result);
		
		callback(result)
	}, function (err) {
    console.trace(err.message);
	});	

}

var getTestSearch = function(callback)
{
	var queryObj = {
		index: 'openmrs',
  		type: 'obs_report'
  		// ,
  		// body: {
  		// 	filter: {
  		// 		term: {
  		// 			location_id: 1
    //   			}
    // 		}
  		// }
	};

	//using  elasticjs to build query body
	var _body = ejs.Request().filter(ejs.TermFilter('location_id', 1));
	queryObj.body = _body;

	console.log('QUery Object: ', JSON.stringify(queryObj));
	// return queryObj;
	client.search(queryObj).then(function (resp) {
		var hits = resp.hits.hits;		
		console.log('Results Preg: ', hits.length);
		callback(hits)
	}, function (err) {
    console.trace(err.message);
	});	
}

var getEnrolledInCare = function(params){
	var conditions = [];
	if(!_.isEmpty(params)) {     
        _handleAgeCondition(params, conditions);
        _handleLocationCondition(params, conditions);
        _handlePeriodCondition(params, conditions);
        _handleGenderCondition(params, conditions);
        
     }

    console.log('Params +++', params);
	var queryObj = {
		index: params['index'] || index,
		type: params['type'] || type
	};

	// basic mult-select query conditions
	var encounterType = ejs.TermsQuery('encounter_type', [1, 2, 3, 4]),
    	location = ejs.TermsQuery('location_id', [4]),
      	obsDatetime = ejs.RangeQuery('obs_datetime').from("2007-01-01").to("2007-12-31");
	
      	//create query
	var _body = ejs.Request()
		.size(0)
		.query(
			ejs.BoolQuery()
			.must(encounterType)
			.must(conditions)
		)
		.agg(
			ejs.TermsAggregation(params.aggsName)
			.field('person_id')
			.size(100000)
			)

	queryObj.body = _body;


	console.log('QUery Object test: ', JSON.stringify(queryObj));
	return queryObj;	
}

var getPregantPatients = function(params) {
	// basic mult-select query conditions
	var conditions = [];
	if(!_.isEmpty(params)) {     
        _handleAgeCondition(params, conditions);
        _handleLocationCondition(params, conditions);
        _handlePeriodCondition(params, conditions);
        _handleGenderCondition(params, conditions);
        
     }
		var _body = ejs.Request()
		.size(0)
		.query(
			ejs.BoolQuery()
			.must(conditions)
			.must(
				ejs.BoolQuery()
				.should(
					ejs.BoolQuery()
					.must(ejs.TermQuery('concept_id', 1856))
					.must(ejs.BoolQuery()
						.must_not(ejs.TermQuery('value_coded', 1175))
					)
				)
				.should(ejs.TermsQuery('concept_id', [1279,5992,1855]))
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [45, 5272]))
					.must(ejs.TermsQuery('value_coded', [703, 1065]))
				)
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [1790, 6042]))
					.must(ejs.TermsQuery('value_coded', [44, 47, 46]))
				)
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [1834, 1835]))
					.must(ejs.TermsQuery('value_coded', [1831]))
				)
				.should(ejs.TermsQuery('concept_id', 1854))
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [1181, 1251]))
					.must(ejs.TermsQuery('value_coded', [1148, 1776]))
				)
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [1992]))
					.must(ejs.TermsQuery('value_coded', [1066, 1067]))
				)
				.should(
					ejs.BoolQuery()
					.must(ejs.TermsQuery('concept_id', [2055]))
					.must(ejs.TermsQuery('value_coded', [1065]))
				)
			)
		)
		.agg(
			ejs.TermsAggregation(params.aggsName)
			.field('person_id')
			.size(100000)
			)

	var queryObj = {
		index: 'openmrs',
		type: 'obs_report',
		body:_body
		
	}

	return queryObj;
}


var getStartingART = function(locationId, reportStartDate, reportEndDate) {
	var queryObj = {
		index: 'openmrs',
		type: 'obs_report',
		body: {
			size:10,
			"query": {
				"bool": {
					"must": [
					{
						"terms": {"location_id": [locationId]}
					},
					{
						"range": {
							"obs_datetime": {
								"from": reportStartDate,
								"to": reportEndDate
                     		}
                  		}
              		},
              		{
                  		"bool": {
                      		"should": [
                         	{
                             	"term": {"concept_id":1250}
                         	},
                         	{
                            	"bool": {
                                	"must": [
                                    {
                                        "term": {"concept_id":1255}
                                    },
                                    {
                                        "term": {"value_coded":1256}
                                    }
                                 	]
                             	}
                         	},
                         	{
                            	"term": {"concept_id":1251}
                         	},
                          	{
                            	"bool": {
                                 	"must": [
                                    {
                                        "term": {"concept_id":2155}
                                    },
                                    {
                                        "terms": {"value_coded":[1776,1185]}
                                    }
                                 	]
                             	}
                         	}
                      		]
                  		}
              		}
           			]
        		}
    		},
   			"aggs":{
        		"patient_bucket":{
            		"terms":{
                		"field":"person_id",                
                		"size":500000
            		},
            		"aggs":{
                		"gender":{
                    		"terms":{
                        		"field":"gender"
                    		},
                     		"aggs":{
                        		"dob":{
                            		"terms":{
                                	"field":"birthdate"
                            		}
                        		}
                    		}
                		}
            		}
        		}
    		}
		}
	}

	return queryObj;
}


var getPatientsOnProphylaxis = function (locationId, reportStartDate, reportEndDate)
{
	
	var queryObj = {
		index: 'openmrs',
		type: 'obs_report',
		body: {
			"size": 10, 
    		"query": {
    			"bool": {
    				"must": [
    				{
    					"terms": {"location_id":[locationId]}
              		},
              		{
                  		"range": {
                      		"obs_datetime": {
                          		"from": reportStartDate,
                          		"to": reportEndDate
                      		}
                		}
              		},
		              {
		                  "bool": {
		                       "should": [                          
		                           {
		                               "bool": {
		                                   "must": [
		                                       {
		                                           "terms": {"concept_id": [1109,1193,1263,2366,6903,8346]}
		                                        },
		                                        {
		                                            "terms": {"value_coded": [916]}
		                                        }
		                                    ]
		                               }
		                           }, 
		                           {
		                               "bool": {
		                                   "must": [
		                                       {
		                                           "term": {"concept_id": 2250}
		                                        },
		                                        {
		                                            "terms": {"value_coded": [1065]}
		                                        }
		                                    ]
		                               }
		                           },  
		                           {
		                               "bool": {
		                                   "must": [
		                                       {
		                                           "term": {"concept_id": 1261}
		                                        },
		                                        {
		                                            "terms": {"value_coded": [1257,1256,1259,981,1850]}
		                                        }
		                                    ]
		                               }
		                           }
		                      ]
		                  }
		              }
           			]
        		}
    		},
		    "aggs":{
		        "patient_bucket":{
		            "terms":{
		                "field":"person_id",		                
		                "size":10000000
		            },
		            "aggs":{
		                "gender":{
		                    "terms":{
		                        "field":"gender"
		                    },
		                     "aggs":{
		                        "dob":{
		                            "terms":{
		                                "field":"birthdate"
		                            }
		                        }
		                    }
		                }
		            }
		        }
		    }
		}
	}

	return queryObj;
}

//Testing some simple stuff
//Test DAO
var testPhoto = function(){
	console.log('Test photo')
}
module.exports = function() {
	test();
	
	return {

		testElastic: function testElastic(request, callback) {
			// var testQuery = getPatientsOnProphylaxis(4, "2007-01-01","2007-12-31")
			// searchData(testQuery,function(resp){
			// 	callback(resp);
			// });
			// getTestSearch(function(data){
			// 	callback(data);
			// });
			// getEnrolledInCare();

			// getTestMultiSearch(function(data){
			// 	callback(data);
			// });
			multiSearchQuery({}, function(data){
				callback(data);
			});
		},
		getEncounterById: function getEncounterById(request, callback) {

			var values = [
				request.params.id
			];

			var sql = "select * from encounter " +
					" where encounter_id = ?"

			db.queryServer({
				sql : sql, 
				values: values,
				callback : callback
			});
		},
		getAllEncounters: function getAllEncounters(params, callback) {

				var values = [
				
			];

			var sql = "select encounter_datetime, encounter_id, encounter_type, creator, uuid "
				sql += "from encounter ";					


			db.queryServer({
				sql : sql, 
				values: values,
				callback : callback
			});
		}
		,
		getData: function getData(request, callback) {
			// console.log('Params ', request.params.userParams)
			var passed_params = request.params.userParams.split('/');
			var table = passed_params[0];
			var	column_name = passed_params[1];
			var	column_value = passed_params[2];
			// console.log('table: ', table)
			// console.log('column: ', column_name)
			// console.log('value: ', column_value)
			var values = [				
				column_value //pass only elements in the where clause
			];

			var sql = "select uuid "
				sql += "from " + table 
				sql += " where " + column_name + " = ? ";	
			
			console.log('SQL ', sql)				


			db.queryServer({
				sql : sql, 
				values: values,
				callback : callback
			});
		}
	};
}();