"use strict";

var es = require('elasticsearch');
var _ = require('lodash');


// Set ElasticSearch location and port
var client = new es.Client({
    host : 'localhost:9200'
});

var test = function(result, callback){
	console.log('THIS FUNCTION IS AWESOME');
}

var searchData = function(query, callback){
	var Results;
	var queryObj = query;

	client.search(queryObj).then(function (resp) {
		var hits = resp.aggregations.patient_bucket.buckets;
		Results = hits;
		console.log('Results: ', hits.length);
		getDisaggregation(Results, function(_feedBack){
			callback(_feedBack);
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

var getDisaggregation = function(result, callback)
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
		indicatorName:"ONCTX",
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

var getTestSearch = function()
{
	var queryObj = {
		index: 'openmrs',
  		type: 'obs',
  		body: {
  			filter: {
  				term: {
  					location_id: 1
      			}
    		}
  		}
	};
	return queryObj;
}

var getPregantPatients = function(location_id, reportStartDate, reportEndDate) {
	var queryObj = {
		index: 'openmrs',
		type: 'obs',
		body: {
		    "query": {
		        "bool": {
		           "must": [
		              {
		                  "terms": {"location_id": [1,2]}
		              },
		              {
		                  "range": {
		                     "obs_datetime": {
		                        "from": "2007-01-01",
		                        "to": "2007-12-31"
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
		                                        "term": {"concept_id":1856}
		                                    },
		                                    {
		                                        "bool": {
		                                            "must_not": [
		                                               {
		                                                   "term": {"value_coded":1175}
		                                               }
		                                            ]
		                                        }
		                                    }
		                                 ]
		                            }
		                        },
		                        {
		                            "terms": {"concept_id": [1279,5992,1855]}
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [45,5272]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [703,1065]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [1790,6042]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [44,47,46]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [1834,1835]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [1831]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "terms": {"concept_id": [1854]}
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [1181,1251]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [1148,1776]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [1992]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [1066,1067]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "bool": {
		                                "must": [
		                                   {
		                                       "terms": {"concept_id": [2055]}
		                                   },
		                                   {
		                                       "terms": {"value_coded": [1065]}
		                                   }
		                                ]
		                            }
		                        },
		                        {
		                            "filtered": {
		                               "query": {
		                               "terms": {"concept_id": [5596]}
		                               },
		                               "filter": {
		                                   "script": {
		                                      "script": "doc['obs_datetime'].value > doc['obs_datetime'].value"
		                                   }
		                               }
		                            }                            
		                        }
		                      ]
		                  }
		              }
		           ]
		        }
		    },
		   	"aggs":{
		        "gender_bucket":{
		            "terms":{
		                "field":"person_id",                
		                "size":10
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


var getStartingART = function(location_id, reportStartDate, reportEndDate) {
	var queryObj = {
		index: 'openmrs',
		type: 'obs',
		body: {
			size:10,
			"query": {
				"bool": {
					"must": [
					{
						"terms": {"location_id": [1,2]}
					},
					{
						"range": {
							"obs_datetime": {
								"from": "2007-01-01",
								"to": "2007-12-31"
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
                		"size":50000
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


var getPatientsOnProphylaxis = function (location_id, reportStartDate, reportEndDate)
{
	
	var queryObj = {
		index: 'openmrs',
		type: 'obs',
		body: {
			"size": 10, 
    		"query": {
    			"bool": {
    				"must": [
    				{
    					"terms": {"location_id":[4]}
              		},
              		{
                  		"range": {
                      		"obs_datetime": {
                          		"from": "2010-01-01",
                          		"to": "2010-12-31"
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
module.exports = function() {
	test();
	
	return {

		testElastic: function testElastic(request, callback) {
			var testQuery = getPatientsOnProphylaxis(4, "2007-01-01","2007-12-31")
			searchData(testQuery,function(resp){
				callback(resp);
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