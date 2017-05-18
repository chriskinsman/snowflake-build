'use strict';

/*
    Utilities for builing warehouses in snowflake.
    
    Contains json that describes schemas and common helper function for creating databases,
    schema, and loading data.
*/

const debug = require('debug')('snowflake-build');
debug.log = console.info.bind(console);

const SnowflakeUtils = require('snowflake-utils');

const _ = require('lodash');
const async = require('async');
const AWS = require('aws-sdk');
const moment = require('moment');
const uuidV1 = require('uuid/v1');

// How many queries to allow simultaneously via eachLimit
const _parallelism = 20;

const Builder = function Builder(account, username, password, redisHost, redisPort, warehouseName, awsAccessKeyId, awsSecretAccessKey, s3Bucket, s3Region, dbPrefix) {
    this._snowflake = new SnowflakeUtils(account, username, password, redisHost, redisPort);
    this._s3 = new AWS.S3({accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey, region: s3Region});
    this._s3Bucket = s3Bucket;
    this._dbPrefix = dbPrefix;
    this._awsAccessKeyId = awsAccessKeyId;
    this._awsSecretAccessKey = awsSecretAccessKey;
    this._warehouseName = warehouseName;
};

Builder.prototype._doesS3KeyExist = function _doesS3KeyExist(bucket, path, callback) {
    var params = {
        Bucket: bucket,
        Prefix: path,
        MaxKeys: 1
    };

    this._s3.listObjects(params, function (err, data) {
        if (err) {
            debug('doesKeyExist err:' + err);
            callback(err);
        }
        else {
            callback(null, data.Contents.length > 0);
        }
    });
};

// Do this before loading tables so we don't create a database if we can't load
Builder.prototype._validateFilesToLoad = function _validateFilesToLoad(steps, callback) {
    const self = this;
    let errors = false;
    debug('Checking all files ready to load');
    async.each(steps, function (step, done) {
        if (step.s3Exists) {
            self._doesS3KeyExist(self._s3Bucket, step.s3Exists, function (err, complete) {
                if (err || !complete) {
                    debug('ETL not complete for ' + step.table, err);
                    errors = true;
                }
                done(null);
            });
        }
        else {
            setImmediate(done);
        }
    }, function (err) {
        if (err || errors) {
            debug('ETL not complete');
            callback('ETL not complete');
        }
        else {
            callback();
        }
    });
};

Builder.prototype._purgeOldDatabases = function _purgeOldDatabases(callback) {
    const self = this;
    debug('Purging old databases');
    async.parallel([
        function (done) {
            self._snowflake.getCurrentDatabase(function(err, databaseName) {
                self._snowflake.execute(self._warehouseName, `show databases like '${self._dbPrefix}%'`, [], { database: '123' }, function (err, rows) {
                    let sevenDaysAgo = moment().subtract(7, 'days');
                    let expiredDatabases = _.filter(rows, function (row) {
                        return sevenDaysAgo.isAfter(row.created_on) && row.name !== databaseName;
                    });
                    async.eachSeries(expiredDatabases, function (database, done) {
                        debug('Dropping database: ' + database.name);
                        self._snowflake.execute(self._warehouseName, 'DROP DATABASE ' + database.name, [], { database: '123' }, done);
                    }, done);
                });
            });
        }
    ], function (err) {
        // Keep due to waterfall
        return callback(err);
    });
};

Builder.prototype._createDatabase = function _createDatabase(callback) {
    debug('Creating database');
    let databaseName = this._dbPrefix + moment().format('YYYYMMDD') + "_" + uuidV1().replace(/-/g, '');

    debug('Creating database: ' + databaseName);
    this._snowflake.execute(this._warehouseName, 'CREATE DATABASE ' + databaseName, [], { database: '123' }, function (err) {
        return callback(err, databaseName);
    });
};

Builder.prototype._createSchema = function _createSchema(databaseName, steps, callback) {
    const self = this;
    debug('Creating schema');
    async.each(steps, function (step, done) {
        debug('Creating table: ' + step.table);
        async.series([
            function (done) {
                self._snowflake.execute(self._warehouseName, 'DROP TABLE IF EXISTS ' + step.table, [], { database: databaseName }, done);
            },
            function (done) {
                self._snowflake.execute(self._warehouseName, step.create, [], { database: databaseName }, done);
            }
        ], done);
    }, callback);
};

Builder.prototype._createEmptyDatabase = function _createEmptyDatabase(steps, callback) {
    const self = this;
    self._createDatabase(function (err, databaseName) {
        if (err) {
            debug('Error creating database: ' + err);
            return callback(err);
        }
        else {
            self._createSchema(databaseName, steps, function (err) {
                callback(err, databaseName);
            });
        }
    });
};

Builder.prototype._loadTable = function _loadTable(databaseName, step, callback) {
    const self = this;
    debug("Loading table: " + step.table);
    self._snowflake.execute(self._warehouseName, `COPY INTO ${step.table} FROM s3://${self._s3Bucket}/${step.s3Location} FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE_UNENCLOSED_FIELD=NONE) CREDENTIALS=(AWS_KEY_ID='${self._awsAccessKeyId}' AWS_SECRET_KEY='${self._awsSecretAccessKey}')`, [], { database: databaseName }, function(err, rows) {
        if (err) {
            debug(`Error loading table: ${step.table} err: ${err}`);
            callback(err);
        } else {
            debug('Finished loading table: ' + step.table, rows);
            callback(null);
        }
    });
};

Builder.prototype._loadS3IntoDatabase = function _loadS3IntoDatabase(databaseName, steps, callback) {
    debug('Loading csvs');
    const self = this;
    const _tablesToLoad = _.filter(steps, 's3Location');
    async.eachLimit(_tablesToLoad, _parallelism, function (loadDef, done) {
        self._loadTable(databaseName, loadDef, done);
    }, function (err) {
        callback(err, databaseName);
    });
};

Builder.prototype._executeStepQueries = function _executeStepQueries(databaseName, step, callback) {
    const self = this;
    if (step.queries) {
        async.eachSeries(step.queries, function (queryDef, done) {
            debug(`${step.table} - ${queryDef.status}`);
            self._snowflake.execute(self._warehouseName, queryDef.query, [], { database: databaseName }, done);
        }, callback);
    }
    else {
        setImmediate(callback, null, databaseName);
    }
};

Builder.prototype._runPostStepQueries = function _runPostStepQueries(databaseName, postSteps, stage, callback) {
    const self = this;
    let stepDefinition = _.filter(postSteps, { stage: stage });
    let queries = _.get(stepDefinition, '[0].queries');
    if (!_.isEmpty(queries)) {
        async.eachSeries(queries, function (queryDef, done) {
            debug(`Post Stage: ${stage} - ${queryDef.status}`);
            self._snowflake.execute(self._warehouseName, queryDef.query, [], { database: databaseName }, done);
        }, callback);
    }
    else {
        setImmediate(callback);
    }
};

Builder.prototype._transformStages = function _transformStages(databaseName, steps, postSteps, callback) {
    const self = this;
    debug('Transforms');
    const sortedStages = _.sortBy(_.uniq(_.map(steps, 'stage')));

    async.eachSeries(sortedStages, function (stage, done) {
        debug(`Stage: ${stage}`);
        const stageSteps = _.filter(steps, { stage: stage });
        async.series([
            function (done) {
                async.eachLimit(stageSteps, _parallelism, function (step, done) {
                    self._executeStepQueries(databaseName, step, done);
                }, done);
            },
            function (done) {
                self._runPostStepQueries(databaseName, postSteps, stage, done);
            }
        ], done);            
    }, callback);
};

Builder.prototype._validateTableHasRows = function _validateTableHasRows(databaseName, tableName, callback) {
    this._snowflake.execute(this._warehouseName, 'SELECT count(*) as row_count FROM ' + tableName, [], { database: databaseName }, function (err, rows) {
        if (err) {
            debug(`Error checking row count`, err);
        }

        if (err || !rows || rows.length === 0 || rows[0].row_count === 0) {
            debug(`Table: ${tableName} is empty or missing`);
            callback(`Table: ${tableName} is empty or missing`);
        }
        else {
            callback();
        }        
    });
};

Builder.prototype._validateTable = function _validateTable(databaseName, step, callback) {
    if (step.validate) {
        debug("Validating table: " + step.table);
        this._validateTableHasRows(databaseName, step.table, callback);
    }
    else {
        setImmediate(callback);
    }
};

Builder.prototype._validateDatabase = function _validateDatabase(databaseName, steps, callback) {
    const self = this;
    async.eachLimit(steps, _parallelism, function (loadDef, done) {
        self._validateTable(databaseName, loadDef, done);
    }, callback);
};

Builder.prototype._updateDatabaseInRedis = function _updateDatabaseInRedis(databaseName, steps, skipRedis, callback) {
    const self = this;
    async.series([
        function (done) {
            debug('Validating database');
            self._validateDatabase(databaseName, steps, done);
        },
        function (done) {
            if (skipRedis) {
                debug('Skipping the update of the db in redis.');
                setImmediate(done);
            } else {
                debug('Putting db: ' + databaseName + ' in redis');
                self._snowflake.setCurrentDatabase(databaseName, done);
            }
        }
    ], callback);
};

Builder.prototype._loadDatabase = function _loadDatabase(steps, postSteps, skipRedis, callback) {
    const self = this;
    async.waterfall([
        function (done) {
            self._createEmptyDatabase(steps, done);
        },
        function (databaseName, done) {
            self._loadS3IntoDatabase(databaseName, steps, done);
        },
        function (databaseName, done) {
            self._transformStages(databaseName, steps, postSteps, function (err) {
                return done(err, databaseName);
            });
        },
        function (databaseName, done) {
            self._updateDatabaseInRedis(databaseName, steps, skipRedis, function (err) {
                return done(err, databaseName);
            });
        }
    ], callback);
};

Builder.prototype.build = function build(steps, postSteps, skipRedis, callback) {
    const self = this;
    debug('Starting ETL');
    async.series([
        function (done) {
            self._validateFilesToLoad(steps, done);
        },
        //this._purgeOldDatabases,
        function (done) {
            self._loadDatabase(steps, postSteps, skipRedis, done);
        }
    ], callback);
};

module.exports = Builder;