#! /usr/bin/env node
'use strict';

console.info('Account: ' + process.env.SNOWFLAKE_ACCOUNT);

const Builder = require('../index');
const builder = new Builder(process.env.SNOWFLAKE_ACCOUNT, process.env.SNOWFLAKE_USERNAME, process.env.SNOWFLAKE_PASSWORD, process.env.SNOWFLAKE_REDISHOST, process.env.SNOWFLAKE_REDISPORT, 'testing', process.env.AWS_ACCESS_KEY_ID, process.env.AWS_SECRET_ACCESS_KEY, 'warehouse-kinsman-net', 'us-west-2', 'CHRISTEST_');

const Tests = {};

const Stages = { Staging: 1, Final: 2 };

const _steps = [
    // Multi file wiht post steps
    {
        table: 'staging_zipcodes',
        stage: Stages.Staging,
        create: `
            CREATE TABLE staging_zipcodes
            (
                zipcode string NOT NULL,
                city string,
                latitude float,
                longitude float,
                mean_income integer,
                median_income integer,
                state string,
                region string,
                density_type string,
                dma integer
            );
        `,
        s3Exists: 'zip_codes.csv.gz',
        s3Location: 'zip_codes.csv.gz',
        queries: [
            { status: 'filter 98*', query: `delete from staging_zipcodes where zipcode like '98%'` }
        ]        
    },
    // Single file
    {
        table: 'staging_dma',
        stage: Stages.Staging,
        create: `
            CREATE TABLE staging_dma
            (
                dma integer not null,
                name string not null
            );
        `,
        s3Exists: 'dma.csv.gz',
        s3Location: 'dma.csv.gz'
    },
    {
        table: 'staging_better_health',
        stage: Stages.Staging,
        create: `
            CREATE TABLE staging_better_health
            (
                year integer,
                state_abbr string,
                state_desc string,
                city_name string,
                geographic_level string,
                data_source string,
                category string,
                unique_id string,
                measure string,
                data_value_unit string,
                data_value_type_id string,
                data_value_type string,
                data_value float,
                low_confidence_limit float,
                high_confidence_limit float,
                data_value_footnote_symbol string,
                data_value_footnote string,
                population_count integer,
                geo_location string,
                category_id string,
                measure_id string,
                city_fips string,
                tract_fips string,
                short_question_text string
            );
        `,
        s3Exists: 'better_health/COMPLETE',
        s3Location: 'better_health/'
    },
    {
        table: 'zipcodes',
        validate: true,
        stage: Stages.Final,
        create: `
            CREATE TABLE zipcodes
            (
                zipcode string NOT NULL  ,
                city string ,
                latitude FLOAT ,
                longitude FLOAT ,
                mean_income INTEGER ,
                median_income INTEGER ,
                state string ,
                region string ,
                density_type string ,
                dma INTEGER
            );
        `,
        queries: [
            { status: 'insert', query: `INSERT INTO zipcodes(SELECT zipcode, city, latitude, longitude, mean_income, median_income, state, region, density_type, dma FROM staging_zipcodes)` }
        ]
    },    
    {
        table: 'dma',
        validate: true,
        stage: Stages.Final,
        create: `
            CREATE TABLE dma
            (
                dma integer not null,
                name string not null
            );
        `,
        queries: [
            { status: 'insert', query: `INSERT INTO dma(SELECT dma, name FROM staging_dma)` }
        ]
    },    
    {
        table: 'dma_zip',
        validate: true,
        stage: Stages.Final,
        create: `
            CREATE TABLE dma_zip
            (
                zipcode string not null,
                name string not null
            );
        `,
        queries: [
            { status: 'insert', query: 'INSERT INTO dma_zip(zipcode, name) SELECT zipcode, name FROM zipcodes INNER JOIN dma ON zipcodes.dma = dma.dma'}
        ]
    },    
    {
        table: 'better_health',
        validate: true,
        stage: Stages.Final,
        create: `
            CREATE TABLE better_health
            (
                year integer,
                state_abbr string,
                state_desc string,
                city_name string,
                geographic_level string,
                category string,
                measure string,
                data_value_unit string,
                data_value_type_id string,
                data_value_type string,
                data_value float,
                low_confidence_limit float,
                high_confidence_limit float,
                population_count integer,
                category_id string,
                measure_id string,
                short_question_text string
            );        
        `,
        queries: [
            {
                status: 'insert', query: `
                    INSERT INTO better_health(
                        year, state_abbr, state_desc, city_name, geographic_level, category,
                        measure, data_value_unit, data_value_type_id, data_value_type, data_value,
                        low_confidence_limit, high_confidence_limit, population_count, category_id, measure_id, short_question_text)
                    SELECT
                        year, state_abbr, state_desc, city_name, geographic_level, category,
                        measure, data_value_unit, data_value_type_id, data_value_type, data_value,
                        low_confidence_limit, high_confidence_limit, population_count, category_id, measure_id, short_question_text
                    FROM staging_better_health` }
        ]
    }
];

const _postStepQueries = [
    {
        stage: Stages.Staging,
        queries: [
        ]
    },
    {
        stage: Stages.Final,
        queries: [
            { table: 'dma_zip', status: 'build', query: `INSERT INTO dma_zip(SELECT zipcode, name FROM zipcodes inner join dma on zipcodes.dma = dma.dma)` }
        ]
    },
];


Tests.Build = function Build(test) {
    builder.build(_steps, _postStepQueries, false, function (err) {
        test.ifError(err);
        test.done();
    });
};


module.exports = Tests;