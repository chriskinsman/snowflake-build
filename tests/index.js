#! /usr/bin/env node
'use strict';

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
        ]
    },    
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
    builder.build(_steps, _postStepQueries, true, function (err) {
        test.ifError(err);
        test.done();
    });
};


module.exports = Tests;