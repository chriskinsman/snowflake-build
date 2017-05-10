  
  A package to provide a way to build snowflake warehouse in a CI/CD fashion.

## Installation

```bash
npm install snowflake-build --save
```

## Features

  * DSL to define schemas and data to load from S3
  * Updates pointer to 'production' database in REDIS

## Background

We are users of snowflake and currently use a model where we build a new data warehouse each night and then transparently replace the previous nights warehouse by flipping a REDIS key.  This package with the [snowlake-utils](https://github.com/chirskinsman/snowflake-utils) utilities make this use case very simple.

## People

The author is [Chris Kinsman](https://github.com/chriskinsman)

## License

  [MIT](LICENSE)

