"use strict";
const util = require("util");
const axios = require("axios");
const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB();
const lambda = new AWS.Lambda();
const baseUrl = "https://api.binance.com";
const tableName = `candles`;

const ensureTable = async name => {
  try {
    await dynamo.describeTable({ TableName: name }).promise();
  } catch (error) {
    if (error.code !== "ResourceNotFoundException") {
      throw error;
    }
    await dynamo
      .createTable({
        TableName: name,
        AttributeDefinitions: [
          {
            AttributeName: "ExchangeSymbolInterval",
            AttributeType: "S"
          },
          {
            AttributeName: "Time",
            AttributeType: "N"
          }
        ],
        KeySchema: [
          {
            AttributeName: "ExchangeSymbolInterval",
            KeyType: "HASH"
          },
          {
            AttributeName: "Time",
            KeyType: "RANGE"
          }
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5
        }
      })
      .promise();
  }
};

module.exports.collect = async (event, context) => {
  const { baseAsset, quoteAsset } = event;
  const symbol = baseAsset + quoteAsset;

  let candles = [];
  try {
    const { status, data } = await axios.get(`${baseUrl}/api/v1/klines?symbol=${symbol}&interval=1m&limit=2`, {
      responseType: "json"
    });
    if (status >= 300) {
      console.error(`Unable to fetch candles for ${symbol}`, data);
    } else {
      candles = data;
    }
  } catch (error) {
    console.error(`Unable to fetch candles for ${symbol}`, error);
  }

  let insertedCount = 0;
  for (let candle of candles) {
    const [
      openTime,
      open,
      high,
      low,
      close,
      volume,
      closeTime,
      quoteAssetVolume,
      trades,
      takerBuyBaseVolume,
      takerBuyQuoteVolume
    ] = candle;

    await dynamo
      .putItem({
        TableName: tableName,
        Item: {
          ExchangeSymbolInterval: { S: `Binance-${symbol}-1m` },
          Time: { N: String(openTime) },
          BaseAsset: { S: baseAsset },
          QuoteAsset: { S: quoteAsset },
          Open: { N: String(open) },
          Close: { N: String(close) },
          Low: { N: String(low) },
          High: { N: String(high) },
          Volume: { N: String(volume) }
        }
      })
      .promise();
    insertedCount++;
  }

  return {
    insertedCount
  };
};

module.exports.collectAll = async (event, context) => {
  await ensureTable(tableName);

  const {
    data: { symbols }
  } = await axios.get(`${baseUrl}/api/v1/exchangeInfo`, {
    responseType: "json"
  });

  let invokationCount = 0;
  for (let { baseAsset, quoteAsset } of symbols) {
    await lambda.invoke({
      FunctionName: "binance-collector-dev-collect",
      InvocationType: "Event",
      LogType: "None",
      Payload: JSON.stringify({
        baseAsset,
        quoteAsset
      })
    }, (error, data) => {
      if (error) {
        console.error(error);
      }
    }).promise();

    invokationCount++;
  }

  return {
    invokationCount
  };
};
