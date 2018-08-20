"use strict";
const axios = require("axios");
const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB();
const lambda = new AWS.Lambda();
const baseUrl = "https://api.binance.com";

module.exports.collect = async (event, context) => {
  const { baseAsset, quoteAsset } = event;
  const symbol = baseAsset + quoteAsset;
  const startTime = (new Date().getTime() - new Date().getTime() % (1000*60)) - 1000*60*2;
  const url = `${baseUrl}/api/v1/klines?symbol=${symbol}&interval=1m&startTime=${startTime}`;

  console.log(`Collect Binance ${symbol} 1m: ${url}`);

  let candles = [];
  try {
    const { status, data } = await axios.get(url, { responseType: "json" });
    if (status >= 300) {
      console.error(`Unable to fetch candles for Binance ${symbol}`, url, data);
      throw new Error(`Unable to fetch candles for Binance ${symbol}: ${JSON.stringify(data)}`);
    } else {
      candles = data;
    }
  } catch (error) {
    console.error(`Unable to fetch candles for Binance ${symbol}`, url, error);
    throw new Error(`Unable to fetch candles for Binance ${symbol}: ${error.message}`);
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

    const item = {
      ExchangeSymbolInterval: { S: `Binance-${symbol}-1m` },
      Exchange: { S: "Binance" },
      Interval: { S: "1m" },
      Time: { N: String(openTime) },
      BaseAsset: { S: baseAsset },
      QuoteAsset: { S: quoteAsset },
      Open: { N: String(open) },
      Close: { N: String(close) },
      Low: { N: String(low) },
      High: { N: String(high) },
      Volume: { N: String(volume) }
    };
    console.log(`Save candle Binance ${symbol} 1m: ${JSON.stringify(item)}`);

    await dynamo
      .putItem({
        TableName: process.env.CANDLE_TABLE_NAME,
        Item: item
      })
      .promise();
    insertedCount++;
  }

  if (insertedCount === 0) {
    throw new Error(`No saved candles for Binance ${symbol}`);
  }

  return {
    insertedCount
  };
};

const fetchRegisteredSymbols = async () => {
  const tableName = process.env.SYMBOL_TABLE_NAME;
  const response = await dynamo
    .scan({
      TableName: tableName
    })
    .promise();

  return response.Items;
};

const registerSymbol = async symbol => {
  await dynamo
    .putItem({
      TableName: process.env.SYMBOL_TABLE_NAME,
      Item: symbol
    })
    .promise();
};

module.exports.collectAll = async (event, context) => {
  const {
    data: { symbols }
  } = await axios.get(`${baseUrl}/api/v1/exchangeInfo`, {
    responseType: "json"
  });

  const registeredSymbols = await fetchRegisteredSymbols();
  const registeredSymbolStrings = registeredSymbols.map(registeredSymbol => {
    return registeredSymbol.Symbol.S;
  });

  const missingSymbols = [];
  let invocationCount = 0;
  for (let { symbol, baseAsset, quoteAsset } of symbols) {
    const isMissingSymbol = registeredSymbolStrings.indexOf(symbol) < 0;
    if (isMissingSymbol) {
      missingSymbols.push({
        Exchange: { S: "Binance" },
        Symbol: { S: symbol },
        BaseAsset: { S: baseAsset },
        QuoteAsset: { S: quoteAsset }
      });
    }

    await lambda
      .invoke(
        {
          FunctionName: process.env.COLLECT_FUNCTION_NAME,
          InvocationType: "Event",
          LogType: "None",
          Payload: JSON.stringify({
            baseAsset,
            quoteAsset
          })
        },
        (error, data) => {
          if (error) {
            console.error(error);
          }
        }
      )
      .promise();

    invocationCount++;
  }

  for (let missingSymbol of missingSymbols) {
    await registerSymbol(missingSymbol);
  }

  return {
    invocationCount,
    registeredSymbolCount: missingSymbols.length
  };
};
