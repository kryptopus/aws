"use strict";
const axios = require("axios");
const api = require("binance");
const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB({ region: "eu-west-1" });
const binanceWS = new api.BinanceWS();

const collect = (baseAsset, quoteAsset, interval, candle) => {
  const symbol = baseAsset + quoteAsset;
  const { startTime, endTime, open, close, high, low, volume, final } = candle;

  if (!final) {
    return;
  }

  const item = {
    ExchangeSymbolInterval: { S: `Binance-${symbol}-1m` },
    Exchange: { S: "Binance" },
    Interval: { S: "1m" },
    Time: { N: String(startTime) },
    BaseAsset: { S: baseAsset },
    QuoteAsset: { S: quoteAsset },
    Open: { N: String(open) },
    Close: { N: String(close) },
    Low: { N: String(low) },
    High: { N: String(high) },
    Volume: { N: String(volume) }
  };

  dynamo.putItem(
    {
      TableName: "kryptopus-candles",
      Item: item
    },
    function(error, data) {
      if (error) {
        console.error(`Unable to save ${symbol}: ${error.message}`);
        return;
      }
      console.log(`Saved ${symbol} ${interval}: ${startTime}, ${open}, ${close}, ${low}, ${high}`);
    }
  );
};

const createWebsocket = (baseAsset, quoteAsset, interval) => {
  const symbol = baseAsset + quoteAsset;

  console.info(`Start collecting ${symbol}`);
  let ws = binanceWS.onKline(symbol, interval, data => {
    collect(baseAsset, quoteAsset, interval, data.kline);
  });
  ws.on("close", () => {
    console.error(`Websocket closed: ${symbol}`);
    createWebsocket(baseAsset, quoteAsset, interval);
  });
};

axios
  .get(`https://api.binance.com/api/v1/exchangeInfo`, {
    responseType: "json"
  })
  .then(function(response) {
    const symbols = response.data.symbols;

    for (let symbol of symbols) {
      createWebsocket(symbol.baseAsset, symbol.quoteAsset, "1m");
    }
  });
