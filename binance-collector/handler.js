"use strict";
const axios = require("axios");
const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB();
const lambda = new AWS.Lambda();
const baseUrl = "https://api.binance.com";

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

module.exports.discoverSymbols = async (event, context) => {
  const {
    data: { symbols }
  } = await axios.get(`${baseUrl}/api/v1/exchangeInfo`, {
    responseType: "json"
  });

  const registeredSymbols = await fetchRegisteredSymbols();
  const registeredSymbolStrings = registeredSymbols.map(registeredSymbol => {
    return registeredSymbol.Symbol.S;
  });

  const missingSymbolCount = 0;
  for (let { symbol, baseAsset, quoteAsset } of symbols) {
    const isMissingSymbol = registeredSymbolStrings.indexOf(symbol) < 0;
    if (isMissingSymbol) {
      await registerSymbol({
        Exchange: { S: "Binance" },
        Symbol: { S: symbol },
        BaseAsset: { S: baseAsset },
        QuoteAsset: { S: quoteAsset }
      });
      missingSymbolCount++;
    }
  }

  return {
    registeredSymbolCount: missingSymbolCount
  };
};
