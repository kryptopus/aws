"use strict";
const AWS = require("aws-sdk");
const lambda = new AWS.Lambda();
const dynamo = new AWS.DynamoDB();

const fetchCandles = async (exchange, baseAsset, quoteAsset, interval, timeStart, timeEnd) => {
  const tableName = process.env.CANDLE_TABLE_NAME;
  const symbol = baseAsset + quoteAsset;
  const primaryValue = `${exchange}-${symbol}-${interval}`;
  const response = await dynamo
    .query({
      TableName: tableName,
      ProjectionExpression: "#primary_att, #time_att, #open_att, #close_att, #low_att, #high_att, #volume_att",
      KeyConditionExpression: "#primary_att = :primary_value AND #time_att BETWEEN :time_start AND :time_end",
      ExpressionAttributeNames: {
        "#primary_att": "ExchangeSymbolInterval",
        "#time_att": "Time",
        "#open_att": "Open",
        "#close_att": "Close",
        "#low_att": "Low",
        "#high_att": "High",
        "#volume_att": "Volume"
      },
      ExpressionAttributeValues: {
        ":primary_value": { S: primaryValue },
        ":time_start": { N: String(timeStart) },
        ":time_end": { N: String(timeEnd) }
      }
    })
    .promise();

  return response.Items;
};

const isConsecutiveCandles = (candles, timeStart, candleDuration, expectedCount) => {
  const candleCount = candles.length;
  if (candleCount !== expectedCount) {
    console.error(`Expected ${expectedCount} candles, get ${candleCount}`);
    return false;
  }

  let count = 0;
  let expectedTime = Number(timeStart);
  for (let index = 0; index < candleCount; index++) {
    const candle = candles[index];
    if (candle.Time.N != expectedTime) {
      console.error(`Expected time ${expectedTime}, get ${candle.Time.N}`);
      return false;
    }
    expectedTime += Number(candleDuration);
    count++;
  }

  if (count !== expectedCount) {
    console.error(`Expected ${expectedCount} candles, get ${count} consecutive candles`);
    return false;
  }

  return true;
};

const computeCandleFromSmallerCandles = candles => {
  const candleCount = candles.length;
  const firstCandle = candles[0];
  const lastCandle = candles[candleCount - 1];

  const computedCandle = {
    Time: { N: firstCandle.Time.N },
    Open: { N: firstCandle.Open.N },
    Close: { N: lastCandle.Close.N },
    Low: { N: firstCandle.Low.N },
    High: { N: firstCandle.High.N },
    Volume: { N: "0" }
  };
  for (let candle of candles) {
    computedCandle.Volume.N = String(Number(computedCandle.Volume.N) + Number(candle.Volume.N));
    if (candle.Low.N < computedCandle.Low.N) {
      computedCandle.Low.N = candle.Low.N;
    }
    if (candle.High.N > computedCandle.High.N) {
      computedCandle.High.N = candle.High.N;
    }
  }

  return computedCandle;
};

const saveCandle = async candle => {
  await dynamo
    .putItem({
      TableName: process.env.CANDLE_TABLE_NAME,
      Item: candle
    })
    .promise();
};

module.exports.compute = async (event, context) => {
  const {
    sourceIntervalName,
    sourceIntervalDuration,
    targetIntervalName,
    targetIntervalDuration,
    exchange,
    baseAsset,
    quoteAsset
  } = event;
  const symbol = baseAsset + quoteAsset;

  const now = new Date().getTime();
  const timeStart = now - (now % targetIntervalDuration) - targetIntervalDuration;
  const timeEnd = now - (now % targetIntervalDuration) - sourceIntervalDuration;
  const candles = await fetchCandles(exchange, baseAsset, quoteAsset, sourceIntervalName, timeStart, timeEnd);

  const expectedCount = targetIntervalDuration / sourceIntervalDuration;
  if (!isConsecutiveCandles(candles, timeStart, sourceIntervalDuration, expectedCount)) {
    throw new Error(`Invalid candles fetched from database (${exchange}, ${symbol}, ${sourceIntervalName}): ${JSON.stringify(candles)}`);
  }

  const computedCandle = computeCandleFromSmallerCandles(candles);
  computedCandle.Exchange = { S: exchange };
  computedCandle.Interval = { S: targetIntervalName };
  computedCandle.BaseAsset = { S: baseAsset };
  computedCandle.QuoteAsset = { S: quoteAsset };
  computedCandle.ExchangeSymbolInterval = { S: `${exchange}-${symbol}-${targetIntervalName}` };

  await saveCandle(computedCandle);
  return {
    computedCandle
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

module.exports.computeAll = async (event, context) => {
  /*
  const sourceIntervalName = process.env.SOURCE_INTERVAL_NAME;
  const sourceIntervalDuration = process.env.SOURCE_INTERVAL_DURATION;
  const targetIntervalName = process.env.TARGET_INTERVAL_NAME;
  const targetIntervalDuration = process.env.TARGET_INTERVAL_DURATION;
  */
  const {
    sourceIntervalName,
    sourceIntervalDuration,
    targetIntervalName,
    targetIntervalDuration,
  } = event;

  const registeredSymbols = await fetchRegisteredSymbols();

  let invocationCount = 0;
  for (let registeredSymbol of registeredSymbols) {
    const exchange = registeredSymbol.Exchange.S;
    const baseAsset = registeredSymbol.BaseAsset.S;
    const quoteAsset = registeredSymbol.QuoteAsset.S;

    await lambda
      .invoke(
        {
          FunctionName: process.env.COMPUTE_FUNCTION_NAME,
          InvocationType: "Event",
          LogType: "None",
          Payload: JSON.stringify({
            exchange,
            baseAsset,
            quoteAsset,
            sourceIntervalName,
            sourceIntervalDuration,
            targetIntervalName,
            targetIntervalDuration
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

  return {
    invocationCount
  };
};
