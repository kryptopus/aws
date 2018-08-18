"use strict";


module.exports.compute = async (event, context) => {
  return {
    interval: process.env.INTERVAL
  };
};
