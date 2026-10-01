const { createApp } = require('../src/app');

// Serverless handler for Vercel
const { app } = createApp();

module.exports = app;
