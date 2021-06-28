require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const port = 3000;
const host = '0.0.0.0';
const cotacoes = require('./routes/cotacoes');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(cors());

app.get('/', (req, res) => {
    res.status(200).json({
        message: 'Welcome to cotacoes api'
    });
});

app.use(cotacoes());

app.listen(3000, () => {
    console.log(`Listen on port 3000`);
});