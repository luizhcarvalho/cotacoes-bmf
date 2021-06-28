const Pool = require('pg').Pool;

const pool = () => {
    const p = new Pool({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_DATABASE,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT
    });
    return p;
};

module.exports = {
    pool
};