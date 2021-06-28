const http = require('http');
const fs = require('fs');
const unzipper = require('unzipper');
const readline = require('readline');
const moment = require('moment');
const connection = require('../db/connection');

const tmpFolder = './files/';
const url = 'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/';

const downloadFile = async (date, url, dest) => {
    let zipName = 'COTAHIST_D' + date + '.ZIP';
    const file = fs.createWriteStream(dest + zipName);

    return new Promise((resolve, reject) => {
        // tenta baixar o arquivo
        const request = http.get(url + zipName, (response) => {
            response.pipe(file);
            file.on('finish', () => {
                resolve(zipName);
            })
        }).on('error', (err) => {
            // remove o arquivo baixado
            fs.unlink(file);
            reject(err);
        });
    });
};

const unzipFile = async (zippedFileName) => {
    let fileName = '';
    return new Promise((resolve, reject) => {
        fs.createReadStream(tmpFolder + zippedFileName)
            .pipe(unzipper.Parse())
            .on('entry', (entry) => {
                // guarda o nome do arquivo descompactado
                fileName = entry.path;
                entry.pipe(fs.createWriteStream(tmpFolder + fileName));
            })
            .on('finish', () => {
                // remove o arquivo compactado
                fs.unlinkSync(tmpFolder + zippedFileName);
                // retorna o nome do arquivo descompactado
                resolve(fileName);
            })
            .on('error', (error) => {
                fs.unlinkSync(tmpFolder + zippedFileName);
                reject(error);
            });
    });
};

const readFile = async (filename) => {
    return new Promise((resolve, reject) => {
        try {
            const fileStream = fs.createReadStream(tmpFolder + filename);

            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity
            });

            const lines = [];
            rl.on('line', (line) => {
                if (line.trim().length === 245) {
                    lines.push(readLine(line));
                }
            }).on('close', () => {
                resolve(lines);
            });

        } catch (error) {
            console.log(error);
            reject(error);
        }
    });
};

const readLine = (line) => {
    let lineF = '(';
    lineF += line.substring(0, 2); // tipo de registro
    lineF += ',\'' + line.substring(2, 10) + '\''; // data do pregao
    lineF += ',\'' + line.substring(10, 12) + '\''; // codigo bci
    lineF += ',\'' + line.substring(12, 24).trim() + '\''; // codigo de negociacao do papel
    lineF += ',' + line.substring(24, 27); // tipo de mercado
    lineF += ',\'' + line.substring(27, 39).trim() + '\''; // nome resumido da empresa emissora do papel
    lineF += ',\'' + line.substring(39, 49).trim() + '\''; // especificacao do papel
    lineF += ',\'' + line.substring(49, 52) + '\''; // prazo em dias do mercado a termo
    lineF += ',\'' + line.substring(52, 56).trim() + '\''; // moeda de referencia
    lineF += ',' + addDot(line.substring(56, 69)); // preco de abertura do papel
    lineF += ',' + addDot(line.substring(69, 82)); // preco maximo do papel
    lineF += ',' + addDot(line.substring(82, 95)); // preco minimo do papel
    lineF += ',' + addDot(line.substring(95, 108)); // preco medio do papel
    lineF += ',' + addDot(line.substring(108, 121)); // preco do ultimo negocio
    lineF += ',' + addDot(line.substring(121, 134)); // preco da melhor oferta de compra
    lineF += ',' + addDot(line.substring(134, 147)); // preco da melhor oferta de venda
    lineF += ',' + line.substring(147, 152); // numero de negocios efetuados com o papel
    lineF += ',' + line.substring(152, 170); // quantidade total de titulos negociados neste papel
    lineF += ',' + addDot(line.substring(170, 188)); // volume total de titulos negociados neste papel
    lineF += ',' + addDot(line.substring(188, 201)); // preco de exercicio para o mercado de opcoes ou valor do contrato para o mercado
    lineF += ',' + line.substring(201, 202); // indicador de correcao de precos de exercicios ou valores de contrato para os mercados de opcoes ou termo secundario
    lineF += ',\'' + line.substring(202, 210) + '\''; // data do vencimento para os mercados de opcoes ou termo secundario
    lineF += ',' + line.substring(210, 217); // fator de cotacao do papel
    lineF += ',' + addDot(line.substring(217, 230), 6); // preco de exercicio em pontos para opcoes referenciadas em dolar ou valor de contrato em pontos para termo secundario
    lineF += ',\'' + line.substring(230, 242) + '\''; // codigo do papel no sistema ISIN ou codigo interno do papel
    lineF += ',' + line.substring(242, 245); // numero de distribuicao do papel
    lineF += ')'; // final da linha
    return lineF;
};

const addDot = (str, right) => {
    if (!right) {
        right = 2;
    }
    return str.substring(0, str.length - right) + '.' + str.substring(str.length - right);
};

const updatePrize = async (date) => {

    return new Promise(async (resolve, reject) => {
        try {
            const zipname = await downloadFile(date, url, tmpFolder);
            const filename = await unzipFile(zipname);
            const linesF = await readFile(filename);

            let d = moment(date, 'DDMMYYYY');
            console.log(`Atualizando os dados do dia ${d}`);
            let sql = 'DELETE FROM cotacoes.cotacoes WHERE dia_pregao = $1;';

            let sql2 = 'INSERT INTO cotacoes.cotacoes VALUES ';
            sql2 += linesF[0];
            for (let i = 1; i < linesF.length; i++) {
                sql2 += ', ' + linesF[i];
            }

            const client = await connection.pool().connect();

            try {
                await client.query('BEGIN');
                const res = await client.query(sql, [d.format('YYYY-MM-DD')]);
                const res2 = await client.query(sql2);
                await client.query('COMMIT');
                resolve({
                    status: 201,
                    message: 'Datas atualizadas com sucesso'
                });
            } catch (error) {
                await client.query('ROLLBACK');
                console.log(error);
                reject({
                    status: 500,
                    message: error
                });
            } finally {
                client.release();
            }

        } catch (error) {
            reject({
                status: 500,
                message: error
            });
        }
    });
};

const updatePapersDaysLists = async () => {
    return new Promise(async (resolve, reject) => {
        const sql = `
            WITH dados AS (
                SELECT DISTINCT
                    dia_pregao
                FROM
                    cotacoes.cotacoes
            )
            INSERT INTO cotacoes.lista_dias
                SELECT
                    dia_pregao
                FROM
                    dados
                    LEFT JOIN cotacoes.lista_dias
                        USING (dia_pregao)
                WHERE
                    lista_dias.dia_pregao is null
            ;
        `;

        const sql2 = `
            WITH dados AS (
                SELECT DISTINCT
                    tipo_mercado,
                    cod_negociacao,
                    nome_resumido
                FROM
                    cotacoes.cotacoes
            )
            INSERT INTO cotacoes.lista_papeis
                SELECT
                    tipo_mercado,
                    cod_negociacao,
                    nome_resumido
                FROM
                    dados
                    LEFT JOIN cotacoes.lista_papeis
                        USING (tipo_mercado, cod_negociacao, nome_resumido)
                where
                    lista_papeis.tipo_mercado IS NULL
            ;
        `;

        const client = await connection.pool().connect();

        try {
            await client.query('BEGIN');
            const res = await client.query(sql);
            const res2 = await client.query(sql2);
            await client.query('COMMIT');
            resolve({
                status: 201,
                message: res2.rowCount
            });
        } catch (error) {
            await client.query('ROLLBACK');
            console.log(error);
            reject({
                status: 500,
                message: error
            });
        } finally {
            client.release();
        }
    });
};

module.exports = {
    updatePrize,
    updatePapersDaysLists
};