const express = require('express');
const router = express.Router();

const connection = require('../db/connection');
const aux = require('./aux');

module.exports = () => {

    // router.get('/cotacoes/dias/:date', async (req, res, next) => {

    //     try {
    //         const result = await aux.updatePrize(req.params.date);
    //         const result2 = await aux.updatePapersDaysLists();
    //         res.status(result2.status).json(result2.message);
    //     } catch (error) {
    //         res.status(error.status).json(error.message);
    //     }

    // });

    router.post('/cotacoes/dias', async (req, res, next) => {
        const sql = `
            SELECT
                dia::TEXT AS dia,
                to_char(dia, 'ddMMyyyy')::TEXT AS dia_f
            FROM
                datas.mapa_dias
                LEFT JOIN cotacoes.lista_dias AS cotacoes ON
                    mapa_dias.dia = cotacoes.dia_pregao
            WHERE
                dia_util
                AND dia >= '2020-01-01'
                AND dia < CURRENT_DATE
                AND cotacoes.dia_pregao IS NULL
                AND NOT (
                    extract('day' from dia) in (24, 31)
                    and extract('month' from dia) = 12
                )
                AND NOT (
                    extract('day' from dia) in (25)
                    and extract('month' from dia) = 1
                )
            ORDER BY
                dia
            ;
        `;

        connection.pool().query(sql, async (error, results) => {
            if (error) {
                console.log(error);
                res.status(500).json(error);
            } else {
                try {
                    // atualiza as cotacoes para cada dia da lista
                    for (let i = 0; i < results.rows.length; i++) {
                        const r = await aux.updatePrize(results.rows[i].dia_f);
                    }
                    // atualiza a lista de dias e papeis
                    await aux.updatePapersDaysLists();

                    res.status(201).json('ok');
                } catch (err) {
                    console.log(err);
                    res.status(500).json(err);
                }

            }
            connection.pool().end();
        });
    });

    router.get('/papeis/:marketType/:searchTerm', (req, res, next) => {

        const sql = `
            SELECT
                cod_negociacao AS cod,
                nome_resumido AS nome
            FROM
                cotacoes.lista_papeis
            WHERE
                tipo_mercado = $1
                AND (
                    cod_negociacao LIKE $2
                    OR nome_resumido LIKE $3
                )
            ;
        `;

        const term = `%${req.params.searchTerm.toUpperCase()}%`

        connection.pool().query(sql, [req.params.marketType, term, term], (error, results) => {
            if (error) {
                console.log(error);
                res.status(500).json(error);
            } else {
                res.status(200).json(results.rows);
            }
            connection.pool().end();
        });
    });

    router.get('/cotacoes/papeis/:papers', (req, res, next) => {
        const arr = req.params.papers.split(',');

        let sql = `
            WITH dias AS (
                SELECT DISTINCT
                    cotacoes.dia_pregao
                FROM
                    cotacoes.cotacoes
                ORDER BY
                    1
            )
            , papeis AS (
                SELECT DISTINCT
                    lista.cod_negociacao
                FROM
                    cotacoes.lista_papeis AS lista
                WHERE
                    cod_negociacao = ANY ($1)
            )
            SELECT
                dias.dia_pregao::TEXT AS dia,
                papeis.cod_negociacao AS cod,
                COALESCE(cotacoes.preco_ultimo_negocio,0) AS valor
            FROM
                dias
                CROSS JOIN papeis
                LEFT JOIN cotacoes.cotacoes
                    USING (dia_pregao, cod_negociacao)
            ORDER BY
                dia_pregao
            ;
        `;

        connection.pool().query(sql, [arr], (error, results) => {
            if (error) {
                console.log(error);
                res.status(500).json(error);
            } else {
                res.status(200).json(results.rows);
            }
            connection.pool().end();
        })
    });

    router.get('/papeis/tiposMercado', (req, res, next) => {
        const sql = `
            SELECT
                tipo_mercado AS tipo,
                nome_tipo_mercado AS nome
            FROM
                cotacoes.lista_tipos_mercado
            ORDER BY
                tipo_mercado
        `;

        connection.pool().query(sql, (error, results) => {
            if (error) {
                console.log(error);
                res.status(500).json(error);
            } else {
                res.status(200).json(results.rows);
            }
            connection.pool().end();
        });
    });

    return router;
}