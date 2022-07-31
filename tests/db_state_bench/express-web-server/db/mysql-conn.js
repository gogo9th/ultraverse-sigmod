const tables = require('./mysql-table');
const procedures = require('./mysql-procedure');
const mysql = require('mysql2/promise');

const db_host = 'localhost';
const db_port = 3306;
const db_user = 'root';
const db_pass = '123456';
const db_name = 'express-db';

var pool = (async () => {
  var connection = await mysql.createConnection({
    host: db_host,
    port: db_port,
    user: db_user,
    password: db_pass
  });

  await connection.query(`DROP DATABASE IF EXISTS \`${db_name}\``);
  await connection.query(`CREATE DATABASE \`${db_name}\``);
  await connection.end();

  var pool = mysql.createPool({
    host: db_host,
    port: db_port,
    user: db_user,
    password: db_pass,
    database: db_name,
    connectionLimit: 10
  });
  console.log('mysql connection done');

  for (var i = 0, len = tables.length; i < len; i++) {
    await pool.query(tables[i]);
  }
  console.log('mysql create table done');

  for (var i = 0, len = procedures.length; i < len; i++) {
    await pool.query(procedures[i]);
  }
  console.log('mysql create procedure done');

  await pool.query('DELETE FROM Bills WHERE bill_id = 1');
  await pool.query('DELETE FROM Users WHERE user_id = "none"');

  await pool.query('INSERT INTO Users (user_id, first_name, last_name, email, password) VALUES ("none", "", "", "", "")');
  await pool.query('INSERT INTO Bills (bill_id, creator_id, payer_id) VALUES (1, "none", "none")');

  return pool;
})();


module.exports = pool;
