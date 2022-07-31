var pool = require('./mysql-conn');
const microtime = require('microtime');

class db_state_handler {
  constructor(pool) {
    this.pool = pool;
  }

  toDateString(epoch_time) {
    function pad(number) {
      if (number < 10) {
        return '0' + number;
      }
      return number;
    }

    var date = new Date(epoch_time * 1000);
    return date.getFullYear() +
      '-' + pad(date.getMonth() + 1) +
      '-' + pad(date.getDate()) +
      ' ' + pad(date.getHours()) +
      ':' + pad(date.getMinutes()) +
      ':' + pad(date.getSeconds()) +
      '.' + epoch_time.slice(-6);
  }

  async runTransaction(callback) {
    try {
      var conn = await this.getConnection();

      conn.beginTransaction();

      // group 처리를 위한 쿼리들은 재실행시 제외 되어야 함
      // group 처리가 반복 될때의 처리 필요

      await conn.query("SET @@SESSION.state_log_group_flag=1");

      // 실제 메인 함수 실행
      await callback.apply(null, [conn].concat(Array.prototype.slice.call(arguments, 1)));

      conn.commit();

      var ret = await conn.query("SELECT @@session.state_log_group_id AS group_id");
      const group_id = ret[0][0].group_id;

      await conn.query("SET @@SESSION.state_log_group_flag=0");

      conn.release();

      return group_id;

    } catch (err) {
      conn.rollback();

      await conn.query("SET @@SESSION.state_log_group_flag=0");

      conn.release();

      throw err;
    }
  }

  async runSelect(query) {
    return (await this.pool).query(query);
  }

  async getConnection() {
    return (await this.pool).getConnection(async conn => conn);
  }
};

module.exports = new db_state_handler(pool);
