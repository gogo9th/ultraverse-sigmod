const crypto = require("crypto");

class User {
  constructor() {
    this.user_id = crypto.randomBytes(4).toString('hex');
    this.password = crypto.randomBytes(4).toString('hex');
  }
}

module.exports = User;