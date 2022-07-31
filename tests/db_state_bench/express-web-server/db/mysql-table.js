Users = `CREATE TABLE IF NOT EXISTS Users
        ( user_id VARCHAR(32) PRIMARY KEY,
          first_name VARCHAR(16) NOT NULL,
          last_name VARCHAR(16) NOT NULL,
          email VARCHAR(64) NOT NULL,
          password VARCHAR(32) NOT NULL
        );`

Bills = `CREATE TABLE IF NOT EXISTS Bills
        ( bill_id INT AUTO_INCREMENT PRIMARY KEY,
          bill_name TEXT,
          description TEXT,
          created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          due_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          creator_id VARCHAR(32),
          payer_id VARCHAR(32),
          is_paid BOOL DEFAULT FALSE,
          is_recurring BOOL,
          recurring_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          recurring_end TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          recurring_is_weekly BOOL, 
          recurring_period INT, 
          FOREIGN KEY (creator_id) REFERENCES Users (user_id) ON DELETE SET NULL,
          FOREIGN KEY (payer_id) REFERENCES Users (user_id) ON DELETE SET NULL
        );`

Items = `CREATE TABLE IF NOT EXISTS Items
        ( item_id INT AUTO_INCREMENT PRIMARY KEY,
          item_name TEXT,
          description TEXT,
          is_onetime BOOL,
          price FLOAT NOT NULL,
          discount FLOAT NOT NULL,
          creator_id VARCHAR(32),
          FOREIGN KEY (creator_id) REFERENCES Users (user_id) ON DELETE SET NULL
        );`

BillItems = `CREATE TABLE IF NOT EXISTS BillItems
        ( bill_id INT,
          item_id INT,
          quantity INT,
          PRIMARY KEY (bill_id, item_id),
          FOREIGN KEY (bill_id) REFERENCES Bills (bill_id) ON DELETE CASCADE,
          FOREIGN KEY (item_id) REFERENCES Items (item_id) ON DELETE CASCADE
        );`

Payments = `CREATE TABLE IF NOT EXISTS Payments
        ( payment_id INT AUTO_INCREMENT PRIMARY KEY,
          payment_name TEXT,
          creator_id VARCHAR(32),
          recipient_id  VARCHAR(32),
          invoice_id  VARCHAR(32),
          bill_id  INT,
          amount FLOAT NOT NULL,
          payment_type INT,
          description TEXT,
          created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          is_recurring BOOL,
          recurring_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          recurring_end TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          recurring_is_weekly BOOL,
          recurring_period INT,
          FOREIGN KEY (recipient_id) REFERENCES Users (user_id) ON DELETE SET NULL,
          FOREIGN KEY (invoice_id) REFERENCES Users (user_id) ON DELETE SET NULL
        );`

Statistics = `CREATE TABLE IF NOT EXISTS Statistics
        ( user_id VARCHAR(32),
          total_gain FLOAT DEFAULT 0.0,
          total_paid FLOAT DEFAULT 0.0,
          total_unpaid FLOAT DEFAULT 0.0,
          total_balance FLOAT DEFAULT 0.0,
          FOREIGN KEY (user_id) REFERENCES Users (user_id) ON DELETE CASCADE
        );`

Sessions = `CREATE TABLE IF NOT EXISTS Sessions
        ( session_id INT AUTO_INCREMENT PRIMARY KEY,
          user_id VARCHAR(32),
          last_active_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          cookie VARCHAR(128)
        );`

Cookies = `CREATE TABLE IF NOT EXISTS Cookies
        ( session_id INT,
          active_bill_id INT
        );`

module.exports = [Users, Bills, Items, BillItems, Payments, Statistics, Sessions, Cookies];
