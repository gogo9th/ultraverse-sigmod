var express = require('express');
var db_handler = require('../db/db-state-handler');

var router = express.Router();

async function post(req, res) {
  var post = req.body;

  try {
    var ret = await db_handler.runSelect(`CALL GetUserID(@session_id)`);
    var user_id = ret[0][0][0].user_id;
    if (user_id.length < 1) {
      throw new Error('session is expired');
    }

    await db_handler.runTransaction(async function (conn, post) {
      await conn.query(`CALL VerifyUserID('${user_id}')`);
      await conn.query(`CALL VerifyItemCreatorID(${post.item_id}, '${user_id}')`);
      await conn.query(`CALL VerifyBillCreatorID(${post.bill_id}, '${user_id}')`);

      var ret = await conn.query(`INSERT INTO BillItems
      (bill_id, item_id, quantity) VALUES
      ('${post.bill_id}', '${post.item_id}', '${post.quantity}')`);

      if (ret[0].affectedRows != 1) {
        throw new Error('affectedRows invalid');
      }
    },
      post);

    await db_handler.runSelect(`CALL CookieUpdate(@session_id, ${post.bill_id})`);

  } catch (err) {
    req.session.err_msg = err.message;
    res.redirect(req.baseUrl + '/failed');
    return;
  }

  req.session.err_msg = 'SUCCESS';
  res.redirect(req.baseUrl + '/home');
};

router.get('/', function (req, res, next) {
  if (req.session.user_id === undefined) {
    req.session.err_msg = 'login first';
    res.redirect(req.baseUrl + '/home');
  }
  else {
    var bill_id_list = [];
    var item_id_list = [];

    var promise_1 = db_handler.runSelect(`SELECT bill_id FROM Bills WHERE bill_id != 1`).then(
      rows => {
        for (var i = 0, len = rows[0].length; i < len; i++) {
          bill_id_list.splice(-1, 0, rows[0][i].bill_id);
        }
        bill_id_list.sort();
      }
    ).catch(err => { });

    var promise_2 = db_handler.runSelect(`SELECT item_id FROM Items`).then(
      rows => {
        for (var i = 0, len = rows[0].length; i < len; i++) {
          item_id_list.splice(-1, 0, rows[0][i].item_id);
        }
        item_id_list.sort();
      }
    ).catch(err => { });

    Promise.all([promise_1, promise_2]).then(v => {
      res.render('add-item-bill', {
        title: 'Add Item to Bill',
        session_id: req.session.session_id,
        bill_id_list: bill_id_list,
        item_id_list: item_id_list,
      });
    });
  }
});

router.post('/', post);

router.get('/home', function (req, res, next) {
  res.send(`<script type="text/javascript">
              alert("${req.session.err_msg}");
              window.setTimeout( function() {
                window.location = "/";
              }, 100 );
            </script>`);
});

router.get('/failed', function (req, res, next) {
  res.send(`<script type="text/javascript">
              alert("${req.session.err_msg}");
              window.setTimeout( function() {
                window.location = "..${req.baseUrl}";
              }, 100 );
            </script>`);
});

module.exports = {
  router: router,
  post: post
};
