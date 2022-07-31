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
      await conn.query(`CALL VerifyBillPayerID(${post.bill_id}, '${user_id}')`);

      var ret = await conn.query(
        `UPDATE Bills SET
          is_paid = 1 WHERE
          bill_id  = '${post.bill_id}' AND
          payer_id  = '${user_id}'`
      );

      if (ret[0].affectedRows != 1) {
        throw new Error('affectedRows invalid');
      }
    },
      post);
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
    db_handler.runSelect(`SELECT bill_id FROM Bills WHERE bill_id != 1`).then(
      rows => {
        var id_list = [];
        for (var i = 0, len = rows[0].length; i < len; i++) {
          id_list.splice(-1, 0, rows[0][i].bill_id);
        }
        id_list.sort();

        res.render('pay-bill', {
          title: 'Pay Bill',
          base: true,
          session_id: req.session.session_id,
          bill_id_list: id_list,
        });
      }
    ).catch(
      err => {
        res.render('pay-bill', {
          title: 'Pay Bill',
          base: true,
          session_id: req.session.session_id,
          bill_id_list: [],
        });
      }
    );
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
