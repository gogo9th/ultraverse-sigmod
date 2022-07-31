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

      const id_vec = post.bill_item_id.split(' - ');
      const bill_id = id_vec[0];
      const item_id = id_vec[1];

      await conn.query(`CALL VerifyItemCreatorID(${item_id}, '${user_id}')`);
      await conn.query(`CALL VerifyBillCreatorID(${bill_id}, '${user_id}')`);
      await conn.query(`CALL VerifyBillItem(${bill_id}, '${item_id}')`);

      var ret = await conn.query(`DELETE FROM BillItems WHERE bill_id = ${bill_id} AND item_id = ${item_id}`);

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
    db_handler.runSelect(`SELECT CONCAT(bill_id, ' - ', item_id) AS id FROM BillItems`).then(
      rows => {
        var id_list = [];
        for (var i = 0, len = rows[0].length; i < len; i++) {
          id_list.splice(-1, 0, rows[0][i].id);
        }
        id_list.sort();

        res.render('delete-item-bill', {
          title: 'Delete Item in Bill',
          base: true,
          session_id: req.session.session_id,
          id_list: id_list,
        });
      }
    ).catch(
      err => {
        res.render('delete-item-bill', {
          title: 'Delete Item in Bill',
          base: true,
          session_id: req.session.session_id,
          id_list: [],
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
