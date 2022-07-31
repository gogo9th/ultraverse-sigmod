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
      await conn.query(`CALL VerifyUserID('${post.payer_id}')`);

      var is_recurring = 1;
      var recurring_is_weekly = 1;

      if (post.is_recurring === undefined) {
        is_recurring = 0;
      }
      if (post.recurring_is_weekly === undefined) {
        recurring_is_weekly = 0;
      }

      var ret = await conn.query(`SELECT COUNT(*) + 1 AS id FROM Bills`);
      id = ret[0][0].id;

      ret = await conn.query(`INSERT INTO Bills
        (bill_id, bill_name, description, due_date,
        creator_id, payer_id, is_paid,
        is_recurring, recurring_start, recurring_end,
        recurring_is_weekly, recurring_period) VALUES
        (${id}, '${post.bill_name}', '${post.description}', '${post.due_date}',
         '${user_id}', '${post.payer_id}', 0,
         '${is_recurring}', '${post.recurring_start}', '${post.recurring_end}',
         '${recurring_is_weekly}', '${post.recurring_period}')`);

      if (ret[0].affectedRows != 1) {
        throw new Error('affectedRows invalid');
      }
    },
      post);

    await db_handler.runSelect(`CALL CookieUpdate(@session_id, ${id})`);

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
    res.render('create-bill', {
      title: 'Create bill',
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
