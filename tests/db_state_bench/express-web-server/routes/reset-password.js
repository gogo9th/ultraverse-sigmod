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
      var ret = await conn.query(`UPDATE Users SET password = SUBSTR(UUID(), 1, 31) WHERE user_id = '${user_id}'`);

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
    res.render('reset-password', {
      title: 'Reset password',
      user_id: req.session.user_id,
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
