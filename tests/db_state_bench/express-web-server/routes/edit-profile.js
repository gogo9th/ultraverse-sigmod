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
      var ret = await conn.query(
        `UPDATE Users SET
          first_name = '${post.first_name}',
          last_name = '${post.last_name}',
          email = '${post.email}'
         WHERE user_id = '${user_id}'`
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
    db_handler.runSelect(`SELECT first_name, last_name, email FROM Users WHERE user_id = '${req.session.user_id}'`).then(
      rows => {
        res.render('edit-profile', {
          title: 'Edit profile',
          user_id: req.session.user_id,
          first_name: rows[0][0].first_name,
          last_name: rows[0][0].last_name,
          email: rows[0][0].email,
        });
      }
    ).catch(
      err => {
        res.render('edit-profile', {
          title: 'Edit profile',
          user_id: req.session.user_id,
          first_name: '',
          last_name: '',
          email: '',
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
