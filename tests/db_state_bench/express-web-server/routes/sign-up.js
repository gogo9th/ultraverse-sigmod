var express = require('express');
var db_handler = require('../db/db-state-handler');

var router = express.Router();

async function post(req, res) {
  var post = req.body;

  try {
    await db_handler.runTransaction(async function (conn, post) {
      var ret = await conn.query(`INSERT INTO Users (user_id, first_name, last_name, email, password) VALUES
      ('${post.user_id}', '${post.first_name}', '${post.last_name}', '${post.email}', '${post.password}')`);

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
  res.render('sign-up', {
    title: 'Sign Up'
  });
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
