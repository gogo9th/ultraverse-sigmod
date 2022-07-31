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

      var is_onetime = 1;

      if (post.is_onetime === undefined) {
        is_onetime = 0;
      }

      var ret = await conn.query(`SELECT COUNT(*) + 1 AS id FROM Items`);
      id = ret[0][0].id;

      ret = await conn.query(`INSERT INTO Items 
      (item_id, item_name, description, is_onetime,
         price, discount, creator_id) VALUES
      (${id}, '${post.item_name}', '${post.description}', ${is_onetime},
       '${post.price}', '${post.discount}', '${user_id}')`);

      if (ret[0].affectedRows != 1) {
        throw new Error('affectedRows invalid');
      }
    },
      post).then(gid => {
        req.session.gid = gid;
      });
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
    res.render('create-item', {
      title: 'Create item',
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
