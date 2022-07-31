var express = require('express');
var db_handler = require('../db/db-state-handler');

var router = express.Router();

router.get('/', function (req, res, next) {
  if (req.session.user_id === undefined) {
    req.session.err_msg = 'login first';
    res.redirect(req.baseUrl + '/home');
  }
  else {
    db_handler.runSelect(`SELECT * FROM Statistics WHERE user_id = '${req.session.user_id}'`).then(
      rows => {
        res.render('view-statistics', {
          title: 'View Statistics',
          user_id: req.session.user_id,
          total_gain: rows[0][0].total_gain,
          total_paid: rows[0][0].total_paid,
          total_unpaid: rows[0][0].total_unpaid,
          total_balance: rows[0][0].total_balance,
        });
      }
    ).catch(
      err => {
        res.render('view-statistics', {
          title: 'View Statistics',
          user_id: req.session.user_id,
          total_gain: '',
          total_paid: '',
          total_unpaid: '',
          total_balance: '',
        });
      }
    );
  }
});

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
};
