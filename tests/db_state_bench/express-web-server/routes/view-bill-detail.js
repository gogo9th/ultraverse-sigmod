var express = require('express');
var db_handler = require('../db/db-state-handler');

var router = express.Router();

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

router.get('/:id', function (req, res, next) {
  if (req.session.user_id === undefined) {
    req.session.err_msg = 'login first';
    res.redirect(req.baseUrl + '/home');
  }
  else {
    const id_vec = req.params.id.split(' - ');
    const bill_id = id_vec[0];
    const bill_name = id_vec[1];

    db_handler.runSelect(`SELECT * FROM Bills WHERE bill_id = ${bill_id}`).then(
      rows => {
        res.render('view-bill-detail', {
          title: 'View Bill Detail',
          base: false,
          bill_id: bill_id,
          bill_name: rows[0][0].bill_name,
          description: rows[0][0].description,
          created_time: rows[0][0].created_time,
          due_date: rows[0][0].due_date,
          creator_id: rows[0][0].creator_id,
          payer_id: rows[0][0].payer_id,
          is_paid: rows[0][0].is_paid,
          is_recurring: rows[0][0].is_recurring,
          recurring_start: rows[0][0].recurring_start,
          recurring_end: rows[0][0].recurring_end,
          recurring_is_weekly: rows[0][0].recurring_is_weekly,
          recurring_period: rows[0][0].recurring_period,
        });
      }
    ).catch(
      err => {
        res.render('view-bill-detail', {
          title: 'View Bill Detail',
          base: false,
          item_id: bill_id,
          bill_name: '',
          description: '',
          created_time: '',
          due_date: '',
          creator_id: '',
          payer_id: '',
          is_paid: '',
          is_recurring: '',
          recurring_start: '',
          recurring_end: '',
          recurring_is_weekly: '',
          recurring_period: '',
        });
      }
    );
  }
});

router.get('/', function (req, res, next) {
  if (req.session.user_id === undefined) {
    req.session.err_msg = 'login first';
    res.redirect(req.baseUrl + '/home');
  }
  else {
    db_handler.runSelect(`SELECT bill_id, bill_name FROM Bills WHERE bill_id != 1`).then(
      rows => {
        var id_list = [];
        for (var i = 0, len = rows[0].length; i < len; i++) {
          var name = rows[0][i].bill_name;
          if (rows[0][0].bill_name === null) {
            name = 'null';
          }
          var id = rows[0][i].bill_id + ' - ' + name;

          id_list.splice(-1, 0, id);
        }
        id_list.sort();

        res.render('view-bill-detail', {
          title: 'View Bill Detail',
          base: true,
          id_list: id_list,
        });
      }
    ).catch(
      err => {
        res.render('view-bill-detail', {
          title: 'View Bill Detail',
          base: true,
          id_list: [],
        });
      }
    );
  }
});

module.exports = {
  router: router,
};
