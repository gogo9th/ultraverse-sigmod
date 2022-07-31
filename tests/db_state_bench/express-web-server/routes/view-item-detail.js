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
    const item_id = id_vec[0];
    const item_name = id_vec[1];

    db_handler.runSelect(`SELECT * FROM Items WHERE item_id = ${item_id}`).then(
      rows => {
        res.render('view-item-detail', {
          title: 'View Item Detail',
          base: false,
          item_id: item_id,
          item_name: rows[0][0].item_name,
          description: rows[0][0].description,
          is_onetime: rows[0][0].is_onetime,
          price: rows[0][0].price,
          discount: rows[0][0].discount,
          creator_id: rows[0][0].creator_id,
        });
      }
    ).catch(
      err => {
        res.render('view-item-detail', {
          title: 'View Item Detail',
          base: false,
          item_id: item_id,
          item_name: '',
          description: '',
          is_onetime: '',
          price: '',
          discount: '',
          creator_id: '',
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
    db_handler.runSelect(`SELECT item_id, item_name FROM Items WHERE item_id != 1`).then(
      rows => {
        var id_list = [];
        for (var i = 0, len = rows[0].length; i < len; i++) {
          var name = rows[0][i].item_name;
          if (rows[0][0].item_name === null) {
            name = 'null';
          }
          var id = rows[0][i].item_id + ' - ' + name;

          id_list.splice(-1, 0, id);
        }
        id_list.sort();

        res.render('view-item-detail', {
          title: 'View Item Detail',
          base: true,
          id_list: id_list,
        });
      }
    ).catch(
      err => {
        res.render('view-item-detail', {
          title: 'View Item Detail',
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
