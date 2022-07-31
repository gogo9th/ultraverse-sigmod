const db_handler = require('../db/db-state-handler');
const httpMocks = require('node-mocks-http');
const crypto = require("crypto");
const fs = require('fs');
const { exit, argv } = require('process');
const User = require('./user');


function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const out_filepath = argv[2];
const num_init_users = 1000;
const num_bills_per_user = 10; //getRandomInt(8, 12);
const num_items_per_user = 10; //getRandomInt(8, 12);
const num_add_item_bill_per_user = 5; //getRandomInt(5, 8);
const idx_user = 1;

if (num_add_item_bill_per_user > Math.min(num_bills_per_user, num_items_per_user)) {
  console.log(`invalid parameter num_add_item_bill_per_user(${num_add_item_bill_per_user}) > min(num_bills_per_user(${num_bills_per_user}), num_items_per_user(${num_items_per_user}))`);
  exit(1);
}


function make_req_res(user_id, session_id) {
  let req = httpMocks.createRequest({
    url: '/',
    method: 'POST',
    session: {
      'user_id': user_id,
      'session_id': session_id,
    }
  });

  let res = httpMocks.createResponse({
    eventEmitter: require('events').EventEmitter
  });

  return [req, res];
};


(async () => {
  console.log('start');

  await db_handler.runSelect('SELECT \'START\'');

  user_list = new Array();
  item_id_list = new Array();
  bill_id_list = new Array();

  last_item_id = 1; //id start from 1
  last_bill_id = 2; //id start from 2 : bill_id 1 is reserved for none

  console.log(`create user ${num_init_users}`);
  {
    let user_map = new Map();
    for (let i = 0; i < num_init_users;) {
      let u = new User();

      if (user_map.has(u.user_id)) {
        continue;
      }

      user_list.push(u);
      user_map.set(u.user_id, u);
      ++i;
    }

    for (let i = 0; i < user_list.length; ++i) {
      // sign-up
      [req, res] = make_req_res(user_list[i].user_id, '');

      req._addBody('user_id', user_list[i].user_id);
      req._addBody('first_name', crypto.randomBytes(2).toString('hex'));
      req._addBody('last_name', crypto.randomBytes(2).toString('hex'));
      req._addBody('email', crypto.randomBytes(2).toString('hex') + '@' + crypto.randomBytes(2).toString('hex'));
      req._addBody('password', user_list[i].password);

      await require('../routes/sign-up').post(req, res);
    }
  }

  for (var i = 0; i < num_init_users; ++i) {
    console.log(`transaction for user ${i + 1} / ${num_init_users}`);

    var user_id = user_list[i].user_id;
    var password = user_list[i].password;
    var session_id = '';

    item_id_list = [];
    bill_id_list = [];

    // login
    [req, res] = make_req_res(user_id, '');

    req._addBody('user_id', user_id);
    req._addBody('password', password);

    await require('../routes/login').post(req, res);

    if (res._getRedirectUrl().indexOf('home') < 0) {
      console.log(`login error: ${req.session.err_msg}`);
      exit(1);
    }
    session_id = req.session.session_id;

    // create-bill
    for (var j = 0; j < num_bills_per_user; ++j) {
      [req, res] = make_req_res(user_id, session_id);

      var today = new Date();

      req._addBody('bill_name', 'bill_name_' + last_bill_id.toString());
      req._addBody('description', 'bill_description_' + last_bill_id.toString());
      req._addBody('due_date', today.toISOString().substring(0, 10));
      req._addBody('payer_id', user_id);
      if (getRandomInt(0, 1)) {
        req._addBody('is_recurring', 1);
      }
      req._addBody('recurring_start', today.toISOString().substring(0, 10));
      req._addBody('recurring_end', today.toISOString().substring(0, 10));
      if (getRandomInt(0, 1)) {
        req._addBody('recurring_is_weekly', 1);
      }
      req._addBody('recurring_period', getRandomInt(1, 10));

      await require('../routes/create-bill').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`create-bill error: ${req.session.err_msg}`);
        exit(1);
      }

      bill_id_list.push(last_bill_id);
      ++last_bill_id;
    }

    // delete-bill
    if (bill_id_list.length > 0 && getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      let bill_id = bill_id_list.pop();
      --last_bill_id;

      req._addBody('bill_id', bill_id);

      await require('../routes/delete-bill').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`delete-bill error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // create-item
    for (var j = 0; j < num_items_per_user; ++j) {
      [req, res] = make_req_res(user_id, session_id);

      req._addBody('item_name', 'item_name_' + last_item_id.toString());
      req._addBody('description', 'item_description_' + last_item_id.toString());
      if (getRandomInt(0, 1)) {
        req._addBody('is_onetime', 1);
      }
      req._addBody('price', getRandomInt(100, 1000));
      req._addBody('discount', getRandomInt(1, 10));

      await require('../routes/create-item').post(req, res).then(() => {
        curr_i = i;
        curr_j = j;
        g_id = req.session.gid;
        i_id = last_item_id;

        if (curr_i == idx_user && curr_j == 1) {
          return [g_id, i_id];
        }
        else {
          return [];
        }
      }).then(async (arr) => {
          if (arr.length > 0) {
            g_id = arr[0];
            i_id = arr[1];

            ret = await db_handler.runSelect(`SELECT UNIX_TIMESTAMP(row_start) - 0.000001 AS undo_time, UNIX_TIMESTAMP(row_start) + 0.000001 AS redo_time FROM Items WHERE item_id=${last_item_id}`);
            undo_time = ret[0][0].undo_time;
            redo_time = ret[0][0].redo_time;

            console.log(`target group id: ${g_id}`);
            console.log(`target item id: ${i_id}`);
            console.log(`target undo time: ${undo_time}`);
            console.log(`target redo time: ${redo_time}`);
            buffer = g_id + ' ' + i_id + ' ' + undo_time + ' ' + redo_time;
            fs.writeFile(out_filepath, buffer, (err) => {
              if (err) throw err;
              console.log(`${out_filepath} has been saved!`);
            });
          }
        });

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`create-item error: ${req.session.err_msg}`);
        exit(1);
      }

      item_id_list.push(last_item_id);
      ++last_item_id;
    }

    // edit-item
    if (item_id_list.length > 0 && getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      let item_id = item_id_list.pop();

      req._addBody('item_id', item_id);
      req._addBody('item_name', 'item_edit_name_' + i.toString());
      req._addBody('description', 'item_edit_description_' + i.toString());
      req._addBody('price', getRandomInt(100, 1000));
      req._addBody('discount', getRandomInt(1, 10));

      await require('../routes/edit-item').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`edit-item error: ${req.session.err_msg}`);
        exit(1);
      }

      item_id_list.push(item_id);
    }

    // delete-item
    if (item_id_list.length > 0 && getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      let item_id = item_id_list.pop();
      --last_item_id;

      req._addBody('item_id', item_id);

      await require('../routes/delete-item').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`delete-item error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // add-item-bill
    for (var j = 0; j < num_add_item_bill_per_user; ++j) {
      [req, res] = make_req_res(user_id, session_id);

      req._addBody('bill_id', bill_id_list.pop());
      req._addBody('item_id', item_id_list.pop());
      req._addBody('quantity', getRandomInt(1, 10));

      await require('../routes/add-item-bill').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`add-item-bill error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // change-password
    if (getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      req._addBody('password', crypto.randomBytes(6).toString('hex'));

      await require('../routes/change-password').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`change-password error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // reset-password
    if (getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      await require('../routes/reset-password').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`reset-password error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // edit-profile
    if (getRandomInt(0, 1)) {
      [req, res] = make_req_res(user_id, session_id);

      req._addBody('first_name', crypto.randomBytes(2).toString('hex'));
      req._addBody('last_name', crypto.randomBytes(2).toString('hex'));
      req._addBody('email', crypto.randomBytes(2).toString('hex') + '@' + crypto.randomBytes(2).toString('hex'));
      await require('../routes/edit-profile').post(req, res);

      if (res._getRedirectUrl().indexOf('home') < 0) {
        console.log(`edit-profile error: ${req.session.err_msg}`);
        exit(1);
      }
    }

    // logout
    await require('../routes/logout').post(req, res);
  }

  console.log('end');

  exit(0);
})();
