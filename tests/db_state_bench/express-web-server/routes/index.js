var express = require('express');
var router = express.Router();

router.get('/', function (req, res, next) {
  res.render('index', {
    title: 'Express',
    pages: ['sign-up', 'login', 'logout',
      'reset-password', 'change-password', 'edit-profile',
      'create-bill', 'delete-bill',
      'create-item', 'delete-item', 'edit-item',
      'add-item-bill', 'delete-item-bill', 'pay-bill',
      'make-payment', 'purge-account',
      'view-bill-detail', 'view-item-detail', 'view-payment-detail',
      'view-statistics']
  });
});

module.exports = {
  router: router,
};
