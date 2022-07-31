const createError = require('http-errors');
const express = require('express');
const path = require('path');
const cookieParser = require('cookie-parser');
const logger = require('morgan');
const session = require('express-session');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: 'keyboard cat',
  resave: false,
  saveUninitialized: true
}));

app.use('/', require('./routes/index').router);
app.use('/sign-up', require('./routes/sign-up').router);
app.use('/login', require('./routes/login').router);
app.use('/logout', require('./routes/logout').router);
app.use('/reset-password', require('./routes/reset-password').router);
app.use('/change-password', require('./routes/change-password').router);
app.use('/edit-profile', require('./routes/edit-profile').router);
app.use('/create-bill', require('./routes/create-bill').router);
app.use('/delete-bill', require('./routes/delete-bill').router);
app.use('/create-item', require('./routes/create-item').router);
app.use('/delete-item', require('./routes/delete-item').router);
app.use('/edit-item', require('./routes/edit-item').router);
app.use('/add-item-bill', require('./routes/add-item-bill').router);
app.use('/delete-item-bill', require('./routes/delete-item-bill').router);
app.use('/pay-bill', require('./routes/pay-bill').router);
app.use('/make-payment', require('./routes/make-payment').router);
app.use('/purge-account', require('./routes/purge-account').router);
app.use('/view-bill-detail', require('./routes/view-bill-detail').router);
app.use('/view-item-detail', require('./routes/view-item-detail').router);
app.use('/view-payment-detail', require('./routes/view-payment-detail').router);
app.use('/view-statistics', require('./routes/view-statistics').router);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
