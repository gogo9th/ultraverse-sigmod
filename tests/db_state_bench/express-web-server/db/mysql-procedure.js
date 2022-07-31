GetUserID = `
CREATE OR REPLACE PROCEDURE GetUserID (IN in_session_id INT)
BEGIN
   DECLARE var_user_id VARCHAR(32);
   DECLARE var_is_expired BOOLEAN;

   SET var_user_id = (SELECT user_id FROM Sessions WHERE session_id = in_session_id LIMIT 1);
   IF var_user_id IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'user_id is null';
   END IF;

   CALL UpdateSessionTime(in_session_id, var_is_expired);
   IF var_is_expired = TRUE THEN
      SELECT '' as user_id;
   ELSE
      SELECT var_user_id as user_id;
   END IF;
END
`

UpdateSessionTime = `
CREATE OR REPLACE PROCEDURE UpdateSessionTime (IN in_session_id INT, OUT out_is_expired BOOLEAN)
BEGIN
   DECLARE var_last_active_time TIMESTAMP;

   SET var_last_active_time = (SELECT last_active_time FROM Sessions WHERE session_id = in_session_id LIMIT 1);

   IF TIMESTAMPDIFF(MINUTE, var_last_active_time, CURRENT_TIMESTAMP()) > 10 THEN
      DELETE FROM Sessions WHERE session_id = in_session_id;
      SET out_is_expired = TRUE;
   ELSE
      UPDATE Sessions SET last_active_time = CURRENT_TIME() WHERE session_id = in_session_id;
      SET out_is_expired = FALSE;
   END IF;
END
`

VerifyUserID = `
CREATE OR REPLACE PROCEDURE VerifyUserID (IN in_user_id VARCHAR(32))
BEGIN
   IF (SELECT user_id FROM Users WHERE user_id = in_user_id LIMIT 1) IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'user_id is null';
   END IF;
END
`

VerifyBillCreatorID = `
CREATE OR REPLACE PROCEDURE VerifyBillCreatorID (IN in_bill_id INT, IN in_user_id VARCHAR(32))
BEGIN
   IF (SELECT creator_id FROM Bills WHERE bill_id = in_bill_id AND creator_id = in_user_id LIMIT 1) IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'creator_id is null';
   END IF;
END
`

VerifyItemCreatorID = `
CREATE OR REPLACE PROCEDURE VerifyItemCreatorID (IN in_item_id INT, IN in_user_id VARCHAR(32))
BEGIN
   IF (SELECT creator_id FROM Items WHERE item_id = in_item_id AND creator_id = in_user_id LIMIT 1) IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'creator_id is null';
   END IF;
END
`

VerifyBillItem = `
CREATE OR REPLACE PROCEDURE VerifyBillItem (IN in_bill_id INT, in_item_id INT)
BEGIN
   DECLARE num INT;

   SET num = (SELECT COUNT(*) FROM BillItems WHERE bill_id = in_bill_id AND item_id = in_item_id LIMIT 1);
   IF num = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'count is zero';
   END IF;
END
`

VerifyBillPayerID = `
CREATE OR REPLACE PROCEDURE VerifyBillPayerID (IN in_bill_id INT, IN in_user_id VARCHAR(32))
BEGIN
   IF (SELECT payer_id FROM Bills WHERE bill_id = in_bill_id AND payer_id = in_user_id LIMIT 1) IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'payer_id is null';
   END IF;
END
`

CookieUpdate = `
CREATE OR REPLACE PROCEDURE CookieUpdate (IN in_session_id INT, IN in_bill_id INT)
BEGIN
   IF (SELECT session_id FROM Cookies WHERE session_id = in_session_id LIMIT 1) IS NULL THEN
      IF in_session_id IS NOT NULL AND (SELECT session_id FROM Sessions WHERE session_id = in_session_id LIMIT 1) IS NOT NULL THEN
         DELETE FROM Cookies WHERE session_id = in_session_id;
         INSERT INTO Cookies (session_id, active_bill_id) VALUES (in_session_id, in_bill_id);
      END IF;
   ELSE
      UPDATE Cookies SET active_bill_id = in_bill_id WHERE session_id = in_session_id;
   END IF;
END
`

Login = `
CREATE OR REPLACE PROCEDURE Login (IN in_user_id VARCHAR(32), IN in_password VARCHAR(32), OUT out_session_id INT)
BEGIN
	SET out_session_id = NULL;

   IF (SELECT user_id FROM Users WHERE user_id = in_user_id AND password = in_password LIMIT 1) IS NULL THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'password wrong';
   END IF;

   DELETE FROM Sessions WHERE user_id = in_user_id;

   myloop: LOOP
      SET out_session_id = (CAST(RAND() * CONV('7FFFFFFF', 16, 10) AS SIGNED) + 1);
      IF (SELECT session_id FROM Sessions WHERE session_id = out_session_id LIMIT 1) IS NULL THEN
         INSERT INTO Sessions (session_id, user_id) VALUES (out_session_id, in_user_id);
         LEAVE myloop;
      END IF;
   END LOOP;

   SELECT out_session_id as session_id;
END
`

Logout = `
CREATE OR REPLACE PROCEDURE Logout (IN in_user_id VARCHAR(32), IN in_session_id INT)
BEGIN
   DELETE FROM Sessions WHERE user_id = in_user_id;
   DELETE FROM Cookies WHERE session_id = in_session_id;
END
`

MakePayment = `
CREATE OR REPLACE PROCEDURE MakePayment (IN in_user_id VARCHAR(32), IN in_amount FLOAT)
BEGIN
   DECLARE var_total_gain FLOAT;
   DECLARE var_total_balance FLOAT;

   SELECT total_gain, total_balance INTO var_total_gain, var_total_balance FROM Statistics WHERE user_id = in_user_id;
   IF var_total_gain IS NULL THEN
      INSERT INTO Statistics (user_id, total_gain, total_paid, total_unpaid, total_balance) VALUES (in_user_id, in_amount, 0, 0, in_amount);
   ELSE
      UPDATE Statistics SET total_gain = var_total_gain + in_amount, total_balance = var_total_balance + in_amount WHERE user_id = in_user_id;
   END IF;
END
`

PurgeAccount = `
CREATE OR REPLACE PROCEDURE PurgeAccount (IN in_user_id VARCHAR(32), IN in_session_id INT)
BEGIN
   UPDATE Bills SET creator_id = NULL WHERE creator_id = in_user_id;
   UPDATE Bills SET payer_id = NULL WHERE payer_id = in_user_id;
   UPDATE Payments SET recipient_id = NULL WHERE recipient_id = in_user_id;
   UPDATE Payments SET invoice_id = NULL WHERE invoice_id = in_user_id;

   DELETE FROM Users WHERE user_id = in_user_id;
   DELETE FROM Cookies WHERE session_id = in_session_id;
END
`


module.exports = [GetUserID, UpdateSessionTime, VerifyUserID,
   VerifyBillCreatorID, VerifyItemCreatorID, VerifyBillItem,
   VerifyBillPayerID, CookieUpdate, Login, Logout,
   MakePayment, PurgeAccount];
