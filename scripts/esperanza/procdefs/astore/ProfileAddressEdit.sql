CREATE PROCEDURE ProfileAddressEdit(IN req_params_id INT,
                                    IN req_body_fullname VARCHAR(50),
                                    IN req_body_streetAddress VARCHAR(255),
                                    IN req_body_postcode VARCHAR(5),
                                    IN req_body_city VARCHAR(28),
                                    IN req_body_country VARCHAR(28),
                                    IN req_body_phone VARCHAR(12),
                                    IN req_body_password VARCHAR(24),
                                    IN req_user_password VARCHAR(24),
                                    IN bcrypt_nodejs_compareSync_1_output BOOLEAN)
    ProfileAddressEdit_Label:BEGIN

                DECLARE var_s_id INT DEFAULT -1;


IF (NOT bcrypt_nodejs_compareSync_1_output) THEN
                    LEAVE ProfileAddressEdit_Label;
END IF;
SELECT * FROM Addresses WHERE AddressID = req_params_id;
UPDATE Addresses SET Fullname = req_body_fullname,
                     StreetAddress = req_body_streetAddress,
                     PostCode = req_body_postcode,
                     City = req_body_city,
                     Country = req_body_country,
                     Phone = req_body_phone
WHERE AddressID = req_params_id;
END