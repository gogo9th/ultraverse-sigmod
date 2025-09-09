CREATE PROCEDURE UpdateSubscriberData(IN var_s_id INT,
                                IN var_bit_1 TINYINT,
                                IN var_data_a SMALLINT,
                                IN var_sf_type TINYINT)
            UpdateSubcriberData_Label:BEGIN

                DECLARE var_s_id INT DEFAULT -1;

                UPDATE subscriber SET bit_1 = var_bit_1 WHERE s_id = var_s_id;
                UPDATE special_facility SET data_a = var_data_a WHERE s_id = var_s_id AND sf_type = var_sf_type;
END
