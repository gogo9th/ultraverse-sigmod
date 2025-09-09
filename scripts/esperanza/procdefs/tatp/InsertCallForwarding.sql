CREATE PROCEDURE InsertCallForwarding(IN var_sub_nbr VARCHAR(15),
                                      IN var_sf_type TINYINT,
                                      IN var_start_time TINYINT,
                                      IN var_end_time TINYINT,
                                      IN var_numberx VARCHAR(15))
    InsertCallForwarding_Label:BEGIN
                DECLARE var_s_id INT DEFAULT -1;

    SELECT s_id INTO var_s_id FROM subscriber WHERE sub_nbr = var_sub_nbr;
    SELECT sf_type FROM special_facility WHERE s_id = var_s_id;
    INSERT INTO call_forwarding (s_id, sf_type, start_time, end_time, numberx) VALUES (var_s_id, var_sf_type, var_start_time, var_end_time, var_numberx);
END