CREATE PROCEDURE DeleteCallForwarding(IN var_sub_nbr VARCHAR(15),
                                IN var_sf_type TINYINT,
                                IN var_start_time TINYINT)
            DeleteCallForwarding_Label:BEGIN

                DECLARE var_s_id INT DEFAULT -1;
                SELECT s_id INTO var_s_id FROM subscriber WHERE sub_nbr = var_sub_nbr;

                DELETE FROM call_forwarding WHERE s_id = var_s_id AND sf_type = var_sf_type AND start_time = var_start_time;
END
