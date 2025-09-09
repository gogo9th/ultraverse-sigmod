CREATE PROCEDURE UpdateLocation(IN loc INT,
                                IN var_sub_nbr VARCHAR(15))
            UpdateLocation_Label:BEGIN
                DECLARE var_s_id INT DEFAULT -1;

                SELECT s_id INTO var_s_id FROM subscriber WHERE sub_nbr = var_sub_nbr;
                UPDATE subscriber SET vlr_location = loc WHERE s_id = var_s_id;
END
