CREATE PROCEDURE UpdateCustomer(IN var_c_id VARCHAR(128),
                                IN var_c_id_str VARCHAR(64),
                                IN var_attr0 BIGINT,
                                IN var_attr1 BIGINT)
UpdateCustomer_Label:BEGIN

   DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
        UUID_SHORT(), 'UpdateCustomer',
        var_c_id, var_c_id_str, var_attr0, var_attr1
   );

   DECLARE var_c_base_ap_id BIGINT DEFAULT -1;

   INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

   IF var_c_id IS NULL THEN
      SELECT c_id INTO var_c_id FROM customer2 WHERE c_id_str = var_c_id_str;
   END IF;

   IF var_c_id IS NULL THEN
      SELECT "c_id is not found";
      LEAVE UpdateCustomer_Label;
   END IF;

   SELECT c_base_ap_id INTO var_c_base_ap_id FROM customer2 WHERE c_id = var_c_id;

   IF var_c_base_ap_id = -1 THEN
      SELECT "c_base_ap_id is not found";
      LEAVE UpdateCustomer_Label;
   END IF;

   SELECT * FROM airport, country WHERE ap_id = var_c_base_ap_id AND ap_co_id = co_id;

   UPDATE customer2 SET c_iattr00 = var_attr0, c_iattr01 = var_attr1 WHERE c_id = var_c_id;

   UPDATE frequent_flyer SET ff_iattr00 = var_attr0, ff_iattr01 = var_attr1 WHERE ff_c_id = var_c_id AND ff_al_id IN (SELECT ff_al_id FROM (SELECT ff_al_id FROM frequent_flyer WHERE ff_c_id = var_c_id) AS frequent_flyer_alias);

END
