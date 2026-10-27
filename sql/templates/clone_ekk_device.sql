-- TEMPLATE: copy a verified device map to a NEW device/site. Not a migration.
-- Copies all map rows, including disabled ones. Source records are untouched.
-- Verify the same model, firmware/map version and site-specific settings first.
DO $$
DECLARE
    p_source_device_id integer := NULL; -- Existing device with verified map
    p_site_id integer := NULL; -- NEW site_id, NOT power_plant_id
    p_serial_number text := NULL;
    p_name text := NULL;
    p_modbus_host inet := NULL;
    p_modbus_port integer := 502;
    p_modbus_unit_id integer := 1;
    p_web_base_url text := NULL;
    v_source public.ekk_device%ROWTYPE;
    v_device_id integer;
    v_count integer;
BEGIN
    IF p_source_device_id IS NULL OR p_site_id IS NULL OR p_site_id <= 0
       OR NULLIF(btrim(p_serial_number), '') IS NULL
       OR NULLIF(btrim(p_name), '') IS NULL OR p_modbus_host IS NULL THEN
        RAISE EXCEPTION 'Fill source_device_id, site_id, serial_number, name and modbus_host first';
    END IF;
    IF p_modbus_port IS NULL OR p_modbus_port NOT BETWEEN 1 AND 65535
       OR p_modbus_unit_id IS NULL OR p_modbus_unit_id NOT BETWEEN 0 AND 255 THEN
        RAISE EXCEPTION 'Invalid Modbus port or unit ID';
    END IF;
    SELECT * INTO STRICT v_source FROM public.ekk_device WHERE id = p_source_device_id;
    IF NOT EXISTS (SELECT 1 FROM public.ekk_device_register_map
                   WHERE ekk_device_id = p_source_device_id AND is_enabled) THEN
        RAISE EXCEPTION 'Source device % has no enabled map rows', p_source_device_id;
    END IF;

    INSERT INTO public.ekk_device (
        site_id, serial_number, name, brand, model, status,
        modbus_host, modbus_port, modbus_unit_id, web_base_url
    ) VALUES (
        p_site_id, p_serial_number, p_name, v_source.brand, v_source.model, 0,
        p_modbus_host, p_modbus_port, p_modbus_unit_id, p_web_base_url
    ) RETURNING id INTO v_device_id;

    INSERT INTO public.ekk_device_register_map (
        ekk_device_id, quantity, modbus_register, number_of_modbus_registers,
        modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
        is_enabled, display_order, notes
    )
    SELECT v_device_id, quantity, modbus_register, number_of_modbus_registers,
           modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
           is_enabled, display_order, notes
    FROM public.ekk_device_register_map WHERE ekk_device_id = p_source_device_id;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Created passive device id=%, model=%, source_device_id=%, map rows=%',
                 v_device_id, v_source.model, p_source_device_id, v_count;
END;
$$;
