-- TEMPLATE: run manually after editing the parameters below. Not a migration.
-- Source: ION9000_Modbus_Map_004.005.000.xlsx, FW 004.005.000, Detailed Modbus Map.
-- Native map register numbers are retained (NOT legacy 4xxxx references).
-- Requires the model-aware Python reader with INT64 support.
-- Validate actual readings against the device screen before production use.
-- Device is inserted with status=0. No existing device/map is overwritten.
DO $$
DECLARE
    p_site_id integer := NULL; -- Required: site_id, NOT power_plant_id
    p_serial_number text := NULL; -- Required: actual device serial number
    p_name text := NULL; -- Required: site/device display name
    p_modbus_host inet := NULL; -- Required: IP only
    p_modbus_port integer := 502;
    p_modbus_unit_id integer := 1; -- Verify on site
    p_web_base_url text := NULL; -- Optional
    v_device_id integer;
    v_count integer;
BEGIN
    IF p_site_id IS NULL OR p_site_id <= 0
       OR NULLIF(btrim(p_serial_number), '') IS NULL
       OR NULLIF(btrim(p_name), '') IS NULL OR p_modbus_host IS NULL THEN
        RAISE EXCEPTION 'Fill site_id, serial_number, name and modbus_host first';
    END IF;
    IF p_modbus_port IS NULL OR p_modbus_port NOT BETWEEN 1 AND 65535
       OR p_modbus_unit_id IS NULL OR p_modbus_unit_id NOT BETWEEN 0 AND 255 THEN
        RAISE EXCEPTION 'Invalid Modbus port or unit ID';
    END IF;

    INSERT INTO public.ekk_device (
        site_id, serial_number, name, brand, model, status,
        modbus_host, modbus_port, modbus_unit_id, web_base_url
    ) VALUES (
        p_site_id, p_serial_number, p_name, 'Schneider Electric',
        'PowerLogic ION9000', 0, p_modbus_host, p_modbus_port,
        p_modbus_unit_id, p_web_base_url
    ) RETURNING id INTO v_device_id;

    -- modbus_units is the TARGET unit after scale_multiplier.
    -- Only core measurements with matching meaning are enabled here.
    -- PF/unbalance retained disabled until scale/sign conventions are verified.
    INSERT INTO public.ekk_device_register_map (
        ekk_device_id, quantity, modbus_register, number_of_modbus_registers,
        modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
        is_enabled, display_order, notes
    )
    SELECT v_device_id, q, reg, words, dtype, unit, tag, multiplier,
           enabled, ord,
           'ION9000 FW 004.005.000; native map numbering; ' || note
    FROM (VALUES
        ('i_a',3000,2,'FLOAT32','A','operation.current',1.0,true,10,'Current A; source A'),
        ('i_b',3002,2,'FLOAT32','A','operation.current',1.0,true,20,'Current B; source A'),
        ('i_c',3004,2,'FLOAT32','A','operation.current',1.0,true,30,'Current C; source A'),
        ('i4',3006,2,'FLOAT32','A','power_quality.current_harmonics',1.0,true,40,'Current N; source A; legacy display group retained'),
        ('i5',3008,2,'FLOAT32','A','power_quality.current_harmonics',1.0,true,50,'Current G; source A; legacy display group retained'),
        ('i_avg',3010,2,'FLOAT32','A','operation.current',1.0,true,60,'Current Avg; source A'),
        ('freq',3110,2,'FLOAT32','Hz','operation.frequency',1.0,true,70,'Frequency; source Hz'),
        ('v_unbal',3052,2,'FLOAT32','%','operation.voltage',1.0,false,80,'Voltage Unbalance L-N Worst; source unit unspecified; verify percent scale'),
        ('i_unbal',3018,2,'FLOAT32','%','operation.current',1.0,false,90,'Current Unbalance Worst; source unit unspecified; verify percent scale'),
        ('vln_a',3028,2,'FLOAT32','V','operation.voltage',1.0,true,100,'Voltage A-N; source V'),
        ('vln_b',3030,2,'FLOAT32','V','operation.voltage',1.0,true,110,'Voltage B-N; source V'),
        ('vln_c',3032,2,'FLOAT32','V','operation.voltage',1.0,true,120,'Voltage C-N; source V'),
        ('vln_avg',3036,2,'FLOAT32','V','operation.voltage',1.0,true,130,'Voltage L-N Avg; source V'),
        ('vll_ab',3020,2,'FLOAT32','V','operation.voltage',1.0,true,140,'Voltage A-B; source V'),
        ('vll_bc',3022,2,'FLOAT32','V','operation.voltage',1.0,true,150,'Voltage B-C; source V'),
        ('vll_ca',3024,2,'FLOAT32','V','operation.voltage',1.0,true,160,'Voltage C-A; source V'),
        ('vll_avg',3026,2,'FLOAT32','V','operation.voltage',1.0,true,170,'Voltage L-L Avg; source V'),
        ('kw_a',3054,2,'FLOAT32','kW','operation.power',0.001,true,180,'Active Power A; source W'),
        ('kw_b',3056,2,'FLOAT32','kW','operation.power',0.001,true,190,'Active Power B; source W'),
        ('kw_c',3058,2,'FLOAT32','kW','operation.power',0.001,true,200,'Active Power C; source W'),
        ('kw_total',3060,2,'FLOAT32','kW','operation.power',0.001,true,210,'Active Power Total; source W'),
        ('kvar_a',3062,2,'FLOAT32','kVAR','operation.power',0.001,true,230,'Reactive Power A; source VAR'),
        ('kvar_b',3064,2,'FLOAT32','kVAR','operation.power',0.001,true,240,'Reactive Power B; source VAR'),
        ('kvar_c',3066,2,'FLOAT32','kVAR','operation.power',0.001,true,250,'Reactive Power C; source VAR'),
        ('kvar_total',3068,2,'FLOAT32','kVAR','operation.power',0.001,true,260,'Reactive Power Total; source VAR'),
        ('kva_a',3070,2,'FLOAT32','kVA','operation.power',0.001,true,280,'Apparent Power A; source VA'),
        ('kva_b',3072,2,'FLOAT32','kVA','operation.power',0.001,true,290,'Apparent Power B; source VA'),
        ('kva_c',3074,2,'FLOAT32','kVA','operation.power',0.001,true,300,'Apparent Power C; source VA'),
        ('kva_total',3076,2,'FLOAT32','kVA','operation.power',0.001,true,310,'Apparent Power Total; source VA'),
        ('kwh_del',3204,4,'INT64','kWh','consumption.energy',0.001,true,330,'Active Energy Delivered Into Load; source Wh; verify site direction'),
        ('kwh_rec',3208,4,'INT64','kWh','consumption.energy',0.001,true,340,'Active Energy Received Out of Load; source Wh; verify site direction'),
        ('kvarh_del',3220,4,'INT64','kVARh','consumption.energy',0.001,true,350,'Reactive Energy Delivered; source VARh'),
        ('kvarh_rec',3224,4,'INT64','kVARh','consumption.energy',0.001,true,360,'Reactive Energy Received; source VARh'),
        ('kvah_del_rec',3244,4,'INT64','kVAh','consumption.energy',0.001,true,370,'Apparent Energy Delivered + Received; source VAh'),
        ('pf_sign_a',3144,2,'FLOAT32','%','operation.power_factor',1.0,false,380,'Power Factor A; multiplier provisional; verify percent/ratio and sign'),
        ('pf_sign_b',3146,2,'FLOAT32','%','operation.power_factor',1.0,false,390,'Power Factor B; multiplier provisional; verify percent/ratio and sign'),
        ('pf_sign_c',3148,2,'FLOAT32','%','operation.power_factor',1.0,false,400,'Power Factor C; multiplier provisional; verify percent/ratio and sign'),
        ('pf_sign_total',3150,2,'FLOAT32','%','operation.power_factor',1.0,false,410,'Power Factor Total; multiplier provisional; verify percent/ratio and sign')
    ) AS seed(q, reg, words, dtype, unit, tag, multiplier, enabled, ord, note);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Created passive device id=%, site_id=%, map rows=%', v_device_id, p_site_id, v_count;
END;
$$;
