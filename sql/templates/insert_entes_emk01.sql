-- TEMPLATE: new ENTES EMK-01; manual execution, not a migration.
-- Source: EMK-01_Modbus_Register_Table.pdf, pages 1-3, Big Endian, FC03.
-- Store printed addresses. Verify address convention with tools/probe_entes_emk01.py.
-- Live-verified reader offset=0: printed 40001 is sent as PDU address 40001.
-- No existing devices, map IDs, histories or schemas are changed.
DO $$
DECLARE
    p_site_id integer := NULL; -- Required: site_id, NOT power_plant_id
    p_serial_number text := NULL; -- Screenshot: 19011706; verify actual device
    p_name text := NULL;
    p_modbus_host inet := NULL; -- WAN public IP, or LAN device IP
    p_modbus_port integer := 502; -- Replace with external NAT port for WAN
    p_modbus_unit_id integer := NULL; -- Required: verify; screenshots do not show it
    p_web_base_url text := NULL;
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
        RAISE EXCEPTION 'Verify and fill Modbus TCP unit ID and port first';
    END IF;

    INSERT INTO public.ekk_device (
        site_id, serial_number, name, brand, model, status,
        modbus_host, modbus_port, modbus_unit_id, web_base_url
    ) VALUES (
        p_site_id, p_serial_number, p_name, 'ENTES', 'EMK-01', 0,
        p_modbus_host, p_modbus_port, p_modbus_unit_id, p_web_base_url
    ) RETURNING id INTO v_device_id;

    INSERT INTO public.ekk_device_register_map (
        ekk_device_id, quantity, modbus_register, number_of_modbus_registers,
        modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
        is_enabled, display_order, notes
    )
    SELECT v_device_id, q, reg, words, dtype, unit, tag, multiplier,
           enabled, ord, 'ENTES EMK-01; source table pp.1-3; ' || note
    FROM (VALUES
        ('i_a',40033,2,'FLOAT32','A','operation.current',1.0,true,10,'PROUD_L1; source A'),
        ('i_b',40035,2,'FLOAT32','A','operation.current',1.0,true,20,'PROUD_L2; source A'),
        ('i_c',40037,2,'FLOAT32','A','operation.current',1.0,true,30,'PROUD_L3; source A'),
        ('i4',40039,2,'FLOAT32','A','power_quality.current_harmonics',1.0,true,40,'PROUD_L4; source A; measured L4, not phase average'),
        ('freq',40081,2,'FLOAT32','Hz','operation.frequency',1.0,true,70,'FREQUENCY; source Hz'),
        ('vln_a',40001,2,'FLOAT32','V','operation.voltage',1.0,true,100,'U_LN_L1; source V'),
        ('vln_b',40003,2,'FLOAT32','V','operation.voltage',1.0,true,110,'U_LN_L2; source V'),
        ('vln_c',40005,2,'FLOAT32','V','operation.voltage',1.0,true,120,'U_LN_L3; source V'),
        ('vll_ab',40009,2,'FLOAT32','V','operation.voltage',1.0,true,140,'U_LL_L1-2; source V'),
        ('vll_bc',40011,2,'FLOAT32','V','operation.voltage',1.0,true,150,'U_LL_L2-3; source V'),
        ('vll_ca',40013,2,'FLOAT32','V','operation.voltage',1.0,true,160,'U_LL_L3-1; source V'),
        ('kw_a',40097,2,'FLOAT32','kW','operation.power',0.001,true,180,'P_P1; source W; preserve sign'),
        ('kw_b',40099,2,'FLOAT32','kW','operation.power',0.001,true,190,'P_P2; source W; preserve sign'),
        ('kw_c',40101,2,'FLOAT32','kW','operation.power',0.001,true,200,'P_P3; source W; preserve sign'),
        ('kw_total',40121,2,'FLOAT32','kW','operation.power',0.001,true,210,'SUMA_3F_P; source W; L1..L3'),
        ('kva_a',40089,2,'FLOAT32','kVA','operation.power',0.001,true,280,'S_L1; source VA'),
        ('kva_b',40091,2,'FLOAT32','kVA','operation.power',0.001,true,290,'S_L2; source VA'),
        ('kva_c',40093,2,'FLOAT32','kVA','operation.power',0.001,true,300,'S_L3; source VA'),
        ('kva_total',40129,2,'FLOAT32','kVA','operation.power',0.001,true,310,'SUMA_3F_S; source VA; L1..L3'),
        ('kvar_import_a',40105,2,'FLOAT32','kVAR','operation.power',0.001,true,320,'Q_CONSUMPTION_L1; source var; consumption direction, not net Q'),
        ('kvar_import_b',40107,2,'FLOAT32','kVAR','operation.power',0.001,true,321,'Q_CONSUMPTION_L2; source var; preserve returned sign'),
        ('kvar_import_c',40109,2,'FLOAT32','kVAR','operation.power',0.001,true,322,'Q_CONSUMPTION_L3; source var; preserve returned sign'),
        ('kvar_export_a',40113,2,'FLOAT32','kVAR','operation.power',0.001,true,323,'Q_DISTRIBUTION_L1; source var; supply direction, not net Q'),
        ('kvar_export_b',40115,2,'FLOAT32','kVAR','operation.power',0.001,true,324,'Q_DISTRIBUTION_L2; source var; preserve returned sign'),
        ('kvar_export_c',40117,2,'FLOAT32','kVAR','operation.power',0.001,true,325,'Q_DISTRIBUTION_L3; source var; preserve returned sign'),
        ('kvar_import_total',40137,2,'FLOAT32','kVAR','operation.power',0.001,true,326,'SUMA_3F_Q_CONSUMPTION; source var; L1..L3 consumption'),
        ('kvar_export_total',40145,2,'FLOAT32','kVAR','operation.power',0.001,true,327,'SUMA_3F_Q_DISTRIBUTION; source var; L1..L3 supply'),
        ('kwh_del',55000,4,'INT64','kWh','consumption.energy',0.1,true,330,'Consumed Active Energy; source kWh/10; confirm raw/10 against FLOAT alternative 55024'),
        ('kwh_rec',55012,4,'INT64','kWh','consumption.energy',0.1,true,340,'Supplied Active Energy; source kWh/10; confirm raw/10 against FLOAT alternative 55030'),
        ('kvarh_import_inductive',55004,4,'INT64','kVARh','consumption.energy',0.1,true,350,'Consumed Inductive Reactive Energy; source kvarh/10'),
        ('kvarh_import_capacitive',55008,4,'INT64','kVARh','consumption.energy',0.1,true,351,'Consumed Capacitive Reactive Energy; source kvarh/10'),
        ('kvarh_export_inductive',55016,4,'INT64','kVARh','consumption.energy',0.1,true,352,'Supplied Inductive Reactive Energy; source kvarh/10'),
        ('kvarh_export_capacitive',55020,4,'INT64','kVARh','consumption.energy',0.1,true,353,'Supplied Capacitive Reactive Energy; source kvarh/10'),
        ('pf_sign_a',40057,2,'FLOAT32','%','operation.power_factor',100.0,false,380,'POWERFACTOR_L1; true PF ratio to percent; verify sign/scale; not CosPhi'),
        ('pf_sign_b',40059,2,'FLOAT32','%','operation.power_factor',100.0,false,390,'POWERFACTOR_L2; true PF ratio to percent; verify sign/scale; not CosPhi'),
        ('pf_sign_c',40061,2,'FLOAT32','%','operation.power_factor',100.0,false,400,'POWERFACTOR_L3; true PF ratio to percent; verify sign/scale; not CosPhi'),
        ('pf_sign_total',40065,2,'FLOAT32','%','operation.power_factor',100.0,false,410,'POWERFACTOR_SUMA_3F; true PF L1..L3; verify before enabling'),
        ('v1_thd_instant',40017,2,'FLOAT32','%','power_quality.voltage_harmonics',1.0,true,420,'THDU_LN_L1; source percent; instantaneous, not maximum; window unspecified'),
        ('v2_thd_instant',40019,2,'FLOAT32','%','power_quality.voltage_harmonics',1.0,true,430,'THDU_LN_L2; source percent; instantaneous, not maximum; window unspecified'),
        ('v3_thd_instant',40021,2,'FLOAT32','%','power_quality.voltage_harmonics',1.0,true,440,'THDU_LN_L3; source percent; instantaneous, not maximum; window unspecified'),
        ('i1_thd_instant',40041,2,'FLOAT32','%','power_quality.current_harmonics',1.0,true,450,'THDI_L1; source percent; instantaneous, window unspecified'),
        ('i2_thd_instant',40043,2,'FLOAT32','%','power_quality.current_harmonics',1.0,true,460,'THDI_L2; source percent; instantaneous, window unspecified'),
        ('i3_thd_instant',40045,2,'FLOAT32','%','power_quality.current_harmonics',1.0,true,470,'THDI_L3; source percent; instantaneous, window unspecified'),
        ('i1_k_factor',40281,2,'FLOAT32',NULL,'power_quality.current_harmonics',1.0,true,480,'K_FACTOR_L1; device-native K-factor; verify independently'),
        ('i2_k_factor',40283,2,'FLOAT32',NULL,'power_quality.current_harmonics',1.0,true,490,'K_FACTOR_L2; device-native K-factor; verify independently'),
        ('i3_k_factor',40285,2,'FLOAT32',NULL,'power_quality.current_harmonics',1.0,true,500,'K_FACTOR_L3; device-native K-factor; verify independently'),
        ('cos_phi_a',40049,2,'FLOAT32','ratio','operation.power_factor',1.0,false,580,'COS_FI_L1; CosPhi, not true PF; verify sign'),
        ('cos_phi_b',40051,2,'FLOAT32','ratio','operation.power_factor',1.0,false,590,'COS_FI_L2; CosPhi, not true PF; verify sign'),
        ('cos_phi_c',40053,2,'FLOAT32','ratio','operation.power_factor',1.0,false,600,'COS_FI_L3; CosPhi, not true PF; verify sign'),
        ('pst_a',40241,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,620,'SHORT_FLICKER_L1; dimensionless short flicker Pst'),
        ('pst_b',40243,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,621,'SHORT_FLICKER_L2; dimensionless short flicker Pst'),
        ('pst_c',40245,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,622,'SHORT_FLICKER_L3; dimensionless short flicker Pst'),
        ('plt_a',40249,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,623,'LONG_FLICKER_L1; dimensionless long flicker Plt'),
        ('plt_b',40251,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,624,'LONG_FLICKER_L2; dimensionless long flicker Plt'),
        ('plt_c',40253,2,'FLOAT32',NULL,'power_quality.flicker',1.0,true,625,'LONG_FLICKER_L3; dimensionless long flicker Plt')
    ) AS seed(q, reg, words, dtype, unit, tag, multiplier, enabled, ord, note);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Created passive EMK-01 device id=%, site_id=%, map rows=%; verify addressing and energy scale before activating',
                 v_device_id, p_site_id, v_count;
END;
$$;
