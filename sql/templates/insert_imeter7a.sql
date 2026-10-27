-- TEMPLATE: new CET iMeter 7A device; manual execution, not a migration.
-- Source: iMeter 7A English User Manual V1.1 (20230221), protocol v7.0.
-- Printed pp.134-136 (Basic Measurement), p.138 (INT64 Energy).
-- ZERO-BASED decimal addresses; DO NOT subtract 1 or add 40001.
-- RMS measurements only; Fundamental is a separate measurement family.
-- Use model-aware reader supporting iMeter 7A before activating.
DO $$
DECLARE
    p_site_id integer := NULL; -- Required: site_id, NOT power_plant_id
    p_serial_number text := NULL; -- Required: actual serial, not Ethernet name
    p_name text := NULL;
    p_modbus_host inet := NULL; -- Reachable IP; public IP if using WAN NAT
    p_modbus_port integer := 502; -- External port if using WAN NAT
    p_modbus_unit_id integer := 100; -- Screenshot: Modbus TCP ID=100
    p_web_base_url text := NULL;
    -- IMPORTANT: verify Setup > Basic wiring mode before running.
    -- Per-phase Uln, P/Q/S and PF registers are RESERVED in 3P3W mode.
    -- Set false for 3P3W. Screenshot suggests phase values, but verify on device.
    p_enable_per_phase boolean := true;
    v_device_id integer;
    v_count integer;
BEGIN
    IF p_site_id IS NULL OR p_site_id <= 0
       OR NULLIF(btrim(p_serial_number), '') IS NULL
       OR NULLIF(btrim(p_name), '') IS NULL OR p_modbus_host IS NULL THEN
        RAISE EXCEPTION 'Fill site_id, serial_number, name and modbus_host first';
    END IF;
    IF p_modbus_port IS NULL OR p_modbus_port NOT BETWEEN 1 AND 65535
       OR p_modbus_unit_id IS NULL OR p_modbus_unit_id NOT BETWEEN 0 AND 255
       OR p_enable_per_phase IS NULL THEN
        RAISE EXCEPTION 'Invalid Modbus port, unit ID or wiring mode selection';
    END IF;

    INSERT INTO public.ekk_device (
        site_id, serial_number, name, brand, model, status,
        modbus_host, modbus_port, modbus_unit_id, web_base_url
    ) VALUES (
        p_site_id, p_serial_number, p_name, 'CET Electric Technology',
        'iMeter 7A', 0, p_modbus_host, p_modbus_port,
        p_modbus_unit_id, p_web_base_url
    ) RETURNING id INTO v_device_id;

    INSERT INTO public.ekk_device_register_map (
        ekk_device_id, quantity, modbus_register, number_of_modbus_registers,
        modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
        is_enabled, display_order, notes
    )
    SELECT v_device_id, q, reg, words, dtype, unit, tag, multiplier,
           enabled AND (NOT needs_phase OR p_enable_per_phase), ord,
           'CET iMeter 7A V1.1 protocol v7.0; zero-based; ' || note
    FROM (VALUES
        ('i_a',16,2,'FLOAT32','A','operation.current',1.0,true,false,10,'RMS Ia; source A'),
        ('i_b',18,2,'FLOAT32','A','operation.current',1.0,true,false,20,'RMS Ib; source A'),
        ('i_c',20,2,'FLOAT32','A','operation.current',1.0,true,false,30,'RMS Ic; source A'),
        ('i4',60,2,'FLOAT32','A','power_quality.current_harmonics',1.0,true,false,40,'RMS I4; source A; legacy display group retained'),
        ('i_avg',22,2,'FLOAT32','A','operation.current',1.0,true,false,60,'RMS I Avg; source A'),
        ('freq',56,2,'FLOAT32','Hz','operation.frequency',1.0,true,false,70,'Frequency; source Hz'),
        ('vln_a',0,2,'FLOAT32','V','operation.voltage',1.0,true,true,100,'RMS Ua; source V; reserved in 3P3W'),
        ('vln_b',2,2,'FLOAT32','V','operation.voltage',1.0,true,true,110,'RMS Ub; source V; reserved in 3P3W'),
        ('vln_c',4,2,'FLOAT32','V','operation.voltage',1.0,true,true,120,'RMS Uc; source V; reserved in 3P3W'),
        ('vln_avg',6,2,'FLOAT32','V','operation.voltage',1.0,true,true,130,'RMS ULN Avg; source V; disable for 3P3W'),
        ('vll_ab',8,2,'FLOAT32','V','operation.voltage',1.0,true,false,140,'RMS Uab; source V'),
        ('vll_bc',10,2,'FLOAT32','V','operation.voltage',1.0,true,false,150,'RMS Ubc; source V'),
        ('vll_ca',12,2,'FLOAT32','V','operation.voltage',1.0,true,false,160,'RMS Uca; source V'),
        ('vll_avg',14,2,'FLOAT32','V','operation.voltage',1.0,true,false,170,'RMS ULL Avg; source V'),
        ('kw_a',24,2,'FLOAT32','kW','operation.power',0.001,true,true,180,'RMS Pa; source W; reserved in 3P3W'),
        ('kw_b',26,2,'FLOAT32','kW','operation.power',0.001,true,true,190,'RMS Pb; source W; reserved in 3P3W'),
        ('kw_c',28,2,'FLOAT32','kW','operation.power',0.001,true,true,200,'RMS Pc; source W; reserved in 3P3W'),
        ('kw_total',30,2,'FLOAT32','kW','operation.power',0.001,true,false,210,'RMS P Total; source W; preserve sign'),
        ('kvar_a',32,2,'FLOAT32','kVAR','operation.power',0.001,true,true,230,'RMS Qa; source var; reserved in 3P3W'),
        ('kvar_b',34,2,'FLOAT32','kVAR','operation.power',0.001,true,true,240,'RMS Qb; source var; reserved in 3P3W'),
        ('kvar_c',36,2,'FLOAT32','kVAR','operation.power',0.001,true,true,250,'RMS Qc; source var; reserved in 3P3W'),
        ('kvar_total',38,2,'FLOAT32','kVAR','operation.power',0.001,true,false,260,'RMS Q Total; source var'),
        ('kva_a',40,2,'FLOAT32','kVA','operation.power',0.001,true,true,280,'RMS Sa; source VA; reserved in 3P3W'),
        ('kva_b',42,2,'FLOAT32','kVA','operation.power',0.001,true,true,290,'RMS Sb; source VA; reserved in 3P3W'),
        ('kva_c',44,2,'FLOAT32','kVA','operation.power',0.001,true,true,300,'RMS Sc; source VA; reserved in 3P3W'),
        ('kva_total',46,2,'FLOAT32','kVA','operation.power',0.001,true,false,310,'RMS S Total; source VA'),
        ('kwh_del',500,4,'INT64','kWh','consumption.energy',0.001,true,false,330,'RMS Import energy; source Wh; mapped to delivered into load; verify site direction'),
        ('kwh_rec',504,4,'INT64','kWh','consumption.energy',0.001,true,false,340,'RMS Export energy; source Wh; mapped to received out of load; verify site direction'),
        ('kvarh_del',508,4,'INT64','kVARh','consumption.energy',0.001,true,false,350,'RMS Import reactive energy; source varh'),
        ('kvarh_rec',512,4,'INT64','kVARh','consumption.energy',0.001,true,false,360,'RMS Export reactive energy; source varh'),
        ('kvah_del_rec',516,4,'INT64','kVAh','consumption.energy',0.001,true,false,370,'Total apparent energy; source VAh'),
        ('pf_sign_a',48,2,'FLOAT32','%','operation.power_factor',100.0,false,true,380,'RMS PFa ratio to signed percent; verify PF Convention register 40580 before enabling; reserved in 3P3W'),
        ('pf_sign_b',50,2,'FLOAT32','%','operation.power_factor',100.0,false,true,390,'RMS PFb ratio to signed percent; verify PF Convention register 40580 before enabling; reserved in 3P3W'),
        ('pf_sign_c',52,2,'FLOAT32','%','operation.power_factor',100.0,false,true,400,'RMS PFc ratio to signed percent; verify PF Convention register 40580 before enabling; reserved in 3P3W'),
        ('pf_sign_total',54,2,'FLOAT32','%','operation.power_factor',100.0,false,false,410,'RMS PF Total ratio to signed percent; verify IEC/IEEE/-IEEE setting at register 40580 before enabling')
    ) AS seed(q, reg, words, dtype, unit, tag, multiplier, enabled, needs_phase, ord, note);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Created passive iMeter 7A device id=%, site_id=%, map rows=%; verify wiring and PF convention',
                 v_device_id, p_site_id, v_count;
END;
$$;
