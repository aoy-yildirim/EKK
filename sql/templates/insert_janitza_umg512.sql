-- TEMPLATE: new Janitza UMG512; manual execution, not a migration.
-- Source: janitza-mal-umg512-en.pdf, document 1.054.016.2.d.
-- Native decimal PDU addresses (offset 0), Big Endian FLOAT32, FC03.
-- Other devices and existing map IDs are not changed.
DO $$
DECLARE
    p_site_id integer := NULL; -- Required: site_id, NOT power_plant_id
    p_serial_number text := NULL; -- Screenshot: 4203.4521; verify actual device
    p_name text := NULL;
    p_modbus_host inet := NULL; -- Public IP for WAN, local IP for LAN
    p_modbus_port integer := 502; -- Set NAT external port for WAN
    p_modbus_unit_id integer := 1; -- Screenshot shows RS485 address 1; verify TCP
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
        RAISE EXCEPTION 'Invalid Modbus port or unit ID';
    END IF;

    INSERT INTO public.ekk_device (
        site_id, serial_number, name, brand, model, status,
        modbus_host, modbus_port, modbus_unit_id, web_base_url
    ) VALUES (
        p_site_id, p_serial_number, p_name, 'Janitza electronics GmbH',
        'UMG512', 0, p_modbus_host, p_modbus_port, p_modbus_unit_id, p_web_base_url
    ) RETURNING id INTO v_device_id;

    -- Target units are stored in modbus_units after scale_multiplier.
    -- Instantaneous totals cover L1..L3; i4 is separately measured L4.
    -- Energy is FLOAT32 here, NOT INT64. Preserve the source type.
    INSERT INTO public.ekk_device_register_map (
        ekk_device_id, quantity, modbus_register, number_of_modbus_registers,
        modbus_data_type, modbus_units, web_page_tag, scale_multiplier,
        is_enabled, display_order, notes
    )
    SELECT v_device_id, q, reg, 2, 'FLOAT32', unit, tag, multiplier,
           enabled, ord, 'Janitza UMG512 map 1.054.016.2.d; ' || note
    FROM (VALUES
        ('i_a',19012,'A','operation.current',1.0,true,10,'_G_ILN[0]; current L1'),
        ('i_b',19014,'A','operation.current',1.0,true,20,'_G_ILN[1]; current L2'),
        ('i_c',19016,'A','operation.current',1.0,true,30,'_G_ILN[2]; current L3'),
        ('i4',3859,'A','power_quality.current_harmonics',1.0,true,40,'_ILN[3]; measured L4 current; not vector sum'),
        ('freq',19050,'Hz','operation.frequency',1.0,true,70,'_G_FREQ; measured frequency'),
        ('vln_a',19000,'V','operation.voltage',1.0,true,100,'_G_ULN[0]; voltage L1-N'),
        ('vln_b',19002,'V','operation.voltage',1.0,true,110,'_G_ULN[1]; voltage L2-N'),
        ('vln_c',19004,'V','operation.voltage',1.0,true,120,'_G_ULN[2]; voltage L3-N'),
        ('vll_ab',19006,'V','operation.voltage',1.0,true,140,'_G_ULL[0]; voltage L1-L2'),
        ('vll_bc',19008,'V','operation.voltage',1.0,true,150,'_G_ULL[1]; voltage L2-L3'),
        ('vll_ca',19010,'V','operation.voltage',1.0,true,160,'_G_ULL[2]; voltage L3-L1'),
        ('kw_a',19020,'kW','operation.power',0.001,true,180,'_G_PLN[0]; source W'),
        ('kw_b',19022,'kW','operation.power',0.001,true,190,'_G_PLN[1]; source W'),
        ('kw_c',19024,'kW','operation.power',0.001,true,200,'_G_PLN[2]; source W'),
        ('kw_total',19026,'kW','operation.power',0.001,true,210,'_G_P_SUM3; source W; signed sum L1..L3'),
        ('kvar_a',19036,'kVAR','operation.power',0.001,true,230,'_G_QLN[0]; source var; FUNDAMENTAL reactive power'),
        ('kvar_b',19038,'kVAR','operation.power',0.001,true,240,'_G_QLN[1]; source var; FUNDAMENTAL reactive power'),
        ('kvar_c',19040,'kVAR','operation.power',0.001,true,250,'_G_QLN[2]; source var; FUNDAMENTAL reactive power'),
        ('kvar_total',19042,'kVAR','operation.power',0.001,true,260,'_G_Q_SUM3; source var; FUNDAMENTAL reactive power L1..L3'),
        ('kva_a',19028,'kVA','operation.power',0.001,true,280,'_G_SLN[0]; source VA'),
        ('kva_b',19030,'kVA','operation.power',0.001,true,290,'_G_SLN[1]; source VA'),
        ('kva_c',19032,'kVA','operation.power',0.001,true,300,'_G_SLN[2]; source VA'),
        ('kva_total',19034,'kVA','operation.power',0.001,true,310,'_G_S_SUM3; source VA; arithmetic sum L1..L3'),
        ('kwh_del',6508,'kWh','consumption.energy',0.001,true,330,'_WH_V[4]; source Wh; CONSUMED all rates; import into load; not Janitza delivered'),
        ('kwh_rec',6520,'kWh','consumption.energy',0.001,true,340,'_WH_Z[4]; source Wh; DELIVERED all rates; export out of load'),
        ('kvah_del_rec',6472,'kVAh','consumption.energy',0.001,true,370,'_WH_S[4]; source VAh; apparent energy L1..L3'),
        ('pf_sign_a',3893,'%','operation.power_factor',100.0,false,380,'_PFLN[0]; TRUE PF ratio to percent; verify sign before enabling; not cos-phi'),
        ('pf_sign_b',3895,'%','operation.power_factor',100.0,false,390,'_PFLN[1]; TRUE PF ratio to percent; verify sign before enabling; not cos-phi'),
        ('pf_sign_c',3897,'%','operation.power_factor',100.0,false,400,'_PFLN[2]; TRUE PF ratio to percent; verify sign before enabling; not cos-phi'),
        ('v1_thd_200ms',19110,'%','power_quality.voltage_harmonics',1.0,true,420,'_G_THD_ULN[0]; current 200ms THD percent; not max or ION 3s'),
        ('v2_thd_200ms',19112,'%','power_quality.voltage_harmonics',1.0,true,430,'_G_THD_ULN[1]; current 200ms THD percent; not max or ION 3s'),
        ('v3_thd_200ms',19114,'%','power_quality.voltage_harmonics',1.0,true,440,'_G_THD_ULN[2]; current 200ms THD percent; not max or ION 3s'),
        ('i1_thd_200ms',19116,'%','power_quality.current_harmonics',1.0,true,450,'_G_THD_ILN[0]; current 200ms THD percent'),
        ('i2_thd_200ms',19118,'%','power_quality.current_harmonics',1.0,true,460,'_G_THD_ILN[1]; current 200ms THD percent'),
        ('i3_thd_200ms',19120,'%','power_quality.current_harmonics',1.0,true,470,'_G_THD_ILN[2]; current 200ms THD percent'),
        ('i1_k_factor',3837,NULL,'power_quality.current_harmonics',1.0,true,480,'_KFACT[0]; dimensionless'),
        ('i2_k_factor',3839,NULL,'power_quality.current_harmonics',1.0,true,490,'_KFACT[1]; dimensionless'),
        ('i3_k_factor',3841,NULL,'power_quality.current_harmonics',1.0,true,500,'_KFACT[2]; dimensionless'),
        ('i1_crest_factor',4021,NULL,'power_quality.current_harmonics',1.0,true,510,'_IL_CF[0]; dimensionless'),
        ('i2_crest_factor',4023,NULL,'power_quality.current_harmonics',1.0,true,520,'_IL_CF[1]; dimensionless'),
        ('i3_crest_factor',4025,NULL,'power_quality.current_harmonics',1.0,true,530,'_IL_CF[2]; dimensionless'),
        ('kwh_net',6484,'kWh','consumption.energy',0.001,true,540,'_WH[4]; source Wh; signed net energy, not import/export'),
        ('kvarh_net',6496,'kVARh','consumption.energy',0.001,true,550,'_QH[4]; source varh; fundamental net reactive energy'),
        ('kvarh_inductive',6580,'kVARh','consumption.energy',0.001,true,560,'_IQH[4]; source varh; inductive, not import'),
        ('kvarh_capacitive',6592,'kVARh','consumption.energy',0.001,true,570,'_CQH[4]; source varh; capacitive, not export'),
        ('cos_phi_a',19044,'ratio','operation.power_factor',1.0,false,580,'_G_COS_PHI[0]; FUNDAMENTAL cos-phi; verify sign; not true PF'),
        ('cos_phi_b',19046,'ratio','operation.power_factor',1.0,false,590,'_G_COS_PHI[1]; FUNDAMENTAL cos-phi; verify sign; not true PF'),
        ('cos_phi_c',19048,'ratio','operation.power_factor',1.0,false,600,'_G_COS_PHI[2]; FUNDAMENTAL cos-phi; verify sign; not true PF'),
        ('cos_phi_total',3929,'ratio','operation.power_factor',1.0,false,610,'_COS_SUM3; calculated from Psum3/Qsum3; not true PF Total')
    ) AS seed(q, reg, unit, tag, multiplier, enabled, ord, note);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Created passive UMG512 device id=%, site_id=%, map rows=%; compare readings to device screen',
                 v_device_id, p_site_id, v_count;
END;
$$;
