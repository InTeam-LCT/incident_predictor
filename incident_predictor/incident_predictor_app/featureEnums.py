NUM_FEATURES_COLS = [
    # 'week_day',
    # 'random_1', 'random_2', 'random_3',
    'floor_area',
    'square',
    # 'mkd_tech_n_floors',
    # 'n_entrances',
    'n_flats',
    # 'mkd_tech_square',
    'mkd_tech_living_square',
    'mkd_tech_nonliving_square',
    # 'wear_and_tear',
    # 'n_elevators',
    # 'n_big_elevators',
    'days_from_date_address_register',
    'days_from_state_address_register',
    'days_from_date_document_address_register',
    'distance_to_moscow_center',
    'angle_to_moscow_center',
    'lat',
    'lon',
    # 'temp_min_7_days',
    # 'temp_max_7_days',
    'temp_var_7_days',
    'temp_mean_7_days',
    # 'temp_median_7_days',
    'temp_shift_7_days',
    # 'temp_min_3_days',
    # 'temp_max_3_days',
    # 'temp_var_3_days',
    'temp_mean_3_days',
    # 'temp_median_3_days',
    'temp_shift_3_days',
    # 'temp_min_day',
    # 'temp_max_day',
    # 'temp_var_day',
    # 'temp_mean_day',
    # 'temp_median_day',
    # 'temp_shift_day',
    'days_from_commissioning',
    'heat_load_dhw_average',
    # 'heat_load_dhw_actual',
    'heat_load_building',
    'heat_load_ventilation_building',
    'tp_connections_count',
]

CAT_FEATURES_COLS = [
    # 'month',
    # 'year',
    'material',
    # 'class_structure',
    'usage_priority_type',
    'assignment_structure',
    'area',
    # 'mkd_tech_wall_materials',
    # 'mkd_tech_is_emergency',
    'roof_cleaning_order',
    'roof_material',
    # 'fond_type',
    # 'mkd_status',
    'weather',
    'heat_source',
    'placement_type',
    'is_dispatching'
]

FEATURES_COLS = NUM_FEATURES_COLS + CAT_FEATURES_COLS
