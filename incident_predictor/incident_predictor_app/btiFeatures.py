from collections import defaultdict


def get_unom_to_bti_meta(bti_df):
    unom_to_bti_meta = defaultdict(dict)
    for row in bti_df.itertuples():
        unom = row.unom
        unom_to_bti_meta[unom]['material'] = row.material if row.material is not None else 'unknown'
        unom_to_bti_meta[unom]['assignment_structure'] = row.assignment_structure \
            if row.assignment_structure is not None else 'unknown'
        unom_to_bti_meta[unom]['class_structure'] = row.class_structure
        unom_to_bti_meta[unom]['floor_area'] = row.floor_area
        unom_to_bti_meta[unom]['square'] = row.square
        unom_to_bti_meta[unom]['usage_priority_type'] = row.usage_priority_type
    return dict(unom_to_bti_meta)


def add_bti_features(pool, unom_to_bti_meta):
    pool['material'] = pool.UNOM.apply(lambda x: unom_to_bti_meta[x]['material'])
    pool['class_structure'] = pool.UNOM.apply(lambda x: unom_to_bti_meta[x]['class_structure'])
    pool['floor_area'] = pool.UNOM.apply(lambda x: unom_to_bti_meta[x]['floor_area'])
    pool['square'] = pool.UNOM.apply(lambda x: unom_to_bti_meta[x]['square'])
    pool['usage_priority_type'] = pool.UNOM.apply(lambda x: str(int(unom_to_bti_meta[x]['usage_priority_type'])))
    pool['assignment_structure'] = pool.UNOM.apply(lambda x: unom_to_bti_meta[x]['assignment_structure'])
