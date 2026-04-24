import pandas as pd
import re


def filter_dataframe(
    df,
    type_filter=None,
    alpha_count=None,
    data_coverage=None,
    coverage=None,
    user_count=None,
    field_name_patterns=None,
    field_name_exclude_patterns=None,
    description_patterns=None,
    description_exclude_patterns=None,
):
    if df.empty:
        return df

    # ── Type filter ──────────────────────────────────────────────────────────
    if type_filter and 'type' in df.columns:
        df = df[df['type'] == type_filter]

    # ── Field name whitelist ─────────────────────────────────────────────────
    if field_name_patterns and 'id' in df.columns:
        combined = '|'.join(f'(?:{p})' for p in field_name_patterns)
        combined = re.sub(r'\((?!\?)', '(?:', combined)
        mask = df['id'].str.contains(combined, flags=re.IGNORECASE, regex=True, na=False)
        df = df[mask]

    # ── Field name blacklist ─────────────────────────────────────────────────
    if field_name_exclude_patterns and 'id' in df.columns:
        combined_ex = '|'.join(f'(?:{p})' for p in field_name_exclude_patterns)
        combined_ex = re.sub(r'\((?!\?)', '(?:', combined_ex)
        mask_ex = df['id'].str.contains(combined_ex, flags=re.IGNORECASE, regex=True, na=False)
        df = df[~mask_ex]

    # ── Description whitelist ────────────────────────────────────────────────
    if description_patterns and 'description' in df.columns:
        combined_d = '|'.join(f'(?:{p})' for p in description_patterns)
        combined_d = re.sub(r'\((?!\?)', '(?:', combined_d)
        mask_d = df['description'].str.contains(combined_d, flags=re.IGNORECASE, regex=True, na=False)
        df = df[mask_d]

    # ── Description blacklist ────────────────────────────────────────────────
    if description_exclude_patterns and 'description' in df.columns:
        combined_dex = '|'.join(f'(?:{p})' for p in description_exclude_patterns)
        combined_dex = re.sub(r'\((?!\?)', '(?:', combined_dex)
        mask_dex = df['description'].str.contains(combined_dex, flags=re.IGNORECASE, regex=True, na=False)
        df = df[~mask_dex]

    # ── Numeric range filters ────────────────────────────────────────────────
    def apply_range_filter(df_in, col_name, val_range):
        if not val_range or col_name not in df_in.columns:
            return df_in
        series = pd.to_numeric(df_in[col_name], errors='coerce')
        condition = pd.Series(True, index=df_in.index)
        if isinstance(val_range, (list, tuple)) and len(val_range) == 2:
            min_val, max_val = val_range
            if min_val is not None and str(min_val).strip() != "":
                condition &= (series >= float(min_val))
            if max_val is not None and str(max_val).strip() != "":
                condition &= (series <= float(max_val))
        elif isinstance(val_range, (str, int, float)) and str(val_range).strip():
            condition &= (series >= float(val_range))
        return df_in[condition]

    df = apply_range_filter(df, 'alphaCount',   alpha_count)
    df = apply_range_filter(df, 'dateCoverage', data_coverage)
    df = apply_range_filter(df, 'coverage',     coverage)
    df = apply_range_filter(df, 'userCount',    user_count)

    return df


def generate_alphas(datafields_df, alpha_groups):
    """
    alpha_groups: list of group dicts, each containing:
        - datasets: list of dataset config dicts  (same schema as before)
        - alpha_templates: list of template strings with {datafield} placeholder
        - simulation_settings: dict of Brain simulation settings
        - label (optional): human-readable name shown in logs
    
    Returns a flat list of simulation payload dicts, each tagged with
    'group_label' for reporting purposes.
    """
    alpha_list = []

    for group in alpha_groups:
        group_label    = group.get('label', 'unlabeled')
        templates      = group.get('alpha_templates', [])
        sim_settings   = group.get('simulation_settings', {})
        datasets_conf  = group.get('datasets', [])

        if not templates:
            print(f"   ⚠️  Group '{group_label}': no alpha_templates defined, skipping.")
            continue

        print(f"\n   📦 Group: [{group_label}]  ({len(templates)} template(s))")

        for dataset_conf in datasets_conf:
            ds_id = dataset_conf.get('id')

            if 'target_dataset_id' in datafields_df.columns:
                df_filtered = datafields_df[datafields_df['target_dataset_id'] == ds_id].copy()
            else:
                df_filtered = datafields_df.copy()

            df_filtered = filter_dataframe(
                df_filtered,
                type_filter                  = dataset_conf.get('type_filter'),
                alpha_count                  = dataset_conf.get('alpha_count_filter'),
                data_coverage                = dataset_conf.get('data_coverage_filter'),
                coverage                     = dataset_conf.get('coverage_filter'),
                user_count                   = dataset_conf.get('user_count_filter'),
                field_name_patterns          = dataset_conf.get('field_name_patterns'),
                field_name_exclude_patterns  = dataset_conf.get('field_name_exclude_patterns'),
                description_patterns         = dataset_conf.get('description_patterns'),
                description_exclude_patterns = dataset_conf.get('description_exclude_patterns'),
            )

            if df_filtered.empty or 'id' not in df_filtered.columns:
                print(f"      ⚠️  {ds_id}: 0 fields after filtering — check filter config.")
                continue

            fields = df_filtered['id'].values
            print(f"      ✅ {ds_id}: {len(fields)} field(s) → {list(fields)}")

            for datafield in fields:
                for template in templates:
                    expression = template.format(datafield=datafield)
                    alpha_list.append({
                        'type':        'REGULAR',
                        'settings':    sim_settings,
                        'regular':     expression,
                        'group_label': group_label,   # carried through for reporting
                    })

    print(f"\n   🔢 Total alpha payloads generated: {len(alpha_list)}")
    return alpha_list