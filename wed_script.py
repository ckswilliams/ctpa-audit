import pandas as pd

from medphunc.image_analysis import water_equivalent_diameter

# File location for csv file containing study instance uids to process.
INPUT_STUDIES_CSV_FN = 'ctpa_accessions_for_processing.csv'

# Output file location, which will be REUSED if we want to rerun the script
# so that we don't end up repeatedly processing the same studies.
PROCESSED_STUDIES_CSV_FN = 'ctpa_wed_data.csv'

# In-progress output file, useful if the script is interrupted partway through.
INTERMEDIATE_OUTPUT_CSV_FN = 'intermediate_ctpa_wed_data.csv'


if __name__ == '__main__':
    data_df = pd.read_csv(INPUT_STUDIES_CSV_FN)

    try:
        done_df = pd.read_csv(PROCESSED_STUDIES_CSV_FN)
    except FileNotFoundError:
        done_df = pd.DataFrame(columns=['study_instance_uid'])

    try:
        partial_df = pd.read_csv(INTERMEDIATE_OUTPUT_CSV_FN)
    except FileNotFoundError:
        partial_df = pd.DataFrame(columns=['study_instance_uid'])

    processed_uids = set(done_df.study_instance_uid) | set(partial_df.study_instance_uid)

    output_data = []
    for i in data_df.index:
        row = data_df.loc[i, :]
        if row.study_instance_uid in processed_uids:
            continue
        try:
            wed_value = water_equivalent_diameter.wed_from_scout_via_uid(study_instance_uid=row.study_instance_uid)
        except Exception as e:
            print(f'Skipping {row.study_instance_uid}: {e}')
            continue
        result = row.to_frame().T.copy()
        result['scout_wed'] = wed_value
        output_data.append(result)
        processed_uids.add(row.study_instance_uid)
        stacked_output = pd.concat(output_data, axis=0)
        stacked_output.to_csv(INTERMEDIATE_OUTPUT_CSV_FN, index=False)

    # Carry forward any partial results not yet in the processed file,
    # in case the script previously crashed before the final save.
    carry_over = partial_df.loc[~partial_df.study_instance_uid.isin(set(done_df.study_instance_uid))]

    stacked_output = pd.concat(output_data, axis=0)
    completed_df = pd.concat([done_df, carry_over, stacked_output], axis=0)
    completed_df.to_csv(PROCESSED_STUDIES_CSV_FN, index=False)