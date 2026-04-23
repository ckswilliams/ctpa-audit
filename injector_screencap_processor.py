
from medphunc.image_analysis import image_utility as iu
from medphunc.pacs import thanks
import pydicom

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from medphunc.parsers.image_capture_parsing import convert_header_rows_to_columns, paddle_results_to_dataframe

from paddleocr import PaddleOCR
from paddleocr import PPStructure,draw_structure_result,save_structure_res
from PIL import Image

from paddleocr.ppstructure.recovery.recovery_to_doc import sorted_layout_boxes
from paddleocr.ppstructure.recovery.recovery_to_markdown import convert_info_markdown

table_engine = PPStructure(show_log=True, lang='en')

# set some file pathes
# File location for csv file containing study instance uids and accession numbers to process.
INPUT_STUDIES_CSV_FN = 'ctpa_accessions_for_processing.csv'

# Output file location, which will be REUSED if we want to rerun the script
# so that we don't end up repeatedly processing the same studies.
PROCESSED_STUDIES_CSV_FN = 'ctpa_injection_data.csv'

# In process output file location, which might be useful if the script is interrupted and you don't want to lose all progress.
INTERMEDIATE_OUTPUT_CSV_FN = 'intermediate_ctpa_injection_data.csv'



def get_first_injection_dicoms(accession_number:str = None,
                        study_instance_uid:str = None,
                              skip_move=False) -> pydicom.Dataset:
    t_series = thanks.Thank.from_study_uid_or_accession(study_instance_uid, accession_number)
    t_series.find()

    injector_rows = t_series.result.loc[lambda x: x.SeriesDescription == 'MEDRAD Injection Images']
    for i in injector_rows.index:
        t_instance = t_series.drill_down(i)
        t_instance.find()
        first_image_index = t_instance.result.loc[lambda x: x.InstanceNumber==1].index[0]
        ds = t_instance.retrieve_or_move_and_retrieve(first_image_index)
        d = ds[0]
        yield d



def process_injector_template_1(d):
    if d.SoftwareVersions not in  ['2024.0101.24025.1112',
                                   '2022.0306.22091.0719',
                                  '2025.0403.25111.1536']:
        print('Function called on a dicom file with software version other than expected. Error may result.')

    ROI = (488, 168, 420, 363)
    im = iu.apply_cv_roi(d.pixel_array, ROI, color_index='last')
    result = table_engine(im)
    df=paddle_results_to_dataframe(result, bin_cols=14, bin_rows=30, do_pivot=True, col_alignment='left')
    df = convert_header_rows_to_columns(df, 2)
    for col in df.columns:
        if 'unavailable' in col:
            return None
    if len(df.columns) != 4:
        raise(ValueError('Wrong number of columns for this software version'))
    df.columns = ['injection_id', 'contrast','saline','total']
    df = df.iloc[:-2,:]
    return df
    
        
def process_injector_template_2(d):
    if d.SoftwareVersions not in ['4.81.21210.1057']:
        print('Function called on a dicom file with software version other than expected. Error may result.')
    adjustment = d.Rows-985
    ROI = (375, 176, 367, 127)
    ROI = list(ROI)
    ROI[3] += adjustment
    im = iu.apply_cv_roi(d.pixel_array, ROI, color_index='last')
    result = table_engine(im)
    df=paddle_results_to_dataframe(result, bin_cols=14, bin_rows=30, do_pivot=True, col_alignment='left')
    df = convert_header_rows_to_columns(df, 1)
    if len(df.columns) != 5:
        raise(ValueError('Wrong number of columns for this software version'))
    df.columns = ['injection_id', 'flow_rate','contrast','saline','total']
    df = df.iloc[:-2,:]
    df = df.loc[~df.injection_id.str.contains('Pause')]
    return df
    
    
software_version_map = {
    '2024.0101.24025.1112':process_injector_template_1,
    '4.81.21210.1057':process_injector_template_2,
    '2022.0306.22091.0719':process_injector_template_1,
    '2025.0403.25111.1536':process_injector_template_1
}

    
def process_contrast_table_results(injections_df):
    out = {}
    injections_df.contrast = pd.to_numeric(injections_df.contrast)
    injections_df.saline = pd.to_numeric(injections_df.saline)
    injections_df.total = pd.to_numeric(injections_df.total)
    
    saline_flushes = injections_df.loc[lambda x: (x.contrast == 0)  & (x.saline > 0),:]
    out['saline_flush'] = {
    'number_of_saline_flushes':saline_flushes.shape[0],
    'total_saline_flushed':saline_flushes.saline.sum()
    }

    contrast_injections =  injections_df.loc[lambda x: (x.contrast >0),:]
    contrast_injections = contrast_injections.reset_index()
    
    
    out['contrast_injection'] = {}
    for i in contrast_injections.index:
        out['contrast_injection'][i]={'contrast':contrast_injections.loc[i,'contrast'],
                                      'saline':contrast_injections.loc[i,'saline']
                                     }
    return pd.json_normalize(out)


def process_injection_for_study(study_instance_uid=None,accession_number=None, skip_move=False):
    dcms = list(get_first_injection_dicoms(study_instance_uid=study_instance_uid,
                                           accession_number=accession_number,
                                           skip_move=skip_move))
    if len(dcms) == 0:
        return
    df_study = []
    dcms = sorted(dcms, key=lambda d: d.SeriesTime)
    for d in dcms:
        injector_template_func = software_version_map.get(d.SoftwareVersions)
        if injector_template_func is None:
            print(f'Unknown software version: {d.SoftwareVersions}. Skipping.')
            continue
        df = injector_template_func(d)
        df_study.append(df)
    df_study = [df for df in df_study if df is not None]
    if not df_study:
        return
    df = pd.concat(df_study, axis=0)
    df = process_contrast_table_results(df)
    return df



if __name__ == '__main__':
    done_df = pd.read_csv(PROCESSED_STUDIES_CSV_FN)
    data_df = pd.read_csv(INPUT_STUDIES_CSV_FN)
    partial_df = pd.read_csv(INTERMEDIATE_OUTPUT_CSV_FN)

    processed_uids = set(done_df.study_instance_uid) | set(partial_df.study_instance_uid)

    # Pre-populate output_data with partial results not yet in the final output,
    # so they are preserved in every intermediate write even if the script crashes.
    carry_over = partial_df.loc[~partial_df.study_instance_uid.isin(set(done_df.study_instance_uid))]
    output_data = [carry_over] if not carry_over.empty else []

    for i in data_df.index:
        row = data_df.loc[i, :]
        if row.study_instance_uid in processed_uids:
            continue
        contrast_summary_df = process_injection_for_study(row.study_instance_uid)
        if contrast_summary_df is not None:
            contrast_summary_df['study_instance_uid'] = row.study_instance_uid
            output_data.append(contrast_summary_df)
            processed_uids.add(row.study_instance_uid)
            stacked_output = pd.concat(output_data, axis=0)
            stacked_output.to_csv(INTERMEDIATE_OUTPUT_CSV_FN, index=False)

    if output_data:
        stacked_output = pd.concat(output_data, axis=0)
        stacked_output = stacked_output.sort_values('contrast_injection.0.contrast', na_position='last').drop_duplicates('study_instance_uid')
        completed_df = pd.concat([done_df, stacked_output])
    else:
        completed_df = done_df
    completed_df.to_csv(PROCESSED_STUDIES_CSV_FN, index=False)

