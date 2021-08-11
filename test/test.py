from t_combo import process_single_file, process_multiple_files

if __name__ == '__main__':
    input_file = "../prep_time_batch_preparation_query.hql"
    output_file = "../prep_time_batch_preparation_query1.hql"
    process_single_file(input_file, output_file)

    config_file = "../config.json"
    process_multiple_files(config_file)
