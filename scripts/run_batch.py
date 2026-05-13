import glob, os, yaml
from concurrent.futures import ProcessPoolExecutor
from prerocessing import process_tsv_file, data_extract_category
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, yaml_config_path, MAX_WORKERS

def run_batch_processing(input_path):
    """
    This functions has been run single file, before I run it for all files in the directory, 
    I want to make sure that the processing works correctly for one file.
    """
    fileName = os.path.basename(input_path)
    category = data_extract_category(fileName)

    process_tsv_file(
        input_file=input_path,
        output_base=PROCESSED_DATA_PATH
    )

    return category

def save_categories_yaml(categories, output_yaml):
    """
    Save the list of categories to a YAML file.
    """
    data = {
        "categories": sorted(list(set(categories)))
    }
    with open(output_yaml, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)


if __name__ == "__main__":
    input_files = glob.glob(os.path.join(RAW_DATA_PATH, "*.tsv"))
    categories = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for category in executor.map(run_batch_processing, input_files):
            categories.append(category)

    save_categories_yaml(categories, yaml_config_path)
