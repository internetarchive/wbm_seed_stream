import pandas as pd
import os
import sys

def convert_parquet_to_tsv(input_dir: str, output_tsv_path: str):
    if not os.path.isdir(input_dir):
        sys.exit(1)

    all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.parquet')]

    if not all_files:
        return

    df_list = []
    for f in all_files:
        try:
            df_list.append(pd.read_parquet(f))
        except:
            continue

    if not df_list:
        return

    combined_df = pd.concat(df_list, ignore_index=True)
    os.makedirs(os.path.dirname(output_tsv_path), exist_ok=True)
    
    try:
        combined_df.to_csv(output_tsv_path, sep='\t', index=False)
    except:
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)

    input_parquet_directory = sys.argv[1]
    output_tsv_file_path = sys.argv[2]

    convert_parquet_to_tsv(input_parquet_directory, output_tsv_file_path)