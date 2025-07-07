import pandas as pd
import os
import sys

def convert_parquet_to_csv(input_dir: str, output_csv_path: str):
    """
    Reads all Parquet files from an input directory and combines them into a single CSV.
    """
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.parquet')]

    if not all_files:
        print(f"No Parquet files found in {input_dir}")
        return

    print(f"Found {len(all_files)} Parquet files in {input_dir}. Combining into {output_csv_path}...")

    # Read each parquet file and concatenate
    df_list = []
    for f in all_files:
        try:
            df_list.append(pd.read_parquet(f))
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")
            continue

    if not df_list:
        print("No data could be read from Parquet files.")
        return

    combined_df = pd.concat(df_list, ignore_index=True)

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    # Write to CSV
    try:
        combined_df.to_csv(output_csv_path, index=False)
        print(f"Successfully combined and saved {len(combined_df)} rows to {output_csv_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/parquet_to_csv_converter.py <input_parquet_dir> <output_csv_path>")
        sys.exit(1)

    input_parquet_directory = sys.argv[1]
    output_csv_file_path = sys.argv[2]

    convert_parquet_to_csv(input_parquet_directory, output_csv_file_path) 