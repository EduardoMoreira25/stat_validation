import shutil
import os

def zip_directory(source_dir, output_filename):
    """
    Zips a source directory into a specified file path.

    Args:
        source_dir (str): The directory to be zipped (e.g., './reports/bulk').
        output_filename (str): The desired name and path for the zip file 
                                (e.g., './reports/zipped_reports').
                                The '.zip' extension is added automatically.
    """
    try:
        # Check if the source directory exists
        if not os.path.isdir(source_dir):
            print(f"Error: Source directory not found at '{source_dir}'")
            return

        # shutil.make_archive(base_name, format, root_dir)
        # - base_name: The name of the resulting archive file (without the format extension)
        # - format: The archive format, like 'zip', 'tar', 'gztar', etc.
        # - root_dir: The directory that will be archived.
        shutil.make_archive(
            base_name=output_filename,
            format='zip',
            root_dir=source_dir
        )
        print(f"✅ Successfully zipped '{source_dir}' to '{output_filename}.zip'")
        
    except Exception as e:
        print(f"❌ An error occurred during zipping: {e}")

# --- Configuration ---
# The directory you want to zip (your ./reports/bulk)
SOURCE_DIR = './reports/bulk' 

# The desired output path and base name (your ./reports/zipped_reports)
# Note: 'shutil' automatically appends the '.zip' extension.
OUTPUT_BASE_NAME = './reports/zipped_reports' 
# ---

if __name__ == "__main__":
    zip_directory(SOURCE_DIR, OUTPUT_BASE_NAME)