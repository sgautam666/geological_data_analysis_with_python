import requests
import re
import os
import time
from io import StringIO
import pandas as pd 


def download_kgs_ascii_data(output_dir="data"):
    """
    Downloads all ASCII data files from the KGS page into the specified folder.

    Parameters:
        output_dir (str): The folder to save the downloaded files. Defaults to 'data'.
    """
    # Page with file list
    page_url = "https://www.kgs.ku.edu/Mathgeo/Books/Stat/ascii.html"
    # Correct download base
    download_base = "https://www.kgs.ku.edu/Mathgeo/Books/Stat/ASCII/"

    # Create output folder if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Fetch page
    response = requests.get(page_url)
    response.raise_for_status()

    # Extract filenames (ASCII .TXT files)
    txt_files = re.findall(r'\b[A-Z0-9_]+\.TXT\b', response.text)
    txt_files = list(set(txt_files))  # remove duplicates

    print(f"Found {len(txt_files)} files.")

    # Download each file
    for file_name in txt_files:
        file_url = download_base + file_name
        save_path = os.path.join(output_dir, file_name)

        if os.path.exists(save_path):
            print(f"Skipping (exists): {file_name}")
            continue

        try:
            r = requests.get(file_url)
            r.raise_for_status()
            with open(save_path, "wb") as f:
                f.write(r.content)
            print(f"Downloaded: {file_name}")
            time.sleep(0.2)  # polite pause between requests

        except Exception as e:
            print(f"Failed: {file_name} ({e})")

    print(f"Done. All files are saved in '{output_dir}'.")


# Read a data from link
def load_kgs_data_from_link(name, save_dir="data", force_download=False):
    """
    Load a KGS ASCII dataset into a pandas DataFrame.

    Parameters:
        name (str): Dataset name (e.g., 'ABOC' or 'ABOC.TXT')
        save_dir (str): Directory to store downloaded files
        force_download (bool): If True, re-download even if file exists

    Returns:
        pd.DataFrame
    """

    # Normalize filename
    if not name.upper().endswith(".TXT"):
        filename = name.upper() + ".TXT"
    else:
        filename = name.upper()

    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, filename)

    # Download if needed
    if force_download or not os.path.exists(filepath):
        url = f"https://www.kgs.ku.edu/Mathgeo/Books/Stat/ASCII/{filename}"
        response = requests.get(url)
        response.raise_for_status()

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)

    # Read file content
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Try different parsing strategies
    for sep in [",", "\t", r"\s+"]:
        try:
            df = pd.read_csv(StringIO(content), sep=sep, engine="python")
            if df.shape[1] > 1:  # reasonable parse
                return df
        except Exception:
            continue

    # Fallback: single column
    return pd.DataFrame({"raw": content.splitlines()}) 


### Load locally saved data
def load_kgs_local(name, data_dir="data"):
    """
    Load a KGS ASCII dataset from local directory into pandas.

    Parameters:
        name (str): Dataset name (e.g., 'ABOC' or 'ABOC.TXT')
        data_dir (str): Directory where files are stored

    Returns:
        pd.DataFrame
    """

    # Normalize filename
    if not name.upper().endswith(".TXT"):
        filename = name.upper() + ".TXT"
    else:
        filename = name.upper()

    filepath = os.path.join(data_dir, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"{filepath} not found")

    # Try multiple parsing strategies
    for sep in [",", "\t", r"\s+"]:
        try:
            df = pd.read_csv(filepath, sep=sep, engine="python")

            # Heuristic: if it produces multiple columns, it's probably correct
            if df.shape[1] > 1:
                return df

        except Exception:
            continue

    # Fallback: read as raw lines
    with open(filepath, "r") as f:
        lines = f.readlines()

    return pd.DataFrame({"raw": [line.strip() for line in lines]})