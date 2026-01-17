import os
import paramiko
import pandas as pd
import re
import math
from datetime import datetime, timedelta

# ------------------------- Config ------------------------- #
HOST = "172.20.122.231"
USER = "iccsadmin"
PASSWORD = "Xs0a0@bdpkgo"
REMOTE_BASE = "/home/iccsadmin/Portal_Data"

LOCAL_UPLOADS_BASE = "media/uploads"
LOCAL_PROCESSED_BASE = "media/processed"
LOCAL_MAP = "media/map/map.csv"
LOCAL_ID_MAP = "media/map/id.csv"
FAIL_BASE = "media/fail"

os.makedirs(os.path.dirname(LOCAL_MAP), exist_ok=True)
os.makedirs(FAIL_BASE, exist_ok=True)

# ------------------------- Utility Functions ------------------------- #
def time_to_seconds(time_str):
    """Convert HH:MM:SS string to total seconds."""
    if isinstance(time_str, str):
        try:
            h, m, s = map(int, time_str.split(":"))
            return h * 3600 + m * 60 + s
        except ValueError:
            return 0
    return 0

def load_mapping():
    """Load mapping file into a dictionary."""
    mapping = {}
    if os.path.exists(LOCAL_MAP):
        df = pd.read_csv(LOCAL_MAP, header=None, names=["Dir", "EmpCode", "Formula"])
        for _, row in df.iterrows():
            mapping[row["Dir"]] = {
                "EmpCode": row["EmpCode"],
                "Formula": row["Formula"]
            }
    else:
        print(f"⚠️ Mapping file not found: {LOCAL_MAP}")
    return mapping

def load_id_map():
    """Load email -> ATS mapping."""
    id_map = {}
    if os.path.exists(LOCAL_ID_MAP):
        try:
            df = pd.read_csv(LOCAL_ID_MAP, header=None, names=["Email", "ATS"], encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(LOCAL_ID_MAP, header=None, names=["Email", "ATS"], encoding="latin1")
        id_map = dict(zip(df["Email"], df["ATS"]))
    else:
        print(f"⚠️ ID mapping file not found: {LOCAL_ID_MAP}")
    return id_map

# ------------------------- File Processing ------------------------- #
def process_file(local_file, dir_name, mapping, id_map):
    """Process a CSV file according to the mapping."""
    if dir_name not in mapping:
        print(f"⚠️ No mapping found for {dir_name}, skipping {local_file}")
        return

    try:
        df = pd.read_csv(local_file)
    except Exception as e:
        print(f"❌ Error reading {local_file}: {e}")
        return

    emp_col = mapping[dir_name]["EmpCode"]
    formula = mapping[dir_name]["Formula"]

    if emp_col not in df.columns:
        print(f"⚠️ Column {emp_col} not found in {local_file}, skipping")
        return

    if "Raw Date" not in df.columns:
        print(f"⚠️ 'Raw Date' column missing in {local_file}, skipping")
        return

    # ---------------- Convert time columns to seconds ---------------- #
    try:
        # cols_in_formula = [c.strip() for c in re.split(r'[+-]', formula)]
        # for col in cols_in_formula:
        #     if col in df.columns:
        #         df[col + "_sec"] = df[col].apply(time_to_seconds)
        #     else:
        #         df[col + "_sec"] = 0

        # formula_sec = formula
        # for col in cols_in_formula:
        #     col_sec = col + "_sec"
        #     if re.search(r'\W', col):
        #         formula_sec = formula_sec.replace(col, f"`{col_sec}`")
        #     else:
        #         formula_sec = formula_sec.replace(col, col_sec)

        # --- Improved formula handling ---
        # Detect all valid column names inside the formula (alphanumeric + underscore)
        cols_in_formula = re.findall(r'\b[A-Za-z_][A-Za-z0-9_ ]*\b', formula)


        # Convert time columns to seconds
        for col in cols_in_formula:
            col_clean = col.strip()
            if col_clean in df.columns:
                df[col_clean + "_sec"] = df[col_clean].apply(time_to_seconds)
            else:
                df[col_clean + "_sec"] = 0

        # Safely replace column names in formula with their "_sec" versions
        formula_sec = formula
        for col in cols_in_formula:
            col_clean = col.strip()
            formula_sec = re.sub(
                rf'\b{re.escape(col_clean)}\b',
                f'`{col_clean}_sec`',
                formula_sec
            )

        df["TotalSeconds"] = df.eval(formula_sec)
        df["Minutes"] = (df["TotalSeconds"] / 60).apply(math.ceil)

    except Exception as e:
        print(f"❌ Error applying formula '{formula}' on {local_file}: {e}")
        return

    # ---------------- Ensure EmpCode in ATS format ---------------- #
    # def map_empcode(val):
    #     if str(val).startswith("ATS"):
    #         return val
    #     return id_map.get(val, None)

    def map_empcode(val):
        val = str(val).strip()

        # Extract the ATS ID (anything like atsXXXXX)
        match = re.search(r'(ats\d+)', val, re.IGNORECASE)
        if match:
            ats = match.group(1).upper()  # Convert to ATS12345
            return ats

        # Fallback to map file
        return id_map.get(val, None)


    df["EmpCode_Mapped"] = df[emp_col].apply(map_empcode)

    # Rows without valid ATS code
    fail_rows = df[df["EmpCode_Mapped"].isna()]
    if not fail_rows.empty:
        fail_dir = os.path.join(FAIL_BASE, dir_name)
        os.makedirs(fail_dir, exist_ok=True)
        fail_file = os.path.join(fail_dir, os.path.basename(local_file))
        fail_rows.to_csv(fail_file, index=False)
        print(f"⚠️ Rows with unmapped EmpCode saved to: {fail_file}")

    # Keep only rows with valid ATS
    df_valid = df[df["EmpCode_Mapped"].notna()]

    # ---------------- Save processed file ---------------- #
    result = pd.DataFrame({
        "EmpCode": df_valid["EmpCode_Mapped"],
        "Date": df_valid["Raw Date"],
        "Minutes": df_valid["Minutes"],
        "IsWH": "N"
    })

    processed_dir = os.path.join(LOCAL_PROCESSED_BASE, dir_name)
    os.makedirs(processed_dir, exist_ok=True)
    processed_file = os.path.join(processed_dir, os.path.basename(local_file))
    result.to_csv(processed_file, index=False)
    print(f"✅ Processed saved: {processed_file}")

# ------------------------- Main Script ------------------------- #
def main():
    mapping = load_mapping()
    id_map = load_id_map()
    if not mapping:
        print("❌ No mapping found. Exiting.")
        return

    yesterday = (datetime.now() - timedelta(days=5)).date()

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, username=USER, password=PASSWORD)
        sftp = ssh.open_sftp()

        for dir_name in mapping.keys():
            remote_subdir = f"{REMOTE_BASE}/{dir_name}/APR_Clean"
            try:
                files = sftp.listdir(remote_subdir)
            except FileNotFoundError:
                print(f"⚠️ Skipping {remote_subdir}: No such directory")
                continue

            for filename in files:
                if not filename.endswith(".csv"):
                    continue

                remote_file = f"{remote_subdir}/{filename}"
                try:
                    attr = sftp.stat(remote_file)
                    file_mtime = datetime.fromtimestamp(attr.st_mtime).date()
                    # print(f"Checking file: {remote_file}, mtime={file_mtime}, expected={yesterday}")
                except Exception as e:
                    print(f"❌ Could not get file info for {remote_file}: {e}")
                    continue

                if file_mtime != yesterday:
                    continue  # Skip files not from yesterday

                local_dir = os.path.join(LOCAL_UPLOADS_BASE, dir_name)
                os.makedirs(local_dir, exist_ok=True)
                local_file = os.path.join(local_dir, filename)

                print(f"⬇️ Fetching {remote_file} to {local_file}")
                try:
                    sftp.get(remote_file, local_file)
                except Exception as e:
                    print(f"❌ Failed to fetch {remote_file}: {e}")
                    continue

                process_file(local_file, dir_name, mapping, id_map)

        sftp.close()
        ssh.close()

    except Exception as e:
        print(f"❌ SSH connection failed: {e}")

if __name__ == "__main__":
    main()
