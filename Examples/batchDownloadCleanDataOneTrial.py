"""
---------------------------------------------------------------------------
OpenCap: batchDownloadData.py
---------------------------------------------------------------------------

Copyright 2022 Stanford University and the Authors

Author(s): Scott Uhlrich, Antoine Falisse

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


This script dowloads data from multiple sessions from the database.


You will need login credentials from app.opencap.ai to run this script. You 
can quickly create credentials from the home page: https://app.opencap.ai. 
We recommend first running the Examples/createAuthenticationEnvFile.py script
prior to running this script. Otherwise, you will need to to login every time 
you run the script.
"""

# %% Imports

import sys
import os
import time
preferred_utils_path = os.path.abspath('Users/yanw/opencap-core-nmd/')  # Adjust this path to your opencap-core-nmd directory
# Add the preferred path to the beginning of sys.path to prioritize it
sys.path.insert(0, preferred_utils_path)
import utils 
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import subprocess
# %% User inputs

# Edited by Sydney Covitz

# Sessions to download. List of strings of identifiers (36-character) string
# at the end of the session url app.opencap.ai/session/<session_id>

#with open("/Users/sydneycovitz/NMD_OpenCap/missing_sessions.txt", "r") as f:
#    session_ids = [line.strip() for line in f]

# session_ids = [
#               'f276bd7d-f602-4740-a0c8-bffcdfd56d46', 
#               'ccca82e1-5dac-43a2-96df-20b9be210020'
#             ] # list of session identifiers as strings

# Local path to download sessions to
#downloadFolder = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','Data'))

downloadFolder = '/Users/yanw/NMD_OpenCap/cs230_NMD/CLEAN_NMD_DS/'

trial_df = pd.read_csv('/Users/yanw/NMD_OpenCap/cs230_NMD/trial_info_latest.csv')
trial_df = trial_df.iloc[slice(0, 200, None), slice(None, None, None)]  # for testing, limit to first 200 rows

# build clean_trials dictionary
clean_sids = []
clean_rows = []
for row in range(len(trial_df)):
        if not pd.isna(trial_df.loc[row, "trial_clean"]):
            if '5xsts' in trial_df.loc[row, 'trial_clean']:        #IMPORTANT: specify which one trial task to extract from trial_clean column
                if 'test' not in trial_df.loc[row, 'pid'] and 'Test' not in trial_df.loc[row, 'pid']:
                    if 'noID' not in trial_df.loc[row, 'pid'] and 'noDIGBI' not in trial_df.loc[row, 'pid']:
                        clean_rows.append(trial_df.loc[row])
                        clean_sids.append(trial_df.loc[row, 'sid'])

clean_df = pd.DataFrame(clean_rows)
print(clean_df)
clean_sids_trials = clean_df.groupby("sid")["trial"].apply(list).to_dict()

session_ids = set(clean_sids)

# Name downloaded folder with subject name from web app (True), or the session_id (False)
useSubjectIdentifierAsFolderName = False

# %% Processing

def download_one(session_id):
    if not Path('/Users/yanw/NMD_OpenCap/cs230_NMD/CLEAN_NMD_DS/' + session_id).exists():
        print(f'Downloading session id {session_id}')
        try:
            utils.downloadAndZipSession_cleanTrialsOnly(session_id, clean_sids_trials[session_id], justDownload=True, data_dir=downloadFolder,
                                        useSubjectNameFolder=useSubjectIdentifierAsFolderName,
                                        include_pose_pickles=False)
            return True
        except Exception as e:
            print(f"Error downloading session {session_id}: {e}")
            return False
    else:
        print(f"Skipping {session_id} (already exists)")
        return True

start_time = time.time()

# Run parallel downloads (safe for I/O-bound tasks)
MAX_WORKERS = 6   
success_count = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_one, sid) for sid in session_ids]
    for fut in as_completed(futures):
        try:
            if fut.result():
                success_count += 1
        except Exception as e:
            print(f"Unexpected error: {e}")

end_time = time.time()
elapsed = end_time - start_time

print(f"\n Finished all downloads in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes).")
print(f" Successfully downloaded {success_count}/{len(session_ids)} sessions.")

# move session directory into correct format (or reformat later?)



# %%
