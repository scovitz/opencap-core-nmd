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
sys.path.append(os.path.abspath('./..'))
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

downloadFolder = '/Users/sydneycovitz/CLEAN_NMD_DS/'

trial_df = pd.read_csv('/Users/sydneycovitz/NMD_OpenCap/csvs/trial_info_latest.csv')

# build clean_trials dictionary
clean_sids = []
clean_rows = []
for row in range(len(trial_df)):
    if not pd.isna(trial_df.loc[row, "trial_clean"]):
        if 'test' not in trial_df.loc[row, 'pid'] and 'Test' not in trial_df.loc[row, 'pid']:
            if 'noID' not in trial_df.loc[row, 'pid'] and 'noDIGBI' not in trial_df.loc[row, 'pid']:
                clean_rows.append(trial_df.loc[row])
                clean_sids.append(trial_df.loc[row, 'sid'])

clean_df = pd.DataFrame(clean_rows)

clean_sids_trials = clean_df.groupby("sid")["trial"].apply(list).to_dict()

# Name downloaded folder with subject name from web app (True), or the session_id (False)
useSubjectIdentifierAsFolderName = False

# checking for missing sessions from download
root_ds = '/Users/sydneycovitz/CLEAN_NMD_DS/'

missing_sids_from_download = False
for sid in clean_sids_trials:
    if not Path(root_ds + sid).exists():
        print(sid)
        missing_sids_from_download = True
print("Missing sids form download: ", missing_sids_from_download)

# checking for missing trials from download 
missing_trials_from_download = False
sids_missing_trials_from_download = {}
for sid in clean_sids_trials:
    for trial in clean_sids_trials[sid]:
        if type(trial) != str: 
            continue
        trc_path = '/MarkerData/PostAugmentation/' + trial
        mot_path = '/OpenSimData/Kinematics/' + trial + '.mot'
        vid_path = '/Videos/Cam0/InputMedia/' + trial
        if not Path(root_ds + sid + trc_path).exists():
            print(root_ds + sid + trc_path)
            missing_trials_from_download = True
            if sid not in sids_missing_trials_from_download: 
                sids_missing_trials_from_download[sid] = []
            if trial not in sids_missing_trials_from_download[sid]:
                sids_missing_trials_from_download[sid].append(trial)
        if not Path(root_ds + sid + mot_path).exists():
            print(mot_path)
            missing_trials_from_download = True
            if sid not in sids_missing_trials_from_download: 
                sids_missing_trials_from_download[sid] = []
            if trial not in sids_missing_trials_from_download[sid]:
                sids_missing_trials_from_download[sid].append(trial)
        if not Path(root_ds + sid + vid_path).exists():
            print(vid_path)
            missing_trials_from_download = True
            if sid not in sids_missing_trials_from_download: 
                sids_missing_trials_from_download[sid] = []
            if trial not in sids_missing_trials_from_download[sid]:
                sids_missing_trials_from_download[sid].append(trial)


# Name downloaded folder with subject name from web app (True), or the session_id (False)
useSubjectIdentifierAsFolderName = False

session_ids = sids_missing_trials_from_download.keys()

#%% Processing
start_time = time.time()

# Batch download. 
for session_id in session_ids:
    print(f'Downloading session id {session_id}')
    try:
        utils.downloadAndZipSession_cleanTrialsOnly(session_id, sids_missing_trials_from_download[session_id], justDownload=True, data_dir=downloadFolder,
                                    useSubjectNameFolder=useSubjectIdentifierAsFolderName,
                                    include_pose_pickles=False)

    except Exception as e:
        print(f"Error downloading session {session_id}: {e}")

