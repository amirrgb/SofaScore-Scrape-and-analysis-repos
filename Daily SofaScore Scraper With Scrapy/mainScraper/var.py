import os
import time
from datetime import datetime
from tqdm import tqdm
import shutil

current_path = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
main_path = current_path + '/datas/'
current_date = str(datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))

# path to the folder everytime the program is run, the data will be stored in a new folder named with date and time
new_path = os.path.join(main_path, current_date + '/')
# path to the folder where the incidents data will be stored
incidents_path = os.path.join(new_path, 'incidents/')
# path to the folder where the lineups data will be stored
lineups_path = os.path.join(new_path, 'lineups/')
# path to the folder where the odds data will be stored
odds_path = os.path.join(new_path, 'odds/')
# path to the folder where the teams data will be stored
next_teams_path = os.path.join(new_path, 'teams/')
# path to the folder where the teams data will be stored
last_teams_path = os.path.join(new_path, 'teams_old/')
# path to the text file where the errors will be stored
error_path = os.path.join(new_path, 'error.txt')
# path to the text file collected match_ids and their corresponding team_ids will be stored
next_match_ids_path = os.path.join(new_path, 'next_match_ids.txt')
last_match_ids_path = os.path.join(new_path, 'last_match_ids.txt')
# path to the text file where the match_ids that are not found will be stored to check every time
not_found_match_ids_path = os.path.join(new_path, 'not_found_match_ids.txt')


def file_handlerer():
    os.makedirs(os.path.dirname(next_teams_path), exist_ok=True)
    os.makedirs(os.path.dirname(last_teams_path), exist_ok=True)
    open(error_path, 'w').close()
    open(next_match_ids_path, 'w').close()
    open(last_match_ids_path, 'w').close()
    os.makedirs(os.path.dirname(incidents_path), exist_ok=True)
    os.makedirs(os.path.dirname(lineups_path), exist_ok=True)
    os.makedirs(os.path.dirname(odds_path), exist_ok=True)
    os.makedirs(os.path.dirname(not_found_match_ids_path), exist_ok=True)


def validate_proccess():
    datas_folder = main_path
    datas_folder_last_folder = os.listdir(datas_folder)[-1]  # get the last folder in the datas folder
    # rename the last folder with adding _old to the end of the folder name
    os.rename(os.path.join(datas_folder, datas_folder_last_folder),
              os.path.join(datas_folder, datas_folder_last_folder + '_valid'))


def clear_invalid_folders():
    print("Clearing invalid folders...")
    datas_folder = main_path
    with tqdm(total=len(os.listdir(datas_folder))) as pbar:
        for folder in os.listdir(datas_folder):
            if not folder.endswith('_valid'):
                delete_directory(os.path.join(datas_folder, folder))
            pbar.update(1)

def delete_directory(path):
    """Recursively deletes a directory and all its contents."""
    if os.path.isdir(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        os.rmdir(path)

