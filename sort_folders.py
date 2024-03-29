import os
import shutil
import datetime

def name_folder(name):
    for root, dirs, files in os.walk(name):
        try:
            return dirs[0]
        except IndexError:
            pass


def sort_folders(name_dest_folder):
    with os.scandir("lib\\temp_folder") as files:

        src_path = ('t2_mobile', 'megafon', 'beeline')
        date_fmt = datetime.datetime.now()
        final_folder = f"__{date_fmt.date()}_{date_fmt.hour}_{date_fmt.minute}_{date_fmt.second}"
        os.mkdir(f'Результат\{name_dest_folder}_{final_folder}')

        subdir = [file.name for file in files if file.is_dir()]

        for src in src_path:
            os.mkdir(f'Результат\{name_dest_folder}_{final_folder}\{src}')
            [shutil.move(f'lib\\temp_folder\{t}', f'Результат\{name_dest_folder}_{final_folder}\{src}\{t}') for t in subdir if f'{src}' in t]
