import os
import shutil

def save_name_file():

    name_files_dir = os.listdir('input')
    name_files_dir.sort()
    name_files_dir_blacklist = None
    name_files_dir_terminals = None
    name_files_dir_transactions = None

    for name in name_files_dir:
        if 'passport_blacklist' in name and name_files_dir_blacklist == None:
            name_files_dir_blacklist = name
        if 'terminals' in name and name_files_dir_terminals == None:    
            name_files_dir_terminals = name
        if 'transactions' in name and name_files_dir_transactions == None:    
            name_files_dir_transactions = name

    return name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions

def trasfer_file(name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions):
    shutil.move(f"input/{name_files_dir_blacklist}", f"archive/{name_files_dir_blacklist}.backup")
    shutil.move(f"input/{name_files_dir_terminals}", f"archive/{name_files_dir_terminals}.backup")
    shutil.move(f"input/{name_files_dir_transactions}", f"archive/{name_files_dir_transactions}.backup")