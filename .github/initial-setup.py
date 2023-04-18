import os
import shutil
import re
import sys
import json


def replace_in_file(filename, mapping):
    print('==> Editing '+filename)
    infile = open(filename)
    outfile = open(filename+'.tmp', 'w+')
    for line in infile:
        for k, v in mapping.items():
            line = line.replace(k, v)
        outfile.write(line)
    infile.close()
    outfile.close()
    shutil.copyfile(filename+'.tmp', filename)
    os.remove(filename+'.tmp')
    os.system(f'git add {filename}')

def list_files_to_edit(root, extensions,
        exclude_directories=[],
        exclude_files=[]):
    result = []
    for dirname, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename in exclude_files:
                continue
            for ext in extensions:
                if filename.endswith(ext):
                    result.append(os.path.join(dirname, filename))
                    break
        for d in exclude_directories:
            if d in dirnames:
                dirnames.remove(d)
    return result

def rename_files_and_directories(root, extensions,
        mapping,
        exclude_directories=[],
        exclude_files=[]):
    for dirname, dirnames, filenames in os.walk(root):
        # exclude directories
        for d in exclude_directories:
            if d in dirnames:
                dirnames.remove(d)
        # rename folders
        names_to_changes = {}
        for subdirname in dirnames:
            new_name = subdirname
            for k, v in mapping.items():
                new_name = new_name.replace(k, v)
            if new_name != subdirname:
                print("==> Renaming "+os.path.join(dirname, subdirname)+" into "+os.path.join(dirname, new_name))
                os.system(f'git mv {os.path.join(dirname, subdirname)} {os.path.join(dirname, new_name)}')
                #shutil.move(os.path.join(dirname, subdirname),
                #            os.path.join(dirname, new_name))
                dirnames.remove(subdirname)
                dirnames.append(new_name)
        # rename files
        for filename in filenames:
            if filename in exclude_files:
                continue
            for ext in extensions:
                if filename.endswith(ext):
                    new_name = filename
                    for  k, v in mapping.items():
                        new_name = new_name.replace(k, v)
                    if new_name != filename:
                        print("==> Renaming "+os.path.join(dirname, filename)+" into "+os.path.join(dirname, new_name))
                        os.system(f'git mv {os.path.join(dirname, filename)} {os.path.join(dirname, new_name)}')
                        #os.rename(os.path.join(dirname, filename),
                        #          os.path.join(dirname, new_name))
                    break # don't try the next extension for this file


if __name__ == '__main__':
    with open('initial-setup.json') as f:
        info = json.loads(f.read())
    service_name = info['service_name']
    if(not re.match('[a-zA-Z_][a-zA-Z\d_]*', service_name)):
        print("Error: service name must start with a letter and consist of letters, digits, or underscores")
        sys.exit(-1)
    resource_name = info['resource_name']
    if(not re.match('[a-zA-Z_][a-zA-Z\d_]*', resource_name)):
        print("Error: resource name must start with a letter and consist of letters, digits, or underscores")
        sys.exit(-1)
    mapping = {
        'alpha' : service_name,
        'ALPHA' : service_name.upper(),
        'Alpha' : service_name.capitalize(),
        'resource' : resource_name,
        'RESOURCE' : resource_name.upper(),
        'Resource' : resource_name.capitalize()
    }
    files_to_edit = list_files_to_edit('.',
        extensions=['.cpp', '.h', '.hpp', '.txt', '.in'],
        exclude_directories=['.git', '.github', 'build', '.spack-env', 'munit'],
        exclude_files=['uthash.h'])
    for f in files_to_edit:
        replace_in_file(f, mapping)
    rename_files_and_directories('.',
        extensions=['.cpp', '.h', '.hpp', '.txt', '.in'],
        mapping=mapping,
        exclude_directories=['.git', '.github', 'build', '.spack-env', 'munit'],
        exclude_files=['uthash.h'])
    os.system('git rm .github/initial-setup.py')
    os.system('git rm initial-setup.json')
    os.system('git rm .github/workflows/setup.yml')
    os.system('git rm COPYRIGHT')
    with open('README.md', 'w+') as f:
        f.write(f'Your project "{service_name}" has been setup!\n Enjoy programming with Mochi!')
