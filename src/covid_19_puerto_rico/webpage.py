import glob
import itertools
import logging
import os
from pathlib import Path
import shutil
from wand.image import Image

class Website:
    def __init__(self, args):
        self.template_dir = args.template_dir
        self.source_material_dir = args.source_material_dir
        self.output_dir = args.output_dir


def generate_webpage(template_dir, source_material_dir, output_dir, bulletin_date):
    destination = Path(f'{output_dir}/{bulletin_date}')
    destination.mkdir(exist_ok=True)

    source_material_subdir = f'{source_material_dir}/{bulletin_date}'
    for png in glob.iglob(f'{source_material_subdir}/*.png'):
        logging.info('Copying %s to %s', png, destination)
        shutil.copy(png, destination)

    for jpg in itertools.chain(glob.iglob(f'{source_material_subdir}/*.jpg'),
                               glob.iglob(f'{source_material_subdir}/*.jpeg')):
        logging.info('Copying %s to a PNG in %s', jpg, destination)
        copy_to_png(jpg, destination)

    for dirpath, dirnames, filenames in os.walk(template_dir):
        for dirname in dirnames:
            destination_subdir = f'{destination}/{dirpath}/{dirname}'
            logging.info("Making subdirectory: %s", destination_subdir)
            Path(destination_subdir).mkdir(exist_ok=True)

        for filename in filenames:
            logging.info("Copying file: dirpath = %s, filename = %s", dirpath, filename)
            shutil.copyfile(f'{dirpath}/{filename}',
                            f'{destination}/{filename}')


def copy_to_png(origin, destination):
    (base, extension) = analyze_path(origin)
    if (extension.lower() == 'png'):
        shutil.copy(origin, destination)
    else:
        with Image(filename=origin) as original:
            with original.convert('png') as converted:
                converted.save(filename=f'{destination}/{base}.png')

def analyze_path(path):
    """Split a path into (dir_path, base_file_name, extension)"""
    root, ext = os.path.splitext(path)
    root, base = os.path.split(root)
    return (base, ext)