# This code file is used to present the information about our package and by specifying the requirements.txt file in the setup.py file by install this file the requirements will also get installed

from setuptools import find_packages, setup
from typing import List

HYPEN_E_DOT = '-e .' # It is a flag that specifies to install the things in editable mode

def get_requirements(file_path: str) -> List[str]:
    '''
    this function will return the list of requirements
    '''
    requirements = []
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n", "") for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)

    return requirements


setup(
    name='Movie_Recommendation_Project',
    version='0.0.1',
    author='Yuvraj Singh',
    author_email='ys7233831@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)