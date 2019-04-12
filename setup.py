import ast
import os
import re
import setuptools

with open(os.path.join(os.path.dirname(__file__), 'hai', '__init__.py')) as infp:
    version = ast.literal_eval(re.search('__version__ = (.+?)$', infp.read(), re.M).group(1))

dev_dependencies = [
    'flake8',
    'isort',
    'pydocstyle',
    'pytest-cov',
    'moto',
    'boto3',
]

if __name__ == '__main__':
    setuptools.setup(
        name='hai',
        description='Toolbelt library',
        version=version,
        url='https://github.com/valohai/hai',
        author='Valohai',
        author_email='hait@valohai.com',
        maintainer='Aarni Koskela',
        maintainer_email='akx@iki.fi',
        license='MIT',
        install_requires=[],
        tests_require=dev_dependencies,
        extras_require={'dev': dev_dependencies},
        packages=setuptools.find_packages('.', exclude=('hai_tests', 'hai_tests.*',)),
        include_package_data=True,
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
    )
