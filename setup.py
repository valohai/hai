import setuptools

dev_dependencies = [
    'flake8',
    'isort',
    'pydocstyle',
    'pytest-cov',
]

if __name__ == '__main__':
    setuptools.setup(
        name='hai',
        description='Toolbelt library',
        version='0.0.2',
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
    )
