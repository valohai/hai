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
        version='0.0.1',
        url='https://github.com/valohai/hai',
        author='Valohai',
        maintainer='Aarni Koskela',
        maintainer_email='akx@iki.fi',
        license='MIT',
        install_requires=[],
        tests_require=dev_dependencies,
        extras_require={'dev': dev_dependencies},
        packages=setuptools.find_packages('.'),
        include_package_data=True,
    )
