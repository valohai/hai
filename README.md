# hai — Utility Library for Python

`hai` is a collection of helpful Python snippets used at [Valohai](https://valohai.com/).

# Usage

```bash
pip install hai

python
>>> import hai; print(hai.__version__)
```

# Development

Installing editable library version in the current virtual environment for development.

```bash
# development dependencies require Python 3.8+
pip install -e . -r requirements-test.txt pre-commit && pre-commit install

# if you want to manually run lints...
pre-commit run --all-files

# if you want to run tests...
pytest

# if you want to try it out...
python
>>> import hai; print(hai.__version__)
```

# Releases

The library is released to both GitHub Releases (https://github.com/valohai/hai)
and PyPI (https://pypi.org/project/hai/) simultaneously.
