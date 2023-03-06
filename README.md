# hai — Utility Library for Python

`hai` is a collection of helpful Python snippets used at [Valohai](https://valohai.com/).

# Usage

```bash
pip install hai

python
>>> import hai; print(hai.__version__)
```

# Development

Installing editable library version in the current virtual environment.

```bash
# development dependencies require Python 3.8+
pip install -e . -r requirements-lint.txt -r requirements-test.txt
pytest

python
>>> import hai; print(hai.__version__)
```

# Releases

The library is released to both GitHub Releases (https://github.com/valohai/hai) 
and PyPI (https://pypi.org/project/hai/) simultaneously.
