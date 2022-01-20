# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="minavrfpy",
    version="1.0.0",
    description="Mina VRF checker",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bakebrain/minavrfpy",
    author="Jan Backes",
    author_email="bakebrain@gmail.com",
    package_dir={"": "minavrfpy"},
    packages=find_packages(where="minavrfpy"),
    python_requires=">=3.6, <4",
    install_requires=[
        "base58",
        "numpy",
        "orjson",
        "pandas==1.3.4",
        "splitstream",
        "MinaClient@git+https://github.com//bakebrain/coda-python-client@query-sgqlc-refactoring",
    ],
    project_urls={
        "Bug Reports": "https://github.com/bakebrain/minavrfpy/issues",
        "Source": "https://github.com/bakebrain/minavrfpy/",
    },
)
