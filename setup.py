import setuptools
import fastentrypoints

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="csvdiff3",
    version="0.99.7",
    author="Stephen Tweedie",
    author_email="sct@redhat.com",
    description="3-way CSV file merging utility",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sctweedie/csvdiff3",
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'csvmerge3 = csvdiff3.cli_merge3:cli_merge3',
            'csvdiff = csvdiff3.cli_diff2:cli_diff2',
            'csvdiff_git = csvdiff3.cli_diff2:cli_diff2_git',
            'csvhooks = csvdiff3.tools:csvhooks_cli'
        ],
    },
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
