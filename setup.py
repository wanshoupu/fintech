from setuptools import setup, find_packages

# Load requirements from file
with open("requirements.in") as f:
    requirements = f.read().splitlines()

setup(
    name="fintech",
    version="0.1.0",
    packages=find_packages(),
    install_requires=requirements,
)
