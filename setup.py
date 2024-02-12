from setuptools import setup, find_packages

setup(
    name="scout",
    version="0.0.1",
    description="A scout for your filesystem!",
    author="Marcus Grant",
    author_email="marcusfg@pm.me",
    packages=find_packages(),
    install_requires=[
        # List project dependencies
        "attrs",
        "sqlalchemy",
    ],
    classifiers=[
        # Classifiers help users find your project by category
        # For a list of valid classifiers, see https://pypi.org/classifiers/
        "Development Status :: 1 - Planning",
        "Environment :: Console",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.11",
        "Framework :: AsyncIO",
    ],
    python_requires=">=3.11.1",
)
