from setuptools import setup, find_packages

setup(
    name='pltLib',
    author="Jean-Marie Bertoncelli",
    author_email="xxxx@yyy.com",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/jmbertoncelli/pltLib",
    include_package_data=True,
    version='1.0',
    packages=find_packages(exclude=[]),
    install_requires=[
        # list your project's dependencies here
        'cryptography>=42.0.5',
        'streamsets>=6.2.0',
        'chardet>=4.0.0'
    ],
    extras_requires=[
        # list your project's dependencies here
        'twine>=5.0.0'
    ]
)
