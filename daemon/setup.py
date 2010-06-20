import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(
    name = "dbAlerter",
    version = "0.4.0",
    packages = find_packages(),

    install_requires = ['MySQL-python>=1.2.3c1', 'xmpppy>=0.5.0rc1'],

    package_data = {
        'dbalerter': ['images/*.jpg'],
    },

    entry_points = {
        'setuptools.installation': [
            'eggsecutable = dbalerter.dbalerter:bootup',
        ]
    },

    # metadata for upload to PyPI
    author = "Alan Snelson",
    author_email = "Alan@Wave2.org",
    description = "dbAlerter",
    license = "BSD",
    keywords = "dbAlerter Monitor",
    url = "http://www.wave2.org/w2wiki/dbalerter/",
)
