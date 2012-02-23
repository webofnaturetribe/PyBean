import os
from distutils.core import setup
import pybean
# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
        return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
        name="pybean",
        version=pybean.__version__,
        description="Python implementation of RedBeanPHP, easy to use ORM.",
        author=pybean.__author__,
        author_email=pybean.__email__,
        py_modules=["pybean"],
        long_description=read('README.md'),
        classifiers=[
            "Development Status :: 2 - Pre-Alpha",
            "Intended Audience :: Developers",
            "Topic :: Database :: Front-Ends",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python",
            ]
)
