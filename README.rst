================
scottbrian-algo1
================

Intro
=====

The algo1 project interfaces with IBAPI. The algo_api.py module has the AlgoApp class that inherits from the IBAPI
EWrapper and EClient classes, and methods to connect to IBAPI, send requests, and received replies.


>>> from scottbrian_algo1.algo_api import AlgoApp
>>> algo_app = AlgoApp()
>>> print(algo_app)
AlgoApp()


.. image:: https://img.shields.io/badge/security-bandit-yellow.svg
    :target: https://github.com/PyCQA/bandit
    :alt: Security Status

.. image:: https://readthedocs.org/projects/pip/badge/?version=stable
    :target: https://pip.pypa.io/en/stable/?badge=stable
    :alt: Documentation Status


The algo_api.py module contains:

1. AlgoApp class with function to connect to IBAPI, request financial instrument information, and make trades.



Installation
============

Windows:

The following instructions are for the latest version of ibapi at this time, meaning version 10.24.1. The
scottbrian_algo1 project may be updated for newer ibapi versions as needed.

    1) You will need the ibapi package, and *not* from PyPI using pip install. Instead, go to the Interactive Brokers
       site to find it, download it, and install it. By default it should go into: C:/TWS API
    2) create a virtual environment for your project
    3) Open a terminal in PyCharm for your project and get into directory: C:/TWS API/source/pythonclient
    4) run: py -m pip install --upgrade pip build setuptools wheel
    5) run: py -m build
    6) You should now have a new dist folder and two new files in C:/TWS API/source/pythonclient/dist
       a) ibapi-10.24.1.tar.gz
       b) ibapi-10.24.1-py3-none-any.whl
    7) get into your virtual environment folder
    8) run ``pip install scottbrian-algo1``

Note that the scottbrian_algo1 pyproject.toml file [project] section has a dependencies specification for:
"ibapi @ file://localhost/TWS%20API/source/pythonclient/dist/ibapi-10.24.1-py3-none-any.whl"

To install ibapi into venv312:
    1) get into venv312 folder
    2) run: py -m pip install file:///C:/TWS%20API/source/pythonclient/dist/ibapi-10.24.1-py3-none-any.whl
    3) in pyproject.toml dependencies section add line:
       "ibapi @ file://localhost/TWS%20API/source/pythonclient/dist/ibapi-10.24.1-py3-none-any.whl",

To install beta versions of paratools for testing:
    1) get into venv312 folder
    2) run: py -m pip install file:///C:/Users/Tiger/PycharmProjects/scottbrian_paratools/dist/scottbrian_paratools-1.0.0-py3-none-any.whl
    3) in pyproject.toml dependencies section, comment out the official line and add temporary file line:
           commented out line:
               # "scottbrian_paratools ~=1.0",
           add temporary line:
               "scottbrian_paratools @  file://C:/Users/Tiger/PycharmProjects/scottbrian_paratools/dist/scottbrian_paratools-1.0.0-py3-none-any.whl",

To install beta versions of utils for testing:
    1) get into venv312 folder
    2) run: py -m pip install file:///C:/Users/Tiger/PycharmProjects/scottbrian_utils/dist/scottbrian_utils-4.1.0-py3-none-any.whl
       or
       run: py -m pip install --upgrade --force-reinstall file:///C:/Users/Tiger/PycharmProjects/scottbrian_utils/dist/scottbrian_utils-4.1.0-py3-none-any.whl
    3) in pyproject.toml dependencies section, comment out the official line and add temporary file line:
       commented out line:
           # "scottbrian_utils ~=4.0",
       add temporary line:
           "scottbrian_utils @  file://C:/Users/Tiger/PycharmProjects/scottbrian_utils/dist/scottbrian_utils-4.1.0-py3-none-any.whl",


Usage examples:
===============



Development setup
=================

See tox.ini

Release History
===============

* 1.0.0
    * Initial release

Meta
====

Scott Tuttle

Distributed under the MIT license. See ``LICENSE`` for more information.


Contributing
============

1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
