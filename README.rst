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

Linux:

``pip install scottbrian-algo1``


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
