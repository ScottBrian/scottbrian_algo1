from doctest import ELLIPSIS

from sybil import Sybil
from sybil.parsers.rest import PythonCodeBlockParser

from scottbrian_utils.doc_checker import DocCheckerTestParser


pytest_collect_file = Sybil(
    parsers=[
        DocCheckerTestParser(
            optionflags=ELLIPSIS,
        ),
        PythonCodeBlockParser(),
    ],
    patterns=["*.rst", "*.py"],
    # excludes=['log_verifier.py']
).pytest()
