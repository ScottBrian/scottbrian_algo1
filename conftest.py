from doctest import ELLIPSIS
from doctest import OutputChecker as BaseOutputChecker

import pytest
from sybil import Sybil
from sybil.example import Example  # sbt
# from sybil.parsers.capture import parse_captures
# from sybil.parsers.codeblock import PythonCodeBlockParser

# from sybil.parsers.doctest import DocTestParser
# from sybil.parsers.doctest import DocTest  # sbt

# from sybil.parsers.rest import DocTestParser
from sybil.parsers.rest import PythonCodeBlockParser, ClearNamespaceParser
from sybil.parsers.abstract import DocTestStringParser
from sybil.evaluators.doctest import DocTestEvaluator, DocTest
from sybil.document import Document
from sybil.region import Region

# from scottbrian_utils.time_hdr import get_datetime_match_string

import re
from typing import Any, Iterable


class SbtOutputChecker(BaseOutputChecker):
    def __init__(self):
        self.mod_name = None
        self.msgs = []

    def check_output(self, want, got, optionflags):
        old_want = want
        old_got = got

        def repl_dt(match_obj: Any) -> str:
            return found_items.__next__().group()

        # if self.mod_name == 'time_hdr' or self.mod_name == 'README':
        #     # find the actual occurrences and replace in want
        #     for time_hdr_dt_format in ["%a %b %d %Y %H:%M:%S",
        #                                "%m/%d/%y %H:%M:%S"]:
        #         match_str = get_datetime_match_string(time_hdr_dt_format)
        #
        #         match_re = re.compile(match_str)
        #         found_items = match_re.finditer(got)
        #         want = match_re.sub(repl_dt, want)
        #
        #     # replace elapsed time in both want and got
        #     match_str = 'Elapsed time: 0:00:00.[0-9| ]{6,6}'
        #     replacement = 'Elapsed time: 0:00:00       '
        #     want = re.sub(match_str, replacement, want)
        #     got = re.sub(match_str, replacement, got)
        #
        # if self.mod_name == 'file_catalog' or self.mod_name == 'README':
        #     match_str = r'\\'
        #     replacement = '/'
        #     got = re.sub(match_str, replacement, got)
        #
        #     match_str = '//'
        #     replacement = '/'
        #     got = re.sub(match_str, replacement, got)
        #
        # if self.mod_name == 'diag_msg' or self.mod_name == 'README':
        #     for diag_msg_dt_fmt in ["%H:%M:%S.%f", "%a %b-%d %H:%M:%S"]:
        #         match_str = get_datetime_match_string(diag_msg_dt_fmt)
        #
        #         match_re = re.compile(match_str)
        #         found_items = match_re.finditer(got)
        #         want = match_re.sub(repl_dt, want)
        #
        #     # match_str = "<.+?>"
        #     if self.mod_name == 'diag_msg':
        #         match_str = "diag_msg.py\[0\]>"
        #     else:
        #         match_str = "README.rst\[0\]>"
        #     replacement = '<input>'
        #     got = re.sub(match_str, replacement, got)
        #
        # if self.mod_name == 'pauser':
        #     match_str = 'pauser.min_interval_secs=0.0[0-9]{1,2}'
        #
        #     found_item = re.match(match_str, got)
        #     if found_item:
        #         want = re.sub(match_str, found_item.group(), want)
        #
        #     match_str = ('metrics.pause_ratio=1.0, '
        #                  'metrics.sleep_ratio=0.[0-9]+')
        #
        #     found_item = re.match(match_str, got)
        #     if found_item:
        #         want = re.sub(match_str, found_item.group(), want)
        #     # match_re = re.compile(match_str)
        #     # found_items = match_re.finditer(got)
        #     # want = match_re.sub(repl_dt, want)
        #     # replacement = old_want
        #     # # got = re.sub(match_str, replacement, got)
        #     # got = replacement

        self.msgs.append([old_want, want, old_got, got])
        return BaseOutputChecker.check_output(self, want, got, optionflags)


class SbtDocTestEvaluator(DocTestEvaluator):
    def __init__(self, optionflags=0):
        DocTestEvaluator.__init__(self, optionflags=optionflags)

        # set our checker which will modify the test cases as needed
        self.runner._checker = SbtOutputChecker()

    def __call__(self, sybil_example: Example) -> str:
        example = sybil_example.parsed
        namespace = sybil_example.namespace
        output = []

        # set the mod name for our check_output in SbtOutputChecker
        mod_name = sybil_example.path.rsplit(sep=".", maxsplit=1)[0]
        mod_name = mod_name.rsplit(sep="\\", maxsplit=1)[1]
        self.runner._checker.mod_name = mod_name

        self.runner.run(
            DocTest([example], namespace, name=sybil_example.path,
                    filename=None, lineno=example.lineno, docstring=None),
            clear_globs=False,
            out=output.append
        )
        # print(f'{self.runner._checker.msgs=}')
        self.runner._checker.msgs = []
        return ''.join(output)


class SbtDocTestParser:
    def __init__(self, optionflags=0):
        self.string_parser = DocTestStringParser(
            SbtDocTestEvaluator(optionflags))

    def __call__(self, document: Document) -> Iterable[Region]:
        return self.string_parser(document.text, document.path)


pytest_collect_file = Sybil(
    parsers=[
        # DocTestParser(optionflags=ELLIPSIS),
        SbtDocTestParser(optionflags=ELLIPSIS),
        PythonCodeBlockParser(),
        ClearNamespaceParser()
    ],
    patterns=['*.py'],
    # patterns=['*.rst', '*.py'],
    # excludes=['log_verifier.py']
).pytest()