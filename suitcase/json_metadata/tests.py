from collections import defaultdict
import event_model
from event_model import _apply_to_dict_recursively
from . import export
import json
import pytest


def create_expected(collector):
    '''This collects the metadata into a dict to compare to the loaded data
    '''

    def _tuples_to_lists(val):
        ''' Recursively convert tuples to lists'''
        if type(val) in [tuple, list]:
            items = []
            for item in val:
                items.append(_tuples_to_lists(item))
            val = items
        return val

    expected = {'metadata': {}}
    expected['metadata']['descriptors'] = defaultdict(dict)
    for name, doc in collector:
        if name in ['start', 'stop']:
            sanitized_doc = _apply_to_dict_recursively(doc, _tuples_to_lists)
            expected['metadata'][name] = doc
        elif name == 'descriptor':
            sanitized_doc = event_model.sanitize_doc(doc)
            expected['metadata']['descriptors'
                                 ][doc.get('name')][doc['uid']] = sanitized_doc

    return expected


def test_export(tmp_path, example_data):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = example_data()
    expected = create_expected(collector)
    artifacts = export(collector, tmp_path, file_prefix='')

    for filename in artifacts['run_metadata']:
        with open(filename) as f:
            actual = json.load(f)
        assert actual == expected
