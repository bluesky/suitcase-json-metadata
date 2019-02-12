from collections import defaultdict
import event_model
from .. import export
import json


def create_expected(collector):
    '''This collects the metadata into a dict to compare to the loaded data
    '''
    expected = {'metadata': {}}
    expected['metadata']['descriptors'] = defaultdict(dict)
    for name, doc in collector:
        if name in ['start', 'stop']:
            sanitized_doc = event_model.sanitize_doc(doc)
            expected['metadata'][name] = sanitized_doc
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
