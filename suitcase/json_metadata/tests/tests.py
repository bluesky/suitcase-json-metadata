from collections import defaultdict
import event_model
from .. import export
import json
import pytest


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


@pytest.mark.parametrize("kwargs", [{'indent': 4},
                                    {'sort_keys': True, 'indent': 2,
                                     'separators': (',', ': ')}])
def test_export_with_kwargs(tmp_path, example_data, kwargs):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = example_data()
    expected = create_expected(collector)
    artifacts = export(collector, tmp_path, file_prefix='', **kwargs)

    for filename in artifacts['run_metadata']:
        with open(filename) as f:
            content = f.read()
            actual = json.loads(content)
        n = 20
        first_n_simbols = ('{\n' + ' ' * kwargs['indent'] +
                           '"metadata": {\n' + ' ' * kwargs['indent'])[:n]
        assert content[:n] == first_n_simbols
        assert actual == expected


def test_file_prefix_formatting(file_prefix_list, example_data, tmp_path):
    '''Runs a test of the ``file_prefix`` formatting.
    ..note::
        Due to the `file_prefix_list` and `example_data` `pytest.fixture`'s
        this will run multiple tests each with a range of file_prefixes,
        detectors and event_types. See `suitcase.utils.conftest` for more info.
    '''
    collector = example_data()
    file_prefix = file_prefix_list()
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    for name, doc in collector:
        if name == 'start':
            templated_file_prefix = file_prefix.format(
                start=doc).partition('-')[0]
            break

    if artifacts:
        unique_actual = set(str(artifact).split('/')[-1].partition('-')[0]
                            for artifact in artifacts['run_metadata'])
        assert unique_actual == set([templated_file_prefix])
