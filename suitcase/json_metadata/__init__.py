from collections import defaultdict
import event_model
import json
import numpy
from pathlib import Path
import suitcase.utils
from ._version import get_versions


__version__ = get_versions()['version']
del get_versions


class NumpyEncoder(json.JSONEncoder):
    # Credit: https://stackoverflow.com/a/47626762/1221924
    def default(self, obj):
        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def export(gen, directory, file_prefix='{start[uid]}-',
           cls=event_model.NumpyEncoder, **kwargs):
    """
    Export the meta data from a stream of documents to a JSON file.

    This creates a file named ``<directory>/<file_prefix>meta.json``

    The structure of the JSON is:
    {'metadata': {'start': start_doc, 'stop': stop_doc,
                    'descriptors': {stream_name1: {
                                        'descriptor uid':descriptor_doc},
                                    stream_name2: ...}}}

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        expected to yield ``(name, document)`` pairs
    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.
        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        inferface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase/) for details.
    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``,
        which are populated from the RunStart document. The default value is
        ``{start[uid]}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.
    cls : class, optional
        This is a ``json.JSONEncoder``, or child, class that will be passed to
        ``json.dump`` as a kwarg which ensures that all items are encoded
        correctly. The defualt is ``event_model.NumpyEncoder`` which also
        ensures that all ``numpy`` objects are converted to built-in python
        ones.
    **kwargs : kwargs
        kwargs to be passed to ``json.dump``.

    Returns
    -------
    artifacts : dict
        Maps 'labels' to lists of artifacts (e.g. filepaths)

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{plan_name}-{motors}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{time:%Y-%m-%d_%H:%M}')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    with Serializer(directory, file_prefix, cls=cls, **kwargs) as serializer:
        for item in gen:
            serializer(*item)

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """
    Serialize the metadata from a stream of documents to a JSON file.

    This creates one file named ``<directory>/<file_prefix>meta.json``

    The structure of the JSON is::
    {'metadata': {'start': start_doc, 'stop': stop_doc,
                  'descriptors': {stream_name1: {
                                      'descriptor uid':descriptor_doc, ...},
                                  stream_name2: ...}}}
    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.
        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        inferface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase/) for details.
    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``,
        which are populated from the RunStart document. The default value is
        ``{start[uid]}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.
    cls : class, optional
        This is a ``json.JSONEncoder``, or child, class that will be passed to
        ``json.dump`` as a kwarg which ensures that all items are encoded
        correctly. The defualt is ``event_model.NumpyEncoder`` which also
        ensures that all ``numpy`` objects are converted to built-in python
        ones.
    **kwargs : kwargs
        kwargs to be passed to ``json.dump``.

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{start[plan_name]}-{start[motors]}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{start[time]:%Y-%m-%d_%H:%M}')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    def __init__(self, directory, file_prefix='{start[uid]}-',
                 cls=event_model.NumpyEncoder, **kwargs):

        if isinstance(directory, (str, Path)):
            self._manager = suitcase.utils.MultiFileManager(directory)
        else:
            self._manager = directory

        self._meta = defaultdict(dict)  # to be exported as JSON at the end
        self._meta['metadata']['descriptors'] = defaultdict(lambda:
                                                            defaultdict(dict))
        self._file_prefix = file_prefix
        self._templated_file_prefix = ''
        self._kwargs = dict(cls=cls, **kwargs)

    @property
    def artifacts(self):
        # The manager's artifacts attribute is itself a property, and we must
        # access it a new each time to be sure to get the latest content.
        return self._manager.artifacts

    def start(self, doc):
        '''Add `start` document information to the metadata dictionary.
        This method adds the start document information to the metadata
        dictionary. In addition it checks that only one `start` document is
        seen.
        Parameters:
        -----------
        doc : dict
            RunStart document
        '''

        # raise an error if this is the second `start` document seen.
        if 'start' in self._meta['metadata']:
            raise RuntimeError(
                "The serializer in suitcase.json expects documents from one "
                "run only. Two `start` documents where sent to it")

        # add the start doc to self._meta and format self._file_prefix
        self._meta['metadata']['start'] = doc
        self._templated_file_prefix = self._file_prefix.format(start=doc)

    def stop(self, doc):
        '''Add `stop` document information to the metadata dictionary.
        This method adds the stop document information to the metadata
        dictionary. In addition it also creates the metadata '.json' file and
        exports the metadata dictionary to it.
        Parameters:
        -----------
        doc : dict
            RunStop document
        '''
        # add the stop doc to self._meta.
        self._meta['metadata']['stop'] = doc

        # open a json file for the metadata and add self._meta to it.
        f = self._manager.open('run_metadata',
                               f'{self._templated_file_prefix}meta.json', 'xt')
        json.dump(self._meta, f, **self._kwargs)
        self.close()

    def descriptor(self, doc):
        '''Add `descriptor` document information to the metadata dictionary.
        This method adds the descriptor document information to the metadata
        dictionary. In addition it also creates the file for data with the
        stream_name given by the descriptor doc for later use.
        Parameters:
        -----------
        doc : dict
            EventDescriptor document
        '''
        # extract some useful info from the doc
        stream_name = doc.get('name')
        # replace numpy objects with python ones to ensure json compatibility
        sanitized_doc = event_model.sanitize_doc(doc)
        # Add the doc to self._meta
        self._meta['metadata'
                   ]['descriptors'][stream_name][sanitized_doc['uid']
                                                 ] = sanitized_doc

    def close(self):
        '''Close all of the files opened by this Serializer.
        '''
        self._manager.close()

    def __enter__(self):
        return self

    def __exit__(self, *exception_details):
        self.close()
