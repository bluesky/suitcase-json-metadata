from collections import defaultdict
import event_model
import json
from pathlib import Path
import suitcase.utils
from ._version import get_versions


__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{uid}-', **kwargs):
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
        inferface. See the suitcase documentation (LINK ONCE WRITTEN) for
        details.
    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.
    **kwargs : kwargs
        kwargs to be passed to ``json.dump``.
    Returns
    -------
    dest : dict
        dict mapping the 'labels' to lists of file names
    Examples
    --------
    Generate files with unique-identifer names in the current directory.
    >>> export(gen, '')
    Generate files with more readable metadata in the file names.
    >>> export(gen, '', '{plan_name}-{motors}-')
    Include the experiment's start time formatted as YY-MM-DD_HH-MM.
    >>> export(gen, '', '{time:%%Y-%%m-%%d_%%H:%%M}')
    Place the files in a different directory, such as on a mounted USB stick.
    >>> export(gen, '/path/to/my_usb_stick')
    """
    serializer = Serializer(directory, file_prefix, **kwargs)
    try:
        for item in gen:
            serializer(*item)
    finally:
        serializer.close()

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
        inferface. See the suitcase documentation (LINK ONCE WRITTEN) for
        details.
    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.
    **kwargs : kwargs
        kwargs to be passed to ``json.dump``.
    Returns
    -------
    dest : dict
        dict mapping the 'labels' to lists of file names
    """
    def __init__(self, directory, file_prefix='{uid}-', **kwargs):

        if isinstance(directory, (str, Path)):
            self.manager = suitcase.utils.MultiFileManager(directory)
        else:
            self.manager = directory

        self.artifacts = self.manager._artifacts
        self._meta = defaultdict(dict)  # to be exported as JSON at the end
        self._meta['metadata']['descriptors'] = defaultdict(lambda:
                                                            defaultdict(dict))
        self._stream_names = {}  # maps stream_names to each descriptor uids
        self._files = {}  # map descriptor uid to file handle of tiff file
        self._filenames = {}  # map descriptor uid to file names of tiff files
        self._file_prefix = file_prefix
        self._templated_file_prefix = ''
        self._kwargs = kwargs

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
        self._templated_file_prefix = self._file_prefix.format(**doc)

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
        f = self.manager.open('run_metadata',
                              f'{self._templated_file_prefix}meta.json', 'xt')
        json.dump(self._meta, f)
        self._files['meta'] = f

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
        for file in self._files.values():
            file.close()