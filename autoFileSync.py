#!/usr/bin/env python

from os import linesep, listdir, makedirs, path, walk
from shutil import copy2, move
import sys


DEFAULT_WORKSPACE_DIRNAME = '.workspace'
DEFAULT_JOURNAL_FILENAME = '.journal'

# Things to do
#   * Add command line arg parsing
#   * Need to add a signal handler for SIGINT


def copy_file(source_dir_path, destination_dir_path, relative_file_path):
    source_file_path = path.join(source_dir_path, relative_file_path)
    destination_file_path = path.join(destination_dir_path, relative_file_path)

    destination_dirname = path.dirname(destination_file_path)
    if not path.exists(destination_dirname):
        os.makedirs(destination_dirname)

    copy2(source_file_path, destination_file_path)


def move_file(source_dir_path, destination_dir_path, relative_file_path):
    source_file_path = path.join(source_dir_path, relative_file_path)
    destination_file_path = path.join(destination_dir_path, relative_file_path)

    destination_dirname = path.dirname(destination_file_path)
    if not path.exists(destination_dirname):
        os.makedirs(destination_dirname)

    move(source_file_path, destination_file_path)


def get_empty_directory_list(dir_path):
    return []


def get_source_file_list(source_path, restrict_to_file_extensions):
    results = []

    for root, dirs, files in walk(source_path):
        for filename in files:
            file_pathname = path.join(root, filename)

            if restrict_to_file_extensions != None and len(restrict_to_file_extensions) > 0:
                f, ext = path.splitext(filename)
                if ext not in restrict_to_file_extensions:
                    continue

            # Make sure the source_path has an ending slash
            source_path = path.join(source_path, '')
            relative_file_path = file_pathname[len(source_path):]
            results.append(relative_file_path)

    print 'Source files:' + str(results)

    return results


def get_new_file_list(all_current_files_list, previous_files_list):
    previous_files_set = set(previous_files_list)
    return [ x for x in all_current_files_list if x not in previous_files_set ]


def recursive_remove_empty_directories(dir_path):
    # TODO
    pass


def remove_empty_directory(dir_path):
    # TODO
    pass


def get_last_file_list(journal_file_path, workspace_journal_file_path):
    results = []

    if path.exists(journal_file_path):
        with open(journal_file_path, 'r') as journal_file:
            for relative_filename in journal_file:
                results.append(relative_filename.rstrip(os.linesep))

    if path.exists(workspace_journal_file_path):
        with open(workspace_journal_file_path, 'r') as journal_file:
            for relative_filename in journal_file:
                results.append(relative_filename.rstrip(os.linesep))

    return results


def append_journal(journal_file_path, source_file):
    if not path.exists(journal_file_path):
        dirname = path.dirname(journal_file_path)
        if not path.exists(dirname):
            os.makedirs(dirname)

    with open(journal_file_path, 'a') as journal_file:
        temp_filename = journal_file.name

        journal_file.write(source_file)
        journal_file.write(os.linesep)


def main(argv):
    # 1. Parse command line args

    # TODO


    # 2. Setup all the required variables

    use_source_dir_lock_file = False
    use_destination_dir_lock_file = False
    source_dir_path = '/Users/mcompton/src/autoFileSyncDaemon.git/test/source'
    destination_dir_path = '/Users/mcompton/src/autoFileSyncDaemon.git/test/destination'
    workspace_dir_path = None
    restrict_to_file_extensions = []
    journal_filename = None
    is_move_file = False
    is_cleanup_empty_destination_dirs = False


    if workspace_dir_path == None:
        workspace_dir_path = path.join(destination_dir_path, DEFAULT_WORKSPACE_DIRNAME)

    if journal_filename == None:
        journal_filename = DEFAULT_JOURNAL_FILENAME

    journal_file_path = path.join(destination_dir_path, journal_filename)
    workspace_journal_file_path = path.join(workspace_dir_path, journal_filename)


    # 3. Before doing work, lock the directories

    with ConditionalFileLock(source_dir_path, use_source_dir_lock_file):
        with ConditionalFileLock(destination_dir_path, use_destination_dir_lock_file):


            # 4. Check if need to cleanup previously created destination dirs

            if is_cleanup_empty_destination_dirs:
                empty_dir_list = get_empty_directory_list(destination_dir_path)


            # 5. Move or copy new files in source to workspace

            source_files = get_source_file_list(source_dir_path, restrict_to_file_extensions)
            previous_files = get_last_file_list(journal_file_path, workspace_journal_file_path)

            print 'Previous files:' + str(previous_files)
            new_source_files = get_new_file_list(source_files, previous_files)

            print 'Copying files:' + str(new_source_files)

            for new_relative_filename in new_source_files:
                if is_move_file:
                    move_file(source_dir_path, destination_dir_path, new_relative_filename)
                else:
                    copy_file(source_dir_path, destination_dir_path, new_relative_filename)

                append_journal(workspace_journal_file_path, new_relative_filename)


            #6. TODO MOVE workspace file to destination


            #7. TODO remove empty directories from destination

            if is_cleanup_empty_destination_dirs:
                for dir_path in empty_dir_list:
                    recursive_remove_empty_directories(dir_path)




# Code copied from https://github.com/raphendyr/FileLock/tree/master/filelock

# Copyright (c) 2009, Evan Fosmark
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are those
# of the authors and should not be interpreted as representing official policies,
# either expressed or implied, of the FreeBSD Project.

import os
import time
import errno

class FileLockException(Exception):
    pass

class FileLock(object):
    """ A file locking mechanism that has context-manager support so
        you can use it in a with statement. This should be relatively cross
        compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """

    __slots__ = ('fd', 'is_locked', 'lockfile', 'file_name', 'timeout', 'delay')

    def __init__(self, file_name, timeout=10, delay=.05):
        """ Prepare the file locker. Specify the file to lock and optionally
            the maximum timeout and the delay between each attempt to lock.
        """
        self.is_locked = False
        self.lockfile = os.path.abspath(os.path.expanduser(os.path.expandvars("%s.lock" % file_name)))
        self.file_name = file_name
        self.timeout = timeout
        self.delay = delay


    def acquire(self):
        """ Acquire the lock, if possible. If the lock is in use, it check again
            every `wait` seconds. It does this until it either gets the lock or
            exceeds `timeout` number of seconds, in which case it throws
            an exception.
        """
        start_time = time.time()
        pid = os.getpid()
        while True:
            try:
                self.fd = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
                os.write(self.fd, "%d" % pid)
                break;
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if (time.time() - start_time) >= self.timeout:
                    raise FileLockException("Timeout occured.")
                time.sleep(self.delay)
        self.is_locked = True


    def release(self):
        """ Get rid of the lock by deleting the lockfile.
            When working in a `with` statement, this gets automatically
            called at the end.
        """
        if self.is_locked:
            os.close(self.fd)
            os.unlink(self.lockfile)
            self.is_locked = False


    def __enter__(self):
        """ Activated when used in the with statement.
            Should automatically acquire a lock to be used in the with block.
        """
        if not self.is_locked:
            self.acquire()
        return self


    def __exit__(self, type, value, traceback):
        """ Activated at the end of the with statement.
            It automatically releases the lock if it isn't locked.
        """
        if self.is_locked:
            self.release()


    def __del__(self):
        """ Make sure that the FileLock instance doesn't leave a lockfile
            lying around.
        """
        self.release()


# My class
class ConditionalFileLock(FileLock):

    def __init__(self, file_name, actually_lock=True, timeout=10, delay=.05):
        FileLock.__init__(self, file_name, timeout, delay)
        self.actually_lock = actually_lock

    def aquire(self):
        if self.actually_lock:
            FileLock.aquire(self)

    def __del__(self):
        FileLock.__del__(self)



if __name__ == "__main__":
    main(sys.argv)
