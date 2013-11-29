#!/usr/bin/env python

import argparse
from os import kill, linesep, listdir, makedirs, path, rename, rmdir, walk
from tempfile import mkdtemp, NamedTemporaryFile
from time import sleep
from shutil import copy2, move
import signal
import sys


# Things TODO:
#   * Use sets instead of lists for files to copy since files should be unique
#   * Add access/modify time thresholds when copying, moving, or removing


DEFAULT_WORKSPACE_DIRNAME = '.workspace'
DEFAULT_JOURNAL_FILENAME = '.journal'
DEFAULT_LOCK_FILENAME = '.lock'
HIDDEN_FILE_PREFIX = '.'

DEFAULT_DIR_LOCK_WAIT_SECS = 10
DEFAULT_SYNC_COUNT = 1
DEFAULT_SYNC_INTERVAL_SECS = 5

is_verbose = False
is_terminate_now = False


def check_negative(value):
    ivalue = int(value)
    if ivalue < 0:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue


parser = argparse.ArgumentParser(
    description='A utility copy or move files from a source directory to the specified destination directory')

mv_cp_group = parser.add_mutually_exclusive_group()

mv_cp_group.add_argument('-c', '--copy', action='store_true',
    help='Specifies that files should be copied instead of moved, the default behavior')
parser.add_argument('-C', '--sync-count', type=check_negative, default=DEFAULT_SYNC_COUNT, metavar='COUNT',
    help='Specifies how many times to sync before exiting, zero means keep running until killed, defaults to %d' % DEFAULT_SYNC_COUNT)
parser.add_argument('-E', '--include-extensions', metavar='EXT1,EXT2,...',
    help='If specified, sync only files with extensions in the given comma-separated list, otherwise sync ALL files')
parser.add_argument('-H', '--include-hidden-paths', action='store_true',
    help='If specified, includes files and directories that begin with \'%s\', otherwise they are excluded' % HIDDEN_FILE_PREFIX)
parser.add_argument('-i', '--sync-interval', type=check_negative, default=DEFAULT_SYNC_INTERVAL_SECS, metavar='SECS',
    help='Specifies the number of seconds between syncs, only used when sync-count is NOT 1 and defaults to %d' % DEFAULT_SYNC_INTERVAL_SECS)
parser.add_argument('-l', '--lock-source-dir', action='store_true',
    help='Check for and create a .lock file in <source-dir>')
parser.add_argument('-L', '--lock-destination-dir', action='store_true',
    help='Check for and create a .lock file in <destination-dir>')
mv_cp_group.add_argument('-m', '--move', action='store_true',
    help='Specifies that files should be moved instead of copied, the default is to copy')
parser.add_argument('-p', '--purge-empty-destination-dirs', action='store_true',
    help='If specified, empty directories in <destination-dir> will be deleted')
parser.add_argument('-t', '--lock-dir-timeout', type=check_negative, default=DEFAULT_DIR_LOCK_WAIT_SECS, metavar='SECS',
    help='Specify the number of seconds to wait if the dir is already locked, defaults to %d' % DEFAULT_DIR_LOCK_WAIT_SECS)
parser.add_argument('-v', '--verbose', action='store_true',
    help='Print additional debugging messages')
parser.add_argument('-w', '--workspace-dir', metavar='DIR',
    help='Specify a workspace dir, defaults to <destination-dir>/%s' % DEFAULT_WORKSPACE_DIRNAME)

parser.add_argument('source_dir', help='Dir to sync files from, must exist')
parser.add_argument('destination_dir', help='Dir to sync files to, will be created if does not exist')


def debug(message):
    if is_verbose:
        print message


def error(message):
    sys.stderr.write(message + '\n')


def copy_file(workspace_dir_path, source_dir_path, destination_dir_path, relative_file_path):
    source_file_path = path.join(source_dir_path, relative_file_path)

    # Copy to tmp location
    tmpdir = mkdtemp()
    tmp_file_path = path.join(tmpdir, path.basename(relative_file_path))

    copy2(source_file_path, tmp_file_path)

    print 'Copied %s to %s' % (source_file_path, tmp_file_path)

    safe_move_with_workspace(tmp_file_path, workspace_dir_path, destination_dir_path, relative_file_path)


def move_file(workspace_dir_path, source_dir_path, destination_dir_path, relative_file_path):
    source_file_path = path.join(source_dir_path, relative_file_path)

    safe_move_with_workspace(source_file_path, workspace_dir_path, destination_dir_path, relative_file_path)


def safe_move_with_workspace(source_file_path, workspace_dir_path, destination_dir_path, relative_file_path):
    destination_file_path = path.join(destination_dir_path, relative_file_path)
    workspace_file_path = path.join(workspace_dir_path, relative_file_path)

    # 1. Create destination dirs in workspace
    workspace_dirname = path.dirname(workspace_file_path)
    if not path.exists(workspace_dirname):
        makedirs(workspace_dirname)


    # 2. Move file to workspace destination
    if source_file_path is not workspace_file_path:
        move(source_file_path, workspace_file_path)


    # 3. Calculate which destination dirs don't exists and move from workspace
    workspace_dir_to_remove = ''
    relative_dir_path = path.dirname(relative_file_path)
    if path.exists(path.join(destination_dir_path, relative_dir_path)):
        debug('Move1 "%s" to "%s"' % (workspace_file_path, destination_file_path))
        move(workspace_file_path, destination_file_path)
        workspace_dir_to_remove = path.dirname(relative_file_path)
    else:
        dir_to_move = get_first_non_existent_dir(destination_dir_path, relative_dir_path)
        source_file_path = path.join(workspace_dir_path, dir_to_move)
        destination_file_path =  path.join(destination_dir_path, dir_to_move)
        debug('Move2 "%s" to "%s"' % (source_file_path, destination_file_path))
        move(source_file_path, destination_file_path)
        workspace_dir_to_remove = path.dirname(dir_to_move)


    # 4. Delete remaining dirs created in workspace
    if workspace_dir_to_remove:
        recursive_remove_empty_directories(workspace_dir_path,
            path.join(workspace_dir_path, workspace_dir_to_remove))


def get_first_non_existent_dir(dir_base_path, dir_relative_path):
    cur_relative_path = dir_relative_path

    while cur_relative_path:
        next_relative_path = path.dirname(dir_relative_path)
        if ( not path.exists(path.join(dir_base_path, cur_relative_path)) and
            path.exists(path.join(dir_base_path, next_relative_path)) ):
            return cur_relative_path

        cur_relative_path = next_relative_path

    return ''


def get_empty_directory_list(dir_path):
    results = []

    if not dir_path or not path.exists(dir_path):
        return results

    for root, dirs, files in walk(dir_path):
        if not files and not dirs:
            results.append(root)

    return results;


def get_file_list(source_path, restrict_to_file_extensions, is_include_hidden_paths):
    results = []

    if not source_path or not path.exists(source_path):
        return results

    for root, dirs, files in walk(source_path):
        if not is_include_hidden_paths:
            for dirname in dirs:
                if dirname[0] == HIDDEN_FILE_PREFIX:
                    dirs.remove(dirname)


        for filename in files:
            if ( (not is_include_hidden_paths and filename[0] == HIDDEN_FILE_PREFIX) or
                    filename == DEFAULT_LOCK_FILENAME ):
                continue

            file_pathname = path.join(root, filename)

            if restrict_to_file_extensions != None and len(restrict_to_file_extensions) > 0:
                f, ext = path.splitext(filename)
                if ext not in restrict_to_file_extensions:
                    continue

            # Make sure the source_path has an ending slash
            source_path = path.join(source_path, '')
            relative_file_path = file_pathname[len(source_path):]
            results.append(relative_file_path)

    return results


def get_new_file_list(all_current_files_list, previous_files_list):
    previous_files_set = set(previous_files_list)
    return [ x for x in all_current_files_list if x not in previous_files_set ]


def get_already_synced_file_list(all_current_files_list, previous_files_list):
    previous_files_set = set(previous_files_list)
    return [ x for x in all_current_files_list if x in previous_files_set ]


def recursive_remove_empty_directories(root_dir_path, dir_path):
    if ( not root_dir_path or root_dir_path == '/' or not path.exists(root_dir_path) or
            len(root_dir_path) > len(dir_path) or len(path.commonprefix([ root_dir_path, dir_path ])) < 2 ):
        error('Cannot remove empty dirs, invalid root_dir_path=' + str(root_dir_path))
        return

    if not dir_path or not path.exists(dir_path):
        error('Cannot remove empty dirs, invalid dir_path=' + str(dir_path))

    for root, dirs, files in walk(dir_path, topdown=False):
        try:
            if not files and not dirs:
                rmdir(root)
        except OSError as ex:
            error('Failed to remove dir=' + root + ', ' + str(ex))


def get_last_file_list(journal_file_path):
    results = []

    if path.exists(journal_file_path):
        with open(journal_file_path, 'r') as journal_file:
            for relative_filename in journal_file:
                results.append(relative_filename.rstrip(os.linesep))

    return results


def update_journal(journal_file_path, journal_entries):
    if not path.exists(journal_file_path):
        dirname = path.dirname(journal_file_path)
        if not path.exists(dirname):
            makedirs(dirname)

    tmp_file_path = None
    with NamedTemporaryFile(delete=False) as tmpfile:
        tmp_file_path = tmpfile.name
        debug('Writing journal to ' + tmp_file_path)
        tmpfile.write(os.linesep.join(journal_entries))

    try:
        rename(tmp_file_path, journal_file_path)
    except OSError:
        error('Failed to move updated journal to ' + journal_file_path)

def signal_handler(signal, frame):
    global is_terminate_now
    is_terminate_now = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def sync_files(args):
    # 1. Setup all the required variables

    is_lock_source_dir = args.lock_source_dir
    is_lock_destination_dir = args.lock_destination_dir
    lock_dir_timeout = 2
    workspace_dir_path = args.workspace_dir
    restrict_to_file_extensions = args.include_extensions.split(',') if args.include_extensions else []
    is_move_file = args.move
    is_purge_empty_destination_dirs = args.purge_empty_destination_dirs
    is_include_hidden_paths = args.include_hidden_paths
    source_dir_path = args.source_dir
    destination_dir_path = args.destination_dir

    if restrict_to_file_extensions:
        cleaned_extensions = []
        for ext in restrict_to_file_extensions:
            if ext and ext.strip() and ext.strip()[1] is not '.':
                cleaned_extensions.append('.' + ext.strip())
            else:
                cleaned_extensions.append(ext.strip())

        restrict_to_file_extensions = cleaned_extensions


    if restrict_to_file_extensions:
        debug('Only copying files with the following extensions: %s' % restrict_to_file_extensions)


    if workspace_dir_path == None:
        workspace_dir_path = path.join(destination_dir_path, DEFAULT_WORKSPACE_DIRNAME)

    journal_file_path = path.join(workspace_dir_path, DEFAULT_JOURNAL_FILENAME)


    # 2. Validate the source and destination directories

    if not path.isdir(source_dir_path):
        error('Failed to sync, invalid source_directory=%s' % source_dir_path)
        return

    if not path.exists(destination_dir_path):
        debug('Creating destination_directory=%s' % destination_dir_path)
        makedirs(destination_dir_path)
    elif not path.isdir(destination_dir_path):
        error('Failed to sync, invalid destination_directory=%s' % destination_dir_path)
        return


    if is_terminate_now:
        return


    # 3. Before doing work, lock the directories

    with ( ConditionalDirLock(source_dir_path, is_lock_source_dir, lock_dir_timeout) and
            ConditionalDirLock(destination_dir_path, is_lock_destination_dir, lock_dir_timeout) ):

        # 4. See if there was a previous work from another failed sync left
        #    in the workspace directory, and clean it up
        workspace_files = get_file_list(workspace_dir_path, [], False)

        debug('Workspace files to cleanup: %s' % workspace_files)

        for workspace_relative_filename in workspace_files:
            if is_terminate_now:
                return

            try:
                workspace_file_path = path.join(workspace_dir_path, workspace_relative_filename)
                safe_move_with_workspace(workspace_file_path, workspace_dir_path, destination_dir_path, workspace_relative_filename)
            except Exception as e:
                error('Error syncing previous workspace file: %s, %s' % (workspace_file_path, e))
                raise


        # 5. Check if need to cleanup previously created destination dirs

        if is_purge_empty_destination_dirs:
            empty_dir_list = get_empty_directory_list(destination_dir_path)


        # 6. Move or copy new files in source to workspace

        source_files = get_file_list(source_dir_path, restrict_to_file_extensions, is_include_hidden_paths)
        debug('Source files: %s' % source_files)

        previous_files = get_last_file_list(journal_file_path)
        previous_files += get_file_list(destination_dir_path, restrict_to_file_extensions, is_include_hidden_paths)
        debug('Previous files: %s' % previous_files)

        new_source_files = get_new_file_list(source_files, previous_files)
        synced_files = get_already_synced_file_list(source_files, previous_files)

        debug('Copying files: %s' % new_source_files)

        for new_relative_filename in new_source_files:
            if is_terminate_now:
                return

            try:
                if is_move_file:
                    move_file(workspace_dir_path, source_dir_path, destination_dir_path, new_relative_filename)
                else:
                    copy_file(workspace_dir_path, source_dir_path, destination_dir_path, new_relative_filename)
            except Exception as e:
                error('Error syncing file:' + str(path.join(source_dir_path, new_relative_filename)) +
                    ", " + str(e))
            else:
                synced_files.append(new_relative_filename)
                update_journal(journal_file_path, synced_files)


        # 7. Purge empty directories from destination

        if is_purge_empty_destination_dirs:
            debug('Directories to purge:' + str(empty_dir_list))

            for dir_path in empty_dir_list:
                if is_terminate_now:
                    return

                recursive_remove_empty_directories(destination_dir_path, dir_path)


def main(argv):
    print 'argv=%s' % argv

    # Parse command line args
    args = parser.parse_args(argv[1:])

    global is_verbose
    is_verbose = args.verbose

    debug('args=%s' % args)

    # sync_interval_secs = args.sync_interval
    # sync_count = args.sync_count
    sync_interval_secs = args.sync_interval
    sync_count = args.sync_count

    while sync_count and not is_terminate_now:
        print 'is_terminate_now=%s' % is_terminate_now
        sync_files(args)

        sync_count -= 1

        if sync_count and not is_terminate_now:
            debug('Sleeping for %ds' % sync_interval_secs)
            sleep(sync_interval_secs)


################################################################################
# Code copied from https://github.com/raphendyr/FileLock/tree/master/filelock
################################################################################


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


################################################################################
# END copied code
################################################################################


# My subclass
class ConditionalDirLock(FileLock):

    def __init__(self, dir_name, actually_lock=True, timeout=10, delay=.05):
        FileLock.__init__(self, path.join(dir_name, ''), timeout, delay)
        self.actually_lock = actually_lock

        if actually_lock and not path.isdir(self.file_name):
            error('Invalid directory to lock, not a directory=' + self.file_name)
            actually_lock = False


    def acquire(self):
        if self.actually_lock:
            debug('Locking ' + self.lockfile)
            try:
                FileLock.acquire(self)
            except:
                with open(self.lockfile, 'r') as lf:
                    pid = -1
                    try:
                        pid = int(lf.readline())
                    except ValueError as e:
                        error('Failed read PID from lockfile: ' + str(e))
                        raise
                    else:
                        debug('Failed to acquire lock, checking if owner of lock (PID=%d) is still running' % pid)
                        if not self.pid_exists(pid):
                            debug('Owning process no longer running, deleting lockfile=%s' % self.lockfile)
                            os.unlink(self.lockfile)
                            FileLock.acquire(self)
                        else:
                            error('Failed to acquire lock=%s, owner PID=%d is still running' % (self.lockfile, pid))
                            raise


    def __del__(self):
        FileLock.__del__(self)


    def pid_exists(self, pid):
        """Check whether pid exists in the current process table."""
        if pid < 0:
            return False
        try:
            kill(pid, 0)
        except OSError, e:
            return e.errno == errno.EPERM
        else:
            return True


if __name__ == "__main__":
    main(sys.argv)
