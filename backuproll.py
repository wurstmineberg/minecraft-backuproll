#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Backup roll script for one or more Minecraft servers

Usage:
  backuproll [options] cron [<world>]
  backuproll [options] backup [<world>]
  backuproll [options] rotate [<world>]
  backuproll [options] cleanup [<world>]
  backuproll [options] restore-interactive
  backuproll [options] restore <world> <backup>
  backuproll -h | --help
  backuproll --version

Options:
  -h, --help         Print this message and exit.
  --cleanup          cron: Clean up the backup directory before operation
  --config=<config>  Path to the config file [default: /opt/wurstmineberg/config/backuproll2.json].
  --no-backup        cron: Do everything but don't run the backup command
  --no-rotation      cron: Don't rotate the backup directory
  --simulate         Don't do any destructive operation, implies --verbose
  --verbose          Print things.
  --version          Print version info and exit.
"""

import sys

import contextlib
import curses
import datetime
import docopt
import io
import json
import os
import pathlib
import shutil
import subprocess
import tarfile
import threading
import time

from curses import panel

from version import __version__

from wmb import get_config, from_assets

CONFIG = get_config("backuproll2",
                    base = from_assets(__file__),
                    argparse_configfile = True)

RETENTION_RECENT = 'recent'
RETENTION_DAILY = 'daily'
RETENTION_WEEKLY = 'weekly'
RETENTION_MONTHLY = 'monthly'

RETENTION_GROUPS = [
    RETENTION_RECENT,
    RETENTION_DAILY,
    RETENTION_WEEKLY,
    RETENTION_MONTHLY
]

RETENTION_MANUAL = [
    'pre-update',
    'reverted'
]

class MinecraftBackupRollError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class BackupError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class BackupStoreReadonlyError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class BackupRotationError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


"""Overwrite tarfile._LowLevelFile.write() to block if not all bytes could be written"""
def __my_write(self, buf):
    written = 0
    bytelen = len(buf)
    while written < bytelen:
        written += os.write(self.fd, buf[written:])
        # yield for other threads if buffer is full
        if written < bytelen:
            time.sleep(0)
tarfile._LowLevelFile.write = __my_write

"""Implementation from https://github.com/hbock/byte-fifo

Thread safe
"""
class BytesFIFO(object):
    """
    A FIFO that can store a fixed number of bytes.
    """
    def __init__(self, init_size):
        """ Create a FIFO of ``init_size`` bytes. """
        self._buffer = io.BytesIO(b"\x00"*init_size)
        self._size = init_size
        self._filled = 0
        self._read_ptr = 0
        self._write_ptr = 0
        self.lock = threading.Lock()

    def __bool__(self):
        return self._size > 0

    def read(self, size=-1):
        """
        Read at most ``size`` bytes from the FIFO.

        If less than ``size`` bytes are available, or ``size`` is negative,
        return all remaining bytes.
        """
        self.lock.acquire()
        if size < 0:
            size = self._filled

        # Go to read pointer
        self._buffer.seek(self._read_ptr)

        # Figure out how many bytes we can really read
        size = min(size, self._filled)
        contig = self._size - self._read_ptr
        contig_read = min(contig, size)

        ret =  self._buffer.read(contig_read)
        self._read_ptr += contig_read
        if contig_read < size:
            leftover_size = size - contig_read
            self._buffer.seek(0)
            ret += self._buffer.read(leftover_size)
            self._read_ptr = leftover_size

        self._filled -= size

        self.lock.release()
        return ret

    def write(self, data):
        """
        Write as many bytes of ``data`` as are free in the FIFO.

        If less than ``len(data)`` bytes are free, write as many as can be written.
        Returns the number of bytes written.
        """
        self.lock.acquire()
        free = self._free()
        write_size = min(len(data), free)

        if write_size:
            contig = self._size - self._write_ptr
            contig_write = min(contig, write_size)
            # TODO: avoid 0 write
            # TODO: avoid copy
            # TODO: test performance of above
            self._buffer.seek(self._write_ptr)
            self._buffer.write(data[:contig_write])
            self._write_ptr += contig_write

            if contig < write_size:
                self._buffer.seek(0)
                self._buffer.write(data[contig_write:write_size])
                #self._buffer.write(buffer(data, contig_write, write_size - contig_write))
                self._write_ptr = write_size - contig_write

        self._filled += write_size

        self.lock.release()
        return write_size

    def flush(self):
        """ Flush all data from the FIFO. """
        self._filled = 0
        self._read_ptr = 0
        self._write_ptr = 0

    def _free(self):
        """ Return an approximate number of bytes that can be written to the FIFO. """
        return self._size - self._filled

    def free(self):
        """ Return the number of bytes that can be written to the FIFO. """
        self.lock.acquire()
        size = self._free()
        self.lock.release()
        return size

    def capacity(self):
        """ Return the total space allocated for this FIFO. """
        return self._size

    def __len__(self):
        """ Return the approximate amount of data filled in FIFO """
        return self._filled

    def __nonzero__(self):
        """ Return ```True``` if the FIFO is not empty. """
        return self._filled > 0

    def resize(self, new_size):
        """
        Resize FIFO to contain ``new_size`` bytes. If FIFO currently has
        more than ``new_size`` bytes filled, :exc:`ValueError` is raised.
        If ``new_size`` is less than 1, :exc:`ValueError` is raised.

        If ``new_size`` is smaller than the current size, the internal
        buffer is not contracted (yet).
        """
        self.lock.acquire()
        if new_size < 1:
            raise ValueError("Cannot resize to zero or less bytes.")

        if new_size < self._filled:
            raise ValueError("Cannot contract FIFO to less than {} bytes, "
                             "or data will be lost.".format(self._filled))

        # original data is non-contiguous. we need to copy old data,
        # re-write to the beginning of the buffer, and re-sync
        # the read and write pointers.
        if self._read_ptr >= self._write_ptr:
            old_data = self.read(self._filled)
            self._buffer.seek(0)
            self._buffer.write(old_data)
            self._filled = len(old_data)
            self._read_ptr = 0
            self._write_ptr = self._filled

        self._size = new_size
        self.lock.release()


class Backup:
    def __init__(self, retain_group, name, dateformat, prefix="", suffix="", in_progress=False, readonly=False):
        self.retain_group = retain_group
        self.name = name
        self.in_progress = in_progress
        if not in_progress:
            self.directory = retain_group.directory / name
        else:
            self.directory = retain_group.directory / '{}.in-progress'.format(name)
            if not self.directory.exists():
                self.directory.mkdir(parents=True)
        self.dateformat = dateformat
        self.prefix = prefix
        self.suffix = suffix
        self.readonly = readonly

        self.datetime = datetime.datetime.strptime(name, prefix + dateformat + suffix)

    def __repr__(self):
        in_progress = '<IN PROGRESS> ' if self.in_progress else ''
        return "<Backup {}'{}' in retain group '{}' of collection '{}'>".format(in_progress, self.name, self.retain_group, self.retain_group.collection.name)

    def finalize(self):
        if self.readonly:
            raise BackupStoreReadonlyError("Can't finalize backup. Store is readonly.")
        if self.in_progress:
            newdir = self.retain_group.directory / self.name
            self.directory.rename(newdir)
            self.directory = newdir
        else:
            raise ValueError("Can't finalize an already finalized backup!")

    def delete(self):
        if self.readonly:
            raise BackupStoreReadonlyError("Can't delete backup. Store is readonly.")
        shutil.rmtree(self.directory)


    def create_tar_file(self, filename, subdir='', compression='gz'):
        """Creates a tar file with 'compression' (valid: 'gz', 'xz', 'bz2', '') of the backup contents at 'filename'
        If 'subdir' is specified only create the tar file for the subdirectory
        """
        if subdir is None:
            subdir = self.retain_group.collection.name
        with tarfile.open(filename, mode='w:'+compression) as f:
            f.add(str(self.directory / subdir), arcname='')

    def tar_file_generator(self, subdir=None, bufsize=10*1024*1024, compression='gz'):
        """Creates a tar file and yields the results in blocks of 'bufsize'"""
        if subdir is None:
            subdir = self.retain_group.collection.name
        buf = BytesFIFO(bufsize)
        is_done_event = threading.Event()
        abort_event = threading.Event()
        def tar_write_thread(fileobj, mode, path, is_done_event):
            try:
                import tarfile
                def my_add(tar, name, arcname=None):
                    if abort_event.is_set():
                        raise GeneratorExit
                    if arcname is None:
                        arcname = name
                    tar.add(name, arcname, recursive=False)
                    if os.path.isdir(name):
                        for f in os.listdir(name):
                            my_add(tar, os.path.join(name, f), os.path.join(arcname, f))

                with tarfile.open(fileobj=buf, mode=mode) as f:
                    my_add(f, path, arcname='')
                is_done_event.set()
            except GeneratorExit:
                return

        path = self.directory / subdir
        threading.Thread(target=tar_write_thread, args=(buf, 'w|'+compression, str(path), is_done_event), name="tarfile").start()

        try:
            while True:
                data = buf.read(102400)
                if len(buf) < bufsize*0.2:
                    time.sleep(0)
                if len(data) > 0:
                    yield data
                elif len(buf) == 0 and is_done_event.is_set():
                    break
        except GeneratorExit:
            abort_event.set()


class BackupRetainGroup:
    def __init__(self, collection, name, dateformat, prefix=None, suffix=None, readonly=False):
        self.collection = collection
        self.directory = collection.directory / name
        if not self.directory.exists():
            self.directory.mkdir(parents=True)
        self.dateformat = dateformat
        if not prefix:
            self.prefix = self.collection.name + '_'
        else:
            self.prefix = prefix
        if not suffix:
            self.suffix = ""
        else:
            self.suffix = suffix
        self.name = name
        self.readonly = readonly

    def get_backup(self, name):
        path = self.directory / name
        if not path.exists():
            return None
        in_progress = False
        if name.endswith('.in-progress'):
            name = name[:-len('.in-progress')]
            in_progress = True
        return Backup(self, name, self.dateformat, self.prefix, self.suffix, in_progress=in_progress, readonly=self.readonly)

    def get_backups_for_date(self, date):
        backups = self.list_backups()
        if self.name == RETENTION_DAILY or self.name == RETENTION_RECENT:
            return [b for b in backups if b.datetime.date() == date]
        elif self.name == RETENTION_WEEKLY:
            weeknumber = date.isocalendar()[1]
            return [b for b in backups if b.datetime.isocalendar()[1] == weeknumber]
        elif self.name == RETENTION_MONTHLY:
            return [b for b in backups if b.datetime.month == date.month]
        raise BackupRotationError("Unkown retention plan!")

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<BackupRetainGroup with name '{}' for collection '{}''>".format(self.name, self.collection)

    def new_backup_named(self, name):
        if self.readonly:
            raise BackupStoreReadonlyError("Can't create new backup. Store is readonly!")
        filename = self.prefix + name + self.suffix
        backup = Backup(self, filename, self.dateformat, self.prefix, self.suffix, in_progress=True)
        return backup

    def new_empty_backup(self):
        dt = datetime.datetime.now(datetime.timezone.utc)
        datestr = datetime.datetime.strftime(dt, self.dateformat)
        return self.new_backup_named(datestr)

    def sorted_backups(self, backups):
        return sorted(backups, key=lambda b: b.datetime)

    def list_all_backups(self):
        try:
            folders = [f.name for f in self.directory.iterdir() if f.is_dir() and
                f.name.startswith(self.prefix) and f.name.endswith(self.suffix)]
            backups = [self.get_backup(f) for f in folders]
            return self.sorted_backups(backups)
        except FileNotFoundError:
            return []

    def list_in_progress_backups(self):
        return [backup for backup in self.list_all_backups() if backup.in_progress]

    def list_backups(self):
        return [backup for backup in self.list_all_backups() if not backup.in_progress]

    def get_latest_backup(self):
        backups = self.list_backups()
        if len(backups) > 0:
            return backups[-1]
        return None

class BackupCollection:
    def __init__(self, store, name, dateformat, prefix=None, suffix=None, readonly=False):
        self.store = store
        self.name = name
        self.directory = store.directory / name
        self.dateformat = dateformat
        self.prefix = prefix
        self.suffix = suffix
        self.readonly = readonly

        # Create groups if they don't exist yet
        if not self.readonly:
            for group in RETENTION_GROUPS + RETENTION_MANUAL:
                self.get_retain_group(group)

    def get_retain_group(self, name):
        if name in RETENTION_GROUPS or name in RETENTION_MANUAL:
            return BackupRetainGroup(self, name, self.dateformat, self.prefix, self.suffix, readonly=self.readonly)
        else:
            raise BackupError("Invalid retain group {}".format(name))

    def list_retain_groups(self):
        return [self.get_retain_group(f.name) for f in self.directory.iterdir()
            if f.is_dir() and not f.is_symlink()]

    def __repr__(self):
        return "<BackupCollection at {}>".format(self.directory)

class BackupStore:
    def __init__(self, directory, dateformat, readonly=False):
        self.directory = directory
        self.dateformat = dateformat
        self.readonly = readonly

    def get_collection(self, name, prefix=None, suffix=None):
        path = self.directory / name
        if not path.exists():
            if self.readonly:
                raise BackupStoreReadonlyError("Collection not found. Not creating because readonly flag is set.")
            else:
                path.mkdir(parents=True)
        return BackupCollection(self, name, self.dateformat, prefix=prefix, suffix=suffix, readonly=self.readonly)

    def list_collections(self):
        return [self.get_collection(f.name) for f in self.directory.iterdir() if f.is_dir()]

    def __repr__(self):
        return "<BackupStore at {}>".format(self.directory)

class BackupRotation:
    def __init__(self, collection, dateformat, retention_plan, prefix=None, suffix=None, simulate=False, verbose=False):
        self.collection = collection
        self.dateformat = dateformat
        self.retention_plan = retention_plan
        self.prefix = prefix
        self.suffix = suffix
        self.simulate = simulate
        self.verbose = verbose

    def get_next_retain_group(self, retain_group):
        """Returns the retain group one higher in the list if the retention plan is active"""
        retain_name = retain_group.name
        if retain_name == RETENTION_RECENT:
            if self.retention_plan[RETENTION_DAILY] > 0:
                return self.collection.get_retain_group(RETENTION_DAILY)
            retain_name = RETENTION_DAILY
        if retain_name == RETENTION_DAILY:
            if self.retention_plan[RETENTION_WEEKLY] > 0:
                return self.collection.get_retain_group(RETENTION_WEEKLY)
            retain_name = RETENTION_DAILY
        if retain_name == RETENTION_WEEKLY:
            if self.retention_plan[RETENTION_MONTHLY] > 0:
                return self.collection.get_retain_group(RETENTION_MONTHLY)
            retain_name = RETENTION_DAILY
        return None

    def select_promotion_backup(self, from_retain_group, to_retain_group, date):
        """Selects the backup to promote as daily backup for the calendar day given
           This selects the latest backup earlier than 13:00 if possible

           Selects the backup to promote as weekly backup for the first day of the calendar week
           the given day is in

           Selects the backup to promote as monthly backup for the first day of the month
           the given day is in"""
        selected_backup = None
        backups = from_retain_group.list_backups()
        if not to_retain_group:
            return None

        if to_retain_group.name == RETENTION_DAILY:
            # promote to daily backup
            backups = [ b for b in backups if b.datetime.date() == date ]
            if len(backups) >= 1:
                selected_backup = backups[0]
                for backup in backups:
                    if backup.datetime.hour < 13:
                        selected_backup = backup
        elif to_retain_group.name == RETENTION_WEEKLY:
            # promote to weekly backup
            weeknumber = date.isocalendar()[1]
            backups = [ b for b in backups if b.datetime.isocalendar()[1] == weeknumber ]
            selected_backup = None
            if len(backups) >= 1:
                selected_backup = backups[0]
        elif to_retain_group.name == RETENTION_MONTHLY:
            # promote to monthly backup
            month = date.month
            backups = [ b for b in backups if b.datetime.month == month ]
            selected_backup = None
            if len(backups) >= 1:
                selected_backup = backups[0]
            return selected_backup
        else:
            raise BackupRotationError("Unknown retain group!")

        return selected_backup

    def should_promote_backup(self, from_retain_group, date):
        now = datetime.datetime.utcnow()
        to_retain_group = self.get_next_retain_group(from_retain_group)
        if from_retain_group.name == RETENTION_RECENT and now.date() <= date and now.hour < 12:
            # If it is already 13:00 or a later date a backup should be promoted if none exists
            # Otherwise just wait longer
            return False
        if to_retain_group and len(to_retain_group.get_backups_for_date(date)) < 1 and self.retention_plan[to_retain_group.name] > 0:
            return True
        return False

    def list_backups_to_delete(self):
        deletion_backups = []
        for group in RETENTION_GROUPS:
            backups = self.collection.get_retain_group(group).list_backups()
            keep = self.retention_plan[group]
            deletion_backups += backups[:-keep]
        return deletion_backups

    def promote_backup_to_retain_group(self, backup, retain_group):
        backupdir = retain_group.directory / backup.name
        if self.verbose:
            print("Promoting {} to dir: {}".format(backup, backupdir))
        if not self.simulate:
            shutil.copytree(str(backup.directory), str(backupdir), copy_function=os.link) #TODO update to use pathlib
            # This is now the latest backup in the group. create latest symlink.
            link_location = retain_group.directory / 'latest'
            try:
                link_location.unlink()
            except OSError:
                pass
            link_location.symlink_to(backupdir)

    def promote_backups(self):
        date = datetime.datetime.utcnow().date()

        for groupname in RETENTION_GROUPS:
            group = self.collection.get_retain_group(groupname)
            if self.should_promote_backup(group, date):
                to_retain_group = self.get_next_retain_group(group)
                if self.verbose:
                    print("We should promote a '{}' backup".format(str(to_retain_group)))
                backup_to_promote = self.select_promotion_backup(group, to_retain_group, date)
                if backup_to_promote:
                    self.promote_backup_to_retain_group(backup_to_promote, to_retain_group)
                elif self.verbose:
                    print("Can't find a '{}' backup to promote to '{}'. Try later.".format(str(group), str(to_retain_group)))

    def cleanup_backups(self):
        to_delete = self.list_backups_to_delete()
        for backup in to_delete:
            if self.verbose:
                print("Deleting backup '{}'".format(backup))
            if not self.simulate:
                backup.delete()


def get_default_backuproll(world, simulate=True):
    return BackupRoll('/opt/wurstmineberg/backup/{}'.format(world), '{}_'.format(world), '.tar.gz', '%Y-%m-%dT%H:%M:%S', None, simulate=simulate)

class RsyncBackupCommand:
    def __init__(self, source, retain_group, simulate=False, verbose=False, rsync_flags=[]):
        self.source = source
        self.retain_group = retain_group
        self.simulate = simulate
        self.verbose = verbose
        self.rsync_flags = rsync_flags + ['-a', '--delete']
        if self.verbose:
            self.rsync_flags += ['-v']

    def run_rsync(self, args):
        out = None if self.verbose else subprocess.DEVNULL
        command = ['rsync'] + args
        if self.verbose:
            print("Running command: {}".format(command))
        if not self.simulate:
            return subprocess.call(command, stdout=out, stderr=out)
        return 0


    def run_blocking(self, backup_name=None):
        args = list(self.rsync_flags)

        # Find out if there are older backups
        latest_backup = self.retain_group.get_latest_backup()
        if latest_backup:
            args += ['--link-dest={}'.format(latest_backup.directory)]

        if not backup_name:
            new_backup = self.retain_group.new_empty_backup()
        else:
            new_backup = self.retain_group.new_backup_named(backup_name)
        args += [str(self.source), str(new_backup.directory)]

        ret = self.run_rsync(args)
        if not self.simulate:
            if ret == 0:
                new_backup.finalize()
                # update the 'latest' symlinks
                link_locations = [self.retain_group.directory / 'latest', self.retain_group.collection.directory / 'latest']
                for location in link_locations:
                    try:
                        location.unlink()
                    except OSError:
                        pass
                    location.symlink_to(new_backup.directory)
                return True
            return False
        else:
            return True


    def run_restore(self, backup=None, subdirectory=''):
        world_name = backup.retain_group.collection.name
        backup_path = backup.directory / world_name / subdirectory
        restore_path = self.source / subdirectory

        args = list(self.rsync_flags)
        args += [str(backup_path), str(restore_path)]
        ret = self.run_rsync(args)
        if ret != 0:
            return False
        return True


class MinecraftBackupRunner:
    def __init__(self,
                 worldsdir,
                 store,
                 dateformat,
                 pre_backup_command = None,
                 post_backup_command = None,
                 fail_backup_command = None,
                 simulate = False,
                 verbose = False,
                 worldconfig = None):
        self.worldsdir = worldsdir
        self.store = store
        self.pre_backup_command = pre_backup_command
        self.post_backup_command = post_backup_command
        self.fail_backup_command = fail_backup_command
        self.dateformat = dateformat
        self.simulate = simulate
        self.verbose = verbose
        self.worldconfig = worldconfig

    def parse_command(self, command, **kwargs):
        return command.format(**kwargs)

    def cleanup_world(self, world):
        collection = self.store.get_collection(world)
        for retain_group in collection.list_retain_groups():
            for backup in retain_group.list_in_progress_backups():
                if self.verbose:
                    print("Removing backup '{}'".format(backup))
                if not self.simulate:
                    backup.delete()

    def cleanup_worlds(self, worlds):
        for world in worlds:
            self.cleanup_world(world)

    def get_collection(self, world):
        return self.store.get_collection(world, )

    def backup_world(self, world):
        if self.verbose:
            print("Running backup for world {}".format(world))
        worlddir = self.worldsdir / world
        backup_collection = self.get_collection(world)
        retain_group = backup_collection.get_retain_group(RETENTION_RECENT)
        runner = RsyncBackupCommand(worlddir, retain_group, simulate=self.simulate, verbose=self.verbose)
        out = None if self.verbose else subprocess.DEVNULL

        try:
            if self.pre_backup_command:
                cmd = self.parse_command(self.pre_backup_command, world=world)
                if self.verbose:
                    print("Running pre-backup command '{}'".format(cmd))
                if not self.simulate:
                    retcode = subprocess.call(cmd, stdout=out, stderr=out, shell=True)
                    if retcode != 0:
                        raise BackupError("Pre-backup command `{}` exited with error code {}! Bailing!".format(cmd, retcode))

            ret = runner.run_blocking()

            if ret:
                if self.verbose:
                    print("Backup complete.")
                if self.post_backup_command:
                    cmd = self.parse_command(self.post_backup_command, world=world)
                    if self.verbose:
                        print("Running post-backup command '{}'".format(cmd))
                    if not self.simulate:
                        retcode = subprocess.call(cmd, stdout=out, stderr=out, shell=True)
                        if retcode != 0:
                            raise BackupError("Post-Backup command `{}` failed with error code {}! Bailing!".format(cmd, retcode))
            else:
                raise BackupError("Backup command failed!")
        except:
            print("Backup failed!", file=sys.stderr)
            if self.fail_backup_command:
                cmd = self.parse_command(self.fail_backup_command, world=world)
                if self.verbose:
                    print("Running fail-backup command '{}'".format(cmd))
                if not self.simulate:
                    subprocess.call(cmd, stdout=out, stderr=out, shell=True)
            raise


    def backup_worlds(self, worlds):
        for world in worlds:
            self.backup_world(world)

    def rotate_backups(self, worlds):
        for world in worlds:
            if self.verbose:
                print("Rotating backups for world '{}'".format(world))
            if self.worldconfig and world in self.worldconfig and 'keep' in self.worldconfig[world]:
                retention_plan = self.worldconfig[world]['keep']
                backup_collection = self.get_collection(world)
                rotation = BackupRotation(backup_collection, self.dateformat, retention_plan, simulate=self.simulate, verbose=self.verbose)
                rotation.promote_backups()
                rotation.cleanup_backups()

    def restore_world(self, backup, subdirectory, pre_restore_command=None, post_restore_command=None):
        worlddir = self.worldsdir / backup.retain_group.collection.name
        runner = RsyncBackupCommand(worlddir, backup.retain_group, simulate=self.simulate, verbose=self.verbose)
        world_name = backup.retain_group.collection.name
        if verbose:
            print("Restoring world {} from backup {}".format(world_name, backup.name))
        if pre_restore_command:
            cmd = self.parse_command(pre_restore_command, world=world_name)
            if self.verbose:
                print("Running pre-restore command '{}'".format(cmd))
            if not self.simulate:
                retcode = subprocess.call(cmd, stdout=out, stderr=out, shell=True)
                if retcode != 0:
                    raise BackupError("Pre-Restore command exited with non-zero exit code! Bailing!")

        ret = runner.run_restore(backup, subdirectory)

        if ret:
            if self.verbose:
                print("Restore complete.")
            if post_restore_command:
                cmd = self.parse_command(post_restore_command, world=world_name)
                if self.verbose:
                    print("Running post-restore command '{}'".format(cmd))
                if not self.simulate:
                    retcode = subprocess.call(cmd, stdout=out, stderr=out, shell=True)
                    if retcode != 0:
                        raise BackupError("Post-Restore command failed with error code {}! Bailing!".format(retcode))
        else:
            raise BackupError("Restore command failed!")



class CursesMenu(object):
    def __init__(self, items, stdscreen, title=None, exit_title="exit"):
        self.window = stdscreen.subwin(0,0)
        self.window.keypad(1)
        self.panel = panel.new_panel(self.window)
        self.panel.hide()
        panel.update_panels()

        self.position = 0
        self.items = items
        self.items.append((exit_title, 'exit', None))
        self.title = title
        self.should_exit = False

    def navigate(self, n):
        self.position += n
        if self.position < 0:
            self.position = len(self.items)-1
        elif self.position >= len(self.items):
            self.position = 0

    def display(self):
        self.panel.top()
        self.panel.show()
        self.window.clear()

        while True:
            self.window.refresh()
            curses.doupdate()

            if self.title:
                self.window.addstr(1, 1, self.title)

            for index, item in enumerate(self.items):
                if index == self.position:
                    mode = curses.A_REVERSE
                else:
                    mode = curses.A_NORMAL

                msg = '%d. %s' % (index, item[0])
                offset = 1
                if self.title:
                    offset = 2
                    offset += len(self.title.splitlines())
                self.window.addstr(offset+index, 1, msg, mode)

            key = self.window.getch()

            if key in [curses.KEY_ENTER, ord('\n')]:
                if self.position == len(self.items)-1:
                    break
                else:
                    self.items[self.position][1](*self.items[self.position][2])
                    if self.should_exit:
                        break

            elif key == curses.KEY_UP:
                self.navigate(-1)

            elif key == curses.KEY_DOWN:
                self.navigate(1)

        self.window.clear()
        self.panel.hide()
        panel.update_panels()
        curses.doupdate()


class MinecraftInteractiveRestoreInterface:

    def __init__(self,
                 worldsdir,
                 store,
                 dateformat,
                 pre_restore_command = None,
                 post_restore_command = None,
                 simulate = False):
        self.worldsdir = worldsdir
        self.store = store
        self.pre_restore_command = pre_restore_command
        self.post_restore_command = post_restore_command
        self.dateformat = dateformat
        self.simulate = simulate
        self.menus = []
        self.should_do_restore = False

    def display(self):
        def interface(screen):
            self.screen = screen
            screen.clear()
            curses.curs_set(0)

            world_menu = []
            for world in self.store.list_collections():
                world_menu.append((world.name, self.select_retain_group, (world,)))

            main_menu = CursesMenu(world_menu, self.screen, title="Select a world to restore backups from")
            self.menus.append(main_menu)
            main_menu.display()

        curses.wrapper(interface)

    def select_retain_group(self, world):
        items = []
        for retain_group in world.list_retain_groups():
            items.append((retain_group.name, self.select_backup, (retain_group,)))
        menu = CursesMenu(items, self.screen, title="Select a backup group", exit_title="back")
        self.menus.append(menu)
        menu.display()

    def select_backup(self, retain_group):
        items = []
        for backup in retain_group.list_backups():
            items.append((backup.name, self.confirm_pre_restore, (backup,)))
        menu = CursesMenu(items, self.screen, title="Select a backup", exit_title="back")
        self.menus.append(menu)
        menu.display()

    def confirm_pre_restore(self, backup):
        items = [
            ("Run {}".format(self.pre_restore_command), self.confirm_post_restore, (backup, True)),
            ("Don't run pre-restore command", self.confirm_post_restore, (backup, False))
        ]
        menu = CursesMenu(items, self.screen, title="Do you want to run the pre-restore command?", exit_title="back")
        self.menus.append(menu)
        menu.display()

    def confirm_post_restore(self, backup, run_pre_restore):
        items = [
            ("Run {}".format(self.post_restore_command), self.restore_mode, (backup, run_pre_restore, True)),
            ("Don't run post-restore command", self.restore_mode, (backup, run_pre_restore, False))
        ]
        menu = CursesMenu(items, self.screen, title="Do you want to run the post-restore command after restore?", exit_title="back")
        self.menus.append(menu)
        menu.display()

    def restore_mode(self, backup, run_pre_restore, run_post_restore):
        items = [
            ("Only restore the world subdirectory", self.confirm, (backup, run_pre_restore, run_post_restore, True)),
            ("Restore everything", self.confirm, (backup, run_pre_restore, run_post_restore, False))
        ]
        menu = CursesMenu(items, self.screen, title="What do you want to restore?", exit_title="back")
        self.menus.append(menu)
        menu.display()

    def confirm(self, backup, run_pre_restore, run_post_restore, world_only):
        items = [
            ("Confirm. Run Restore now!", self.confirmed_restore, (backup, run_pre_restore, run_post_restore, world_only)),
            ("HALP No! Abort! (exits)", self.exit, ())
        ]
        menu = CursesMenu(items, self.screen, title="Please confirm your selection:\n "
                "Restore {}\n "
                "to {}.\n "
                "Run pre-restore hook: {}.\n "
                "Run post-restore hook: {}.\n "
                "Only restore world subdirectory: {}\n ".format(backup.name,
                str(self.worldsdir / backup.retain_group.collection.name),
                run_pre_restore,
                run_post_restore,
                world_only),
            exit_title="back")
        self.menus.append(menu)
        menu.display()

    def confirmed_restore(self, backup, run_pre_restore, run_post_restore, world_only):
        self.backup = backup
        self.world_only = world_only
        self.run_pre_restore = run_pre_restore
        self.run_post_restore = run_post_restore

        self.should_do_restore = True
        for menu in self.menus:
            menu.should_exit = True

    def exit(self):
        sys.exit(1)


class MinecraftBackupRoll:
    """
    MinecraftBackupRoll - Class that encapsulates a backuproll operation. Will
    usually be instantiated once if run as a script, and subsequently used for
    one or multiple activities.

    Example usage 1:

    minecraft_backup_roll = MinecraftBackupRoll(
        use_pid_file=True,
        selected_worlds=["example_world"],
        verbose=True)

    minecraft_backup_roll.do_activity(do_cleanup=False,
                                    do_rotation=True,
                                    do_backup=True)

    Example usage 2:

    minecraft_backup_roll = MinecraftBackupRoll(
        use_pid_file=True,
        selected_worlds=["example_world"],
        verbose=True)

    minecraft_backup_roll.interactive_restore()

    """
    def __init__(self, simulate=False, verbose=False, config=None,
                 config_file=None, use_pid_file=True, selected_worlds=None):
        """
        Initialize a MinecraftBackupRoll.


        Keyword arguments:

        simulate -- if True, perform no destructive operation (default False).
            Also, implies verbose=True

        verbose -- print things (default False)

        config -- Set config. Will use config loaded by
            wurstmineberg-common-python if None (ie by default).

            assets/backuproll2.default.json contains the default config, and
            therefore a complete list of configuration options.

            (default None)

        config_file -- Deprecated in favor of exclusively using the config
            argument in the spirit of "There should be one -- and preferably
            only one -- obvious way to do it.".

            Parse this file as JSON, updating the contents of config.

            (default '/opt/wurstmineberg/config/backuproll2.json')

        use_pid_file -- use a pidfile to avoid making a huge mess. Needs to be
            True for this MinecraftBackupRoll to perform write operations.

            (default True)

        selected_worlds -- sequence of world names to operate on. If None or
            empty, use the worlds set in the config.

            (default None)
        """
        if selected_worlds is None:
            selected_worlds = []
        if config is None:
            config = CONFIG

        self.config = config.copy()
        self.use_pid_file = use_pid_file

        if not config_file is None:
            config_file='/opt/wurstmineberg/config/backuproll2.json'
            raise DeprecationWarning(
"""
The `config_file` keyword argument for MinecraftBackupRoll.__init__ is
deprecated and will be removed soon. Instead, use the `config` keyword argument
(and something like wurstmineberg-common-python to generate its contents).
""")

            with contextlib.suppress(FileNotFoundError):
                with open(config_file) as file_cfg:
                    self.config.update(json.load(file_cfg))

        if len(selected_worlds) > 0:
            self.selected_worlds = selected_worlds
        else:
            self.selected_worlds = self.config['worlds'].keys()
        if len(self.selected_worlds) == 0:
            print("No world selected and none found in the config file. Exiting.")
            raise MinecraftBackupRollError("Nothing to do.")

        self.backupfolder = pathlib.Path(self.config['backupfolder'])
        self.worldfolder = pathlib.Path(self.config['worldfolder'])
        self.dateformat = self.config['dateformat']
        self.worldconfig = self.config['worlds']
        self.locked = False

        self.pre_backup_command = None
        if 'pre_backup_command' in self.config:
            self.pre_backup_command = self.config['pre_backup_command']

        self.post_backup_command = None
        if 'pre_backup_command' in self.config:
            self.post_backup_command = self.config['post_backup_command']

        self.fail_backup_command = None
        if 'pre_backup_command' in self.config:
            self.fail_backup_command = self.config['fail_backup_command']

        self.pre_restore_command = None
        if 'pre_restore_command' in self.config:
            self.pre_restore_command = self.config['pre_restore_command']

        self.post_restore_command = None
        if 'post_restore_command' in self.config:
            self.post_restore_command = self.config['post_restore_command']

        self.minecraft_backup_runner = MinecraftBackupRunner(self.worldfolder,
            self.store,
            self.dateformat,
            pre_backup_command = self.pre_backup_command,
            post_backup_command = self.post_backup_command,
            fail_backup_command = self.fail_backup_command,
            simulate = simulate,
            verbose = verbose,
            worldconfig = self.worldconfig)


    @classmethod
    def get_readonly_store(self):
        roll = MinecraftBackupRoll(use_pid_file=False)
        return roll.store

    @classmethod
    def get_readwrite_store(self):
        roll = MinecraftBackupRoll(use_pid_file=True)
        return roll.store

    @property
    def store(self):
        readonly = not self.use_pid_file
        return BackupStore(self.backupfolder, self.dateformat, readonly)

    def do_activity(self, do_cleanup=False, do_backup=True, do_rotation=True):
        """
        Perform the actual operation(s).

        Keyword arguments:

        do_cleanup -- Whether to perform a cleanup (default False)
        do_backup -- Whether to make a backup (default True)
        do_rotation -- Whether to perform a rotation (default True)
        """
        if not self.use_pid_file:
            raise MinecraftBackupRollError("Readonly MinecraftBackupRoll")
        if not self.try_lock():
            raise MinecraftBackupRollError("PID file exists and other process still running!")

        try:
            if do_cleanup:
                self.minecraft_backup_runner.cleanup_worlds(self.selected_worlds)
            if do_backup:
                self.minecraft_backup_runner.backup_worlds(self.selected_worlds)
            if do_rotation:
                self.minecraft_backup_runner.rotate_backups(self.selected_worlds)
        finally:
            if self.use_pid_file:
                self.unlock()

    def _do_restore(self, backup, world_only=True, pre_restore_command=None, post_restore_command=None):
        if world_only:
            restore_subdirectory = 'world'
            if not (backup.directory / restore_subdirectory).exists():
                restore_subdirectory = interface.backup.retain_group.collection.name
        else:
            restore_subdirectory = ''
        self.minecraft_backup_runner.restore_world(backup,
            restore_subdirectory,
            pre_restore_command=pre_restore_command,
            post_restore_command=post_restore_command)


    def do_restore(self, backup, world_only=True, pre_restore_command=None, post_restore_command=None):
        self._force_lock_now()
        try:
            self._do_restore(backup, world_only, pre_restore_command, post_restore_command)
        finally:
            self.unlock()

    def interactive_restore(self, simulate=False):
        """
        Interactively restore a world from backup. The world to be restored is
        selected via a text-based interface, and this function blocks until a
        selection has been made (and the backup subsequently restored).

        Keyword arguments:
        simulate -- whether or not to simulate the restore operation
            (default False)
        """
        self._force_lock_now()
        try:
            interface = MinecraftInteractiveRestoreInterface(self.worldfolder,
                                                             self.store,
                                                             self.dateformat,
                                                             pre_restore_command = self.pre_restore_command,
                                                             post_restore_command = self.post_restore_command,
                                                             simulate = simulate)
            interface.display()
            if interface.should_do_restore:
                backup = interface.backup
                pre_cmd = self.pre_restore_command if interface.run_pre_restore else None
                post_cmd = self.post_restore_command if interface.run_post_restore else None
                self._do_restore(backup, interface.world_only, pre_cmd, post_cmd)
        finally:
            self.unlock()

    def unlock(self):
        if self.locked:
            pathlib.Path(self.config['pidfile']).unlink()
        else:
            raise MinecraftBackupRollError("Wasn't locked in the first place.")

    def lock(self):
        """Blocking lock function"""
        while not self.try_lock():
            time.sleep(1)

    def try_lock(self):
        pid_filename = pathlib.Path(self.config['pidfile'])
        if pid_filename.is_file():
            with pid_filename.open() as pidfile:
                try:
                    pid = int(pidfile.read())
                except ValueError:
                    pid = None
            if pid:
                try:
                    os.kill(pid, 0)
                    print('Another backuproll process with PID {} is still running. Terminating.'.format(pid), file=sys.stderr)
                    return False
                except ProcessLookupError:
                    pass
        mypid = os.getpid()
        with pid_filename.open('w+') as pidfile:
            pidfile.write(str(mypid))
        self.locked = True
        return True

    def _force_lock_now(self):
        if not self.use_pid_file:
            raise MinecraftBackupRollError("Readonly MinecraftBackupRoll")
        if not self.try_lock():
            raise MinecraftBackupRollError("PID file exists and other process still running!")


def main():
    arguments = docopt.docopt(__doc__, version='Minecraft backup roll ' + __version__)

    selected_worlds = []
    if arguments['<world>'] and not arguments['--all']:
        selected_worlds = [arguments['<world>']]

    simulate = False
    verbose = False
    if arguments['--simulate']:
        print("Simulating backuproll: No real action will be performed")
        simulate = True
        verbose = True
    if arguments['--verbose']:
        verbose = True

    if arguments['cron']:
        do_cleanup = False
        do_backup = True
        do_rotation = True
    elif arguments['backup']:
        do_backup = True
        do_cleanup = False
        do_rotation = False
    elif arguments['rotate']:
        do_backup = False
        do_cleanup = False
        do_rotation = True
    elif arguments['cleanup']:
        do_backup = False
        do_cleanup = True
        do_rotation = False
    elif arguments['restore']:
        do_backup = False
        do_cleanup = False
        do_rotation = False
        raise NotImplementedError('restore not implemented') #TODO
    elif arguments['restore-interactive']:
        do_backup = False
        do_cleanup = False
        do_rotation = False
    else:
        raise NotImplementedError('Subcommand not implemented')

    if arguments['--cleanup']:
        do_cleanup = True
    if arguments['--no-backup']:
        do_backup = False
    if arguments['--no-rotation']:
        do_rotation = False

    minecraft_backup_roll = MinecraftBackupRoll(
        use_pid_file=True,
        selected_worlds=selected_worlds,
        simulate=simulate,
        verbose=verbose)

    if arguments['restore']:
        minecraft_backup_roll.interactive_restore(backup=arguments['<backup>'])
    elif arguments['restore-interactive']:
        minecraft_backup_roll.interactive_restore()
    else:
        minecraft_backup_roll.do_activity(do_cleanup=do_cleanup, do_rotation=do_rotation, do_backup=do_backup)


if __name__ == "__main__":
    main()
