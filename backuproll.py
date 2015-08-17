#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Backup roll script for one or more Minecraft servers

Usage:
  backuproll [options] [<world>]

Options:
  -h, --help         Print this message and exit.
  --all              Apply the action to all configured worlds. This is the default.
  --config=<config>  Path to the config file [default: /opt/wurstmineberg/config/backuproll.json].
  --version          Print version info and exit.
  --verbose          Print things.
  --simulate         Don't do any destructive operation, implies --verbose

"""

import sys
import datetime
import os
import pathlib
import contextlib
import json
import subprocess

from docopt import docopt

__version__ = '0.1'
DEFAULT_CONFIG = {
    "backupcommand": "/opt/wurstmineberg/bin/minecraft backup",
    "backupfolder": "/opt/wurstmineberg/backup/worlds/",
    "worlds": {
        "testworld": {
            "keep": {
                "recent": 6,
                "daily": 10,
                "weekly": 4,
                "monthly": 6
            }
        }
    },
    "pidfile": "/var/local/wurstmineberg/backuproll.pid"
}

class BackupFile:
    def __init__(self, basedir, filename, prefix, suffix, dateformat):
        self.basedir = basedir
        self.filename = filename
        self.prefix = prefix
        self.suffix = suffix
        self.dateformat = dateformat
        self.path = os.path.join(basedir, filename)

    @property
    def datetime(self):
        datestr = self.filename[len(self.prefix):-len(self.suffix)]
        return datetime.datetime.strptime(datestr, self.dateformat)

    def __repr__(self):
        return "<Backup from date '{}'>".format(self.datetime, self.filename)

class BackupRoll:
    def __init__(self, backupdir, prefix, suffix, dateformat, keepdict, simulate=False, verbose=False):
        self.backupdir = backupdir
        self.prefix = prefix
        self.suffix = suffix
        self.dateformat = dateformat
        self.keepdict = keepdict
        self.simulate = simulate
        self.verbose = verbose

    @property
    def dailydir(self):
        return os.path.join(self.backupdir, 'daily')

    @property
    def weeklydir(self):
        return os.path.join(self.backupdir, 'weekly')

    @property
    def monthlydir(self):
        return os.path.join(self.backupdir, 'monthly')

    def sorted_backups(self, backups):
        return sorted(backups, key=lambda b: b.datetime)

    def select_promote_daily_backup(self, backups, date):
        """Selects the backup to promote as daily backup for the calendar day given
           This selects the latest backup earlier than 13:00 if possible"""
        backups = [ b for b in backups if b.datetime.date() == date ]
        selected_backup = None
        if len(backups) >= 1:
            selected_backup = backups[0]
            for backup in backups:
                if backup.datetime.hour < 13:
                    selected_backup = backup
        return selected_backup

    def select_promote_weekly_backup(self, backups, date):
        """Selects the backup to promote as weekly backup for the first day of the calendar week
           the given day is in"""
        weeknumber = date.isocalendar()[1]
        backups = [ b for b in backups if b.datetime.isocalendar()[1] == weeknumber ]
        selected_backup = None
        if len(backups) >= 1:
            selected_backup = backups[0]
        return selected_backup

    def select_promote_monthly_backup(self, backups, date):
        """Selects the backup to promote as monthly backup for the first day of the month
           the given day is in"""
        month = date.month
        backups = [ b for b in backups if b.datetime.month == month ]
        selected_backup = None
        if len(backups) >= 1:
            selected_backup = backups[0]
        return selected_backup

    def should_promote_daily_backup(self, date):
        now = datetime.datetime.now()
        if now.date() > date or now.hour >= 13:
            # If it is already 13:00 or a later date a backup should be promoted if none exists
            return not self.get_backup_daily_for_date(date) and self.keepdict['daily'] > 0
        return False

    def should_promote_weekly_backup(self, date):
        return not self.get_backup_weekly_for_date(date) and self.keepdict['weekly'] > 0

    def should_promote_monthly_backup(self, date):
        return not self.get_backup_monthly_for_date(date) and self.keepdict['monthly'] > 0

    def list_backups_to_delete(self):
        recents = self.list_backups_recent()
        daily = self.list_backups_daily()
        weekly = self.list_backups_weekly()
        monthly = self.list_backups_monthly()

        recentkeep = self.keepdict['recent']
        dailykeep = self.keepdict['daily']
        weeklykeep = self.keepdict['weekly']
        monthlykeep = self.keepdict['monthly']

        return recents[:-recentkeep] + daily[:-dailykeep] + weekly[:-weeklykeep] + monthly[:-monthlykeep]

    def promote_backup_to_dir(self, backup, directory):
        if self.verbose:
            print("Promoting {} to dir: {}".format(backup, directory))
        if not self.simulate:
            try:
                os.makedirs(directory)
            except FileExistsError:
                pass
            os.link(backup.path, os.path.join(directory, backup.filename))

    def promote_backups(self):
        date = datetime.date.today()
        if self.should_promote_daily_backup(date):
            if self.verbose:
                print("We should promote a daily backup")
            backup_to_promote = self.select_promote_daily_backup(self.list_backups_recent(), date)
            if backup_to_promote:
                self.promote_backup_to_dir(backup_to_promote, self.dailydir)
            elif self.verbose:
                print("Can't find a daily backup to promote. Try later.")

        if self.should_promote_weekly_backup(date):
            if self.verbose:
                print("We should promote a weekly backup")
            backup_to_promote = self.select_promote_weekly_backup(self.list_backups_daily(), date)
            if backup_to_promote:
                self.promote_backup_to_dir(backup_to_promote, self.weeklydir)
            elif self.verbose:
                print("Can't find a weekly backup to promote. Try later.")

        if self.should_promote_monthly_backup(date):
            if self.verbose:
                print("We should promote a monthly backup")
            backup_to_promote = self.select_promote_monthly_backup(self.list_backups_daily(), date)
            if backup_to_promote:
                self.promote_backup_to_dir(backup_to_promote, self.monthlydir)
            elif self.verbose:
                print("Can't find a monthly backup to promote. Try later.")

    def delete_backup(self, backup):
        if self.verbose:
            print("Deleting {}".format(backup))
        if not self.simulate:
            os.remove(backup.path)

    def cleanup_backups(self):
        to_delete = self.list_backups_to_delete()
        for backup in to_delete:
            self.delete_backup(backup)

    def list_backups_from(self, folder):
        try:
            files = [ f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)) and
                f.startswith(self.prefix) and f.endswith(self.suffix) ]
            backups = [ BackupFile(folder, f, self.prefix, self.suffix, self.dateformat) for f in files ]
            return self.sorted_backups(backups)
        except FileNotFoundError:
            return []

    def list_backups_recent(self):
        return self.list_backups_from(self.backupdir)

    def list_backups_daily(self):
        return self.list_backups_from(self.dailydir)

    def list_backups_weekly(self):
        return self.list_backups_from(self.weeklydir)

    def list_backups_monthly(self):
        return self.list_backups_from(self.monthlydir)

    def get_backup_daily_for_date(self, date):
        for backup in self.list_backups_daily():
            if backup.datetime.date() == date:
                return backup

    def get_backup_weekly_for_date(self, date):
        weeknumber = date.isocalendar()[1]
        for backup in self.list_backups_weekly():
            if backup.datetime.isocalendar()[1] == weeknumber:
                return backup

    def get_backup_monthly_for_date(self, date):
        for backup in self.list_backups_monthly():
            if backup.datetime.month == date.month:
                return backup

class BackupRunner:
    def __init__(self, command, simulate=False, verbose=False):
        self.command = command
        self.simulate = simulate
        self.verbose = verbose

    def run_blocking(self):
        if self.verbose:
            print("Running command '{}'".format(self.command))
        if not self.simulate:
            out = None if self.verbose else subprocess.DEVNULL
            retcode = subprocess.call(self.command, stdout=out, stderr=out, shell=True)
            return retcode == 0
        return True

def do_backuproll(worlds, backupcommand, has_world_prefix=True, extension='tar.gz', dateformat='%Y-%m-%d_%Hh%M', simulate=False, verbose=False):
    for world in worlds:
        command = backupcommand + ' ' + world
        runner = BackupRunner(command, simulate, verbose)
        ret = runner.run_blocking()

        if not ret:
            print("Backup failed! Not running backuproll!", file=sys.stderr)
            exit(1)

        prefix = world + '_' if has_world_prefix else ''
        keepdict = CONFIG['worlds'][world]['keep']
        roll = BackupRoll(str(CONFIG['backupfolder'] / world), prefix, '.' + extension, dateformat, keepdict, simulate=simulate, verbose=verbose)
        roll.promote_backups()
        roll.cleanup_backups()

if __name__ == "__main__":
    arguments = docopt(__doc__, version='Minecraft backup roll ' + __version__)
    CONFIG_FILE = pathlib.Path(arguments['--config'])

    CONFIG = DEFAULT_CONFIG.copy()
    with contextlib.suppress(FileNotFoundError):
        with CONFIG_FILE.open() as config_file:
            CONFIG.update(json.load(config_file))
    CONFIG['backupfolder'] = pathlib.Path(CONFIG['backupfolder'])

    backupcommand = CONFIG['backupcommand']

    selected_worlds = CONFIG['worlds'].keys()
    if arguments['<world>'] and not arguments['--all']:
        selected_worlds = [arguments['<world>']]
    if len(selected_worlds) == 0:
        print("No world selected and none found in the config file. Exiting.")
        exit(1)

    verbose = False
    if arguments['--verbose']:
        verbose = True

    simulate = False
    if arguments['--simulate']:
        print("Simulating backuproll: No real action will be performed")
        simulate = True
        verbose = True

    pid_filename = CONFIG['pidfile']
    if os.path.isfile(pid_filename):
        with open(pid_filename, 'r') as pidfile:
            pid = int(pidfile.read())
        try:
            os.kill(pid, 0)
            print("Another backuproll process is still running. Terminating.", file=sys.stderr)
            exit(1)
        except ProcessLookupError:
            pass
    mypid = os.getpid()
    with open(pid_filename, "w+") as pidfile:
        pidfile.write(str(mypid))
    do_backuproll(selected_worlds, backupcommand, simulate=simulate, verbose=verbose)
    os.remove(pid_filename)
