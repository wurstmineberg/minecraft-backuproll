#!/usr/bin/env python3

"""Backup roll script for one or more Minecraft servers

Usage:
  backuproll [options] cron [<world>]
  backuproll [options] backup [<world>]
  backuproll [options] rotate [<world>]
  backuproll [options] cleanup [<world>]
  backuproll [options] restore-interactive
  backuproll [options] restore <world> <timespec>...
  backuproll -h | --help
  backuproll --version

Options:
  -h, --help         Print this message and exit.
  --all              Apply the action to all configured worlds. This is the default.
  --cleanup          cron: Clean up the backup directory before operation
  --config=<config>  Path to the config file [default: /opt/wurstmineberg/config/backuproll2.json].
  --no-backup        cron: Do everything but don't run the backup command
  --no-rotation      cron: Don't rotate the backup directory
  --simulate         Don't do any destructive operation, implies --verbose
  --verbose          Print things.
  --version          Print version info and exit.
"""

import docopt
import timespec

import backuproll.core

def main():
    arguments = docopt.docopt(__doc__, version='Minecraft backup roll ' + backuproll.__version__)

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

    do_backup = False
    do_cleanup = False
    do_rotation = False
    if arguments['cron']:
        do_backup = True
        do_rotation = True
    elif arguments['backup']:
        do_backup = True
    elif arguments['rotate']:
        do_rotation = True
    elif arguments['cleanup']:
        do_cleanup = True
    elif arguments['restore']:
        pass
        #raise NotImplementedError('restore not implemented') #TODO
    elif arguments['restore-interactive']:
        pass
    else:
        raise NotImplementedError('Subcommand not implemented')

    if arguments['--cleanup']:
        do_cleanup = True
    if arguments['--no-backup']:
        do_backup = False
    if arguments['--no-rotation']:
        do_rotation = False

    minecraft_backup_roll = backuproll.core.MinecraftBackupRoll(
        use_pid_file=True,
        selected_worlds=selected_worlds,
        simulate=simulate,
        verbose=verbose)

    if arguments['restore']:
        world = minecraft_backup_roll.get_world(arguments['<world>'])
        all_backups = minecraft_backup_roll.get_all_backups(world)
        timestamp = timespec.parse(arguments['<timespec>'], candidates=all_backups.keys(), reverse=True)
        minecraft_backup_roll.do_restore(backup=all_backups[timestamp])
    elif arguments['restore-interactive']:
        minecraft_backup_roll.interactive_restore()
    else:
        minecraft_backup_roll.do_activity(do_cleanup=do_cleanup, do_rotation=do_rotation, do_backup=do_backup)


if __name__ == "__main__":
    main()
