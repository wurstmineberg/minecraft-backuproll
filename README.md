**backuproll** is a [cron](https://en.wikipedia.org/wiki/Cron) script to create and manage backups for Minecraft servers.

This is version 2.0.0 of backuproll ([semver](http://semver.org/)). The versioned API includes all documented functions of the `MinecraftBackupRoll` class, as well as the configuration file.

# Configuration

The configuration file is a JSON file with an object at its root. The object contains the following pairs, all optional:

* `"backupfolder"`: The directory in which backups will be saved. Defaults to `"/mnt/backup/world"`.
* `"dateformat"`: The datetime format string used to encode/decode dates and times in backup file/directory names, as a [Python 3.4 `strftime`/`strptime` format string](https://docs.python.org/3.4/library/datetime.html#strftime-and-strptime-behavior). Defaults to `"%Y-%m-%dT%H:%M:%S"`.
* `"fail_backup_command"`: A shell command that will be called after a world backup fails. The placeholder `{world}` will be replaced with the world name. Defaults to `"/opt/wurstmineberg/bin/minecraft saves on {world}"`.
* `"pidfile"`: A path where a lock file will be created to make sure multiple instances of backuproll don't run at the same time. Defaults to `"/var/local/wurstmineberg/backuproll.pid"`.
* `"post_backup_command"`: A shell command that will be called after a world is backed up successfully. The placeholder `{world}` will be replaced with the world name. Defaults to `"/opt/wurstmineberg/bin/minecraft saves on {world}"`.
* `"post_restore_command"`: A shell command that will be called after a world backup is restored successfully. The placeholder `{world}` will be replaced with the world name. Defaults to `"/opt/wurstmineberg/bin/minecraft start {world}"`.
* `"pre_backup_command"`: A shell command that will be called before a world is backed up. The placeholder `{world}` will be replaced with the world name. Defaults to `"/opt/wurstmineberg/bin/minecraft saves off {world}"`.
* `"pre_restore_command"`: A shell command that will be called before a world backup is resroted. The placeholder `{world}` will be replaced with the world name. Defaults to `"/opt/wurstmineberg/bin/minecraft saves off {world} && /opt/wurstmineberg/bin/minecraft stop {world}"`.
* `"worldfolder"`: The directory in which the live worlds are located. Should be the same as the `.paths.worlds` config entry in [systemd-minecraft](https://github.com/wurstmineberg/systemd-minecraft). Defaults to `"/opt/wurstmineberg/world"`.
* `"worlds"`: An object mapping world names to objects with the following required keys. Operations will be performed on all listed worlds by default (i.e. if no `<world>` argument is passed on the command line or `MinecraftBackupRoll` is constructed with the default `selected_worlds=None`), and only worlds listed here will be rotated by the `do_rotation` activity (since the `"keep"` information is required for rotation).
    * `"keep"`: An object with the following pairs, all required:
        * `"recent"`: The number of “recent” backups (backups made by the `do_backup` activity and not yet promoted by the `do_rotation` activity) to keep. For the oldest backups exceeding this limit, `do_rotation` will promote one backup per day to “daily” and delete the rest.
        * `"daily"`: The number of “daily” backups to keep. For the oldest backups exceeding this limit, `do_rotation` will promote one backup per week to “weekly” and delete the rest.
        * `"weekly"`: The number of “weekly” backups to keep. For the oldest backups exceeding this limit, `do_rotation` will promote one to “monthly” and delete the rest.
        * `"monthly"`: The number of “monthly” backups to keep. Old backups exceeding this limit will be deleted.
