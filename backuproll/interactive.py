import sys

import curses
from curses import panel

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
