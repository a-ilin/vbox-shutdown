import gc
import logging
import os
import subprocess
import time
import sys
import threading
import win32con
import win32gui_struct

try:
    import winxpgui as win32gui
except ImportError:
    import win32gui

from ctypes import windll
from ctypes.wintypes import BOOL, DWORD, HWND, LPCWSTR
_kernel32 = windll.kernel32
_user32 = windll.user32

SetProcessShutdownParameters = _kernel32.SetProcessShutdownParameters
SetProcessShutdownParameters.restype = BOOL
SetProcessShutdownParameters.argtypes = [DWORD, DWORD]

ShutdownBlockReasonCreate = _user32.ShutdownBlockReasonCreate
ShutdownBlockReasonCreate.restype = BOOL
ShutdownBlockReasonCreate.argtypes = [HWND, LPCWSTR]

ShutdownBlockReasonDestroy = _user32.ShutdownBlockReasonDestroy
ShutdownBlockReasonDestroy.restype = BOOL
ShutdownBlockReasonDestroy.argtypes = [HWND]


class Enum(object):
    def __init__(self, labels):
        self.values = {k: v for v, k in labels.items()}
        self.labels = labels

    def __getattr__(self, name):
        return self.labels[name]

    def __getitem__(self, value):
        return self.values[value]

    def __str__(self):
        return "Enum(labels: %r)" % self.labels
    __repr__ = __str__


class VBCController(object):
    def __init__(self):
        self.ctx = None

        self._enabled = False
        self._acquire_cnt = 0
        self._wq = threading.Condition(threading.Lock())
        self._ev_result = threading.Event()
        self._ev_started = threading.Event()
        self._stop_timer = None
        self._thread = None

        # marshalling
        self._call_method = None
        self._call_args = None
        self._call_result = None

    def __enter__(self):
        self._timer_stop()
        self._acquire_cnt += 1
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._acquire_cnt -= 1
        if self._acquire_cnt == 0:
            self._timer_start()

    def _worker(self):
        self._ev_started.set()
        self.ctx = VBContext()

        with self._wq:
            while self._enabled:
                if self._call_method is not None:
                    try:
                        self._call_result = self._call_method(self.ctx, *self._call_args)
                    except Exception as exc:
                        logging.error("Call to VB context failed: %s", exc)
                    self._call_method = None
                    self._call_args = None
                    self._ev_result.set()
                self._wq.wait()

            self._call_method = None
            self._call_args = None
            self._call_result = None

        # release COM objects
        self.ctx.deinit()
        del self.ctx
        gc.collect()

    def _start(self):
        if not self._enabled:
            self._enabled = True
            self._thread = threading.Thread(target=self._worker)
            self._thread.start()
            self._ev_started.wait()

    def stop(self):
        with self._wq:
            self._enabled = False
            self._wq.notify()
        if self._thread:
            self._thread.join()
        self._ev_started.clear()

    def _timer_start(self):
        def on_timer():
            self._timer_stop()
            self.stop()

        if self._acquire_cnt > 0:
            # do not start timer while on hold
            return
        self._timer_stop()
        self._stop_timer = threading.Timer(15.0, on_timer)
        self._stop_timer.start()

    def _timer_stop(self):
        if self._stop_timer and self._stop_timer.is_alive():
            self._stop_timer.cancel()
            self._stop_timer = None

    def _call_async(self, context_method, *args):
        if not (self._thread and self._thread.is_alive()):
            logging.error("VBController thread is not running!")
            return None
        with self._wq:
            self._call_method = context_method
            self._call_args = args
            self._ev_result.clear()
            self._wq.notify()

    def call(self, context_method, *args):
        self._timer_stop()
        self._start()
        self._call_async(context_method, *args)
        self._ev_result.wait()
        self._timer_start()
        return self._call_result


class VBContext(object):
    # Use machine surrogate to avoid calling COM-methods out of VBCController thread
    class Machine(object):
        def __init__(self, index, vb_machine):
            self.index = index
            self.name = vb_machine.name
            self.state = vb_machine.state

    def __init__(self):
        self._recursion_guard = RecursionGuard()

        try:
            import vboxapi
            self.VBM = vboxapi.VirtualBoxManager()
            self._constants = self.VBM.constants
            self.VB = self.VBM.getVirtualBox()
            self.MachineState = Enum(self._constants.all_values('MachineState'))
            self.AutostopType = Enum(self._constants.all_values('AutostopType'))
            self.LockType = Enum(self._constants.all_values('LockType'))
            self.OffMachineStates = (
                self.MachineState.PoweredOff,
                self.MachineState.Saved,
                self.MachineState.Teleported,
                self.MachineState.Aborted,
            )
            logging.debug("VBContext initialized")
        except ImportError:
            vboxapi = None

            class VB:
                class Machine:
                    name = 'Missing `vboxapi` python package !!'
                    state = 0

                machines = Machine,
            self.MachineState = {0: '?'}

    def deinit(self):
        del self._constants
        del self.VB
        gc.collect()
        self.VBM.deinit()
        del self.VBM
        logging.debug("VBContext released")

    def _machines(self):
        return self.VBM.getArray(self.VB, 'machines')

    def machines(self):
        res = list()
        i = 0
        for m in self._machines():
            try:
                i += 1
                res.append(VBContext.Machine(i-1, m))
            except Exception as exc:
                # this could happen in case of machine data is not available (f.e. located on external drive)
                # logging.error('Error accessing machine with index: %d' % i)
                pass
        return res

    def machines_running(self):
        return list(filter(lambda m: m.state not in self.OffMachineStates, self.machines()))

    def shutdown_machine(self, machine_surrogate):
        with self._recursion_guard:
            try:
                machine = self._machines()[machine_surrogate.index]
                logging.info("Attempting ACPI shutdown for `%s`.", machine.name)
                with VBSession(self, machine, self.LockType.Shared) as (sess, machine):
                    try:
                        for i in range(20):
                            logging.info("Machine `%s` is in state: %s", machine.name, self.MachineState[machine.state])
                            if machine.state == self.MachineState.Paused:
                                sess.console.resume()
                            elif machine.state in self.OffMachineStates:
                                logging.info("Machine `%s` is in state: %s. Were done !", machine.name,
                                             self.MachineState[machine.state])
                                return True
                            else:
                                sess.console.PowerButton()
                            time.sleep(1)
                    except Exception as exc:
                        logging.error("Failed to ACPI shutdown `%s`: %s", machine.name, exc)
            except Exception as exc:
                logging.error("Failed to access machine member: %s", exc)

    def save_machine(self, machine_surrogate):
        with self._recursion_guard:
            try:
                machine = self._machines()[machine_surrogate.index]
                logging.info("Checking machine `%s` ... ", machine.name)
                if machine.state not in self.OffMachineStates:
                    logging.info("Saving machine `%s` ... ", machine.name)
                    with VBSession(self, machine, self.LockType.Shared) as (_, machine):
                        try:
                            machine.saveState()

                            for i in range(20):
                                logging.info("Machine `%s` is in state: %s", machine.name, self.MachineState[machine.state])
                                if machine.state in self.OffMachineStates:
                                    return True
                                time.sleep(1)
                            else:
                                logging.error("Failed to save `%s`: Took too much time to save !", machine.name)
                        except Exception as exc:
                            logging.error("Failed to save `%s`: %s", machine.name, exc)
            except Exception as exc:
                logging.error("Failed to access machine member: %s", exc)


class VBSession(object):
    def __init__(self, vbc, machine, locktype):
        self.vbc = vbc
        self.machine = machine
        self.locktype = locktype

        try:
            self.sess = vbc.VBM.getSessionObject(vbc.VB)
        except Exception as exc:
            logging.debug("Failed to get session object: %s", exc)

    def __enter__(self):
        try:
            if self.sess:
                self.machine.lockMachine(self.sess, self.locktype)
                return self.sess, self.sess.machine
        except Exception as exc:
            logging.debug("Failed to lock machine: %s", exc)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.sess:
                self.sess.unlockMachine()
        except Exception as exc:
            logging.debug("Failed to unlock session: %s", exc)


class RecursionGuard:
    def __init__(self):
        self._value = 0

    def __enter__(self):
        self._value += 1
        if self._value > 1:
            raise RuntimeError('Recursion detected!')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._value -= 1


class ShutdownBlocker(object):
    def __init__(self, hWnd, sReason):
        self.hWnd = hWnd
        self.sReason = sReason
        self.counter = 0

    def enable(self):
        if self.counter == 0:
            ShutdownBlockReasonCreate(self.hWnd, self.sReason)
        self.counter += 1

    def disable(self):
        if self.counter == 1:
            ShutdownBlockReasonDestroy(self.hWnd)
        if self.counter > 0:
            self.counter -= 1

    def __enter__(self):
        self.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()


class VirtualBoxAutoShutdownTray(object):
    CLASS_NAME = 'VirtualBoxAutoShutdownTray'
    HOVER_TEXT = 'VirtualBox automatic shutdown cleaner'

    def __init__(self, vbcc):
        self.vbcc = vbcc

        self.window_class = win32gui.WNDCLASS()
        self.hinstance = self.window_class.hInstance = win32gui.GetModuleHandle(None)
        self.window_class.lpszClassName = self.CLASS_NAME
        self.window_class.style = win32con.CS_VREDRAW | win32con.CS_HREDRAW
        self.window_class.hCursor = win32gui.LoadCursor(0, win32con.IDC_ARROW)
        self.window_class.hbrBackground = win32con.COLOR_WINDOW
        self.window_class.lpfnWndProc = self.dispatch
        self.class_atom = win32gui.RegisterClass(self.window_class)
        self.hwnd = win32gui.CreateWindow(
            self.class_atom,
            self.CLASS_NAME,
            win32con.WS_OVERLAPPED | win32con.WS_SYSMENU,
            0,
            0,
            win32con.CW_USEDEFAULT,
            win32con.CW_USEDEFAULT,
            0,
            0,
            self.hinstance,
            None
        )
        win32gui.UpdateWindow(self.hwnd)
        self.set_icon()
        self.shutdown_blocker = ShutdownBlocker(self.hwnd, "Shutting down VirtualBox VMs...")

    @property
    def dispatch(self):
        return {
            win32gui.RegisterWindowMessage("TaskbarCreated"): self.set_icon,
            win32con.WM_CLOSE: self.on_close,
            win32con.WM_QUIT: self.on_close,
            win32con.WM_QUERYENDSESSION: self.on_queryendsession,
            win32con.WM_ENDSESSION: self.on_close,
            win32con.WM_COMMAND: self.on_command,
            win32con.WM_USER+20: self.on_notify,
            win32con.WM_USER+30: self.on_async_stop,
        }

    def set_icon(self, *_args):
        win32gui.Shell_NotifyIcon(win32gui.NIM_ADD, (
            self.hwnd,
            0,
            win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP,
            win32con.WM_USER+20,
            win32gui.LoadIcon(self.hinstance, 1),
            self.HOVER_TEXT
        ))

    def on_command(self, hwnd, msg, wparam, lparam):
        print('on_command', hwnd, msg, wparam, lparam)
        command_id = win32gui.LOWORD(wparam)
        if command_id == 0:
            self.quit()
        else:
            raise RuntimeError("Unknown command_id %r" % command_id)

    def on_async_stop(self, *args):
        self.stop_machines()
        self.shutdown_blocker.disable()
        return True

    def on_queryendsession(self, *args):
        if len(self.vbcc.call(VBContext.machines_running)) > 0:
            self.shutdown_blocker.enable()
            win32gui.PostMessage(self.hwnd, win32con.WM_USER+30, 0, 0)
            #return False
        return True

    def on_close(self, *args):
        self.stop_machines()
        self.quit()

    def on_notify(self, hwnd, msg, wparam, lparam):
        # print('on_notify', hwnd, msg, wparam, lparam)
        if lparam == win32con.WM_LBUTTONDBLCLK:
            pass
        elif lparam == win32con.WM_RBUTTONUP:
            self.show_menu()
        elif lparam == win32con.WM_LBUTTONUP:
            pass
        return True

    def quit(self, *args):
        win32gui.DestroyWindow(self.hwnd)
        win32gui.Shell_NotifyIcon(win32gui.NIM_DELETE, (self.hwnd, 0))
        win32gui.PostQuitMessage(0)

    @property
    def menu_entries(self):
        with self.vbcc:
            for i, machine in enumerate(self.vbcc.call(VBContext.machines)):
                try:
                    yield "[%s] %s" % (
                        self.vbcc.ctx.MachineState[machine.state], machine.name
                    ), machine.state in self.vbcc.ctx.OffMachineStates, i
                except Exception:
                    pass

    def stop_machine(self, machine):
        try:
            with self.vbcc:
                logging.info("Checking for machine `%s` (state: %s)", machine.name, self.vbcc.ctx.MachineState[machine.state])
            if not self.vbcc.call(VBContext.shutdown_machine, machine):
                self.vbcc.call(VBContext.save_machine, machine)
        except Exception:
            pass

    def stop_machines(self):
        if len(self.vbcc.call(VBContext.machines_running)) > 0:
            with self.shutdown_blocker:
                for machine in self.vbcc.call(VBContext.machines_running):
                    self.stop_machine(machine)

    def show_menu(self):
        menu = win32gui.CreatePopupMenu()
        count = 0

        for count, (option_text, active, option_id) in enumerate(self.menu_entries):
            win32gui.AppendMenu(
                menu,
                win32con.MF_STRING | (win32con.MF_GRAYED | win32con.MF_DISABLED if active else 0),
                option_id + 1,
                option_text
            )

        win32gui.InsertMenu(
            menu, count+1, win32con.MF_BYPOSITION, win32con.MF_SEPARATOR, None
        )
        exit_id = count + 2
        exit_menu_tuple = win32gui_struct.PackMENUITEMINFO(
                text="Quit",
                wID=exit_id,
                hbmpItem=win32con.HBMMENU_MBAR_CLOSE
            )
        win32gui.InsertMenuItem(
            menu, exit_id, 1, exit_menu_tuple[0]
        )

        pos = win32gui.GetCursorPos()
        # See http://msdn.microsoft.com/library/default.asp?url=/library/en-us/winui/menus_0hdi.asp
        win32gui.SetForegroundWindow(self.hwnd)
        selected_id = win32gui.TrackPopupMenu(
            menu,
            win32con.TPM_LEFTALIGN | win32con.TPM_RETURNCMD,
            pos[0],
            pos[1],
            0,
            self.hwnd,
            None
        )
        # win32gui.PostMessage(self.hwnd, win32con.WM_NULL, 0, 0)
        if selected_id != 0:
            if selected_id == exit_id:
                self.quit()
            else:
                self.stop_machine(self.vbcc.call(VBContext.machines)[selected_id - 1])

    @staticmethod
    def run():
        # so we get shutdown first
        SetProcessShutdownParameters(0x3FF, 0)
        win32gui.PumpMessages()


if __name__ == '__main__':
    this_path = os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(level=logging.INFO,
                        filename=os.path.join(this_path, 'vbox-shutdown.log') if len(sys.argv) < 2 else sys.argv[1],
                        format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")
    if 'pythonw.exe' in sys.executable:
        logging.info("Service started")
        vbcc = VBCController()
        app = VirtualBoxAutoShutdownTray(vbcc)
        app.run()
        vbcc.stop()
        logging.info("Service finished")
    else:
        subprocess.Popen([sys.executable.replace('python.exe', 'pythonw.exe'), __file__] + sys.argv[1:])
