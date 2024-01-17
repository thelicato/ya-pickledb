import threading


class DB_Thread(threading.Thread):
    def run(self):
        self.exception = None

        try:
            self.ret_val = self._target(*self._args)
        except BaseException as e:
            self.exception = e

    def join(self):
        super(DB_Thread, self).join()

        if self.exception:
            raise self.exception
        return self.ret_val
