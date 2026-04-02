import time

class MES_controller:
    def run_basic_cycle(self, task_code):
        self.set_app_run(False)
        self.set_release(False)

        print("Waiting for awaitApp...")
        while not self.get_await_app():
            time.sleep(0.2)

        self.set_task_code(task_code)
        self.set_app_run(True)

        print("Waiting for appDone...")
        while not self.get_app_done():
            time.sleep(0.2)

        self.set_app_run(False)
        self.set_release(True)