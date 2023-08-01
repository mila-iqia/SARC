import multiprocessing
import os
import subprocess
import time


def popen_reader(state, function, args, shell=False):
    """Execute a command with the given formatter."""
    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        shell=shell,
    ) as process:
        try:
            while process.poll() is None:
                line = process.stdout.readline()

                if len(line) > 0:
                    function(line, state)

            return process.poll()
        except KeyboardInterrupt:
            print("Stopping due to user interrupt")
            process.kill()

        return -1


def is_ready(line, state):
    state["ready"] = int("mongo_launch" in line)
    state["last_line"] = line.strip()
    state["error"] += int("MongoNetworkError" in line)


class MongoInstance:
    def __init__(
        self,
        path,
        port=8124,
        address="localhost",
        db="sarc",
        admin="admin",
        admin_pass="admin_pass",
        write_name="write_name",
        write_pass="write_pass",
        user_pass="user_pass",
        user_name="user_name",
    ) -> None:
        self.env = {
            "MONGO_PORT": f"{port}",
            "MONGO_ADDRESS": address,
            "MONGO_PATH": path,
            "MONGO_DB": db,
            "MONGO_ADMIN": admin,
            "MONGO_PASS": admin_pass,
            "WRITEUSER_NAME": write_name,
            "WRITEUSER_PWD": write_pass,
            "READUSER_NAME": user_name,
            "READUSER_PWD": user_pass,
            "LAUNCH_MONGO": "1"
        }
        self.path = path
        self.manager = multiprocessing.Manager()
        self.state = self.manager.dict()
        self.state["error"] = 0
        self.proc = None
        self.script = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "..", "scripts", "launch_mongod.sh"
            )
        )

    def shutdown(self):
        subprocess.call(["mongod", "--dbpath", self.path, "--shutdown"])

    def __enter__(self):
        self.shutdown()

        for k, v in self.env.items():
            os.environ[k] = str(v)

        try:
            self.proc = multiprocessing.Process(
                target=popen_reader, args=(self.state, is_ready, ["bash", self.script])
            )
            self.proc.start()
            while self.proc.is_alive() and self.state.get("ready", 0) != 1:
                time.sleep(0.01)

                if line := self.state.get("last_line"):
                    print(line)
                    self.state["last_line"] = None

        except:
            self.shutdown()
            raise

        print("ready")
        return self

    def __exit__(self, *args):
        self.shutdown()
        self.proc.terminate()

        while self.proc.is_alive():
            time.sleep(0.01)


if __name__ == "__main__":
    with MongoInstance("/tmp/path2") as proc:
        pass
