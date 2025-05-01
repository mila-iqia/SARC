import multiprocessing
import os
import subprocess
import sys
import time


def popen_reader(state, function, args, env, shell=False):
    """Execute a command with the given formatter."""
    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, **env},
        shell=shell,
    ) as process:
        try:
            while process.poll() is None:
                line = process.stdout.readline()

                if len(line) > 0:
                    function(line.strip(), state)

            return sys.exit(process.poll())
        except KeyboardInterrupt:
            print("Stopping due to user interrupt")
            process.kill()

        return sys.exit(1)


def is_ready(line, state):
    # Warning: The script contains `set -v` which outputs the commands as they
    # are executed (e.g. `function mongo_launch` or `mongo_launch &`). A more
    # robust solution should probably be looked for rather than lokking into the
    # stdout for the executed command
    state["ready"] = int(state.get("ready", 0) or "mongo_launch" == line)
    state["last_line"] = line
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
        user_name="user_name",
        user_pass="user_pass",
        sarc_config=None,
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
            "LAUNCH_MONGO": "1",
            "SARC_CONFIG": sarc_config or os.getenv("SARC_CONFIG", ""),
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
        subprocess.call(
            ["bash", "-c", f". {self.script} && mongo_stop"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, **self.env, **{"LAUNCH_MONGO": "0"}},
        )

    def __enter__(self):
        self.shutdown()

        try:
            self.proc = multiprocessing.Process(
                target=popen_reader,
                args=(self.state, is_ready, ["bash", self.script], self.env),
            )
            self.proc.start()
            while self.proc.is_alive() and self.state.get("ready", 0) != 1:
                time.sleep(0.01)

                if line := self.state.get("last_line"):
                    # There is no real garanty that we will get last_line here,
                    # it could already have been overriden by the process's call
                    # to is_ready
                    print(line)
                    self.state["last_line"] = None

            if self.proc.exitcode:
                raise multiprocessing.ProcessError(
                    f"Failed to start mongodb with env {self.env}"
                )

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
