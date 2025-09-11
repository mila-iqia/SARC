from fabric import Connection


def run_command(
    connection: Connection, command: str, retries: int
) -> tuple[str | None, list[Exception]]:
    errors: list[Exception] = []

    for _ in range(retries):
        try:
            result = connection.run(command, hide=True)
            return result.stdout, errors

        # pylint: disable=broad-exception-caught
        except Exception as err:
            errors.append(err)

    return None, errors
