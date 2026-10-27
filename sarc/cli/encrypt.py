import getpass
import json
import logging
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from cryptography.fernet import InvalidToken
from ovld import Medley, ovld, recurse
from ovld.dependent import Regexp
from serieux import IncludeFile, Patch, Patcher, serieux
from serieux.ctx import locate
from serieux.features.encrypt import EncryptionKey, Secret, crypt_prefix
from serieux.features.prompt import Promptable
from serieux.formats import FileSource
from serieux.priority import HI9
from simple_parsing import subparsers

logger = logging.getLogger(__name__)


@dataclass
class File:
    path: Path

    def execute(self) -> int:
        ek = EncryptionKey(os.environ.get("SERIEUX_PASSWORD", None))
        content = self.path.read_text()
        if content.startswith(crypt_prefix):
            logger.error("File is already encrypted (or looks like it is)")
            return -1
        self.path.write_text(ek.encrypt(json.loads(content)))
        return 0


@dataclass
class Append:
    path: Path
    key: str
    value: str

    def execute(self) -> int:
        ek = EncryptionKey(os.environ.get("SERIEUX_PASSWORD", None))
        val = ek.decrypt(self.path.read_text())
        val[self.key] = self.value
        self.path.write_text(ek.encrypt(val))
        return 0


@dataclass
class Recrypt(Medley):
    """Serieux feature that switches the master password on everything it walks through.

    During deserialization, any ``~CRYPT~``-prefixed value is decrypted with the old
    key and an in-place patch is declared, through the given ``Patcher``, to rewrite
    the value in its source file encrypted with the new key. This works both for
    values annotated with ``Secret`` and for plain strings, so that encrypted values
    living in otherwise untyped sections of the config (e.g. the user scrapers) are
    picked up as well.
    """

    old: EncryptionKey
    new: EncryptionKey

    def _declare_recrypt(self, obj: str, ctx: Patcher) -> str:
        plain = self.old.decrypt(obj)
        new_key = self.new

        def crypt():
            return new_key.encrypt(plain)

        ctx.declare_patch(
            Patch(crypt, description="Re-encrypt with the new master password")
        )
        return plain

    @ovld(priority=HI9)  # ty: ignore[missing-argument, unknown-argument]
    def deserialize(
        self,
        t: type[Any @ Secret],  # ty: ignore[invalid-type-form, unsupported-operator]
        obj: Regexp[rf"^{crypt_prefix}.*"],  # ty: ignore[invalid-type-form]
        ctx: Patcher,
    ):
        plain = self._declare_recrypt(obj, ctx)
        return recurse(Secret.strip(t), plain, ctx)

    @ovld(priority=HI9)  # ty: ignore[missing-argument, unknown-argument]
    def deserialize(  # noqa: F811  # ovld overloads the same method
        self,
        t: type[str],
        obj: Regexp[rf"^{crypt_prefix}.*"],  # ty: ignore[invalid-type-form]
        ctx: Patcher,
    ):
        plain = self._declare_recrypt(obj, ctx)
        return recurse(t, plain, ctx)


def ask_new_password() -> str:  # pragma: no cover
    while True:
        first = getpass.getpass("New master password: ")
        second = getpass.getpass("Confirm new master password: ")
        if first == second:
            return first
        logger.error("Passwords do not match, try again")


def recrypt_file(path: Path, old: EncryptionKey, new: EncryptionKey) -> str:
    """Return the content of a whole-file-encrypted file, re-encrypted with `new`."""
    content = path.read_text()
    if not content.startswith(crypt_prefix):
        raise ValueError("File is not encrypted (or does not look like it is)")
    return new.encrypt(old.decrypt(content))


class ConfigPatcher(Patcher):
    """Patcher that keys patches by `(file, offset)` instead of just offset.

    The default keying silently overwrites a patch when two different files hold
    encrypted values at the same character offset, which is common between
    near-identical config files (e.g. per-cluster pkey files).
    """

    # serieux types this as dict[int, ...]; we widen the key, and ty complains
    # about the override either way, hence Any
    patches: Any = field(default_factory=dict)

    def declare_patch(self, patch):
        if not isinstance(patch, Patch):
            patch = Patch(patch, ctx=self)
        elif not patch.ctx:
            patch = replace(patch, ctx=self)
        if (loc := locate(patch.ctx)) is not None:
            self.patches[(str(loc.source), loc.start)] = patch


@dataclass
class Rotate:
    """Change the master password for a config and everything encrypted with it.

    The current password must be in the SERIEUX_PASSWORD environment variable.

    All config roots are processed in a single pass, so files shared between roots
    are re-encrypted exactly once.
    """

    # The main config file(s) to rotate (defaults to the SARC_CONFIG environment
    # variable). Other roots in the config's directory (files with a top-level
    # `sarc:` key) are picked up automatically, unless --single_root is given.
    file: list[str] = field(default_factory=list)
    # Extra files that are encrypted whole (e.g. mapping files read by patches).
    # Files inside the config's `patches` directory are rotated automatically.
    extra: list[Path] = field(default_factory=list)
    # The new master password. Prompted for if not given.
    new_password: str | None = None
    # Only rotate the given file(s), do not look for other roots
    single_root: bool = False
    # Show what would be re-encrypted without writing any file
    dry_run: bool = False

    def _patch_files(self, patches_dir: Path, config_dir: Path) -> list[Path]:
        candidates = (
            [patches_dir]
            if patches_dir.is_absolute()
            else [Path.cwd() / patches_dir, config_dir / patches_dir]
        )
        for directory in candidates:
            if directory.is_dir():
                found = []
                for path in sorted(directory.rglob("*")):
                    try:
                        if path.is_file() and path.read_text().startswith(crypt_prefix):
                            found.append(path)
                    except OSError, UnicodeDecodeError:
                        continue
                return found
        logger.warning(f"Patches directory not found, skipping: {patches_dir}")
        return []

    def _main_roots(self) -> list[tuple[FileSource, bool]] | None:
        """Turn the given file names (or $SARC_CONFIG) into main root sources."""
        given = list(self.file) or (
            [os.environ["SARC_CONFIG"]] if "SARC_CONFIG" in os.environ else []
        )
        if not given:
            logger.error("No config file given and SARC_CONFIG is not set")
            return None
        roots = []
        for name in given:
            src = FileSource.serieux_from_string(name)
            if not str(src.path) or not src.path.exists():
                logger.error(f"Config file not found: {name}")
                return None
            if src.field is None:
                raw = src.load()
                # The wrapper makes the file a root; without it, walk the file as-is
                if isinstance(raw, dict) and "sarc" in raw:
                    src = replace(src, field="sarc")
            roots.append((src, True))
        return roots

    def _find_roots(self, config_dir: Path, known: set) -> list[FileSource]:
        """Find config roots in `config_dir`: yaml files with a top-level `sarc:` key."""
        roots = []
        candidates = sorted(
            {
                path
                for pattern in ("*.yaml", "*.yml")
                for path in config_dir.rglob(pattern)
                if not any(
                    part.startswith(".") for part in path.relative_to(config_dir).parts
                )
            }
        )
        for path in candidates:
            src = FileSource(path, field="sarc")
            if (str(path), "sarc") in known:
                continue
            try:
                raw = src.load()
            except Exception:
                continue
            if isinstance(raw, dict):
                known.add((str(path), "sarc"))
                roots.append(src)
        return roots

    def _prepare_whole_files(
        self,
        loaded_configs: list,
        config_dir: Path,
        old_key: EncryptionKey,
        new_key: EncryptionKey,
    ) -> dict[Path, str] | None:
        """Decrypt-then-reencrypt every whole-file encrypted file (or None on failure)."""
        whole_files = dict.fromkeys(
            list(self.extra)
            + [
                path
                for loaded in loaded_configs
                for path in self._patch_files(loaded.patches, config_dir)
            ]
        )
        prepared: dict[Path, str] = {}
        ok = True
        for path in whole_files:
            try:
                prepared[path] = recrypt_file(path, old_key, new_key)
            except (OSError, ValueError, InvalidToken) as err:
                logger.error(f"Could not re-encrypt {path}: {err}")
                ok = False
        if not ok:
            logger.error("Nothing was changed")
        return prepared if ok else None

    def _walk_roots(
        self,
        roots: list[tuple[FileSource, bool]],
        old_key: EncryptionKey,
        new_key: EncryptionKey,
        patcher: "ConfigPatcher",
    ) -> list | None:
        """Deserialize every root with the old password, collecting a patch for each
        encrypted value (plus each root's client section). Returns the loaded configs,
        or None if a main root could not be loaded (a missing shared file between two
        runs of this loop is impossible, since decryption all happens before any
        file is written, so shared files are read in their pre-rotation state)."""
        from sarc.config import Config

        srx = serieux + IncludeFile() + Recrypt(old_key, new_key)  # ty: ignore[no-matching-overload]
        ctx = Promptable() + patcher  # ty: ignore[no-matching-overload]
        loaded_configs = []
        for src, main in roots:
            try:
                loaded = srx.deserialize(Config, src, ctx)
                if src.field == "sarc":
                    from sarc.rest.client import SarcClient

                    client_src = replace(src, field="sarc.client")
                    try:
                        has_client = isinstance(client_src.load(), dict)
                    except KeyError:
                        has_client = False
                    if has_client:
                        srx.deserialize(SarcClient, client_src, ctx)
            except InvalidToken:
                if main:
                    logger.error(
                        "Could not decrypt the config: wrong current password?"
                    )
                    return None
                logger.warning(f"Could not decrypt root {src.path}, skipping it")
                continue
            except Exception as err:
                if main:
                    logger.error(f"Could not load the config {src.path}: {err}")
                    return None
                logger.warning(f"Could not load root {src.path}, skipping it: {err}")
                continue
            loaded_configs.append(loaded)
        return loaded_configs

    def _keys(self) -> tuple[EncryptionKey, EncryptionKey] | None:
        """The (old, new) encryption keys, or None if they cannot be determined."""
        old_password = os.environ.get("SERIEUX_PASSWORD", None)
        if old_password is None:
            logger.error("SERIEUX_PASSWORD must be set to the current master password")
            return None
        new_password = (
            self.new_password if self.new_password is not None else ask_new_password()
        )
        return EncryptionKey(old_password), EncryptionKey(new_password)

    def _write(
        self,
        patcher: "ConfigPatcher",
        prepared: dict[Path, str],
        roots: list[tuple[FileSource, bool]],
        old_key: EncryptionKey,
        new_key: EncryptionKey,
    ) -> int:
        """Report the plan, then apply it (unless this is a dry run)."""
        # From here on, all decryption succeeded, so the writes cannot fail
        # halfway due to a wrong password.
        sources = {src.path for src, _ in roots}
        for patch in patcher.patches.values():
            assert isinstance(patch, Patch)
            trail = ".".join(str(x) for x in patch.ctx.trail)
            if loc := locate(patch.ctx):
                sources.add(loc.source)
            logger.info(f"[{trail}] {patch.description}")
        for path in prepared:
            logger.info(f"Re-encrypting whole file {path}")
        if self.dry_run:
            logger.info("Dry run: no file was written")
            return 0

        patcher.apply_patches()
        for path, content in prepared.items():
            path.write_text(content)

        leftovers = self._check_leftovers(old_key, new_key, sources, prepared)
        if leftovers:
            logger.error(
                f"Rotation incomplete: {leftovers} value(s) are still encrypted "
                "with the old password"
            )
            return -1
        logger.info(
            "Master password rotated. Do not forget to update SERIEUX_PASSWORD "
            "wherever it is stored."
        )
        return 0

    def execute(self) -> int:
        keys = self._keys()
        if keys is None:
            return -1
        old_key, new_key = keys

        main_roots = self._main_roots()
        if main_roots is None:
            return -1
        roots: list[tuple[FileSource, bool]] = list(main_roots)
        known = {(str(src.path), src.field) for src, _ in roots}
        config_dir = roots[0][0].path.parent
        if not self.single_root:
            for src in self._find_roots(config_dir, known):
                roots.append((src, False))
                logger.info(f"Found additional config root: {src.path}")

        # One shared Patcher for every root: patches are keyed by source location
        # and applied once at the end, so files shared between roots are patched once
        patcher = ConfigPatcher()  # ty: ignore[no-matching-overload]
        loaded_configs = self._walk_roots(roots, old_key, new_key, patcher)
        if loaded_configs is None:
            return -1

        prepared = self._prepare_whole_files(
            loaded_configs, config_dir, old_key, new_key
        )
        if prepared is None:
            return -1  # the reason was already reported

        return self._write(patcher, prepared, roots, old_key, new_key)

    def _check_leftovers(
        self,
        old_key: EncryptionKey,
        new_key: EncryptionKey,
        sources: set[Path],
        prepared: dict[Path, str],
    ) -> int:
        """Count encrypted values that survived the rotation, e.g. values sitting in
        config keys that are not traversed by the models we know about. Values that
        the old password cannot decrypt either are only warned about (they may be
        commented out or encrypted with another key)."""
        blob = re.compile(rf"{crypt_prefix}[^\s\"',]+")
        leftovers = 0
        for path in sorted(set(prepared) | sources):
            for match in blob.finditer(path.read_text()):
                token = match.group(0)
                try:
                    new_key.decrypt(token)
                    continue
                except InvalidToken:
                    pass
                try:
                    old_key.decrypt(token)
                except InvalidToken:
                    logger.warning(
                        f"{path} holds a value that neither password can decrypt: "
                        f"{token[:32]}... It may be commented out or encrypted with "
                        "another key"
                    )
                else:
                    leftovers += 1
                    logger.error(
                        f"{path} still holds a value encrypted with the old "
                        f"password: {token[:32]}... (it may sit in an untraversed "
                        "part of the config)"
                    )
        return leftovers


@dataclass
class Encrypt:
    command: File | Append | Rotate = subparsers(
        {"file": File, "append": Append, "rotate": Rotate}
    )

    def execute(self) -> int:
        return self.command.execute()
