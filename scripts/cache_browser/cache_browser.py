#!/usr/bin/env python3
"""Text-based UI to browse a SARC cache directory."""

import argparse
import curses
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _safe_filename(key: str) -> str:
    """Return a filesystem-safe version of a cache key."""
    return key.replace(":", "_").replace("/", "_").replace("\\", "_")


# ---------------------------------------------------------------------------
# Tree node
# ---------------------------------------------------------------------------


@dataclass
class Node:
    label: str
    path: Path
    depth: int
    is_entry: bool = False  # a ZIP cache file
    is_key: bool = False  # a key inside a ZIP entry
    key_name: Optional[str] = None
    key_size: int = 0  # uncompressed size in bytes
    parent: Optional["Node"] = None
    children: list = field(default_factory=list)
    expanded: bool = False
    loaded: bool = False
    child_count: Optional[int] = None  # pre-loaded key count for entries

    @property
    def is_dir(self) -> bool:
        return not self.is_entry and not self.is_key

    def can_expand(self) -> bool:
        return self.is_dir or self.is_entry

    def load_children(self) -> None:
        if self.loaded:
            return
        self.loaded = True

        if self.is_dir:
            try:
                entries = sorted(self.path.iterdir())
            except PermissionError:
                return
            for e in entries:
                if e.name.startswith(".") or e.name.endswith(".current"):
                    continue
                if e.is_dir():
                    self.children.append(
                        Node(label=e.name, path=e, depth=self.depth + 1, parent=self)
                    )
                elif e.is_file():
                    child = Node(
                        label=e.name,
                        path=e,
                        depth=self.depth + 1,
                        is_entry=True,
                        parent=self,
                    )
                    try:
                        with zipfile.ZipFile(e, "r") as zf:
                            child.child_count = len(zf.namelist())
                    except Exception:
                        child.child_count = 0
                    self.children.append(child)

        elif self.is_entry:
            try:
                with zipfile.ZipFile(self.path, "r") as zf:
                    for info in zf.infolist():
                        self.children.append(
                            Node(
                                label=info.filename,
                                path=self.path,
                                depth=self.depth + 1,
                                is_key=True,
                                key_name=info.filename,
                                key_size=info.file_size,
                                parent=self,
                            )
                        )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Browser UI
# ---------------------------------------------------------------------------

_HEADER = " SARC Cache Browser   ↑↓/PgUp/PgDn:move   ←→/Enter:expand   e:extract   q:quit "

# Color pair indices
_CLR_DIR = 1
_CLR_ENTRY = 2
_CLR_KEY = 3
_CLR_SEL = 4


class CacheBrowser:
    def __init__(self, stdscr: "curses.window", cache_dir: Path) -> None:
        self.stdscr = stdscr
        self.root = Node(label=str(cache_dir), path=cache_dir, depth=0)
        self.root.expanded = True
        self.root.load_children()
        self.visible: list[Node] = []
        self.cursor = 0
        self.scroll = 0
        self._rebuild()

    # ------------------------------------------------------------------
    # Tree maintenance
    # ------------------------------------------------------------------

    def _rebuild(self) -> None:
        self.visible = []
        self._collect(self.root)

    def _collect(self, node: Node) -> None:
        self.visible.append(node)
        if node.expanded:
            for child in node.children:
                self._collect(child)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        curses.curs_set(0)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(_CLR_DIR, curses.COLOR_CYAN, -1)
        curses.init_pair(_CLR_ENTRY, curses.COLOR_YELLOW, -1)
        curses.init_pair(_CLR_KEY, curses.COLOR_GREEN, -1)
        curses.init_pair(_CLR_SEL, curses.COLOR_WHITE, curses.COLOR_BLUE)

        while True:
            self._draw()
            key = self.stdscr.getch()

            if key in (ord("q"), ord("Q")):
                break
            elif key == curses.KEY_UP:
                if self.cursor > 0:
                    self.cursor -= 1
            elif key == curses.KEY_DOWN:
                if self.cursor < len(self.visible) - 1:
                    self.cursor += 1
            elif key == curses.KEY_NPAGE:
                h, _ = self.stdscr.getmaxyx()
                self.cursor = min(self.cursor + h - 2, len(self.visible) - 1)
            elif key == curses.KEY_PPAGE:
                h, _ = self.stdscr.getmaxyx()
                self.cursor = max(self.cursor - (h - 2), 0)
            elif key in (curses.KEY_RIGHT, ord("\n"), 10, 13):
                self._toggle_expand()
            elif key == curses.KEY_LEFT:
                self._collapse()
            elif key in (ord("e"), ord("E")):
                self._extract()

            self._fix_scroll()

    def _fix_scroll(self) -> None:
        h, _ = self.stdscr.getmaxyx()
        area = h - 2
        if self.cursor < self.scroll:
            self.scroll = self.cursor
        elif self.cursor >= self.scroll + area:
            self.scroll = self.cursor - area + 1

    # ------------------------------------------------------------------
    # Navigation actions
    # ------------------------------------------------------------------

    def _toggle_expand(self) -> None:
        node = self.visible[self.cursor]
        if not node.can_expand():
            return
        if not node.expanded:
            node.load_children()
            node.expanded = True
        else:
            node.expanded = False
        self._rebuild()

    def _collapse(self) -> None:
        node = self.visible[self.cursor]
        if node.expanded:
            node.expanded = False
            self._rebuild()
        elif node.parent is not None:
            node.parent.expanded = False
            self._rebuild()
            for i, n in enumerate(self.visible):
                if n is node.parent:
                    self.cursor = i
                    break
            self._fix_scroll()

    # ------------------------------------------------------------------
    # Drawing
    # ------------------------------------------------------------------

    def _draw(self) -> None:
        self.stdscr.erase()
        h, w = self.stdscr.getmaxyx()

        self._draw_rev(0, _HEADER, w)

        area = h - 2
        for row, idx in enumerate(
            range(self.scroll, min(self.scroll + area, len(self.visible)))
        ):
            node = self.visible[idx]
            line = self._fmt_node(node, w)
            attr = self._node_attr(node, selected=(idx == self.cursor))
            try:
                self.stdscr.addstr(row + 1, 0, line, attr)
            except curses.error:
                pass

        status = self._fmt_status(w)
        self._draw_rev(h - 1, status, w)

        self.stdscr.refresh()

    def _fmt_node(self, node: Node, w: int) -> str:
        indent = "  " * node.depth
        if node.is_dir or node.is_entry:
            toggle = "[-] " if node.expanded else "[+] "
        else:
            toggle = "    "

        label = node.label
        if node.is_entry and node.child_count is not None and not node.expanded:
            n = node.child_count
            label += f"  ({n} key{'s' if n != 1 else ''})"
        elif node.is_key:
            label += f"  [{_fmt_size(node.key_size)}]"

        return (indent + toggle + label)[: w - 1].ljust(w - 1)

    def _node_attr(self, node: Node, selected: bool) -> int:
        if selected:
            return curses.color_pair(_CLR_SEL) | curses.A_BOLD
        if node.is_key:
            return curses.color_pair(_CLR_KEY)
        if node.is_entry:
            return curses.color_pair(_CLR_ENTRY)
        return curses.color_pair(_CLR_DIR)

    def _fmt_status(self, w: int) -> str:
        if not self.visible:
            return ""
        node = self.visible[self.cursor]
        pos = f"{self.cursor + 1}/{len(self.visible)}"
        if node.is_key:
            hint = f"  {node.key_name}  [{_fmt_size(node.key_size)}]  |  e: extract"
        elif node.is_entry:
            hint = f"  {node.label}  |  Enter: expand"
        else:
            hint = f"  {node.label}"
        return f" {pos}{hint}"

    def _draw_rev(self, y: int, text: str, w: int) -> None:
        try:
            self.stdscr.attron(curses.A_REVERSE)
            self.stdscr.addstr(y, 0, text[: w - 1].ljust(w - 1))
            self.stdscr.attroff(curses.A_REVERSE)
        except curses.error:
            pass

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _extract(self) -> None:
        node = self.visible[self.cursor]
        if not node.is_key:
            self._message(" Select a key (green) to extract.  Press any key.")
            return

        out_dir = self._prompt(" Output directory: ", str(Path.home()))
        if out_dir is None:
            return

        out_path = Path(out_dir).expanduser()
        if not out_path.is_dir():
            self._message(f" Directory not found: {out_dir}  (press any key)")
            return

        out_file = out_path / _safe_filename(node.key_name)
        try:
            with zipfile.ZipFile(node.path, "r") as zf:
                data = zf.read(node.key_name)
            out_file.write_bytes(data)
            self._message(
                f" Extracted {_fmt_size(len(data))} → {out_file}  (press any key)"
            )
        except Exception as exc:
            self._message(f" Error: {exc}  (press any key)")

    # ------------------------------------------------------------------
    # Interactive input helpers
    # ------------------------------------------------------------------

    def _prompt(self, prompt: str, default: str = "") -> Optional[str]:
        """Show an editable input line; return text on Enter, None on Escape."""
        h, w = self.stdscr.getmaxyx()
        buf = list(default)
        pos = len(buf)
        curses.curs_set(1)
        try:
            while True:
                text = (prompt + "".join(buf))[: w - 1].ljust(w - 1)
                try:
                    self.stdscr.attron(curses.A_REVERSE)
                    self.stdscr.addstr(h - 1, 0, text)
                    self.stdscr.attroff(curses.A_REVERSE)
                    self.stdscr.move(h - 1, min(len(prompt) + pos, w - 2))
                except curses.error:
                    pass
                self.stdscr.refresh()

                key = self.stdscr.getch()
                if key in (10, 13, curses.KEY_ENTER):
                    return "".join(buf)
                elif key == 27:
                    return None
                elif key in (curses.KEY_BACKSPACE, 127, 8):
                    if pos > 0:
                        buf.pop(pos - 1)
                        pos -= 1
                elif key == curses.KEY_DC and pos < len(buf):
                    buf.pop(pos)
                elif key == curses.KEY_LEFT and pos > 0:
                    pos -= 1
                elif key == curses.KEY_RIGHT and pos < len(buf):
                    pos += 1
                elif key == curses.KEY_HOME:
                    pos = 0
                elif key == curses.KEY_END:
                    pos = len(buf)
                elif 32 <= key < 128:
                    buf.insert(pos, chr(key))
                    pos += 1
        finally:
            curses.curs_set(0)

    def _message(self, text: str) -> None:
        """Show a message in the status bar and wait for a keypress."""
        h, w = self.stdscr.getmaxyx()
        self._draw_rev(h - 1, text, w)
        self.stdscr.refresh()
        self.stdscr.getch()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Browse a SARC cache directory.")
    parser.add_argument(
        "cache_dir",
        nargs="?",
        help="Path to cache directory (default: read from SARC config)",
    )
    args = parser.parse_args()

    if args.cache_dir:
        cache_dir = Path(args.cache_dir)
    else:
        try:
            from sarc.config import config  # type: ignore

            cfg = config()
            if cfg.cache is None:
                parser.error("SARC config has no cache path configured.")
            cache_dir = Path(cfg.cache)
        except ImportError:
            parser.error("sarc package not found; pass cache_dir as argument.")
        except Exception as exc:
            parser.error(f"Could not load SARC config: {exc}")

    if not cache_dir.exists() or not cache_dir.is_dir():
        parser.error(f"Cache directory not found: {cache_dir}")

    curses.wrapper(lambda stdscr: CacheBrowser(stdscr, cache_dir).run())


if __name__ == "__main__":
    main()
