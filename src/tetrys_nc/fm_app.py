"""Two-panel console file manager (MC-like) over tetrys UDP."""

from __future__ import annotations

import argparse
import threading
from dataclasses import dataclass, field
from pathlib import Path

from .block_state import WAN_BLOCK_K, WAN_SYMBOL_SIZE
from .fm_core import (
    LocalEntry,
    copy_local_to_remote,
    copy_remote_to_local,
    list_local,
    list_remote,
    local_rm,
    remote_mkdir,
    remote_rm,
)


@dataclass
class PanelState:
    side: str
    kind: str  # local | remote
    cwd: str = ""
    entries: list = field(default_factory=list)
    cursor: int = 0
    marked: set[str] = field(default_factory=set)
    error: str = ""


class TransferState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = False
        self.line = ""
        self.done = False
        self.rc = 0
        self.error = ""
        self._abort = False
        self._epoch = 0

    def begin(self) -> int:
        with self.lock:
            self._epoch += 1
            epoch = self._epoch
            self.active = True
            self.done = False
            self.rc = 0
            self.error = ""
            self.line = "starting…"
            self._abort = False
            return epoch

    def progress(self, line: str) -> None:
        with self.lock:
            self.line = line

    def should_abort(self, epoch: int | None = None) -> bool:
        with self.lock:
            if epoch is not None and epoch != self._epoch:
                return True
            return self._abort

    def abort_check(self, epoch: int):
        """Callback bound to one worker generation."""

        def _check() -> bool:
            return self.should_abort(epoch)

        return _check

    def request_abort(self) -> None:
        with self.lock:
            self._abort = True

    def abandon(self) -> None:
        """UI cancel: free the FM immediately; invalidate the in-flight worker."""
        with self.lock:
            self._abort = True
            self.active = False
            self.done = False
            self.error = ""
            self.line = "aborted"
            self._epoch += 1

    def finish(self, rc: int, error: str = "", *, epoch: int | None = None) -> None:
        with self.lock:
            if epoch is not None and epoch != self._epoch:
                return
            self.rc = rc
            self.error = error
            self.done = True
            self.active = False
            if not error and rc == 0:
                self.line = "done"
            elif error:
                self.line = error


def _fmt_size(n: int) -> str:
    if n >= 1048576:
        return f"{n / 1048576:.1f}M"
    if n >= 1024:
        return f"{n / 1024:.0f}K"
    return str(n)


def _entry_label(
    name: str,
    is_dir: bool,
    size: int,
    marked: bool,
    *,
    name_width: int = 40,
) -> str:
    mark = "*" if marked else " "
    kind = "/" if is_dir else " "
    size_s = "<DIR>" if is_dir else _fmt_size(size)
    name_width = max(8, int(name_width))
    if len(name) > name_width:
        name = name[: name_width - 1] + "…"
    return f"{mark}{kind} {name:<{name_width}} {size_s:>8}"


def _with_parent(entries: list) -> list:
    """Prepend MC-style '..' directory entry."""
    parent = LocalEntry("..", True, 0, 0)
    return [parent, *entries]


def build_app(
    *,
    host: str,
    port: int,
    local_root: Path,
    rate_mbit: float | None,
    skip_hash: bool,
):
    from rich.text import Text
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Button, Footer, Header, Label, RichLog, Static

    xfer = TransferState()

    class FilePanel(Static):
        can_focus = True
        # Keep Tab for the app action, not widget focus cycling.
        BINDINGS = [
            Binding("tab", "app.toggle_panel", "Panel", show=False, priority=True),
            Binding("shift+tab", "app.toggle_panel", show=False, priority=True),
        ]

        def __init__(self, state: PanelState, app_ref: "TetrysFM", **kwargs) -> None:
            super().__init__(**kwargs)
            self.state = state
            self._fm = app_ref

        def on_focus(self) -> None:
            self._fm.active = self.state
            self.refresh()
            other = self._fm._left_w if self.state is self._fm.right else self._fm._right_w
            if other is not None:
                other.refresh()

        def on_resize(self, event) -> None:  # noqa: ARG002
            self.refresh()

        def render(self) -> Text:
            st = self.state
            focused = self.has_focus
            # mark + kind + spaces + size column ≈ 12 cells; rest is the name.
            panel_w = max(24, int(self.size.width) - 2)
            name_w = max(12, panel_w - 12)
            out = Text()
            title = f" {st.kind}:{st.cwd or '/'} "
            if len(title) > panel_w:
                title = title[: panel_w - 1] + "…"
            out.append(title, style="bold")
            out.append("\n")
            out.append("-" * panel_w + "\n", style="dim")
            if st.error:
                err = st.error if len(st.error) <= panel_w - 2 else st.error[: panel_w - 3] + "…"
                out.append(f"! {err}\n", style="bold red")
            if not st.entries and not st.error:
                out.append("(empty)\n", style="dim")
            for i, ent in enumerate(st.entries):
                label = _entry_label(
                    ent.name,
                    ent.is_dir,
                    ent.size,
                    ent.name in st.marked,
                    name_width=name_w,
                )
                line = Text(label)
                if i == st.cursor:
                    # MC-style bar: reverse on the active panel, dim reverse if not.
                    if focused:
                        line.stylize("bold reverse")
                    else:
                        line.stylize("reverse dim")
                elif ent.name in st.marked:
                    line.stylize("yellow")
                elif ent.is_dir:
                    line.stylize("cyan")
                out.append(line)
                out.append("\n")
            return out

    class StatusLog(RichLog):
        can_focus = False

    class TetrysFM(App):
        CSS = """
        Screen { layout: vertical; }
        #panels { height: 1fr; }
        FilePanel {
            width: 1fr;
            height: 1fr;
            border: solid $accent;
            padding: 0 1;
        }
        FilePanel:focus { border: heavy $warning; }
        #status { height: 5; border: solid $primary; }
        """
        BINDINGS = [
            Binding("tab", "toggle_panel", "Panel", priority=True),
            Binding("shift+tab", "toggle_panel", show=False, priority=True),
            Binding("up", "move(-1)", "Up", show=False),
            Binding("down", "move(1)", "Down", show=False),
            Binding("shift+up", "mark_move(-1)", "MarkUp", show=False, priority=True),
            Binding("shift+down", "mark_move(1)", "MarkDown", show=False, priority=True),
            Binding("enter", "enter", "Enter"),
            Binding("backspace", "parent", "Up dir"),
            Binding("insert", "mark", "Mark"),
            Binding("f5", "copy", "Copy"),
            Binding("f7", "mkdir", "Mkdir"),
            Binding("f8", "delete", "Delete"),
            Binding("f2", "refresh", "Reload"),
            Binding("q", "quit", "Quit"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self.host = host
            self.port = port
            self.local_root = local_root.resolve()
            self.rate_mbit = rate_mbit
            self.skip_hash = skip_hash
            self.left = PanelState("left", "local", str(self.local_root))
            self.right = PanelState("right", "remote", "")
            self.active = self.left
            self._left_w: FilePanel | None = None
            self._right_w: FilePanel | None = None
            self._progress_modal: ProgressModal | None = None
            self._busy_kind = ""

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Horizontal(id="panels"):
                self._left_w = FilePanel(self.left, self, id="left")
                self._right_w = FilePanel(self.right, self, id="right")
                yield self._left_w
                yield self._right_w
            yield StatusLog(id="status", markup=True)
            yield Footer()

        def on_mount(self) -> None:
            self.title = f"tetrys-fm  {self.host}:{self.port}"
            self.reload_panel(self.left)
            self.reload_panel(self.right)
            if self._left_w is not None:
                self._left_w.focus()
            self.set_interval(0.15, self._tick_transfer)

        def _status(self) -> RichLog:
            return self.query_one("#status", RichLog)

        def _panel_widget(self, panel: PanelState) -> FilePanel | None:
            if panel is self.left:
                return self._left_w
            if panel is self.right:
                return self._right_w
            return None

        def _refresh_panels(self) -> None:
            if self._left_w:
                self._left_w.refresh()
            if self._right_w:
                self._right_w.refresh()

        def _tick_transfer(self) -> None:
            with xfer.lock:
                active = xfer.active
                line = xfer.line
                done = xfer.done
                error = xfer.error
                rc = xfer.rc
            modal = self._progress_modal
            if modal is not None and active:
                try:
                    modal.set_progress(line)
                except Exception:
                    pass
            if done:
                with xfer.lock:
                    xfer.done = False
                kind = self._busy_kind or "copy"
                self._busy_kind = ""
                if modal is not None:
                    self._progress_modal = None
                    try:
                        modal.dismiss(None)
                    except Exception:
                        pass
                if rc == 0 and not error:
                    self._status().write(f"[green]{kind} ok[/green]")
                    self.reload_panel(self.left)
                    self.reload_panel(self.right)
                else:
                    why = error or f"rc={rc}"
                    self._status().write(f"[red]{kind} failed: {why}[/red]")

        class ProgressModal(ModalScreen[None]):
            """Blocks the main UI and shows transfer progress in the center."""

            CSS = """
            ProgressModal {
                align: center middle;
            }
            #progress-box {
                width: 104;
                height: 15;
                max-width: 98%;
                border: heavy $warning;
                background: $surface;
                padding: 1 2;
                overflow: hidden;
            }
            #progress-title {
                height: 1;
                text-align: center;
                text-style: bold;
                overflow-x: hidden;
            }
            #progress-detail {
                height: 1;
                text-align: center;
                color: $text-muted;
                overflow-x: hidden;
            }
            #progress-line-a, #progress-line-b {
                height: 1;
                width: 100%;
                text-align: left;
                overflow-x: hidden;
            }
            #progress-hint {
                height: 1;
                text-align: center;
                color: $text-muted;
                padding-top: 1;
            }
            #progress-btns {
                height: auto;
                width: 100%;
                align: center middle;
                padding-top: 1;
            }
            #cancel {
                width: auto;
                min-width: 16;
                height: 3;
                text-align: center;
                content-align: center middle;
            }
            """
            BINDINGS = [
                Binding("escape", "cancel", "Cancel", show=False),
                Binding("q", "cancel", show=False),
            ]

            # Inner text width: box 104 − borders/padding ≈ 98.
            _LINE_W = 96

            def __init__(self, title: str, detail: str = "") -> None:
                super().__init__()
                self._title = title
                self._detail = detail
                self._last_pair = ("starting…", "")

            @classmethod
            def _clip(cls, text: str) -> str:
                text = text.replace("\t", " ").strip()
                if len(text) <= cls._LINE_W:
                    return text
                # Keep the right side (pct / sizes / rate), trim the name.
                return "…" + text[-(cls._LINE_W - 1) :]

            @classmethod
            def _split_bar_stats(cls, text: str) -> tuple[str, str]:
                """Put `[████…]` on the first row and numbers on the second."""
                idx = text.find("] ")
                if idx == -1 or "[" not in text[:idx]:
                    return text, ""
                return text[: idx + 1].rstrip(), text[idx + 2 :].lstrip()

            def compose(self) -> ComposeResult:
                with Vertical(id="progress-box"):
                    yield Label(self._clip(self._title), id="progress-title")
                    yield Label(
                        self._clip(self._detail) if self._detail else " ",
                        id="progress-detail",
                    )
                    yield Static(self._last_pair[0], id="progress-line-a")
                    yield Static(self._last_pair[1] or " ", id="progress-line-b")
                    yield Label("Esc — abort", id="progress-hint")
                    with Horizontal(id="progress-btns"):
                        yield Button("Cancel", id="cancel", variant="error")

            def on_mount(self) -> None:
                self.query_one("#cancel", Button).focus()

            def set_progress(self, line: str) -> None:
                raw = (line or "").replace("\r", "").strip() or "…"
                if "\n" in raw:
                    rows = [p.strip() for p in raw.splitlines() if p.strip()]
                elif " | " in raw:
                    rows = [p.strip() for p in raw.split(" | ", 1)]
                else:
                    rows = [raw]
                if len(rows) >= 2:
                    # Mux: show total bar+stats, then current file bar+stats
                    # compressed into two rows (prefer numbers via tail clip).
                    a = self._clip(rows[0])
                    b = self._clip(rows[1])
                else:
                    a, b = self._split_bar_stats(rows[0])
                    a, b = self._clip(a), self._clip(b) if b else ""
                pair = (a, b)
                if pair == self._last_pair:
                    return
                self._last_pair = pair
                self.query_one("#progress-line-a", Static).update(a)
                self.query_one("#progress-line-b", Static).update(b or " ")

            def on_button_pressed(self, event: Button.Pressed) -> None:
                if event.button.id == "cancel":
                    self.action_cancel()

            def action_cancel(self) -> None:
                # Free UI immediately; bump epoch so a stale worker cannot
                # re-lock F5 / overwrite a newer transfer.
                xfer.abandon()
                try:
                    if self.app._progress_modal is self:  # type: ignore[attr-defined]
                        self.app._progress_modal = None  # type: ignore[attr-defined]
                    self.dismiss(None)
                except Exception:
                    pass
                try:
                    self.app._status().write(  # type: ignore[attr-defined]
                        "[yellow]cancelled[/yellow]"
                    )
                except Exception:
                    pass

        def reload_panel(self, panel: PanelState) -> None:
            panel.error = ""
            try:
                if panel.kind == "local":
                    panel.entries = _with_parent(list_local(Path(panel.cwd)))
                else:
                    # VfsEntry is compatible enough for display/navigation fields.
                    remote = list_remote(self.host, self.port, panel.cwd)
                    panel.entries = _with_parent(remote)
                if panel.cursor >= len(panel.entries):
                    panel.cursor = max(0, len(panel.entries) - 1)
                panel.marked &= {e.name for e in panel.entries if e.name != ".."}
            except Exception as exc:
                panel.entries = _with_parent([])
                panel.error = str(exc)
                panel.cursor = 0
            widget = self._panel_widget(panel)
            if widget is not None:
                widget.refresh()

        def action_toggle_panel(self) -> None:
            if self.active is self.left:
                self.active = self.right
                if self._right_w:
                    self._right_w.focus()
            else:
                self.active = self.left
                if self._left_w:
                    self._left_w.focus()
            self._refresh_panels()

        def action_move(self, delta: int) -> None:
            panel = self.active
            if not panel.entries:
                return
            panel.cursor = (panel.cursor + delta) % len(panel.entries)
            self._refresh_panels()

        def action_mark_move(self, delta: int) -> None:
            """MC MarkUp/MarkDown: toggle current, then move."""
            panel = self.active
            if not panel.entries:
                return
            name = panel.entries[panel.cursor].name
            if name != "..":
                if name in panel.marked:
                    panel.marked.remove(name)
                else:
                    panel.marked.add(name)
            self.action_move(delta)

        def action_mark(self) -> None:
            self.action_mark_move(1)

        def _selection(self, panel: PanelState) -> list[str]:
            if panel.marked:
                return sorted(n for n in panel.marked if n != "..")
            if not panel.entries:
                return []
            name = panel.entries[panel.cursor].name
            return [] if name == ".." else [name]

        def action_enter(self) -> None:
            panel = self.active
            if not panel.entries:
                return
            ent = panel.entries[panel.cursor]
            if ent.name == "..":
                self.action_parent()
                return
            if not ent.is_dir:
                return
            if panel.kind == "local":
                panel.cwd = str((Path(panel.cwd) / ent.name).resolve())
            else:
                panel.cwd = f"{panel.cwd}/{ent.name}".strip("/")
            panel.cursor = 0
            panel.marked.clear()
            self.reload_panel(panel)

        def action_parent(self) -> None:
            panel = self.active
            if panel.kind == "local":
                parent = Path(panel.cwd).resolve().parent
                if parent == Path(panel.cwd).resolve():
                    return
                panel.cwd = str(parent)
            else:
                if not panel.cwd:
                    return
                parts = panel.cwd.strip("/").split("/")
                panel.cwd = "/".join(parts[:-1])
            panel.cursor = 0
            panel.marked.clear()
            self.reload_panel(panel)

        def action_refresh(self) -> None:
            if xfer.active:
                return
            self.reload_panel(self.active)

        def action_mkdir(self) -> None:
            if xfer.active:
                return
            panel = self.active
            name = "newdir"
            try:
                if panel.kind == "local":
                    (Path(panel.cwd) / name).mkdir(exist_ok=False)
                else:
                    rel = f"{panel.cwd}/{name}".strip("/")
                    remote_mkdir(self.host, self.port, rel)
                self.reload_panel(panel)
            except Exception as exc:
                self._status().write(f"[red]mkdir: {exc}[/red]")

        def _do_delete(self, panel: PanelState, names: list[str]) -> None:
            if xfer.active or self._progress_modal is not None:
                return
            preview = ", ".join(names[:4])
            if len(names) > 4:
                preview += f", … (+{len(names) - 4})"
            title = f"Delete {len(names)} item(s)"
            detail = f"{panel.kind}:{panel.cwd or '/'}  ·  {preview}"
            host, port = self.host, self.port
            kind = panel.kind
            cwd = panel.cwd

            def worker() -> None:
                try:
                    xfer.progress(f"deleting {len(names)} item(s)…")
                    for i, name in enumerate(names, 1):
                        if xfer.should_abort(epoch):
                            xfer.finish(1, "aborted", epoch=epoch)
                            return
                        xfer.progress(f"[{i}/{len(names)}] {name}")
                        if kind == "local":
                            local_rm(Path(cwd) / name)
                        else:
                            remote_rm(host, port, f"{cwd}/{name}".strip("/"))
                    panel.marked.clear()
                    xfer.finish(0, epoch=epoch)
                except Exception as exc:
                    xfer.finish(1, str(exc), epoch=epoch)

            epoch = xfer.begin()
            modal = self.ProgressModal(title, detail)
            self._progress_modal = modal
            self._busy_kind = "delete"
            self.push_screen(modal)
            threading.Thread(target=worker, daemon=True).start()

        def action_delete(self) -> None:
            if xfer.active or self._progress_modal is not None:
                return
            panel = self.active
            names = self._selection(panel)
            if not names:
                return

            class ConfirmDelete(ModalScreen[bool]):
                CSS = """
                ConfirmDelete {
                    align: center middle;
                }
                #confirm-box {
                    width: 64;
                    max-width: 90%;
                    height: auto;
                    border: heavy $error;
                    background: $surface;
                    padding: 1 2;
                }
                #confirm-btns {
                    height: auto;
                    align: center middle;
                    padding-top: 1;
                }
                """
                BINDINGS = [
                    Binding("left", "focus_prev", show=False),
                    Binding("right", "focus_next", show=False),
                    Binding("y", "yes", show=False),
                    Binding("n", "no", show=False),
                    Binding("escape", "no", show=False),
                    Binding("enter", "accept", show=False),
                ]

                def __init__(self, items: list[str]) -> None:
                    super().__init__()
                    self.items = items

                def compose(self) -> ComposeResult:
                    preview = ", ".join(self.items[:8])
                    if len(self.items) > 8:
                        preview += f", … (+{len(self.items) - 8})"
                    with Vertical(id="confirm-box"):
                        yield Label(
                            f"Delete {len(self.items)} item(s) from "
                            f"{panel.kind}:{panel.cwd or '/'}?"
                        )
                        yield Label(preview, id="confirm-names")
                        yield Label("← → choose, Enter confirm, Esc cancel")
                        with Horizontal(id="confirm-btns"):
                            yield Button("Yes", id="yes", variant="error")
                            yield Button("No", id="no", variant="primary")

                def on_mount(self) -> None:
                    self.query_one("#no", Button).focus()

                def on_button_pressed(self, event: Button.Pressed) -> None:
                    self.dismiss(event.button.id == "yes")

                def action_focus_prev(self) -> None:
                    self.focus_previous()

                def action_focus_next(self) -> None:
                    self.focus_next()

                def action_accept(self) -> None:
                    focused = self.focused
                    if isinstance(focused, Button):
                        self.dismiss(focused.id == "yes")
                    else:
                        self.dismiss(False)

                def action_yes(self) -> None:
                    self.dismiss(True)

                def action_no(self) -> None:
                    self.dismiss(False)

            def on_answer(confirmed: bool | None) -> None:
                if confirmed:
                    self._do_delete(panel, names)

            self.push_screen(ConfirmDelete(names), on_answer)

        def action_copy(self) -> None:
            if xfer.active or self._progress_modal is not None:
                return
            src = self.active
            dst = self.right if src is self.left else self.left
            names = self._selection(src)
            if not names:
                self._status().write("nothing selected")
                return
            preview = ", ".join(names[:4])
            if len(names) > 4:
                preview += f", … (+{len(names) - 4})"
            title = f"Copy {len(names)} item(s)"
            detail = (
                f"{src.kind}:{src.cwd or '/'} → {dst.kind}:{dst.cwd or '/'}  ·  {preview}"
            )
            host, port = self.host, self.port
            rate = self.rate_mbit
            skip = self.skip_hash

            def worker() -> None:
                abort = xfer.abort_check(epoch)
                try:
                    if src.kind == "local" and dst.kind == "remote":
                        rc = copy_local_to_remote(
                            host,
                            port,
                            Path(src.cwd),
                            names,
                            remote_dir=dst.cwd,
                            progress=xfer.progress,
                            should_abort=abort,
                            rate_mbit=rate,
                            skip_hash=skip,
                            symbol_size=WAN_SYMBOL_SIZE,
                            block_k=WAN_BLOCK_K,
                        )
                    elif src.kind == "remote" and dst.kind == "local":
                        remotes = [
                            f"{src.cwd}/{name}".strip("/") for name in names
                        ]
                        rc = copy_remote_to_local(
                            host,
                            port,
                            remotes,
                            Path(dst.cwd),
                            progress=xfer.progress,
                            should_abort=abort,
                        )
                    else:
                        xfer.finish(1, "copy only between local and remote", epoch=epoch)
                        return
                    if abort():
                        xfer.finish(1, "aborted", epoch=epoch)
                    elif rc == 0:
                        xfer.finish(0, epoch=epoch)
                    else:
                        xfer.finish(rc, f"rc={rc}", epoch=epoch)
                except InterruptedError:
                    xfer.finish(1, "aborted", epoch=epoch)
                except Exception as exc:
                    xfer.finish(1, str(exc), epoch=epoch)

            epoch = xfer.begin()
            modal = self.ProgressModal(title, detail)
            self._progress_modal = modal
            self._busy_kind = "copy"
            self.push_screen(modal)
            threading.Thread(target=worker, daemon=True).start()


    return TetrysFM


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="tetrys console file manager")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7494)
    p.add_argument(
        "--local",
        type=Path,
        default=Path.cwd(),
        help="left panel starting directory",
    )
    p.add_argument(
        "--rate",
        "--rate-mbit",
        type=float,
        default=None,
        dest="rate_mbit",
        help="lock upload pace (Mbit/s); omit for BlastCc",
    )
    p.add_argument("--skip-hash", action="store_true", default=True)
    args = p.parse_args(argv)
    try:
        import textual  # noqa: F401
    except ImportError:
        print("textual is required: pip install textual")
        return 2
    app = build_app(
        host=args.host,
        port=args.port,
        local_root=args.local,
        rate_mbit=args.rate_mbit,
        skip_hash=args.skip_hash,
    )
    app().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
