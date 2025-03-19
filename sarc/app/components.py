import os
from pathlib import Path

import yaml
from apischema import deserialize, serialize
from hrepr import H
from starbear import Queue
from starbear.components.editor import Editor
from starbear.core.utils import Event, FeedbackEvent

here = Path(__file__).parent


class ContentEditor:
    def __init__(self, title, language, value=""):
        self.title = title
        self.language = language
        self.value = value

    def read(self):
        return self.value

    def write(self, new):
        self.value = new
        return new

    def validate(self, new):
        return new

    async def __live__(self, element):
        async def status(name, message):
            await element[main].js.setAttribute("status", name)
            element[footer].set(message)
            element[buttons].set(
                H.div(
                    H.span("[reset]", onclick=q.tag("reset"))
                    if name in ["dirty", "error"]
                    else "",
                    H.span("[save]", onclick=q.tag("submit"))
                    if name == "dirty"
                    else "",
                )
            )

        q = Queue()
        current = self.read()
        ed = Editor(
            value=current,
            language=self.language,
            onChange=q.tag("edit"),
            bindings={
                "CtrlCmd+KeyS": q.tag("submit"),
                "CtrlCmd+KeyI": q.tag("intelligence"),
            },
        )
        element.print(
            main := H.div["config-editor"](
                H.div["editor-header"](
                    H.div["editor-title"](self.title),
                    buttons := H.div["editor-buttons"]("").ensure_id(),
                ).ensure_id(),
                H.div["editor"](ed),
                footer := H.div["editor-footer"]("Saved").ensure_id(),
                status="saved",
            ).ensure_id()
        )

        obj = None
        valid = False
        try:
            obj = self.validate(current)
            valid = True
            yield Event("init", obj)
        except Exception as exc:
            await status("error", f"{type(exc).__name__}: {exc}")

        async for event in q:
            try:
                if event.tag == "edit" and event["event"] == "change":
                    new = event["content"]
                    if new == current:
                        await status("saved", "Saved")
                    else:
                        obj = self.validate(new)
                        valid = True
                        await status("dirty", "Modified")
                elif event.tag == "filter":
                    self.filter = event.value
                    await element[ed].js.editor.setValue(self.read())
                elif event.tag == "reset":
                    await element[ed].js.editor.setValue(self.read())
                    await status("saved", "Saved")
                elif event.tag == "submit":
                    if valid:
                        await status("saving", "Saving...")
                        await (yield FeedbackEvent("submit", obj))
                        current = self.write(new)
                        await status("saved", "Saved")
                elif event.tag == "intelligence":
                    await status("thinking", "Thinking...")
                    new_content = await (
                        yield FeedbackEvent("intelligence", event["content"])
                    )
                    await element[ed].js.editor.setValue(new_content)

            except Exception as exc:
                valid = False
                await status("error", f"{type(exc).__name__}: {exc}")


class YamlConfigEditor(ContentEditor):
    def __init__(self, file, type, default="", title=None):
        file = Path(file)
        super().__init__(title=file.name if title is None else title, language="yaml")
        self.file = file
        self.type = type
        if not isinstance(default, str):
            default = yaml.safe_dump(serialize(self.type, default))
        self.default = default

    def read(self):
        if self.file.exists():
            return self.file.read_text()
        else:
            return self.default

    def write(self, new):
        if not self.file.parent.exists():
            os.makedirs(self.file.parent, exist_ok=True)
        self.file.write_text(new)
        return new

    def validate(self, new):
        return deserialize(self.type, yaml.safe_load(new))
