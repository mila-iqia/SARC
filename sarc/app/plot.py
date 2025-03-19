import asyncio
import base64
import io
import os
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import plotnine as pn
from apischema import deserializer
from griptape.drivers import OpenAiChatPromptDriver
from griptape.structures import Agent
from hrepr import H
from plotnine import aes, ggplot
from starbear import Queue, bear, here
from starbear.stream.live import live

from ..client.series import load_job_series
from ..common.utils import display_on_gui
from .components import YamlConfigEditor


def llmgen(content):
    prompt_template = (here / "plot-prompt-template.txt").read_text()
    agent = Agent(
        input_template=prompt_template,
        prompt_driver=OpenAiChatPromptDriver(model="gpt-4o"),
    )
    agent.run(content)
    result_text = agent.output_task.output.value
    if result_text.startswith("```"):
        result_text = "\n".join(result_text.split("\n")[1:-1])
    if not result_text.endswith("\n"):
        result_text += "\n"
    return result_text


_current = {
    "query": None,
    "data": None,
}


@dataclass
class Query:
    cluster: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def load(self, progress):
        if _current["query"] == self:
            return _current["data"]
        h = md5(str(self).encode("utf8")).hexdigest()
        file = Path(f"plotting/cache/cached_query_{h}.pkl")
        if file.exists():
            results = pd.read_pickle(file)
        else:
            with display_on_gui(progress):
                results = load_job_series(**vars(self))
                os.makedirs(file.parent, exist_ok=True)
                results.to_pickle(file)
        _current["query"] = self
        _current["data"] = results
        return results


@dataclass
class Aesthetics:
    x: str | None = None
    y: str | None = None
    size: str | None = None
    color: str | None = None
    fill: str | None = None

    def make(self):
        args = {k: v for k, v in vars(self).items() if v is not None}
        return aes(**args)


@dataclass
class PlotElement:
    constructor: Callable
    aes: Aesthetics | None
    args: dict[str, Any]

    def make(self):
        kwargs = {
            k: eval(v[1:], locals(), globals())
            if isinstance(v, str) and v.startswith("=")
            else v
            for k, v in self.args.items()
        }

        if self.aes:
            return self.constructor(self.aes.make(), **kwargs)
        else:
            return self.constructor(**kwargs)


@dataclass
class Plot:
    aes: Aesthetics | None
    parts: list[PlotElement]

    def make(self, df):
        p = ggplot(df, self.aes.make() if self.aes else aes())
        for part in self.parts:
            p += part.make()
        return p

    def image(self, df):
        plot = self.make(df)
        bio = io.BytesIO()
        plot.save(filename=bio, height=10, width=10, units="in")
        bio.seek(0)
        b64 = base64.b64encode(bio.read()).decode("utf8")
        return H.img(src=f"data:image/png;base64, {b64}")


def _parse_parts(conf: dict):
    aes = {}
    args = []
    for k, v in conf.items():
        if k.startswith("~"):
            aes[k[1:]] = v
        else:
            if "#" in k:
                k = k.split("#")[0]
            args.append((k, v))
    return aes, args


@deserializer
def _deserialize_plot(conf: dict) -> Plot:
    aes_parts, args = _parse_parts(conf)
    aes = Aesthetics(**aes_parts) if aes_parts else None
    parts = []
    for key, value in args:
        aes_parts, method_args = _parse_parts(value)
        subaes = Aesthetics(**aes_parts) if aes_parts else None
        method = getattr(pn, key)
        kw = {k: v for k, v in method_args}
        pe = PlotElement(
            constructor=method,
            aes=subaes,
            args=kw,
        )
        parts.append(pe)
    return Plot(
        aes=aes,
        parts=parts,
    )


@dataclass
class Column:
    name: str
    expr: str | None = None
    factor: int = 1
    formatter: str = "{}"

    def format(value):
        return str(value)


@dataclass
class Table:
    filters: list[str] = field(default_factory=list)
    highlight: dict[str, str] = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    new_columns: dict[str, str] = field(default_factory=dict)
    sort: str | None = None
    limit: int = 100

    def __post_init__(self):
        for column in self.columns:
            if ":" in column:
                column_name, expr = column.split(":", 1)
                self.new_columns[column_name] = expr
        self.columns = [column.split(":")[0] for column in self.columns]

    def transform(self, jobs):
        for column_name, expr in self.new_columns.items():
            jobs[column_name] = eval(expr, locals(), globals())
        if sort := self.sort:
            if sort.startswith("-"):
                ascending = False
                sort = sort[1:]
            else:
                ascending = True
            jobs = jobs.sort_values(by=sort, ascending=ascending)
        for filt in self.filters:
            jobs = jobs[eval(filt, locals(), globals())]
        jobs["styles"] = [[] for _ in range(len(jobs))]
        jobs["highlight"] = "black"
        for css_class, hl in self.highlight.items():
            matches = eval(hl, locals(), globals())
            jobs.loc[matches, "styles"] = jobs.loc[matches, "styles"].apply(
                lambda x: x + [css_class]
            )
            jobs.loc[matches, "highlight"] = css_class
        return jobs

    def generate(self, jobs):
        table_rows = []
        for _, row in jobs.iterrows():
            table_row = H.tr(
                [H.td(str(row[column.split(":")[0]])) for column in self.columns],
                **{"class": " ".join(row.styles)},
            )
            if table_row:
                table_rows.append(table_row)
            if len(table_rows) >= self.limit:
                break
        return H.table(
            H.tr(H.th(column.split(":")[0]) for column in self.columns),
            table_rows,
        )


@dataclass(frozen=True)
class Layout:
    editor: str | bool = True
    table: str | bool = False
    plot: str | bool = True


@dataclass
class PlottingConfig:
    query: Query
    layout: Layout = Layout()
    plot: Plot | None = None
    table: Table | None = None


@bear
async def app(page):
    q = Queue()
    dflt = (here / "plot-default-config.yaml").read_text()

    plot_name = page.query_params["plot_name"]
    config_path = Path("plotting/configs/") / f"{plot_name}.yaml"

    async def refresh(value):
        page[progress].set("Refreshing...")
        try:
            df = await asyncio.to_thread(value.query.load, page[progress])
            page[plot_pane].clear()
            df = value.table.transform(df)
            if value.layout.table:
                page[plot_pane].print(value.table.generate(df))
            if value.layout.plot:
                page[plot_pane].print(value.plot.image(df))
            page[progress].set("Done")
        except Exception as exc:
            page[progress].set(f"{type(exc).__name__}: {exc}")

    page.add_resources(here / "style.css")
    content = H.div["double-pane"](
        H.div["editor-pane"](
            H.div["editor-box"](
                live(
                    YamlConfigEditor(
                        file=config_path, type=PlottingConfig, default=dflt
                    ),
                    on_produce=q,
                )
            ),
            progress := H.div["status-box"]().ensure_id(),
        ),
        plot_pane := H.div["plot-pane"]().ensure_id(),
    )
    page.print(content)

    async for event in q:
        if event.type == "intelligence":
            new_text = await asyncio.to_thread(llmgen, event.value)
            # new_text = await llmgen2(event.value)
            event.resolve(new_text)
            continue
        if event.type == "submit":
            event.resolve()
        await refresh(event.value)


__app__ = {"/{plot_name}": app}
