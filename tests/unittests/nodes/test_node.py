from datetime import datetime, timedelta

import prometheus_api_client.prometheus_connect
import pytest

import sarc.nodes.node
from sarc.nodes.node import (
    generate_custom_query,
    generate_label_configs,
    get_nodes_time_series,
    query_prom,
)


@pytest.mark.freeze_time("2021-01-01")
@pytest.mark.parametrize(
    ["enddelta", "strdelta"],
    [
        (timedelta(days=365), "1y"),
        (timedelta(days=30), "4w2d"),
        (timedelta(days=1), "1d"),
        (timedelta(hours=1), "1h"),
        (timedelta(minutes=1), "1m"),
        (timedelta(seconds=1), "1s"),
    ],
)
def test_generate_custom_query_ends(enddelta, strdelta):
    start = datetime(2020, 1, 2)

    ground_truth = f'avg_over_time(some_metric{{node="cn-d003",dimension="user"}}[1s])[{strdelta}:1s] offset 1y'

    assert (
        generate_custom_query(
            "some_metric",
            label_config={"node": "cn-d003", "dimension": "user"},
            start=start,
            end=start + enddelta,
            running_window=timedelta(seconds=1),
        )
        == ground_truth
    )


@pytest.mark.freeze_time
@pytest.mark.parametrize(
    ["current_date", "start_date", "offset_str"],
    [
        ("2022-01-01", datetime(year=2021, month=1, day=1), "1y"),
        ("2022-01-01", datetime(year=2021, month=7, day=1), "26w2d"),
        ("2021-07-01", datetime(year=2021, month=6, day=1), "4w2d"),
    ],
)
def test_generate_custom_query_starts(current_date, start_date, offset_str, freezer):
    freezer.move_to(current_date)

    ground_truth = f'avg_over_time(some_metric{{node="cn-d003",dimension="user"}}[1s])[1y:1s] offset {offset_str}'

    assert (
        generate_custom_query(
            "some_metric",
            label_config={"node": "cn-d003", "dimension": "user"},
            start=start_date,
            end=start_date + timedelta(days=365),
            running_window=timedelta(seconds=1),
        )
        == ground_truth
    )


@pytest.mark.freeze_time("2021-01-01")
def test_generate_custom_query_start_in_future():
    with pytest.raises(ValueError, match="cannot be in the future"):
        generate_custom_query(
            "some_metric",
            label_config={"node": "cn-d003", "dimension": "user"},
            start=datetime(year=2050, month=1, day=1),
            end=datetime(year=2051, month=1, day=1),
            running_window=timedelta(seconds=1),
        )


@pytest.mark.freeze_time("2021-01-01")
def test_generate_custom_query_too_large_running_window():
    with pytest.raises(ValueError, match="cannot be larger than"):
        generate_custom_query(
            "some_metric",
            label_config={"node": "cn-d003", "dimension": "user"},
            start=datetime(year=2020, month=1, day=1),
            end=datetime(year=2020, month=1, day=2),
            running_window=timedelta(days=10),
        )


@pytest.mark.freeze_time("2023-01-01")
@pytest.mark.parametrize(
    ["labels", "labels_ground_truth"],
    [
        ({"node": "cn-d003", "dimension": "user"}, 'node="cn-d003",dimension="user"'),
        (
            {"node": "cn-d003", "cluster_name": "cluster1"},
            'node="cn-d003",cluster_name="cluster1"',
        ),
    ],
)
def test_generate_custom_query_labels(labels, labels_ground_truth):
    ground_truth = (
        f"avg_over_time(some_metric{{{labels_ground_truth}}}[1s])[1y:1s] offset 2y"
    )

    assert (
        generate_custom_query(
            "some_metric",
            label_config=labels,
            start=datetime(year=2021, month=1, day=1),
            end=datetime(year=2022, month=1, day=1),
            running_window=timedelta(seconds=1),
        )
        == ground_truth
    )


@pytest.mark.freeze_time("2023-01-01")
@pytest.mark.parametrize(
    ["running_window", "rw_ground_thruth"],
    [
        (timedelta(days=1), "1d"),
        (timedelta(hours=2.5), "2h30m"),
        (timedelta(seconds=70), "1m10s"),
    ],
)
def test_generate_custom_query_running_window(running_window, rw_ground_thruth):
    ground_truth = f'avg_over_time(some_metric{{node="cn-d003",dimension="user"}}[{rw_ground_thruth}])[1y:{rw_ground_thruth}] offset 2y'

    assert (
        generate_custom_query(
            "some_metric",
            label_config={"node": "cn-d003", "dimension": "user"},
            start=datetime(year=2021, month=1, day=1),
            end=datetime(year=2022, month=1, day=1),
            running_window=running_window,
        )
        == ground_truth
    )


def test_generate_label_configs_no_node_id_no_cluster_name():
    assert list(generate_label_configs(None, None)) == [{}]


def test_generate_label_configs_node_id_no_cluster_name():
    assert list(generate_label_configs("node1", None)) == [{"instance": "node1"}]
    assert list(generate_label_configs(["node1"], None)) == [{"instance": "node1"}]
    assert list(generate_label_configs(["node1", "node2"], None)) == [
        {"instance": "node1"},
        {"instance": "node2"},
    ]


def test_generate_label_configs_no_node_id_cluster_name():
    assert list(generate_label_configs(None, "mila-cluster")) == [
        {"cluster": "mila-cluster"}
    ]
    assert list(generate_label_configs(None, ["mila-cluster"])) == [
        {"cluster": "mila-cluster"}
    ]
    assert list(generate_label_configs(None, ["mila-cluster", "mila-cluster"])) == [
        {"cluster": "mila-cluster"},
        {"cluster": "mila-cluster"},
    ]


def test_generate_label_configs_unsupported_cluster_name():
    with pytest.raises(
        NotImplementedError, match="Only mila-cluster is supported for now"
    ):
        list(generate_label_configs(None, "unsupported-cluster"))


@pytest.mark.freeze_time("2023-01-01")
@pytest.mark.parametrize(
    ["metric_name", "label_config", "start", "end", "running_window", "ground_truth"],
    [
        (
            "the_metric",
            {"node": "cn-d003"},
            datetime(year=2021, month=1, day=1),
            datetime(year=2022, month=1, day=1),
            timedelta(days=7 * 4),
            'avg_over_time(the_metric{node="cn-d003"}[4w])[1y:4w] offset 2y',
        ),
        (
            "another_metric",
            {"cluster": "mila-cluster"},
            datetime(year=2022, month=1, day=1),
            datetime(year=2022, month=2, day=1),
            timedelta(days=1),
            'avg_over_time(another_metric{cluster="mila-cluster"}[1d])[4w3d:1d] offset 1y',
        ),
    ],
)
def test_query_prom(
    metric_name, label_config, start, end, running_window, ground_truth, monkeypatch
):
    def assert_query(self, query):
        assert query == ground_truth

    monkeypatch.setattr(
        prometheus_api_client.prometheus_connect.PrometheusConnect,
        "custom_query",
        assert_query,
    )
    query_prom(
        metric_name=metric_name,
        label_config=label_config,
        start=start,
        end=end,
        running_window=running_window,
    )


# Test get_nodes_time_series
# Test that all calls to `query_prom` were done as expected and
# that df are concatenated properly.


@pytest.mark.freeze_time
def test_get_nodes_time_series_default_end(freezer, monkeypatch):
    freezer.move_to("2023-01-01")

    def fake_sleep(metric_name, label_config, start, end, running_window):
        freezer.move_to(f"2023-01-01 00:{fake_sleep.i}:00")
        fake_sleep.i += 1
        assert end == datetime(year=2023, month=1, day=1)
        assert end != datetime.utcnow()
        return [
            {
                "metric": {
                    "__name__": metric_name,
                    **label_config,
                },
                "values": [
                    [1675109742, "0.0330033"],
                    [1675109772, "0.0333333"],
                    [1675109802, "0.0333333"],
                    [1675109832, "0.0660066"],
                    [1675109862, "0.03367"],
                    [1675109892, "0.0330033"],
                    [1675109922, "0.0333333"],
                    [1675109952, "0.0673676"],
                ],
            }
        ]

    fake_sleep.i = 1

    monkeypatch.setattr(sarc.nodes.node, "query_prom", fake_sleep)

    get_nodes_time_series(["metric1", "metric2"], start=datetime(2021, 1, 1))


@pytest.mark.freeze_time("2023-01-01")
def test_get_nodes_time_series_queries(monkeypatch):
    expected = [
        'avg_over_time(metric1{instance="cn-d003",cluster="mila-cluster"}[4w])[1y:4w] offset 2y',
        'avg_over_time(metric1{instance="cn-d004",cluster="mila-cluster"}[4w])[1y:4w] offset 2y',
        'avg_over_time(metric2{instance="cn-d003",cluster="mila-cluster"}[4w])[1y:4w] offset 2y',
        'avg_over_time(metric2{instance="cn-d004",cluster="mila-cluster"}[4w])[1y:4w] offset 2y',
    ]
    found = []

    def assert_expected_query(self, query):
        assert expected.pop(0) == query
        found.append(query)

        return [
            {
                "metric": {"query": query, "some": "other", "arguments": 0},
                "values": [
                    [1675109742, "0.0330033"],
                    [1675109772, "0.0333333"],
                    [1675109802, "0.0333333"],
                    [1675109832, "0.0660066"],
                    [1675109862, "0.03367"],
                    [1675109892, "0.0330033"],
                    [1675109922, "0.0333333"],
                    [1675109952, "0.0673676"],
                ],
            }
        ]

    monkeypatch.setattr(
        prometheus_api_client.prometheus_connect.PrometheusConnect,
        "custom_query",
        assert_expected_query,
    )

    df = get_nodes_time_series(
        ["metric1", "metric2"],
        start=datetime(2021, 1, 1),
        end=datetime(2022, 1, 1),
        node_id=["cn-d003", "cn-d004"],
        cluster_name="mila-cluster",
        running_window=timedelta(days=7 * 4),
    )

    assert len(expected) == 0

    assert df.shape == (8 * 4, 4)
    for query in found:
        assert df[df["query"] == query].shape == (8, 4)
