import logging

from analyzer.users import UserId
from analyzer.dataset import DatasetId
from analyzer.data_view.data_view_lib import Label, LabelSet
from analyzer.data_view.handler import HistoryKey


logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)


def test_history_key():
    user_id1 = UserId("111")
    user_id2 = UserId("222")
    dataset_id1 = DatasetId("11111")
    dataset_id2 = DatasetId("22222")

    k = {
        0: HistoryKey(user_id1, dataset_id1),
        1: HistoryKey(user_id1, dataset_id1),
        2: HistoryKey(user_id2, dataset_id1),
        3: HistoryKey(user_id1, dataset_id2),
        4: HistoryKey(user_id2, dataset_id2),
    }

    assert k[0] == k[0]
    assert k[0] == k[1]
    assert k[0] != k[2]
    assert k[0] != k[3]
    assert k[0] != k[4]

    assert k[1] == k[0]
    assert k[1] == k[1]
    assert k[1] != k[2]
    assert k[1] != k[3]
    assert k[1] != k[4]

    assert k[2] != k[0]
    assert k[2] != k[1]
    assert k[2] == k[2]
    assert k[2] != k[3]
    assert k[2] != k[4]

    assert k[3] != k[0]
    assert k[3] != k[1]
    assert k[3] != k[2]
    assert k[3] == k[3]
    assert k[3] != k[4]

    assert k[4] != k[0]
    assert k[4] != k[1]
    assert k[4] != k[2]
    assert k[4] != k[3]
    assert k[4] == k[4]


def test_label_set_serialization():
    label_entries = [
        {Label.KEY_NAME: "aaa", Label.KEY_WIDTH: 10, Label.KEY_FONT_SIZE: 10},
        