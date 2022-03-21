from __future__ import annotations

from typing import List

import logging

from analyzer.data_view.data_view_lib import DataView
from analyzer.dataset.dataset_lib import Dataset
from analyzer.users.users_lib import User


log = logging.getLogger(__name__)


class RichDataView(DataView):
    def __init__(self, data_view: DataView, dataset: Dataset, user: User