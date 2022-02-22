from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Callable, DefaultDict
from collections import Counter, defaultdict, OrderedDict
from functools import lru_cache
from time import time

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text

from analyzer.data_view.handler import DataViewHandler
from analyzer.dataset.handler import DatasetHandler
from analyzer.users.users_lib import UserHandler

from analyzer.data_view.data_view_lib import Label, LabelSequence, DataViewId
from analyzer.data_view.rich_data_view import RichDataView
from analyzer.dataset.dataset_lib import Dataset, DatasetId
from analyzer.constraint_lib import (
    TransformResourceHandler, Transform, FilterTransform, EnrichmentTransform,
)

from analyzer.text_processing import WordHistoryProcessor, WordHistoryResult

import logging

log = logging.getLogger(__name__)


DataFrame = pd.DataFrame
TransformLookup = DefaultDict[DatasetId, Dict[DataViewId, Set[Transform]]]


TAB = "\t"
COMMA = ","

stop_words = text.ENGLISH_STOP_WORDS.union(["book"])


class DataFrameCache:
    def __init__(self):
        self._cache: Dict[DataViewId, DataFrame] = {}


class Analyzer:
    DEFAULT_LIMIT = 250

    def __init__(
        self,
        data_dir: Path,
        data_view_handler: DataViewHandler,
        dataset_handler: DatasetHandler,
        user_handler: UserHandler,
        transform_resource_handler: TransformResourceHandler,
    ):
        self.data_dir = Path(data_dir)
        assert self.data_dir.exists()

        self._data_view_handler = data_view_handler
        self._dataset_handler = dataset_handler
        self._user_handler = user_handler
        self.transform_resource_handler = transform_resource_handler

        self._active_dataframe_by_data_view: Dict[RichDataView, DataFrame] = {}
        self._df_cache_by_dataset = defaultdict(OrderedDict)
        self._data_view_transforms_by_dataset_id: TransformLookup = defaultdict(dict)

    @lru_cache(maxsize=128)
    def _get_df(self, data_view: RichDataView) -> DataFrame:
        data_view_id = data_view.id
        dataset_id = data_view.dataset_id

        df_cache = self._df_cache_by_dataset[dataset_id]
        transforms_by_data_view_id = self._data_view_transforms_by_dataset_id[dataset_id]

        if data_view_id not in df_cache:
            log.info("data_view_id %s not in cache", data_view_id)
            # find best starting point
            cached_data_view_id, remaining_transforms = self.get_id_of_best_base_df(
                data_view.transforms, transforms_by_data_view_id,
            )

            log.info("best base_df: %s", cached_data_view_id)

            if cached_data_view_id:
                df = self._get_df(self.rich_data_view(cached_data_view_id))
                log.info(f"generating DataView {data_view_id} from {cached_data_view_id}")
                transforms = remaining_transforms
            else:
                log.info(f"generating DataView {data_view_id} from base")
                df = self.active_dataframe(data_view)
                transforms = data_view.transforms

            for transform in transforms:
                if isins