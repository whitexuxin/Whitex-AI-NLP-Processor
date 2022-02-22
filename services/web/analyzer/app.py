from pathlib import Path
from typing import Dict, List, Optional
import json

from flask import Flask, jsonify, request
from flask_cors import CORS

from analyzer.session import (
    Session, UserId, DatasetId, DataViewId, TransformList,
)

import logging
from logging.handlers import RotatingFileHandler


LOG_FILENAME = "log/app.log"
LOG_MAX_SIZE_BYTES = 500000
LOG_BACKUP_COUNT = 5

DATA_DIR = Path("/data")
CONFIG_DIR = Path("/config")

CONFIG_FILENAME = "config.json"
DATA_VIEWS_FILENAME = "data_views.json"
USERS_FILENAME = "users.json"
DATASETS_FILENAME = "datasets.json"
DATA_VIEW_HISTORY_FILENAME = "data_view_history.json"
TAGS_PREFIX = "tags"

PAYLOAD_KEY = "q"

INDEX_FILENAME = "index.html"

IGNORE_FILENAMES = {"README.md"}


handler = RotatingFileHandler(
    filename=LOG_FILENAME,
    maxBytes=LOG_MAX_SIZE_BYTES,
    backupCount=LOG_BACKUP_COUNT,
)

logging.basicConfig(
    level=logging.DEBUG,
    filename=LOG_FILENAME,
)

log = logging.getLogger(__name__)
log.addHandler(handler)


session = Session(
    config_dir=CONFIG_DIR,
    data_dir=DATA_DIR,
    users_filename=USERS_FILENAME,
    datasets_filename=DATASETS_FILENAME,
    data_views_filename=DATA_VIEWS_FILENAME,
    data_view_history_filename=DATA_VIEW_HISTORY_FILENAME,
    tag_prefix=TAGS_PREFIX,
)


app = Flask(__name__, static_folder="static")
CORS(app)


def extract_payload() -> Optional[Dict]:
    json_payload = request.args.get(PAYLOAD_KEY)
    try:
        return json.loads(json_payload)
    except json.JSONDecodeError as exc:
        log.error("%s: could not decode payload: %s", exc, json_payload)
        return None


@app.route("/")
def index():
    return app.send_static_file(INDEX_FILENAME)


@app.route("/heartbeat")
def ping():
    return "heartbeat"


@app.route("/list_datasets")
def show_data_dir() -> str:
    """The list of viewable files in the data directory"""
    def filter_filenames(filenames: List[str]) -> List[str]:
        filtered_filenames = []
        for filename in filenames:
            if filename in IGNORE_FILENAMES:
                continue
            if filename.startswith("."):
                continue
            filtered_filenames.append(filename)
        return filtered_filenames

    return jsonify(
        filter_filenames(
            [p.name for p in sorted(Path(DATA_DIR).iterdir())]
        )
    )


@app.route("/most_recent_data_view", methods=["GET"])
def most_recent_data_view() -> str:
    key_user_id = "user_id"
    key_dataset_id = "dataset_id"
    payload = extract_payload()

    user_id_str = payload.get(key_user_id, None)

    if not user_id_str:
        return jsonify(
            dict(error=1, msg=f'user_id must be specified, found "{user_id_str}"')
        )

    user_id = UserId(user_id_str)
    dataset_id_str = payload.get(key_dataset_id, None)

    try:
        if not dataset_id_str:
            data_view = session.get_most_recent_data_view(user_id=user_id)
        else:
            data_view = session.get_most_recent_data_view(
                user_id=user_id,
                dataset_id=DatasetId(dataset_id_str)
            )

        return jsonify(dict(error=0, data_view=data_view.serialize()))
    except ValueError as exc:
        return jsonify(dict(error=1, data_view=None, msg=str(exc)))


@app.route("/list_users")
def list_users() -> str:
    """The list of Users"""
    return jsonify(
        [user.serialize() for user in session.user_handler.find()]
    )


@app.route("/show_datasets", methods=["GET"])
def show_datasets() -> str:
    """The list of Datasets"""
    match_string = request.args.get("match", "")

    return jsonify(
        [dataset.serialize() for dataset in session.dataset_handler.find(match_string)]
    )


@app.route("/set_most_recent_dataset", methods=["GET"])
def set_most_recent_dataset() -> str:
    key_user_id = "user_id"
    key_filename = "filename"

    payload = extract_payload()

    user_id_str = payload.get(key_user_id, None)
    filename = payload.get(key_filename, "").strip()

    if not user_id_str:
        return jsonify(dict(error=1, msg="no user_id specified"))
    if not filename:
        return jsonify(dict(error=2, msg="no filename specified"))

    user_id = UserId(user_id_str)

    try:
        dataset = session.set_most_recent_dataset(user_id, filename)
        return jsonify(dict(dataset=dataset.serialize(), user_id=user_id))
    except ValueError as exc:
        return jsonify(dict(error=3, msg=str(exc), dataset=None, user_id=user_id))


@app.route("/add_tags", methods=["GET"])
def add_tags() -> str:
    key_data_view_id = "data_view_id"
    key_primary_key = "primary_key"
    key_primary_key_name = "primary_key_name"
    key_tags = "tags"

    payload = extract_payload()

    data_view_id_str: str = payload.get(key_data_view_id, None)
    primary_key: str = payload.get(key_primary_key, None)
    primary_key_name: str = payload.get(key_primary_key_name, None)
    tags: List[str] = payload.get(key_tags, None)

    if not data_view_id_str:
        return jsonify(dict(error=1, msg="no data_view_id specified"))
    elif not primary_key:
        return jsonify(dict(error=2, msg="no primary_key specified"))
    elif not primary_key_name:
        return jsonify(dict(error=3, msg="no primary_key_name specified"))
    elif not tags:
        return jsonify(dict(error=4, msg="no tags specified"))

    data_view_id = DataViewId(data_view_id_str)

    try:
        updated_tags = session.add_tags(
            tags=tags,
            primary_keys=[primary_key],
            primary_key_name=primary_key_name,
            data_view_id=data_view_id,
        )
        return jsonify(dict(primary_key=primary_key, tags=list(updated_tags)))
    except Val