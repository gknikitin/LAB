import re
from typing import Union

import requests
from loguru import logger

from tools import prepare_text_for_html, prepare_text_for_reposts, add_urls_to_text
from api_requests import get_video_url
from config import VK_TOKEN, REQ_VERSION

"""Сбор постов"""

def parse_post(
    item: dict, repost_exists: bool, item_type: str, group_name: str
) -> dict:
    text = prepare_text_for_html(item["text"])
    if repost_exists:
        text = prepare_text_for_reposts(text, item, item_type, group_name)

    urls: list = []
    videos: list = []
    photos: list = []
    docs: list = []

    if "attachments" in item:
        parse_attachments(item["attachments"], text, urls, videos, photos, docs)

    text = add_urls_to_text(text, urls, videos)
    logger.info(f"{item_type.capitalize()} parsing is complete.")
    return {"text": text, "photos": photos, "docs": docs}


def parse_attachments(attachments, text, urls, videos, photos, docs):
    for attachment in attachments:
        if attachment["type"] == "link":
            url = get_url(attachment, text)
            if url:
                urls.append(url)
        elif attachment["type"] == "video":
            video = get_video(attachment)
            if video:
                videos.append(video)
        elif attachment["type"] == "photo":
            photo = get_photo(attachment)
            if photo:
                photos.append(photo)
        elif attachment["type"] == "doc":
            doc = get_doc(attachment["doc"])
            if doc:
                docs.append(doc)


def get_url(attachment: dict, text: str) -> Union[str, None]:
    url = attachment["link"]["url"]
    return url if url not in text else None


def get_video(attachment: dict) -> str:
    owner_id = attachment["video"]["owner_id"]
    video_id = attachment["video"]["id"]
    access_key = attachment["video"]["access_key"]

    video = get_video_url(VK_TOKEN, REQ_VERSION, owner_id, video_id, access_key)
    return video if video else f"https://vk.com/video{owner_id}_{video_id}"


def get_photo(attachment: dict) -> Union[str, None]:
    sizes = attachment["photo"]["sizes"]
    types = ["w", "z", "y", "x", "r", "q", "p", "o", "m", "s"]

    for type_ in types:
        if next(
            (item for item in sizes if item["type"] == type_),
            False,
        ):
            return re.sub(
                "&([a-zA-Z]+(_[a-zA-Z]+)+)=([a-zA-Z0-9-_]+)",
                "",
                next(
                    (item for item in sizes if item["type"] == type_),
                    False,
                )["url"],
            )
    else:
        return None


def get_doc(doc: dict) -> Union[dict, None]:
    if doc["size"] > 50000000:
        logger.info(
            "The document was skipped due to its size exceeding the "
            f"50MB limit: {doc['size']=}."
        )
        return None
    else:
        response = requests.get(doc["url"])

        with open(f'./temp/{doc["title"]}', "wb") as file:
            file.write(response.content)

    return {"title": doc["title"], "url": doc["url"]}
