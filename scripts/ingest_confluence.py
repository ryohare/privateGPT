#!/usr/bin/env python3

import argparse
import logging
import hashlib

from langchain_community.document_loaders import ConfluenceLoader

from private_gpt.di import global_injector
from private_gpt.server.ingest.ingest_service import IngestService


logger = logging.getLogger(__name__)


class LocalIngestWorker:
    """
    Ingests Confluence spaces into the LangChain platform.

    Args:
        ingest_service (IngestService): The ingestion service to use.
        url (str): The URL of the Confluence instance.
        username (str): The username to use to authenticate with Confluence.
        api_key (str): The API key to use to authenticate with Confluence.

    Attributes:
        ingest_service (IngestService): The ingestion service to use.
        confluence_loader (ConfluenceLoader): The Confluence loader to use.
        total_documents (int): The total number of documents ingested.
        current_document_count (int): The current number of documents ingested.
    """
    def __init__(
        self,
        local_ingest_service: IngestService,
        url: str,
        username: str,
        api_key: str,
    ) -> None:
        self.ingest_service = local_ingest_service

        self.confluence_loader = ConfluenceLoader(
            url=url,
            username=username,
            api_key=api_key,
        )

        self.total_documents = 0
        self.current_document_count = 0

    def ingest_space(self, space: str) -> None:
        """
        Ingests all the documents from a Confluence space

        Args:
            space (str): The Confluence space key.

        Returns:
            None

        """
        documents = self.confluence_loader.load(
            space_key=space,
            include_attachments=True,
            limit=1000,
            max_pages=10000000,
        )

        # send to the ingestion service
        for d in documents:
            # hash function for generating unique doc_ids
            sha1 = hashlib.sha1()
            sha1.update(d.page_content.encode("utf-8"))
            self.ingest_service.ingest_text(sha1.hexdigest(), d.page_content)


parser = argparse.ArgumentParser(prog="ingest_confluence.py")
parser.add_argument(
    "--log-file",
    help="Optional path to a log file. If provided, logs will be written to this file.",
    type=str,
    default=None,
)
parser.add_argument(
    "--confluence-url",
    help="Confluence URL",
    type=str,
    required=True,
)
parser.add_argument(
    "--confluence-username",
    help="Confluence username",
    type=str,
    required=True,
)
parser.add_argument(
    "--confluence-apikey",
    help="Confluence API Key",
    type=str,
    required=True,
)
parser.add_argument(
    "--confluence-space",
    help="Confluence space",
    type=str,
    required=True,
)

args = parser.parse_args()

# Set up logging to a file if a path is provided
if args.log_file:
    file_handler = logging.FileHandler(args.log_file, mode="a")
    file_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(file_handler)

if __name__ == "__main__":

    ingest_service = global_injector.get(IngestService)
    worker = LocalIngestWorker(
        ingest_service,
        args.confluence_url,
        args.confluence_username,
        args.confluence_apikey,
    )
    worker.ingest_space(args.confluence_space)
