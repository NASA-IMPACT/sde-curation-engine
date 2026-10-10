"""Suggest metadata never leaves a title blank: an answer with an empty title is a failed call, so the
row records the error and the next Suggest metadata asks again (tests/integration/test_db_contract.py
checks that a row with an error is counted as missing). Replaces
test_an_empty_title_is_a_failed_call_and_is_asked_again of the old
tests/integration/test_metadata_completeness.py."""

import pytest

from sde_curation.config import Settings
from sde_curation.llm.base import LLMError
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import suggest_metadata_one
from sde_curation.models import Confidence, Division, DocumentType, MetadataSuggestion

SETTINGS = Settings(llm_provider="fake", data_dir="/tmp/unused")
DOC = {"url": "https://ex.org/p3", "title": "Page 3", "text": "page text", "content_hash": "h"}
BLANK_TITLE = "  "  # only whitespace: no title at all


async def test_an_answer_with_a_blank_title_is_a_failed_call():
    answer = MetadataSuggestion(title=BLANK_TITLE, title_confidence=Confidence.HIGH,
                                division=Division.HELIOPHYSICS, division_confidence=Confidence.LOW,
                                document_type=DocumentType.DATA, document_type_confidence=Confidence.LOW)
    fake = FakeProvider(canned=answer.model_dump(mode="json"))

    with pytest.raises(LLMError, match="^the model returned an empty title$"):
        await suggest_metadata_one(fake, DOC, settings=SETTINGS)
