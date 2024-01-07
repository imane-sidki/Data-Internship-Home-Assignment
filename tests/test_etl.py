import pytest
from dags.etl import is_empty_json, clean_html_encoded_text

@pytest.mark.parametrize("input_data, expected_output", [
    ({}, True),  # Test for empty JSON
    ({"key": "value"}, False),  # Test for non-empty JSON
    ({"key1": None, "key2": {}}, True),  #Test for JSON with nested empty values
])
def test_is_empty_json(input_data, expected_output):
    assert is_empty_json(input_data) == expected_output

def test_clean_html_encoded_text():
    # Test HTML-encoded text
    encoded_text = "&lt;p&gt;This is a &quot;test&quot;&lt;/p&gt;"
    expected_output = "<p>This is a \"test\"</p>"
    assert clean_html_encoded_text(encoded_text) == expected_output

    # Test non-HTML-encoded text
    non_encoded_text = "This is a test."
    assert clean_html_encoded_text(non_encoded_text) == non_encoded_text

    # Test empty input
    assert clean_html_encoded_text("") == ""
