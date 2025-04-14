import re

def snakecase(string: str) -> str:
    """Convert string into snake case.

    Args:
        string: String to convert.

    Returns:
        string: Snake cased string.
    """

    # Step 1: Replace separators (-, ., space) with underscores
    string = re.sub(r"[\-\.\s]", "_", string)

    # Step 2: Put underscores:
    #   - Between lowercase/number and an uppercase (e.g., userID → user_ID)
    string = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", string)

    #   - Between capital acronyms and a new capital followed by lowercase (e.g., JSONData → JSON_Data)
    string = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", string)

    # Step 3: Lowercase everything
    string = string.lower()

    # Step 4: Collapse multiple underscores and clean up edges
    return re.sub(r"_{2,}", "_", string).strip("_")