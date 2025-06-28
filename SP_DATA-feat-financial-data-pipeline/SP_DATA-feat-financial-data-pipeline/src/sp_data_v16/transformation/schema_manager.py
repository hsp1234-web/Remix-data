import json
import pathlib

class SchemaManager:
    def __init__(self, schema_path: str):
        """
        Initializes the SchemaManager by loading schema definitions from a JSON file.

        Args:
            schema_path: Path to the JSON file containing schema definitions.
        """
        self.schema_path = pathlib.Path(schema_path)
        self.schemas = {}  # Initialize to empty dict

        try:
            with open(self.schema_path, 'r', encoding='utf-8') as f:
                self.schemas = json.load(f)
        except FileNotFoundError:
            # Log or handle as appropriate for the application
            # For now, self.schemas will remain an empty dict if file not found
            print(f"Warning: Schema file not found at {self.schema_path}")
        except json.JSONDecodeError as e:
            # Log or handle as appropriate
            print(f"Warning: Error decoding JSON from {self.schema_path}: {e}")
            # self.schemas will remain an empty dict or could be reset

    def identify_schema_from_content(self, raw_content: bytes) -> str | None:
        """
        Identifies a schema based on keywords found in the raw content.

        Args:
            raw_content: The raw byte content of a file.

        Returns:
            The name of the identified schema, or None if no schema matches.
        """
        decoded_content: str | None = None
        try:
            decoded_content = raw_content.decode('utf-8').lower()
        except UnicodeDecodeError:
            try:
                decoded_content = raw_content.decode('big5').lower()  # Fallback to BIG5
            except UnicodeDecodeError:
                # print(f"Warning: Could not decode content with utf-8 or big5.")
                return None

        if not self.schemas or not decoded_content:
            return None

        for schema_name, schema_definition in self.schemas.items():
            keywords = schema_definition.get("keywords", [])
            if not keywords:
                continue

            lower_keywords = [kw.lower() for kw in keywords]

            if any(kw in decoded_content for kw in lower_keywords):
                return schema_name  # Match found

        return None  # No schema matched
