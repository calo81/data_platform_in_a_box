import avro.schema
import avro.io
import avro.datafile
import json
import io

def parse_avro_file(avro_file_path):
    """
    Parses an Avro file to extract schema and data.

    :param avro_file_path: Path to the Avro file
    :return: Parsed schema and data records
    """
    try:
        with open(avro_file_path, 'rb') as file:
            reader = avro.datafile.DataFileReader(file, avro.io.DatumReader())
            schema = reader.meta['avro.schema'].decode('utf-8')
            records = [record for record in reader]
            reader.close()
        return avro.schema.parse(schema), records
    except Exception as e:
        raise ValueError(f"Error parsing Avro file: {e}")

def validate_avro_data(schema, data):
    """
    Validates Avro data against a schema.

    :param schema: Parsed Avro schema
    :param data: Avro data to validate (as a dictionary)
    :return: True if valid, raises an exception if invalid
    """
    try:
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return True
    except Exception as e:
        raise ValueError(f"Invalid Avro data: {e}")

def generate_json_schema(avro_schema):
    """
    Generates a JSON schema from an Avro schema.

    :param avro_schema: Parsed Avro schema
    :return: JSON schema as a dictionary
    """
    try:
        return json.loads(json.dumps(avro_schema.to_json()))
    except Exception as e:
        raise ValueError(f"Error generating JSON schema: {e}")

def generate_json_data(avro_schema):
    """
    Generates example JSON data based on an Avro schema.

    :param avro_schema: Parsed Avro schema
    :return: Example JSON data as a dictionary
    """
    def generate_example(field):
        field_type = field.type

        if isinstance(field_type, avro.schema.UnionSchema):
            field_type = field_type.schemas[0]  # Assume the first type in a union schema

        if isinstance(field_type, avro.schema.PrimitiveSchema):
            if field_type.type == 'string':
                return "example_string"
            elif field_type.type == 'int':
                return 0
            elif field_type.type == 'long':
                return 0
            elif field_type.type == 'float' or field_type.type == 'double':
                return 0.0
            elif field_type.type == 'boolean':
                return True
            elif field_type.type == 'bytes':
                return "example_bytes".encode("utf-8")  # Encode bytes to make them JSON serializable
        elif isinstance(field_type, avro.schema.RecordSchema):
            return {f.name: generate_example(f) for f in field_type.fields}
        elif isinstance(field_type, avro.schema.ArraySchema):
            return [generate_example(field_type.items)]
        elif isinstance(field_type, avro.schema.MapSchema):
            return {"example_key": generate_example(field_type.values)}
        return None

    if not isinstance(avro_schema, avro.schema.RecordSchema):
        raise ValueError("Only RecordSchema types are supported for generating JSON data.")

    return {field.name: generate_example(field) for field in avro_schema.fields}

# Example Usage
if __name__ == "__main__":
    avro_file_path = "/Users/cscarioni/Downloads/6b654563-5e94-461a-97e7-81bf1530379d-m0.avro"  # Replace with your Avro file path

    try:
        # Parse the Avro file
        schema, records = parse_avro_file(avro_file_path)

        # Generate JSON schema
        json_schema = generate_json_schema(schema)
        print("Generated JSON Schema:")
        print(json.dumps(json_schema, indent=4))

        # Generate example JSON data
        if records:
            json_data = records[0]  # Use the first record as example
            print("\nGenerated JSON Data:")
            print(json.dumps(json_data, indent=4))

            # Validate example JSON data
            validate_avro_data(schema, json_data)
            print("\nExample JSON data is valid.")
        else:
            print("\nNo records found in the Avro file.")

    except Exception as e:
        print(f"Error: {e}")
