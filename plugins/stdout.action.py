import sys

def register_action(file_path, file_content, file_metadata, machine_metadata):
    print(f"New file detected: {file_path}", file=sys.stdout)