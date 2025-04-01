import os
import sys
import json
import socket
import argparse
import importlib.util
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import re

def load_plugins(plugin_dir):
    plugins = []
    extra_arg_parsers = []
    
    if not os.path.exists(plugin_dir):
        return plugins, extra_arg_parsers
    
    for filename in os.listdir(plugin_dir):
        if filename.endswith(".py"):
            module_name = filename[:-3]
            module_path = os.path.join(plugin_dir, filename)
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            if hasattr(module, "register_action"):
                plugins.append(module.register_action)
            
            if hasattr(module, "register_arguments"):
                extra_arg_parsers.append(module.register_arguments)
    
    return plugins, extra_arg_parsers

class FileWatcher(FileSystemEventHandler):
    def __init__(self, directory, pattern, actions):
        self.directory = directory
        self.pattern = re.compile(pattern)
        self.actions = actions

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        file_name = os.path.basename(file_path)
        
        if not self.pattern.match(file_name):
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            file_metadata = os.stat(file_path)
            machine_metadata = {
                "hostname": socket.gethostname(),
                "ip": socket.gethostbyname(socket.gethostname())
            }

            for action in self.actions:
                action(file_path, file_content, file_metadata, machine_metadata)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Watch a directory for new files and process them.")
    parser.add_argument("--directory", type=str, required=True, help="Directory to watch")
    parser.add_argument("--pattern", type=str, default=r".*$", help="Regex pattern for matching files")
    
    plugins, extra_arg_parsers = load_plugins("plugins")
    
    for arg_parser in extra_arg_parsers:
        arg_parser(parser)
    
    args = parser.parse_args()

    directory = args.directory
    pattern = args.pattern
    actions = plugins  # Plugins handle all actions now

    # Add this line to print the pattern
    print(f"Received pattern from command line: {pattern}")
    if not os.path.exists(directory):
        os.makedirs(directory)

    event_handler = FileWatcher(directory, pattern, actions)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    print(f"Watching directory: {directory}", file=sys.stdout)
    try:
        while True:
            pass  # Keep running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
