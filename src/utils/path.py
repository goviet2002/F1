import os

def get_project_root(marker="data"):
    current = os.path.abspath(os.getcwd())
    while True:
        if os.path.exists(os.path.join(current, marker)):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            raise RuntimeError("Project root not found!")
        current = parent

print(get_project_root("test"))