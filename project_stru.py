#!/usr/bin/env python3
"""
Generate a project structure file that's easy for LLMs to understand.
Usage: python generate_project_structure.py [project_directory] [output_file]
"""

import os
import sys
from pathlib import Path

# Common directories and files to ignore
IGNORE_PATTERNS = {
    '__pycache__', '.git', '.svn', '.hg', 'node_modules', '.venv', 'venv',
    '.env', '.idea', '.vscode', 'dist', 'build', '*.pyc', '.DS_Store',
    '.pytest_cache', '.mypy_cache', 'coverage', '.coverage', 'htmlcov',
    '*.egg-info', '.tox', 'target', 'bin', 'obj', 'packages', '.gradle'
}

def should_ignore(path: Path) -> bool:
    """Check if a path should be ignored."""
    name = path.name
    
    # Check exact matches
    if name in IGNORE_PATTERNS:
        return True
    
    # Check pattern matches (like *.pyc)
    for pattern in IGNORE_PATTERNS:
        if '*' in pattern:
            ext = pattern.replace('*', '')
            if name.endswith(ext):
                return True
    
    # Ignore hidden files/directories (except .gitignore, .env.example, etc.)
    if name.startswith('.') and name not in {'.gitignore', '.env.example', '.dockerignore'}:
        return True
    
    return False

def get_file_info(file_path: Path) -> str:
    """Get basic info about a file."""
    try:
        size = file_path.stat().st_size
        if size < 1024:
            size_str = f"{size}B"
        elif size < 1024 * 1024:
            size_str = f"{size/1024:.1f}KB"
        else:
            size_str = f"{size/(1024*1024):.1f}MB"
        return size_str
    except:
        return "?"

def generate_tree(directory: Path, prefix: str = "", output_lines: list = None, is_last: bool = True) -> list:
    """Generate a tree structure of the directory."""
    if output_lines is None:
        output_lines = []
    
    # Get all items in directory
    try:
        items = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        items = [item for item in items if not should_ignore(item)]
    except PermissionError:
        return output_lines
    
    for i, item in enumerate(items):
        is_last_item = (i == len(items) - 1)
        
        # Determine the tree characters
        if is_last_item:
            current_prefix = "└── "
            next_prefix = prefix + "    "
        else:
            current_prefix = "├── "
            next_prefix = prefix + "│   "
        
        # Format the item
        if item.is_dir():
            output_lines.append(f"{prefix}{current_prefix}{item.name}/")
            generate_tree(item, next_prefix, output_lines, is_last_item)
        else:
            size = get_file_info(item)
            output_lines.append(f"{prefix}{current_prefix}{item.name} ({size})")
    
    return output_lines

def generate_structure(project_dir: str, output_file: str):
    """Generate the complete project structure file."""
    project_path = Path(project_dir).resolve()
    
    if not project_path.exists():
        print(f"Error: Directory '{project_dir}' does not exist.")
        sys.exit(1)
    
    if not project_path.is_dir():
        print(f"Error: '{project_dir}' is not a directory.")
        sys.exit(1)
    
    # Generate the structure
    lines = [
        "=" * 80,
        f"PROJECT STRUCTURE: {project_path.name}",
        f"Location: {project_path}",
        "=" * 80,
        "",
        f"{project_path.name}/",
    ]
    
    tree_lines = generate_tree(project_path)
    lines.extend(tree_lines)
    
    lines.extend([
        "",
        "=" * 80,
        "END OF PROJECT STRUCTURE",
        "=" * 80,
    ])
    
    # Write to file
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✓ Project structure written to: {output_path.resolve()}")
    print(f"✓ Total items: {len(tree_lines)}")

def main():
    if len(sys.argv) < 2:
        project_dir = "."
    else:
        project_dir = sys.argv[1]
    
    if len(sys.argv) < 3:
        output_file = "project_structure.txt"
    else:
        output_file = sys.argv[2]
    
    generate_structure(project_dir, output_file)

if __name__ == "__main__":
    main()
