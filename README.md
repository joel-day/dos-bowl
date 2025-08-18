# Custom Monorepo Template

This template builds a mono-repo with its own virtual environment. It configures tools to ensure the code follows PEP8 guidelines and runs on Linux, Windows, and Mac - as well as Python versions 3.10, 3.11, & 3.12. 

## Installation

 On Github, manually create a new repository 'new-repo-name'

```bash
# Clone the repository
git clone https://github.com/joel-day/custom-monorepo-template.git

# Move into the local repository
cd custom-monorepo-template

# Remove git's connection and all commit history ect from the original repository
Remove-Item -Recurse -Force .git

# Manually create new repository in Github named "new-project-repo" with no README, and rename the locally cloned template to match the name
cd ..
mv custom-monorepo-template new-repo-name
cd new-repo-name

# Push template into the new repo
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/your-username/new-repo-name.git
git push -u origin main

# (OPTIONAL) Ensure you are connected to the new repository
git remote -v
```

## Setup Virtual Environment and Dependencies

```bash
# Create the virtual environment
uv venv .venv

# Activate the virtual environment
source .venv\bin\activate # Mac/Linux
.venv\Scripts\activate   # Windows

# Sync environment based on dependencies in top-level pyproject.toml file
uv sync

# (OPTIONAL) Sync environment based on dependencies across all packages' pyproject.toml files
uv sync --all-packages
```

## Included Tools & Packages

- **UV**: Used for package management and virtual environment creation. Configured to manage environments in a monorepo setup, ensuring consistency across the project.

- **GitHub Actions**: Ensures that the code works across multiple operating systems (Linux, Mac, and Windows) and supports Python versions 3.10, 3.11, and 3.12. It's a part the CI pipeline and is configured to run on pull requests to main.

- **Pytest**: Configured to run tests and verify the correctness of code execution. It ensures that the codebase remains functional and that new changes don’t introduce unexpected behavior.
```bash
pytest
```
- **Flake8**: Used for checking code compliance with PEP8 standards. It helps maintain a clean and consistent code style across the project by enforcing formatting and style guidelines.
```bash
flake8 .
```

