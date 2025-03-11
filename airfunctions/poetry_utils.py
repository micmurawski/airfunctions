from pathlib import Path

from poetry.core.pyproject.toml import PyProjectTOML


def get_lambda_build_config(project_path: Path | str):
    """
    Get the poetry-plugin-lambda-build configuration from pyproject.toml

    Args:
        project_path (str or Path): Path to the project directory containing pyproject.toml

    Returns:
        dict: The lambda build plugin configuration or None if not found
    """
    # Convert to Path object if string
    path = Path(project_path) if isinstance(
        project_path, str) else project_path

    # Get the pyproject.toml file path
    pyproject_path = path / "pyproject.toml"

    if not pyproject_path.exists():
        raise FileNotFoundError(f"No pyproject.toml found at {pyproject_path}")

    # Parse the pyproject.toml file
    pyproject = PyProjectTOML(pyproject_path)

    # Get the raw parsed data as a dictionary
    pyproject_data = pyproject.data

    # Extract the lambda build plugin configuration
    if "tool" in pyproject_data and "poetry-plugin-lambda-build" in pyproject_data["tool"]:
        return pyproject_data["tool"]["poetry-plugin-lambda-build"]

    return None


# Example usage
if __name__ == "__main__":
    project_path = Path(".")
    lambda_config = get_lambda_build_config(project_path)["function-artifact-path"] 
    print(lambda_config)