from pathlib import Path

from poetry.core.pyproject.toml import PyProjectTOML


def get_lambda_build_config(project_path: Path | str):
    path = Path(project_path) if isinstance(project_path, str) else project_path
    pyproject_path = path / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(f"No pyproject.toml found at {pyproject_path}")
    pyproject = PyProjectTOML(pyproject_path)
    pyproject_data = pyproject.data
    if (
        "tool" in pyproject_data
        and "poetry-plugin-lambda-build" in pyproject_data["tool"]
    ):
        return pyproject_data["tool"]["poetry-plugin-lambda-build"]
    return None


if __name__ == "__main__":
    project_path = Path(".")
    lambda_config = get_lambda_build_config(project_path)["function-artifact-path"]
    print(lambda_config)
