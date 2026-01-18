import platform
import re
import subprocess
import shutil
import logging
from pathlib import Path

# Import the standard backend to wrap it
from setuptools.build_meta import *  # noqa: F403
from setuptools.build_meta import build_wheel as _orig_build_wheel
from setuptools.build_meta import build_sdist as _orig_build_sdist
from setuptools.build_meta import build_editable as _orig_build_editable

# --- Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("gatun.backend")

# --- Config ---
PROJECT_ROOT = Path(__file__).parent.resolve()
# Use gradlew.bat on Windows, gradlew on Unix
if platform.system() == "Windows":
    GRADLEW = PROJECT_ROOT / "gradlew.bat"
else:
    GRADLEW = PROJECT_ROOT / "gradlew"
JAVA_SRC_DIR = PROJECT_ROOT / "gatun-core" / "src"
BUILD_SCRIPT = PROJECT_ROOT / "gatun-core" / "build.gradle.kts"

# FlatBuffers schema and output directories
SCHEMA_FILE = PROJECT_ROOT / "schemas" / "commands.fbs"
JAVA_GEN_DIR = PROJECT_ROOT / "gatun-core" / "src" / "main" / "java"
PYTHON_GEN_DIR = PROJECT_ROOT / "src" / "gatun" / "generated"

# Location of the built 'Fat JAR' from Gradle
JAR_SOURCE = PROJECT_ROOT / "gatun-core" / "build" / "libs" / "gatun-server-all.jar"
JAR_DEST_DIR = PROJECT_ROOT / "src" / "gatun" / "jars"
JAR_DEST_FILE = JAR_DEST_DIR / "gatun-server-all.jar"


def _generate_flatbuffers():
    """Generate FlatBuffers code for Java and Python from the schema."""
    if not SCHEMA_FILE.exists():
        raise RuntimeError(f"FlatBuffers schema not found at {SCHEMA_FILE}")

    # Check if flatc is available
    try:
        subprocess.run(["flatc", "--version"], check=True, capture_output=True)
    except FileNotFoundError:
        raise RuntimeError(
            "flatc not found. Install FlatBuffers compiler: "
            "brew install flatbuffers (macOS) or apt install flatbuffers-compiler (Linux)"
        )

    logger.info("--- [Gatun Backend] Generating FlatBuffers code ---")

    # Generate Java code
    JAVA_GEN_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        ["flatc", "--java", "-o", str(JAVA_GEN_DIR), str(SCHEMA_FILE)]
    )

    # Generate Python code
    PYTHON_GEN_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        ["flatc", "--python", "-o", str(PYTHON_GEN_DIR), str(SCHEMA_FILE)]
    )

    # Post-process Java files: remove version check that depends on flatc version
    _patch_java_version_checks()

    # Post-process Python files: fix import paths for nested tables
    _patch_python_imports()

    logger.info("--- [Gatun Backend] FlatBuffers generation complete ---")


def _patch_python_imports():
    """Patch import paths in generated Python files.

    The flatc compiler generates imports like:
        from org.gatun.protocol.Argument import Argument

    But our package structure requires:
        from gatun.generated.org.gatun.protocol.Argument import Argument
    """
    python_protocol_dir = PYTHON_GEN_DIR / "org" / "gatun" / "protocol"
    if not python_protocol_dir.exists():
        return

    import_pattern = re.compile(r"from org\.gatun\.protocol\.(\w+) import (\w+)")
    replacement = r"from gatun.generated.org.gatun.protocol.\1 import \2"

    for py_file in python_protocol_dir.glob("*.py"):
        content = py_file.read_text()
        new_content = import_pattern.sub(replacement, content)
        if new_content != content:
            py_file.write_text(new_content)
            logger.info(f"  Patched imports in {py_file.name}")


# Must match flatbuffers-java version in gatun-core/build.gradle.kts
FLATBUFFERS_RUNTIME_VERSION = "25_2_10"


def _patch_java_version_checks():
    """Patch ValidateVersion calls in generated Java files to match runtime version.

    The flatc compiler generates version checks like:
        public static void ValidateVersion() { Constants.FLATBUFFERS_XX_X_X(); }

    This causes issues when the flatc version doesn't match the runtime library version.
    We replace the version with the one from our Gradle dependency.
    """
    java_protocol_dir = JAVA_GEN_DIR / "org" / "gatun" / "protocol"
    if not java_protocol_dir.exists():
        return

    version_pattern = re.compile(r"Constants\.FLATBUFFERS_\d+_\d+_\d+\(\)")
    replacement = f"Constants.FLATBUFFERS_{FLATBUFFERS_RUNTIME_VERSION}()"

    for java_file in java_protocol_dir.glob("*.java"):
        content = java_file.read_text()
        new_content = version_pattern.sub(replacement, content)
        if new_content != content:
            java_file.write_text(new_content)
            logger.info(f"  Patched version check in {java_file.name}")


def _is_flatbuffers_generation_needed():
    """Returns True if schema is newer than generated files."""
    if not SCHEMA_FILE.exists():
        return False

    schema_mtime = SCHEMA_FILE.stat().st_mtime

    # Check if Java generated files exist
    java_protocol_dir = JAVA_GEN_DIR / "org" / "gatun" / "protocol"
    if not java_protocol_dir.exists() or not any(java_protocol_dir.glob("*.java")):
        logger.info(
            "[Gatun Backend] Java generated files missing. Generation required."
        )
        return True

    # Check if Python generated files exist
    if not PYTHON_GEN_DIR.exists() or not any(PYTHON_GEN_DIR.rglob("*.py")):
        logger.info(
            "[Gatun Backend] Python generated files missing. Generation required."
        )
        return True

    # Check if schema is newer than generated files
    java_mtime = _get_newest_mtime(java_protocol_dir)
    python_mtime = _get_newest_mtime(PYTHON_GEN_DIR)
    oldest_gen = min(java_mtime, python_mtime)

    if schema_mtime > oldest_gen:
        logger.info("[Gatun Backend] Schema changed. Generation required.")
        return True

    return False


def _get_newest_mtime(directory):
    """Recursively finds the newest modification time in a directory."""
    newest = 0
    for path in directory.rglob("*"):
        if path.is_file():
            mtime = path.stat().st_mtime
            if mtime > newest:
                newest = mtime
    return newest


def _is_build_needed():
    """Returns True if Java sources are newer than the destination JAR."""
    # 1. If destination JAR is missing, we must build
    if not JAR_DEST_FILE.exists():
        logger.info("[Gatun Backend] JAR missing. Build required.")
        return True

    # 2. Get JAR timestamp
    jar_mtime = JAR_DEST_FILE.stat().st_mtime

    # 3. Check Build Script (if gradle file changed, always rebuild)
    if BUILD_SCRIPT.exists() and BUILD_SCRIPT.stat().st_mtime > jar_mtime:
        logger.info("[Gatun Backend] build.gradle.kts changed. Build required.")
        return True

    # 4. Check Source Code (deep scan)
    src_mtime = _get_newest_mtime(JAVA_SRC_DIR)

    if src_mtime > jar_mtime:
        logger.info("[Gatun Backend] Java sources changed. Build required.")
        return True

    return False


def _build_java():
    """Compiles Java (only if needed) and ensures the Artifact is present."""

    # Generate FlatBuffers code if needed (before checking Java build)
    if _is_flatbuffers_generation_needed():
        _generate_flatbuffers()

    if _is_build_needed():
        logger.info("--- [Gatun Backend] Compiling Java Server ---")

        if not GRADLEW.exists():
            raise RuntimeError(f"Gradle wrapper not found at {GRADLEW}")

        # 1. Build Fat JAR
        cmd = [str(GRADLEW), ":gatun-core:shadowJar"]
        try:
            subprocess.check_call(cmd, cwd=PROJECT_ROOT)
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Gradle build failed.") from e

        # 2. Copy Artifact (Refresh the dest)
        if not JAR_SOURCE.exists():
            raise RuntimeError(f"Build passed but JAR missing: {JAR_SOURCE}")

        JAR_DEST_DIR.mkdir(parents=True, exist_ok=True)

        if JAR_DEST_FILE.exists():
            JAR_DEST_FILE.unlink()  # Clean replace

        shutil.copy2(JAR_SOURCE, JAR_DEST_FILE)
        logger.info(f"--- [Gatun Backend] Updated JAR at {JAR_DEST_FILE} ---")

    else:
        logger.info("--- [Gatun Backend] Java is up-to-date. Skipping build. ---")


# --- PEP 517 Hooks Overrides ---


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _build_java()
    return _orig_build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    _build_java()
    return _orig_build_sdist(sdist_directory, config_settings)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _build_java()
    return _orig_build_editable(wheel_directory, config_settings, metadata_directory)
