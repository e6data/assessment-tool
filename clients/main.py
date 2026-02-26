import logging
import importlib
import argparse
import os
import shutil
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def zip_output_directory(directory):
    zip_filename = f"{directory}.zip"
    shutil.make_archive(directory, 'zip', directory)
    logger.info(f"Directory {directory} compressed into {zip_filename}")
    shutil.rmtree(directory)


def dynamic_import(engine_type, extraction):
    logger.info(f"Attempting to import module for {engine_type} - {extraction}")
    try:
        module_name = f"{engine_type}.{engine_type}_{extraction}"
        module = importlib.import_module(module_name)
        logger.info(f"Successfully imported module: {module_name}")
        return module
    except ImportError as e:
        logger.error(f"Error importing module {engine_type}_{extraction}: {str(e)}")
        return None


def install_client_dependencies(engine_type):
    requirements_path = os.path.join(
        os.path.dirname(__file__),
        engine_type,
        "requirements.txt"
    )
    if not os.path.exists(requirements_path):
        logger.warning(f"No requirements file found for {engine_type}: {requirements_path}")
        return

    logger.info(f"Installing dependencies from {requirements_path}")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", requirements_path],
        check=True
    )


def extractor(engine_type, install_deps=False):
    logger.info(f"Starting extractor for engine: {engine_type}")
    if install_deps:
        install_client_dependencies(engine_type)

    output_dir = f"{engine_type}-logs"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Directory created : {output_dir}")
    module = dynamic_import(engine_type, 'metadata')
    if module and engine_type != 'athena':
        logger.info(f"Running {engine_type.capitalize()} Metadata extractor...")
        try:
            module.extract_metadata(output_dir)
        except Exception as e:
            logger.error(f"Error during metadata extraction: {str(e)}")
    module = dynamic_import(engine_type, 'querylogs')
    if module and engine_type != 'databricks':
        logger.info(f"Running {engine_type.capitalize()} Query Log extractor...")
        try:
            module.extract_query_logs(output_dir)
        except Exception as e:
            logger.error(f"Error during query log extraction: {str(e)}")
    else:
        logger.error(f"Module not found or failed to load for {engine_type} ")
    zip_output_directory(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('engine_type', type=str)
    parser.add_argument(
        '--install-deps',
        action='store_true',
        help='Install clients/<engine_type>/requirements.txt before extraction'
    )

    args = parser.parse_args()
    logger.info(f"Starting extractor script with client: {args.engine_type}")

    extractor(args.engine_type, install_deps=args.install_deps)
    logger.info("Extractor script completed.")
