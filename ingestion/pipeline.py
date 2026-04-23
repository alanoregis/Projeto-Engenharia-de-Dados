"""
Pipeline dlt: FreeCurrency API → MinIO (filesystem S3-compatible, Parquet).

Particionamento: currency-raw/latest/{date}/{hour}/
O dlt filesystem destination escreve Parquet nativamente quando
pyarrow está instalado.

Credenciais MinIO via variáveis de ambiente:
  DESTINATION__FILESYSTEM__BUCKET_URL
  DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID
  DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY
  DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL
"""

import logging
import os

import dlt
from dlt.destinations import filesystem
from ingestion.source import freecurrency_source

logger = logging.getLogger(__name__)


def build_pipeline() -> dlt.Pipeline:
    """Constrói e retorna o pipeline dlt configurado para o MinIO."""
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]

    destination = filesystem(
        bucket_url=bucket_url,
        credentials={
            "aws_access_key_id": os.environ[
                "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"
            ],
            "aws_secret_access_key": os.environ[
                "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"
            ],
            "endpoint_url": os.environ[
                "DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"
            ],
        },
    )

    pipeline = dlt.pipeline(
        pipeline_name="freecurrency_pipeline",
        destination=destination,
        dataset_name="currency-raw",  # subpasta dentro do bucket
    )

    return pipeline


def run_pipeline() -> dict:
    """
    Executa o pipeline e retorna um resumo com métricas básicas.

    Returns:
        dict com pipeline_name, rows_loaded e load_id.
    """
    pipeline = build_pipeline()

    source = freecurrency_source()

    logger.info("Iniciando ingestão FreeCurrency → MinIO")
    load_info = pipeline.run(
        source,
        loader_file_format="parquet",
    )

    rows_loaded = sum(
        p.jobs["completed_jobs"].__len__()
        for p in load_info.load_packages
        if p.jobs.get("completed_jobs")
    )

    logger.info(
        "Ingestão concluída. load_id=%s | load_packages=%d",
        load_info.loads_ids[0] if load_info.loads_ids else None,
        len(load_info.load_packages),
    )

    return {
        "pipeline_name": pipeline.pipeline_name,
        "load_id": load_info.loads_ids[0] if load_info.loads_ids else None,
        "rows_loaded": rows_loaded,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_pipeline()
    print(result)