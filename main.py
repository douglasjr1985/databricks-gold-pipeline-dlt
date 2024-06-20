# main.py

import os
from dlt_pipeline.pipeline import DeltaLiveTablesPipeline


def main():
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    target = 'gold'
    base_path = os.path.abspath("projects")

    # Verificar cada pipe dentro da pasta projects
    for pipe in os.listdir(base_path):
        pipe_path = os.path.join(base_path, pipe)
        if os.path.isdir(pipe_path):
            name = f"DLT_{pipe}"

            # Obter caminhos absolutos dos arquivos SQL
            sql_paths = [os.path.abspath(os.path.join(pipe_path, file)) for file in os.listdir(pipe_path) if file.endswith(".sql")]

            # Instancia a classe
            dlt_pipeline = DeltaLiveTablesPipeline(token, host)

            # Criação do payload
            payload = dlt_pipeline.create_pipeline_payload(name, target, sql_paths)

            try:
                # Criação do pipeline
                create_response = dlt_pipeline.create_pipeline(payload)
                pipeline_id = create_response["pipeline_id"]
                print(f"Pipeline {name} criado com sucesso! ID: {pipeline_id}")

                # Início do pipeline
                start_response = dlt_pipeline.start_pipeline(pipeline_id)
                print(f"Pipeline {name} iniciado com sucesso! ID da execução: {start_response['update_id']}")

                # Verificação do status
                status_response = dlt_pipeline.get_pipeline_status(pipeline_id)
                print(f"Status do Pipeline {name}: {status_response['state']}")

            except Exception as e:
                print(f"Erro no pipeline {name}: {str(e)}")


if __name__ == "__main__":
    main()
