import os
from dlt_pipeline.pipeline import DeltaLiveTablesPipeline

def main():
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    target = 'gold'
    repo_base_path = "/Repos/Development/databricks-gold-pipeline-dlt/projects"

    dlt_pipeline = DeltaLiveTablesPipeline(token, host)
    
    # Verificar cada pipe dentro da pasta projects no repositório do Databricks
    pipes = dlt_pipeline.list_repo_files(repo_base_path)
    for pipe in pipes:
        if pipe['object_type'] == 'DIRECTORY':
            pipe_path = pipe['path']
            name = f"DLT_{pipe['path'].split('/')[-1]}"

            # Obter caminhos dos arquivos SQL no repositório do Databricks
            sql_files = dlt_pipeline.list_repo_files(pipe_path)
            sql_paths = [f"dbfs:{file['path']}" for file in sql_files if file['object_type'] == 'NOTEBOOK' and file['path'].endswith(".sql")]

            print(f"Pipeline {name}: Encontrou arquivos SQL: {sql_paths}")

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
