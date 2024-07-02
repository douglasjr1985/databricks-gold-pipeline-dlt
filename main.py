import os
import json
from dlt_pipeline.pipeline import DeltaLiveTablesPipeline

def load_pipeline_params(pipe_path):
    """
    Carrega os parâmetros do pipeline a partir do arquivo pipeline_params.json.

    Args:
        pipe_path (str): Caminho da pasta do pipeline.

    Returns:
        dict: Parâmetros do pipeline.
    """
    params_path = os.path.join(pipe_path, 'pipeline_params.json')
    if os.path.exists(params_path):
        with open(params_path, 'r') as f:
            return json.load(f)
    else:
        raise FileNotFoundError(f"Arquivo de parâmetros não encontrado: {params_path}")

def main():
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    repo_base_path = "/Repos/Development/databricks-gold-pipeline-dlt/projects"

    dlt_pipeline = DeltaLiveTablesPipeline(token, host)
    
    # Verificar cada pipeline dentro da pasta projects no repositório do Databricks
    pipes = dlt_pipeline.list_repo_files(repo_base_path)
    for pipe in pipes:
        if pipe['object_type'] == 'DIRECTORY':
            pipe_path = pipe['path']
            name = f"DLT_{pipe['path'].split('/')[-1]}"

            # Obter caminhos dos arquivos SQL no repositório do Databricks
            sql_files = dlt_pipeline.list_repo_files(pipe_path)
            sql_paths = [file['path'] for file in sql_files if file['path'].endswith(".sql")]

            print(f"Pipeline {name}: Encontrou arquivos SQL: {sql_paths}")

            if not sql_paths:
                print(f"Nenhum arquivo SQL encontrado para o pipeline {name}.")
                continue

            try:
                # Carregar os parâmetros do pipeline
                params = load_pipeline_params(pipe_path)
                target = params.get("target", "default")
                catalog = params.get("catalog", "datalake_hml")
                num_workers = params.get("num_workers", 1)

                # Criação do payload
                payload = dlt_pipeline.create_pipeline_payload(
                    name, 
                    target, 
                    sql_paths, 
                    num_workers=num_workers, 
                    catalog=catalog
                )

                try:
                    # Tentar obter o status do pipeline para verificar se ele existe
                    try:
                        status_response = dlt_pipeline.get_pipeline_status(name)
                        pipeline_id = status_response["pipeline_id"]
                        print(f"Pipeline {name} já existe com ID: {pipeline_id}")

                        # Atualização do pipeline
                        update_response = dlt_pipeline.update_pipeline(pipeline_id, payload)
                        print(f"Pipeline {name} atualizado com sucesso! ID: {pipeline_id}")

                    except Exception as e:
                        # Se o pipeline não existir, criar um novo
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
                    error_message = str(e)
                    if "RESOURCE_CONFLICT" in error_message:
                        # Pipeline já existe, obter ID do pipeline existente
                        existing_pipelines = dlt_pipeline.list_pipelines()
                        pipeline_id = None
                        for pipeline in existing_pipelines:
                            if pipeline['name'] == name:
                                pipeline_id = pipeline['pipeline_id']
                                break

                        if pipeline_id:
                            # Atualizar pipeline existente
                            update_response = dlt_pipeline.update_pipeline(pipeline_id, payload)
                            print(f"Pipeline {name} atualizado com sucesso! ID: {pipeline_id}")

                            # Início do pipeline
                            start_response = dlt_pipeline.start_pipeline(pipeline_id)
                            print(f"Pipeline {name} iniciado com sucesso! ID da execução: {start_response['update_id']}")

                            # Verificação do status
                            status_response = dlt_pipeline.get_pipeline_status(pipeline_id)
                            print(f"Status do Pipeline {name}: {status_response['state']}")
                        else:
                            print(f"Erro ao encontrar o pipeline existente para {name}")
                    else:
                        print(f"Erro no pipeline {name}: {error_message}")

            except FileNotFoundError as e:
                print(e)

if __name__ == "__main__":
    main()
