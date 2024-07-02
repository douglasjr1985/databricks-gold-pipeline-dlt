import requests
import json

class DeltaLiveTablesPipeline:
    def __init__(self, token, host):
        """
        Inicializa a classe com token de autenticação e URL da instância Databricks.

        Args:
            token (str): Token de acesso para autenticação.
            host (str): URL do host Databricks.
        """
        self.token = token
        self.host = host
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def create_pipeline_payload(self, name, target, sql_paths, num_workers=2, trigger_interval="1 hour", catalog="risk"):
        """
        Cria o payload JSON para a criação de um pipeline Delta Live Tables.

        Args:
            name (str): Nome do pipeline.
            target (str): Nome do alvo (target) do Delta Live Tables.
            sql_paths (list): Lista de caminhos dos arquivos SQL.
            num_workers (int): Número de trabalhadores no cluster.
            trigger_interval (str): Intervalo de gatilho para execução do pipeline.
            catalog (str): Nome do catálogo Unity Catalog a ser usado.

        Returns:
            dict: Payload JSON.
        """
        libraries = [{"file": {"path": path}} for path in sql_paths]
        payload = {
            "name": name,
            "catalog": catalog,
            "target": target,
            "libraries": libraries,
            "clusters": [
                {
                    "label": "default",
                    "aws_attributes": {
                        "instance_profile_arn": "arn:aws:iam::463684499885:instance-profile/DatabricksGlueHML"
                    },
                    "node_type_id": "m5d.xlarge",
                    "driver_node_type_id": "m5d.xlarge",
                    "num_workers": max(num_workers, 1)  # Garantir pelo menos 1 trabalhador
                },
                {
                    "label": "maintenance",
                    "aws_attributes": {
                        "instance_profile_arn": "arn:aws:iam::463684499885:instance-profile/DatabricksGlueHML"
                    }
                }
            ],
            "configuration": {
                "pipelines.trigger.interval": trigger_interval
            }
        }
        return payload

    def create_pipeline(self, payload):
        """
        Cria um pipeline Delta Live Tables no Databricks.

        Args:
            payload (dict): Payload JSON para a criação do pipeline.

        Returns:
            dict: Resposta da API.
        """
        url = f"{self.host}/api/2.0/pipelines"
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao criar o pipeline: {response.text}")

    def update_pipeline(self, pipeline_id, payload):
        """
        Atualiza um pipeline Delta Live Tables no Databricks.

        Args:
            pipeline_id (str): ID do pipeline a ser atualizado.
            payload (dict): Payload JSON com as atualizações do pipeline.

        Returns:
            dict: Resposta da API.
        """
        url = f"{self.host}/api/2.0/pipelines/{pipeline_id}"
        response = requests.put(url, headers=self.headers, data=json.dumps(payload))

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao atualizar o pipeline: {response.text}")

    def start_pipeline(self, pipeline_id):
        """
        Inicia a execução de um pipeline Delta Live Tables no Databricks.

        Args:
            pipeline_id (str): ID do pipeline a ser iniciado.

        Returns:
            dict: Resposta da API.
        """
        url = f"{self.host}/api/2.0/pipelines/{pipeline_id}/updates"
        response = requests.post(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao iniciar o pipeline: {response.text}")

    def get_pipeline_status(self, pipeline_id):
        """
        Obtém o status de um pipeline Delta Live Tables no Databricks.

        Args:
            pipeline_id (str): ID do pipeline.

        Returns:
            dict: Resposta da API.
        """
        url = f"{self.host}/api/2.0/pipelines/{pipeline_id}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao obter o status do pipeline: {response.text}")

    def list_pipelines(self):
        """
        Lista todos os pipelines Delta Live Tables no Databricks.

        Returns:
            list: Lista de pipelines.
        """
        url = f"{self.host}/api/2.0/pipelines"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json().get("statuses", [])
        else:
            raise Exception(f"Erro ao listar os pipelines: {response.text}")

    def list_repo_files(self, path):
        """
        Lista os arquivos em um caminho especificado no repositório Databricks.

        Args:
            path (str): Caminho no repositório Databricks.

        Returns:
            list: Lista de objetos no caminho especificado.
        """
        url = f"{self.host}/api/2.0/workspace/list"
        response = requests.get(url, headers=self.headers, params={"path": path})
        if response.status_code == 200:
            files = response.json().get('objects', [])
            print(f"Arquivos no caminho {path}: {files}")  # Adiciona log para depuração
            return files
        else:
            raise Exception(f"Erro ao listar arquivos no repositório: {response.text}")

    def get_file_content(self, path):
        """
        Obtém o conteúdo de um arquivo em um caminho especificado no repositório Databricks.

        Args:
            path (str): Caminho no repositório Databricks.

        Returns:
            str: Conteúdo do arquivo.
        """
        url = f"{self.host}/api/2.0/workspace/export"
        response = requests.get(url, headers=self.headers, params={"path": path, "format": "SOURCE"})
        if response.status_code == 200:
            return response.json().get('content', '')
        else:
            raise Exception(f"Erro ao obter o conteúdo do arquivo: {response.text}")
