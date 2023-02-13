# Crawlclima

<p style="font-size:18px" >Este pacote contém os módulos <i><small>PEGATWEETS</i></small> e <i><small>PEGATEMPERATURA</i></small> para captura automática de tweets e dados climáticos.</p>

### Requisitos<small>(Debian)</small>

#### Clone o repositório CrawlClima:
```bash
$ git clone https://github.com/osl-incubator/CrawlClima
```

#### Criando e configurando o ambiente conda:
*Installing Miniconda:*
https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html

*Após a instalação do miniconda execute:*

```bash
$ conda env create -f conda/dev.yml
$ conda activate crawlclima
```

#### Instalando as dependências com Poetry:
```bash
$ poetry install
```

#### Preparando o `.env` para instalação das imagens Docker:
```bash
$ make prepare-env
```
Edite o arquivo `.env` com as credenciais necessárias.

#### Configurando o container com o banco de dados demo:

- *Para poder executar as tarefas de captura com os dados do banco de dados de demonstração baixe o repositório [AlertaDengue/Data](https://github.com/AlertaDengue/Data) . Ou use os [utilitários](https://github.com/AlertaDengueCaptura/AlertaDengueCaptura/blob/crawlclima-refactore/crawlclima/utilities/README.md) para criação do banco e os schemas iniciais.*
- *Modificar as variáveis de conexão com a base de dados de demonstração e as demais variáveis de ambiente no arquivo de ambiente; **AlertaDengueCaptura/crawlclima/.env** do projeto.*

### Deploy do CrawlClima em containers<small>(Docker)</small>

####  Instale o Docker
*https://docs.docker.com/engine/install/ubuntu/*


#### Build e Start dos containers:
> Use os comandos do Makefile diretamente do repositório root `CrawlClima/`:
```bash
$ make container-build
$ make container-start
```
Após o final da inicialização dos containers, o worker, que contém uma instância do Celery Beat ativa, deve configurar as tasks e agendá-las automaticamente, de acordo com o arquivo `crawlclima/celeryapp.py`.
Arquivos de logs serão gerados dentro do diretório `crawlclima/logs`, com os logs das tasks executadas pelo Celery Beat.


##### Para parar os containers, digite o comando:
```
$ make container-stop
```
---
