Esta aplicação em Node.JS, tem o objetivo de comparar dois bancos de dados postgres, onde a estrutura de metadados já estejam exatamente iguais. 
Ou seja, o diff é apenas para registros de um banco para outro.

Ao final, a aplicação apresenta um arquivo `.sql`, com todos os inserts e updates necessários para compatibilização dos bancos.

## Pré-requisitos

É necessário instalar as dependencias via yarn:

* [yarn](https://yarnpkg.com/lang/en/)

## Setup

1. Clone o repositório.

1. Copie o arquivo `.env.sample` para `.env`.

1. Configure o arquivo `.env` com as URLs de conexão com as duas bases postgres e o nome do arquivo `.sql` que será gerado

```
DATABASE_OFFICIAL_URL=postgres://postgres:postgres@localhost:15432/db-official
DATABASE_INTEGRATION_URL=postgres://postgres:postgres@localhost:15432/db-integration
OUTPUT_SCRIPT_FILE=./diff.sql 
```

1. Rode a aplicação e acompanhe o diff através dos logs
```sh
yarn start
```

## Sobre o arquivo `.sql`

O Arquivo gerado não leva em consideração a ordem em que os registros devem ser inseridos ou atualizados no banco de dados, por esse motivo, é necessário desativar as FK's do banco de dados onde será executado o script
