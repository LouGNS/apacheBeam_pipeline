# Teste-Banco-ABC
Este repositório foi criado para compartilhar os resultados do pipeline desenvolvido, além das queries utilizadas e insights obtidos. Nele você encontra-ra scripts e configurações relacionados ao gerenciamento e análise de dados para o banco de dados.


## Estrutura do Projeto

- **`myenv312/`**: Ambiente virtual para Python.
- **`Teste-Banco-ABC/`**: Diretório principal do projeto.
  - **`data/`**: Pasta onde estão os arquivos consumidos para gerar os resultados iniciais.
  - **`etl/`**: Pasta onde esta os arquivos python para criação e teste do apache beam.
  - **`output/`**: Pasta onde os arquivos de saída, como resultados de consultas e parquets, são armazenados.
  - **`requirements.txt`**: Lista de dependências do projeto.
  - **`README.md`**: Este arquivo.


## Instalação das Dependências

1. Crie e ative um ambiente virtual:
   - No Windows:
     ```bash
     python -m venv myenv
     myenv\Scripts\activate
     ```
   - No macOS/Linux:
     ```bash
     python3 -m venv myenv
     source myenv/bin/activate
     ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt


## Arquitetura do projeto:
![Arquitetura completa do projeto](https://viewer.diagrams.net/?tags=%7B%7D&highlight=000000&edit=_blank&layers=1&nav=1&title=Diagrama%20sem%20nome.drawio#R7VtZc%2BK4Fv41VM082CWvwCNLSOgCsjG3J%2F3SJWzFmBjLsWWW%2FPp75H0joe84me6%2BoboT6ViWpbN859Mx6Sij7eHSx956Tk3idGRkHjrKuCPLkirLHf4PmcdY0u32YoHl22YyKBfc2y8kEaJEGtomCUoDGaUOs72y0KCuSwxWkmHfp%2FvysEfqlJ%2FqYYvUBPcGdurSr7bJ1rG0J3dz%2BRWxrXX6ZEnvx1e2OB2c7CRYY5PuCyLloqOMfEpZ3NoeRsThykv1Et83OXE1W5hPXHbODcfpTDG6xL0Ob9Fme%2By%2B%2FOWOhWSWHXbCZMMjx4YJQeXxotkx1QSs3%2BNNexupbLhmWwe6EjR3xGc26Gzg2JYLMka9gnSGV8S5oYHNbMqvrihjdAsDHH5hiI0ny6eha46oQ%2F3oWcpj9IEh0cMGgRebFoEEp51H%2B0DMdAj014xxnxhwDcgTw3QV0QaveLRdk%2FiiAU%2BUJyZmGH5xecAHYVPgohUOiOD5JICdY75KIYAeNoM1IUzYwfOoLzzajsDVq0n8p9QXJLkneq4FS3ikLktcFy4qw0SpsH9yOGktKfMBCB5Ct4T5RxiS3JB6TRI2Ckr6%2B9wJ00haF%2FwvleHE7a1s4twzoJE4xw84ilxzlKWP3QAb9NNVzneV1v1C0%2Bp%2B0UN1v0hlrfuFUgcQ7LNPp%2FhYp5BEreQWqvwvu4VadwvASPzpFf%2BmV%2Bi9Bq9QGrxCeSevSEldwf7EBLqVdKnP1tSiLnYucukwsiK3SWSofMyMcn%2BIPGVDGDsmCRiHjJb9iBxs9ndyO28%2F8DaoJu6ND4VL42Ohc0N8G%2FZN%2FFTmgg7%2BLnYKM%2FFuPlXUS%2BeqsoOA%2BfQpY5M5X%2BCqeN3UoDka%2BgZ5RcWJxRn2LcJeGddrdh2fOOCqu%2FI6WvcDrYYOdVyoKalg0RQ0tgeLnzrER4fujTXkHRG7Lo2D7Tu%2FByfQ4ZBHlkJDATP82OeHHrWB8voXO1B3kDyjajU%2FmRf6gtRD7bA8CVVjVGlA7iaip0rvZZzeGcapgPb%2FiskmecShw05ifTMql4C7GacDQp4cgIkEoKNBHJq%2FwP9N6B3B2AK%2FLgz6UhdpE03pC8V7El7fBgirXRH1y8m5K4npKbFg5b4kNoGxhLqi%2Fl62luq0vmbsHH95WOzXNiP3Ho5AaA%2FRV47MatRAenNSa7vUJZVA6qMaHiqZpJC5UfRpxyKC1NVErVc2ia7V7KFJ%2FTQ2S%2FbQlFTcvjnqbLoj61GArKBh8cYENEx4QSX6gU0a8NRxAI92cTbYT0cbEBJiRlHiq9DNZqtZG%2FTKyiYFAmO%2F4FU0IKJKkGCDxMQFiDV41cBvCOWtbZpRIo9QNlKZNuxo41pqP8N3TjvGqaT6j1ylJ8pldIYH1vxE0huItfpexFrq%2Fg7wzBHAEBCKuLQZGk8JUOMgICwQebFjchCCZwfARhH4IKGrSQcJyaogOyFaP3tSixitK2K3koYbDK31mvCg2xd774YH%2FbeNTVxzwOucPAQd0J9tRNEAZKguLkb1q6GXUeaMJj8UWXIzZc7occ6IH1IK%2Fgo9flc63DuTDsdhVfeQovkbwjyVnU2bkyfccCjMHVBF5aqO2q3AR7zP5K5ijbc6kVaZSK5MFCuiNlHkndm2%2F0GVEL2dwG5D4h%2BleqqCPANgAwnt%2BzMfIRrB7qyM9UP8pJxAcoA7O%2Fe0nGFUOACgChdRG3KM3OB8%2BnvlmBRl3rSi3GBF7D7ZrvXdSF4j%2FL%2BY8e2y7AfbsIlK%2FkJn7JbIvlIFRL0vKvXMngdhiepL72aden30l7JOexUQTa4aqOkk9tHmOadA9bOT7P1%2BL5J9QJ2QryRIGPbeEwx%2BguT5fxJ6DsUmL4zIiHPNCeqm1RJBQu0RbL3Kb3qSKNXtLKEThi7I23%2BBpdRM%2B5E16RLBTq%2F8GMHOKfW5BLsY2Vq7hFvWO%2BcxbuWEy3xMBTpdZmuFr5%2BNkWg9WZR0lH2kKscU6%2FGn9ntiX26Iv%2BpxpD0zNBWb9UxxJXvozyFNLwhxAQqQDkmad4hUll7PGWaFst741CBBQNPi2cVy1sxMM1m8ht%2BgUpYIW%2FCrbpVQoQYcbzoqK%2B%2FmQ01Fkorpt8f0haxorn67Omh71pWV8llU7jdYV%2FpI6ypnVBS4bR99vOWveKrn0VFSAKch80JWv%2F7pDKcPtZImouJHL%2FmG0uAb%2FY8shSv1Ly%2F%2BeizdMN2ovh0IwLbhGMcL49RNyHr8pRC5x4vT0S9VarHs3e1r1dqT3utnDKD4Alpp5OUFcfvGPV2DCp1WeIFjN07DY11I4pfPkxycz%2BAXY2LCgQp8ozNSOoNu9FOJ2Eb0ji59V%2BeBQ8lodDMR32AffIHvvOQ7sqVw5Df5KjEHq7C2eM6USOAlchr8BKu%2BwaZPXcDfmqL5Yg3sYwNQl%2FwMSx2Ydm2RmC8SjuX8fS0aL7%2BPBneXfNY19bFvR6YAO3An%2F2s5%2BuEt8GO9UxWa9q5xY%2FFG%2BMNREi0oqQiVZBHGcYmupqIUB7hU6aXSt2MQRW%2BjSouwyJJSZ4X96CuCtkv804rFjdvwMiDnT8WrqOgRZ0Fk2oHn4GN8KUuOKIXxolmzpBwbiAM%2FF%2FvW6g8JAfyM%2BJYKjT%2Br2mnQYTdToodN0%2BbYzcXyCY31vERNkcuZxACfyHeWL3%2BVZSihvPmEcWSN6mifeASzdLqsWxlVMCHfVPIrGpWkhUFJWPSGXErhtGyzRPVI1GuPSVIivxz6zh9gULzl2TQxa%2FSlBWUQj5Inwc7qyMMD52BDTuV1FSxxc7WQvx2H6urrITRekI2v7pAxpruZYirmUVPmR21nbI3dfDPYz0f9F3Nr2NMr0%2Ft2dUdv7qfHxWhq4cv%2FeN%2FkNUr75tZxTPRlR8bIno8G%2B%2Bl4iqL%2FG7VnXE4QHg2fbu6%2FLObLqbWYPByWY8tafN3DmLuvi%2B38ZbaZH6bjgbWwB8fZ5kGbjh94G%2Ba5COfjCzbbTCXevl4a0Dasua2qK%2BgvYNz8XkWLp%2Fnh%2Bl6VFqPBYXGv7ufHwRHWcZgfQZa2t%2FP9bHkL90wZl1%2BPkDpbWuz6ymP8%2BuJyzuUv8%2FsBjHtg8Az5%2Bh7J0LbmI3SYbQYHvo7ZMlpHuDgiWOt6Ds8DHQwUWBe6fbmN1nQ9UjV4LuzvYj%2Fb3L5Mx7fWfPllA89B0D9Ox%2FNwsbRg3ECdjtfyXN4jWBtbjBAyNwt9agNLGN7E2odW1c5%2FVjGC2awS4XcEQnVpbznAAycDlpYGxlvYMgyB2cEwBHTMxB4vLQ34twxOYwz%2FHkKDLMLQ82C1GCNyDqKhsRYgNTWEcwYhElLS8SklLSFxDthyNvMbCrgC%2FLNf325pa59nn4gI19%2FL6lpTzQw1HHmk6mv39mhx04ujCuewiM%2FTJPafQ3sHhM3jLRIV0wi0Yxr6eQ4%2B%2F5wk66IunyqldvVezSnklkok0M3%2FPjD%2Bjkb%2BV5bKxX8B)


## Campanha de Marketing por E-mail

### Objetivo

Realizar uma campanha de marketing por e-mail para promover o uso mais intenso do cartão de crédito por parte dos clientes que utilizaram o cartão recentemente.

### Requisitos

Para participar da campanha, os clientes devem atender aos seguintes critérios:

1. **Compras Aprovadas**:
   - O cliente deve ter realizado pelo menos R$ 400,00 em compras aprovadas nos últimos dois meses.

2. **Status da Conta e Cartão**:
   - A conta do cliente deve estar ativa.
   - O cartão do cliente deve estar desbloqueado, exceto se o código de bloqueio for igual a “M”.

### Informações a Serem Extraídas

Para os clientes que atendem aos critérios acima, extraimos as seguintes informações:

- **Nome**
- **CPF**
- **E-mail**

### Query SQL

A seguir está a query SQL para extrair as informações necessárias:

```sql
SELECT 
    c.nome,
    c.cpf,
    c.email
FROM clientes c
JOIN contas a ON c.cpf = a.cpfCliente
JOIN cartoes t ON a.numeroConta = t.numeroConta
JOIN transacoes tr ON t.numeroCartao = tr.numeroCartao
WHERE a.ativo = 1
  AND (t.cartaoBloqueado = 0 OR t.codBloqueio = 'M')
  AND tr.aprovado = 1
  AND tr.dataCompra BETWEEN DATE('now', '-2 months') AND DATE('now')
GROUP BY c.nome, c.cpf, c.email
HAVING SUM(tr.valorCompra) >= 400;
```



## Explicação dos Rankings

### Clientes Compradores

**Critérios**:
- Média de compras aprovadas nos últimos 6 meses >= R$ 300,00.
- Utilização de pelo menos 70% do limite do cartão.

**Ranking**: A

### Clientes Não Compradores

**Critérios**:
- Conta ativa e cartão desbloqueado (ou código de bloqueio 'M').
- Não atendem aos critérios para serem classificados como Compradores.

**Ranking**:
- **B**: Entre 70% e 80% do limite do cartão consumido e saldo na conta > R$ 10.000,00.
- **C**: Entre 80% e 90% do limite do cartão consumido e saldo na conta > R$ 15.000,00.
- **D**: Entre 90% e 95% do limite do cartão consumido e saldo na conta > R$ 20.000,00.
- **Ranking vazio**: Não atende a nenhum dos critérios acima.

### Clientes com Ranking Vazio

**Razão para Ranking Vazio**:
- Esses clientes não atendem a nenhum dos critérios de ranking estabelecidos. Eles podem não ter o percentual de limite consumido necessário ou o saldo da conta suficiente para se qualificarem nas categorias B, C ou D.

**Implicação**:
- **Exclusão**: De acordo com a regra de negócio, esses clientes são candidatos à exclusão da base de dados. Eles não se encaixam nos critérios de análise ou decisão de crédito definidos.


# Pipeline ETL com Apache Beam

Este projeto contém um pipeline ETL (Extract, Transform, Load) desenvolvido em Python utilizando a biblioteca Apache Beam. O pipeline processa arquivos CSV e gera arquivos Parquet com dados pré-processados.

## Sumário

- [Descrição do Projeto](#descrição-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Como Usar](#como-usar)
- [Estrutura do Código](#estrutura-do-código)

## Descrição do Projeto

O pipeline ETL realiza as seguintes etapas:

1. **Leitura dos arquivos CSV**:
   - `ranking_clientes.csv`
   - `resultado_query.csv`

2. **Pré-processamento dos dados**:
   - Adição da coluna `DT_CARGA` com a data e hora atual em UTC.

3. **Gravação dos dados em arquivos Parquet**:
   - `ranking_clientes.parquet`
   - `resultado_query.parquet`

## Pré-requisitos

Certifique-se de que você tem os seguintes pré-requisitos instalados:

- Python 3.12.6 ou superior
- Apache Beam
- PyArrow

Você pode instalar as dependências necessárias com o seguinte comando:

```sh
pip install apache-beam pyarrow pandas pytz
```

## Instalação

1. **Clone o repositório**:

   ```bash
   git clone https://github.com/LouGNS/Teste-Banco-ABC.git
   ```
  
# Pipeline Apache Beam para Processamento de Dados

Este projeto contém um pipeline Apache Beam para processar e transformar dados de arquivos CSV em arquivos Parquet. O pipeline realiza as seguintes operações:

1. **Leitura de Dados**: Lê arquivos CSV contendo informações de clientes e resultados de consultas.
2. **Pré-processamento**: Adiciona uma coluna com a data e hora atual, e converte valores para os tipos apropriados.
3. **Gravação em Parquet**: Grava os dados processados em arquivos Parquet.


## Estrutura do Código
O código está organizado da seguinte forma:

**1. Função preprocess_data(element):**

- **Adiciona a coluna DT_CARGA com a data e hora atual em UTC.** 

- **Converte os valores das colunas cpf, numeroConta, e numeroCartao para inteiros, se presente e se o valor for numérico.**


**2. Função run():**

- **Define os caminhos para os arquivos CSV de entrada.**
- **Lê e processa os dados dos arquivos CSV**.
- **Escreve os dados processados em arquivos Parquet.**

## Como Usar

**1. Configure os Caminhos dos Arquivos:**

- **Modifique as variáveis ranking_file_path e resultado_file_path na função run() para os caminhos absolutos dos seus arquivos CSV.**


**2. Execute o Pipeline:**

- **Salve o código em um arquivo Python, por exemplo, pipeline.py.**
- **Execute o script Python:**

 ```bash
python pipeline.py
 ```

## Observações
Certifique-se de que os arquivos CSV estão no formato correto e que os caminhos fornecidos estão corretos.
O pipeline foi configurado para adicionar uma coluna DT_CARGA com a data e hora atual. Ajuste conforme necessário para atender aos requisitos específicos.


