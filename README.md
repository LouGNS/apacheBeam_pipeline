# Teste-Banco-ABC
Este repositório foi criado para compartilhar os resultados do pipeline desenvolvido, além das queries utilizadas e insights obtidos. Nele você encontra-ra scripts e configurações relacionados ao gerenciamento e análise de dados para o banco de dados.


## Estrutura do Projeto

- **`myenv312/`**: Ambiente virtual para Python.
- **`Teste-Banco-ABC/`**: Diretório principal do projeto.
  - **`output/`**: Pasta onde os arquivos de saída, como resultados de consultas, são armazenados.
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
- **Exclusão**: De acordo com a sua regra de negócio, esses clientes são candidatos à exclusão da base de dados. Eles não se encaixam nos critérios de análise ou decisão de crédito definidos.
