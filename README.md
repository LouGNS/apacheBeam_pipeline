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
