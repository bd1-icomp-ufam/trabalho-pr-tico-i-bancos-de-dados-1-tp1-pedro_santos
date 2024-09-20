# Banco de Dados I - Trabalho Prático 1
# Pedro Henrique Souza Santos
# Matrícula: 22060035

import psycopg2
from psycopg2 import sql
from datetime import datetime

try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    print("Conectou ao banco de dados")
    
except psycopg2.Error as e:
    print(f"Erro ao conectar ao banco de dados: {e}")

# Cria um cursor para executar comandos SQL
cur = conn.cursor()

# Função que realiza a criação do esquema AmazonProducts
def create_schema(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS AmazonProducts AUTHORIZATION postgres;"))
        conn.commit()
        print(f"Esquema AmazonProducts criado com sucesso.")
    except psycopg2.Error as e:
        print(f"Erro ao criar o esquema: {e}")

# Função que realiza a criação das tabelas
def create_tables(conn, sql_queries):
    try:
        with conn.cursor() as cursor:
            for query in sql_queries:
                cursor.execute(query)
        conn.commit()
        print("Tabelas criadas com sucesso.")
    except psycopg2.Error as e:
        print(f"Erro ao criar tabelas: {e}")

# Código SQL para criar as tabelas
sql_queries = [
    """
    CREATE SCHEMA IF NOT EXISTS AmazonProducts AUTHORIZATION postgres;

    CREATE TABLE IF NOT EXISTS AmazonProducts.Produto (
        ASIN VARCHAR(10) NOT NULL,
        Titulo VARCHAR(500),
        SalesRank INT,
        "group" VARCHAR(45),
        PRIMARY KEY (ASIN)
    );

    CREATE TABLE IF NOT EXISTS AmazonProducts.Categoria (
        ASIN VARCHAR(10) NOT NULL,
        Nome VARCHAR(500),
        CONSTRAINT produto_categoria
            FOREIGN KEY (ASIN)
            REFERENCES AmazonProducts.Produto (ASIN)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION
    );

    CREATE TABLE IF NOT EXISTS AmazonProducts.ProdutoSimilar (
        ASIN VARCHAR(10) NOT NULL,
        ASIN_SIMILAR VARCHAR(10) NOT NULL,
        CONSTRAINT produto_similar
            FOREIGN KEY (ASIN) 
            REFERENCES AmazonProducts.Produto (ASIN)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION
    );

    CREATE TABLE IF NOT EXISTS AmazonProducts.Feedback (
    ID SERIAL PRIMARY KEY,
    ClienteId VARCHAR(20) NOT NULL,
    ASIN VARCHAR(10),
    "Data" DATE,
    Rating INT,
    Votos INT,
    Helpful INT,
    CONSTRAINT FK_ASIN
        FOREIGN KEY (ASIN)
        REFERENCES AmazonProducts.Produto (ASIN)
        ON DELETE NO ACTION
        ON UPDATE NO ACTION
    );

    """
    ]

# Função que realiza a extração e inserção das categorias na tabela Categoria
def inserir_categorias(asin, categories):
    for category in categories:
        cur.execute(
            "INSERT INTO AmazonProducts.Categoria VALUES (%s, %s)",
            (asin, category)
        )

# Função que realiza a extração e inserção dos feedbacks na tabela Feedback
def inserir_feedbacks(asin, feedback_data):
    for feedback in feedback_data:
        date_obj = feedback['Data']
        customer = feedback['ClienteId']
        rating = feedback['Rating']
        votes = feedback['Votos']
        helpful = feedback['Helpful']
        
        # Formatação da data como string no formato 'YYYY-MM-DD'
        formatted_date = date_obj.strftime('%Y-%m-%d')
        
        cur.execute(
            "INSERT INTO AmazonProducts.Feedback (ClienteId, ASIN, \"Data\" , Rating, Votos, Helpful) VALUES (%s, %s, %s, %s, %s, %s)",
            (customer, asin, formatted_date, int(rating), int(votes), int(helpful))
        )



# Função que realiza a extração das informações do produto na tabela Produto
def inserir_produto(asin, title, group, salesrank, similar):
    cur.execute(
        "INSERT INTO AmazonProducts.Produto VALUES (%s, %s, %s, %s)",
        (asin, title, int(salesrank), group)
    )
    # Inserção dos dados na tabela ProdutoSimilar
    for similar_asin in similar:
        cur.execute(
            "INSERT INTO AmazonProducts.ProdutoSimilar VALUES (%s, %s)",
            (asin, similar_asin.strip())
        )

# Função principal que realiza a leitura do arquivo e inserção no banco de dados
def inserir_dados_do_arquivo(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        asin = None
        title = None
        group = None
        salesrank = None
        similar = []
        categories = []
        feedback_data = []
        cont = 0
        for line in lines:
            if (cont+1) % 100000 == 0:
                print(f"{cont+1} linhas processadas de {len(lines)}...")

            # Seleção das informações mais importantes
            if line.startswith("ASIN:"):
                if asin and title and group and salesrank:
                    inserir_produto(asin, title, group, salesrank, similar)
                    inserir_categorias(asin, categories)
                    inserir_feedbacks(asin, feedback_data)
                    asin = None
                    title = None
                    group = None
                    salesrank = None
                    similar = []
                    categories = []
                    feedback_data = []
                
                asin = line.split(":", 1)[1].strip()

            elif line.startswith("  title:"):
                title = line.split(":", 1)[1].strip()

            elif line.startswith("  group:"):
                group = line.split(":")[1].strip()

            elif line.startswith("  salesrank:"):
                salesrank = line.split(":")[1].strip()
            
            elif line.startswith("  similar:"):
                similar.extend(line.split()[2:])

            elif line.startswith("  categories:"):
                # Informações de categoria
                num_categories = int(line.split()[1])

                for i in range(1, num_categories+1):
                    current_line = lines[cont+i].strip(" |")
                    category = current_line.replace("|", ", ")

                    categories.append(category)

            elif line.startswith("    "):
                # Processamento das informações de feedback
                date_str, _, customer, _, rating, _, votes, _, helpful = line.split()
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

                feedback_data.append({
                    'Data': date_obj,
                    'ClienteId': customer,
                    'Rating': int(rating),
                    'Votos': int(votes),
                    'Helpful': int(helpful)
                })

            cont += 1

        # Inserção dos dados do último produto
        inserir_produto(asin, title, group, salesrank, similar)
        inserir_categorias(asin, categories)
        inserir_feedbacks(asin, feedback_data)

    # Confirmação das alterações no banco de dados
    conn.commit()

def main():
    # Criação do esquema e tabelas
    create_schema(conn)
    create_tables(conn, sql_queries)

    # Chamada de função que realiza o povoamento das tabelas do banco de dados
    inserir_dados_do_arquivo("amazon-meta.txt")

    # Encerramento da conexão
    conn.close()
    print("\nDados inseridos com sucesso!")

if __name__ == "__main__":
    main()